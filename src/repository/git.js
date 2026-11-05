import { createHash, createPrivateKey, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { promisify } from 'node:util';
import { repositoryPaths, ensureRepositoryPaths } from './workspace.js';

const execFileAsync = promisify(execFile);
const defaultExcludes = ['.env', '.env.*', '**/.env', '**/.env.*', '**/*.pem', '**/*.key', 'node_modules/**'];

export function repositoryEnvironment(remoteUrl) {
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('GIT_')));
    Object.assign(env, { GIT_TERMINAL_PROMPT: '0', GIT_EDITOR: 'true', GIT_PAGER: 'cat' });
    if (remoteUrl.startsWith('https://')) {
        const url = new URL(remoteUrl);
        if (url.username || url.password) throw new Error('Repository URLs must not contain credentials.');
    }
    return env;
}

/** Read only this node organization's API credential; never persist it in Git config. */
export async function authenticatedRepositoryEnvironment(server, settings, remoteUrl, signal) {
    const env = repositoryEnvironment(remoteUrl);
    if (!settings.api) return env;
    signal?.throwIfAborted();
    if (typeof settings.api !== 'string' || !settings.api.trim() || !server.organization_id) throw new Error('Repository authentication requires an API reference and node organization.');
    const ssh = /^git@github\.com:[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\.git$/.test(remoteUrl);
    if (!ssh) throw new Error('Repository API credentials require a GitHub SSH repository URL.');
    let response;
    try {
        response = await server.crud.send({
            method: 'object.read', array: 'apis', organization_id: server.organization_id, host: server.host,
            filter: {
                query: { [/^[a-fA-F0-9]{24}$/.test(settings.api) ? '_id' : 'name']: settings.api, enabled: true },
                key: { authentication: 1, privateKey: 1, knownHosts: 1, repository_url: 1 }, limit: 1
            }
        });
    } catch { throw new Error('Unable to read repository authentication.'); }
    signal?.throwIfAborted();
    if (response?.success === false || response?.error?.length || (response?.error && !Array.isArray(response.error))) throw new Error('Unable to read repository authentication.');
    const profile = response?.object?.[0];
    if (profile?.authentication !== 'ssh-deploy-key' || profile.repository_url !== remoteUrl || typeof profile.privateKey !== 'string') throw new Error('Repository deploy-key credentials do not match this repository.');
    try {
        if (createPrivateKey(profile.privateKey).asymmetricKeyType !== 'rsa') throw new Error();
    } catch { throw new Error('Repository SSH private key is invalid.'); }
    const lines = typeof profile.knownHosts === 'string' ? profile.knownHosts.trim().split('\n') : [];
    if (!lines.length || lines.some(line => !/^github\.com (ssh-rsa|ssh-ed25519|ecdsa-sha2-nistp256) [A-Za-z0-9+/]+={0,2}$/.test(line))) throw new Error('Repository SSH host verification keys are missing.');
    const { metadata } = await ensureRepositoryPaths({ ...settings, url: remoteUrl }, server.projectRoot);
    const base = path.join(metadata, `git-${createHash('sha256').update(settings.api).digest('hex')}`);
    const atomicWrite = async (target, value) => {
        const temporary = `${target}.${randomUUID()}`;
        try {
            await fs.writeFile(temporary, value, { mode: 0o600, flag: 'wx' });
            await fs.rename(temporary, target);
        } finally { await fs.rm(temporary, { force: true }); }
    };
    await atomicWrite(`${base}.pem`, profile.privateKey);
    await atomicWrite(`${base}.known_hosts`, profile.knownHosts);
    signal?.throwIfAborted();
    const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";
    const knownHostsOption = `UserKnownHostsFile="${`${base}.known_hosts`.replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`;
    env.GIT_SSH_COMMAND = `ssh -F /dev/null -i ${quote(`${base}.pem`)} -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o GlobalKnownHostsFile=/dev/null -o ${quote(knownHostsOption)}`;
    return env;

}

/** Save only the server organization's existing checkout; never clone or switch branches. */
export async function saveRepository({ server, localFsRoot, settings = {}, reason = 'shutdown', signal }) {
    if (!server.organization_id || !/^[A-Za-z0-9_-]+$/.test(server.organization_id)) throw new Error('Repository shutdown requires the platform organization.');
    signal?.throwIfAborted();
    const { root: configuredRoot, workspace } = repositoryPaths({ ...settings, localFsRoot: localFsRoot || settings.localFsRoot }, server.projectRoot);
    try {
        const stats = await fs.lstat(path.join(workspace, '.git'));
        if (!stats.isDirectory() || stats.isSymbolicLink()) throw new Error('Repository shutdown requires a local Git directory.');
        const root = await fs.realpath(configuredRoot);
        const resolved = await fs.realpath(workspace);
        if (resolved !== path.join(root, path.basename(workspace))) throw new Error('Repository shutdown path escapes the configured workspace.');
    } catch (error) {
        if (error.code === 'ENOENT') return { skipped: true, reason: 'no-checkout' };
        throw error;
    }

    const gitBinary = settings.gitBinary || 'git';
    const env = repositoryEnvironment('');
    const git = async args => {
        signal?.throwIfAborted();
        try {
            return await execFileAsync(gitBinary, ['-c', 'core.hooksPath=/dev/null', '-c', 'core.pager=cat', ...args], {
                cwd: workspace, env, signal, timeout: 10000, maxBuffer: 1024 * 1024, windowsHide: true
            });
        } catch (error) {
            if (signal?.aborted) signal.throwIfAborted();
            // Git diagnostics may contain remote URLs or credentials.
            throw new Error(`Repository shutdown Git operation '${args[0]}' failed.`);
        }
    };
    const branch = (await git(['symbolic-ref', '--quiet', '--short', 'HEAD'])).stdout.trim();
    const remote = settings.remote || 'origin';
    if (!/^[A-Za-z0-9_][A-Za-z0-9_.-]*$/.test(remote)) throw new Error('Repository shutdown requires a named Git remote.');
    const remoteUrl = (await git(['remote', 'get-url', remote])).stdout.trim();
    Object.assign(env, await authenticatedRepositoryEnvironment(server, settings, remoteUrl, signal));
    signal?.throwIfAborted();
    const excludes = settings.exclude ?? defaultExcludes;
    if (!Array.isArray(excludes) || excludes.some(value => typeof value !== 'string' || !value)) {
        throw new Error('Repository shutdown exclusions must be an array of path patterns.');
    }
    const excluded = excludes.map(value => `:(glob)${value}`);
    if (excluded.length && (await git(['diff', '--cached', '--name-only', '-z', '--', ...excluded])).stdout) {
        throw new Error('Repository shutdown found excluded files already staged; checkpoint was not committed.');
    }
    await git(['add', '--all', '--', '.', ...excludes.map(value => `:(glob,exclude)${value}`)]);
    const changes = (await git(['diff', '--cached', '--name-only', '-z'])).stdout;
    if (changes) await git([
        '-c', `user.name=${settings.authorName || 'CoCreate'}`,
        '-c', `user.email=${settings.authorEmail || 'git@cocreate.local'}`,
        'commit', '-m', `Checkpoint before shutdown (${String(reason).replace(/[\r\n]/g, ' ').slice(0, 100)})`
    ]);
    await git(['push', '--porcelain', remote, `HEAD:refs/heads/${branch}`]);
    return { committed: Boolean(changes), pushed: true, branch };
}
