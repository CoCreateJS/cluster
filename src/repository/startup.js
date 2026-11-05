import fs from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { ensureRepositoryPaths } from './workspace.js';
import { authenticatedRepositoryEnvironment } from './git.js';

const exec = promisify(execFile);

/** Prepare this node's platform checkout before the primary forks consumers. */
export async function initializeRepository({ server, config }) {
    const settings = { branch: 'main', remote: 'origin', ...config };
    if (settings.enabled === false || settings.onStartup === false) return { skipped: true, reason: 'disabled' };
    const url = settings.url;
    if (!url) return { skipped: true, reason: 'not-configured' };
    if (typeof url !== 'string' || !url.trim() || url.startsWith('-')) throw new Error('Invalid repository URL.');
    const branch = settings.branch;
    if (typeof branch !== 'string' || !branch || branch.startsWith('-')) throw new Error('Invalid repository startup branch.');
    const remote = settings.remote;
    if (!/^[A-Za-z0-9_][A-Za-z0-9_.-]*$/.test(remote)) throw new Error('Invalid repository remote name.');
    const controller = new AbortController();
    const timeoutMs = Math.max(1, Number(settings.startupTimeoutMs) || 60000);
    const timer = setTimeout(() => controller.abort(new Error('Repository startup deadline exceeded.')), timeoutMs);
    let staging;
    try {
        const env = await authenticatedRepositoryEnvironment(server, settings, url, controller.signal);
        const { workspace, metadata } = await ensureRepositoryPaths(settings, server.projectRoot);
        const git = async (args, cwd = workspace) => {
            controller.signal.throwIfAborted();
            try {
                return (await exec('git', ['-c', 'core.hooksPath=/dev/null', ...args], {
                    cwd, env, signal: controller.signal, timeout: timeoutMs, maxBuffer: 1024 * 1024
                })).stdout.trim();
            } catch {
                throw new Error(`Repository startup Git operation '${args[0]}' failed.`);
            }
        };
        await git(['check-ref-format', '--branch', branch]);
        try {
            const stats = await fs.lstat(path.join(workspace, '.git'));
            if (!stats.isDirectory() || stats.isSymbolicLink()) throw new Error('Repository metadata must be a local directory.');
        } catch (error) {
            if (error.code !== 'ENOENT') throw error;
            if ((await fs.readdir(workspace)).length) throw new Error('Cannot clone into a nonempty workspace without a repository.');
            staging = await fs.mkdtemp(path.join(metadata, 'checkout-'));
            await git(['clone', '--origin', remote, '--branch', branch, '--', url, staging], metadata);
            await fs.rename(staging, workspace);
            staging = undefined;
            return { cloned: true, branch, workspace };
        }
        if (await git(['remote', 'get-url', remote]) !== url) throw new Error('Existing checkout remote does not match the configured repository.');
        const currentBranch = await git(['symbolic-ref', '--quiet', '--short', 'HEAD']);
        if (currentBranch !== branch) {
            if (await git(['status', '--porcelain'])) throw new Error('Cannot switch startup branch while the checkout has local changes.');
            await git(['checkout', branch]);
        }
        return { cloned: false, branch, workspace };
    } finally {
        clearTimeout(timer);
        if (staging) await fs.rm(staging, { recursive: true, force: true });
    }
}
