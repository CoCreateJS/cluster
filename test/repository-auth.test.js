import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { authenticatedRepositoryEnvironment } from '../src/repository-git.js';

const exec = promisify(execFile);
const api = '6b0500000000000000000021';
const remote = 'git@github.com:managed-owner/tenant.git';
async function credentialFixture(root) {
    const identity = path.join(root, 'fixture');
    await exec('ssh-keygen', ['-q', '-t', 'rsa', '-b', '2048', '-m', 'PEM', '-N', '', '-f', identity]);
    return {
        authentication: 'ssh-deploy-key',
        privateKey: await fs.readFile(identity, 'utf8'),
        publicKey: (await fs.readFile(`${identity}.pub`, 'utf8')).trim().split(' ').slice(0, 2).join(' ')
    };
}


test('master reads the tenant API deploy key and configures noninteractive, verified SSH', async t => {
    const localFsRoot = await fs.mkdtemp(path.join(os.tmpdir(), "repository auth's "));
    t.after(() => fs.rm(localFsRoot, { recursive: true, force: true }));
    const credential = await credentialFixture(localFsRoot);
    const profile = { ...credential, enabled: true, repository_url: remote, knownHosts: 'github.com ssh-ed25519 AAAA\n' };
    const server = { organization_id: 'tenant', host: 'tenant.test', crud: { send: async request => {
        assert.equal(request.organization_id, 'tenant');
        assert.equal(request.array, 'apis');
        assert.deepEqual(request.filter.query, { _id: api, enabled: true });
        return { object: [profile] };
    } } };
    const env = await authenticatedRepositoryEnvironment(server, { api, localFsRoot }, remote);
    assert.ok(!JSON.stringify(env).includes(credential.privateKey));
    assert.match(env.GIT_SSH_COMMAND, /StrictHostKeyChecking=yes/);
    assert.match(env.GIT_SSH_COMMAND, /BatchMode=yes/);
    const metadata = path.join(localFsRoot, 'tenant', '.ai');
    const files = await fs.readdir(metadata);
    const identity = path.join(metadata, files.find(file => file.endsWith('.pem')));
    assert.equal((await fs.stat(identity)).mode & 0o777, 0o600);
    assert.equal((await exec('ssh-keygen', ['-y', '-f', identity])).stdout.trim(), credential.publicKey);
    // Parse the actual SSH command, including paths with spaces and apostrophes, without connecting.
    const parsed = await exec('/bin/sh', ['-c', `${env.GIT_SSH_COMMAND} -G git@github.com`]);
    assert.match(parsed.stdout, /stricthostkeychecking true/);
    assert.equal(profile.expires_at, undefined);
    await assert.rejects(authenticatedRepositoryEnvironment(server, { api, localFsRoot }, 'git@github.com:another/repository.git'), /do not match/);
});

test('missing credentials and malformed host verification fail closed without exposing keys', async t => {
    const localFsRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'repository-auth-'));
    t.after(() => fs.rm(localFsRoot, { recursive: true, force: true }));
    const server = { organization_id: 'tenant', crud: { send: async () => ({ object: [] }) } };
    await assert.rejects(authenticatedRepositoryEnvironment(server, { api, localFsRoot }, remote), /do not match/);
    const credential = await credentialFixture(localFsRoot);
    server.crud.send = async () => ({ object: [{ ...credential, repository_url: remote, knownHosts: '' }] });
    await assert.rejects(authenticatedRepositoryEnvironment(server, { api, localFsRoot }, remote), /verification keys/);
});
