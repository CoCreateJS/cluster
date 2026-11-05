import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { saveRepository } from '../src/repository/git.js';

const exec = promisify(execFile);
async function checkout(t) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'ai-checkpoint-'));
    t.after(() => fs.rm(root, { recursive: true, force: true }));
    const workspace = path.join(root, 'app');
    const remote = path.join(root, 'remote.git');
    await fs.mkdir(workspace, { recursive: true });
    const git = async (...args) => (await exec('git', args, { cwd: workspace })).stdout.trim();
    await exec('git', ['init', '--bare', remote]);
    await git('init', '-b', 'dev');
    await git('config', 'user.name', 'Test');
    await git('config', 'user.email', 'test@example.test');
    await fs.writeFile(path.join(workspace, 'app.js'), 'initial');
    await git('add', '.');
    await git('commit', '-m', 'Initial');
    await git('remote', 'add', 'origin', remote);
    await git('push', 'origin', 'dev');
    return { root, workspace, remote, git, server: { organization_id: 'platform' } };
}

test('shutdown commits changes, preserves exclusions, and pushes the current branch', async t => {
    const { root, workspace, remote, git, server } = await checkout(t);
    await fs.writeFile(path.join(workspace, 'app.js'), 'changed');
    await fs.writeFile(path.join(workspace, '.env'), 'secret');
    const result = await saveRepository({ server, localFsRoot: root, settings: { directory: 'app' } });
    assert.deepEqual(result, { committed: true, pushed: true, branch: 'dev' });
    assert.equal(await git('branch', '--show-current'), 'dev');
    assert.equal(await git('status', '--porcelain'), '?? .env');
    assert.equal((await exec('git', ['--git-dir', remote, 'show', 'dev:app.js'])).stdout, 'changed');
    assert.equal((await saveRepository({ server, localFsRoot: root, settings: { directory: 'app' } })).committed, false);
});


test('push failure leaves the local checkpoint intact', async t => {
    const { root, workspace, server, git } = await checkout(t);
    await git('remote', 'set-url', 'origin', path.join(root, 'missing.git'));
    await fs.writeFile(path.join(workspace, 'app.js'), 'saved locally');
    await assert.rejects(saveRepository({ server, localFsRoot: root, settings: { directory: 'app' } }), /push.*failed/);
    assert.equal(await git('show', 'HEAD:app.js'), 'saved locally');
});

test('missing checkout is skipped', async () => {
    assert.deepEqual(await saveRepository({ server: { organization_id: 'platform' }, localFsRoot: '/nonexistent-ai-checkout', settings: { directory: 'app' } }), {
        skipped: true, reason: 'no-checkout'
    });
});
