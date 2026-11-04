import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { initializeRepository } from '../src/repository-startup.js';
import { repositoryEnvironment } from '../src/repository-git.js';

const exec = promisify(execFile);
async function setup(t) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'ai-startup-'));
    t.after(() => fs.rm(root, { recursive: true, force: true }));
    const source = path.join(root, 'source');
    await fs.mkdir(source);
    const git = async (...args) => (await exec('git', args, { cwd: source })).stdout.trim();
    await git('init', '-b', 'main');
    await git('config', 'user.name', 'Test');
    await git('config', 'user.email', 'test@example.test');
    await fs.writeFile(path.join(source, 'app.js'), 'main');
    await git('add', '.');
    await git('commit', '-m', 'initial');
    await git('checkout', '-b', 'dev');
    await fs.writeFile(path.join(source, 'app.js'), 'dev');
    await git('commit', '-am', 'dev');
    await git('checkout', 'main');
    const config = { localFsRoot: path.join(root, 'organizations'), url: source, branch: 'dev' };
    const server = { organization_id: 'platform' };
    return { source, server, config };
}

test('master prepares its branch once and preserves local changes on restart', async t => {
    const options = await setup(t);
    const first = await initializeRepository(options);
    const second = await initializeRepository(options);
    assert.equal(first.cloned, true);
    assert.equal(second.cloned, false);
    const workspace = first.workspace;
    assert.equal(await fs.readFile(path.join(workspace, 'app.js'), 'utf8'), 'dev');
    assert.equal((await exec('git', ['branch', '--show-current'], { cwd: workspace })).stdout.trim(), 'dev');
    await fs.writeFile(path.join(workspace, 'app.js'), 'uncommitted');
    assert.equal((await initializeRepository(options)).cloned, false);
    assert.equal(await fs.readFile(path.join(workspace, 'app.js'), 'utf8'), 'uncommitted');
    options.config.branch = 'main';
    await assert.rejects(initializeRepository(options), /local changes/);
});

test('startup switches a clean checkout to the configured branch without pulling', async t => {
    const options = await setup(t);
    const { workspace } = await initializeRepository(options);
    options.config.branch = 'main';
    await initializeRepository(options);
    assert.equal(await fs.readFile(path.join(workspace, 'app.js'), 'utf8'), 'main');
});

test('repository lookup uses only the node platform identity and stored signup metadata', async t => {
    const options = await setup(t);
    delete options.config.url;
    options.server.crud = { send: async request => {
        assert.equal(request.organization_id, 'platform');
        assert.deepEqual(request.filter.query, { _id: 'platform' });
        assert.deepEqual(request.filter.key, { git: 1 });
        return { object: [{ git: { clone_url: options.source } }] };
    } };
    assert.equal((await initializeRepository(options)).cloned, true);
});

test('startup skips disabled or unconfigured repositories', async () => {
    assert.equal((await initializeRepository({ server: {}, config: { enabled: false } })).reason, 'disabled');
    assert.equal((await initializeRepository({ server: {}, config: { onStartup: false } })).reason, 'disabled');
    assert.equal((await initializeRepository({ server: { organization_id: 'platform', crud: { send: async () => ({ object: [] }) } }, config: { localFsRoot: '/unused' } })).reason, 'not-configured');
});

test('missing branch fails without exposing remote diagnostics or leaving a partial checkout', async t => {
    const options = await setup(t);
    options.config.branch = 'missing';
    await assert.rejects(initializeRepository(options), /clone.*failed/);
    const workspace = path.join(options.config.localFsRoot, 'platform', 'sites');
    assert.deepEqual(await fs.readdir(workspace), []);
    options.config.branch = 'dev';
    assert.equal((await initializeRepository(options)).cloned, true);
});

test('standard Git credentials require no API lookup and URLs cannot embed secrets', async () => {
    const env = repositoryEnvironment('https://github.com/example/repository.git');
    assert.equal(env.GIT_TERMINAL_PROMPT, '0');
    assert.equal(env.GIT_CONFIG_VALUE_0, undefined);
    assert.throws(() => repositoryEnvironment('https://user:secret@github.com/example/repository.git'), /must not contain credentials/);
});
