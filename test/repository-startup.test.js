import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { initializeRepository } from '../src/repository/startup.js';
import { repositoryEnvironment } from '../src/repository/git.js';
import { init as initializeRepositories } from '../src/repository/index.js';

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
    const config = { localFsRoot: path.join(root, 'organizations'), url: source, branch: 'dev', directory: 'app' };
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

test('startup uses only the cluster entry and never falls back to organization metadata', async t => {
    const options = await setup(t);
    delete options.config.url;
    options.server.crud = { send() { assert.fail('Repository initialization must not read organization.git'); } };
    assert.equal((await initializeRepository(options)).reason, 'not-configured');
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
    const workspace = path.join(options.config.localFsRoot, 'app');
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

test('primary prepares sibling repositories and checkpoints each current branch once', async t => {
    const options = await setup(t);
    const root = path.dirname(options.config.localFsRoot);
    const projectRoot = path.join(root, 'server');
    await fs.mkdir(projectRoot);
    const repositories = [];
    for (const directory of ['app', 'library']) {
        const remote = path.join(root, `${directory}.git`);
        await exec('git', ['clone', '--bare', options.source, remote]);
        repositories.push({ url: remote, directory, branch: 'dev', localFsRoot: '../', initialUpload: { enabled: directory === 'app', completed: false } });
    }
    const previous = [process.env.SERVER_REPOSITORY_ROOT, process.env.SERVER_REPOSITORY_ORGANIZATION_ID];
    t.after(() => ['SERVER_REPOSITORY_ROOT', 'SERVER_REPOSITORY_ORGANIZATION_ID'].forEach((name, i) => {
        if (previous[i] === undefined) delete process.env[name]; else process.env[name] = previous[i];
    }));
    const service = await initializeRepositories({
        isPrimary: true, organization_id: 'platform', cluster_id: 'cluster', projectRoot,
        crud: { send: async () => ({ object: [{ repositories }] }) }
    });
    assert.equal(service.repositories.length, 2);
    assert.equal(process.env.SERVER_REPOSITORY_ROOT, path.join(root, 'app'));
    assert.equal(service.repositories[0].initialUpload.completed, false);
    assert.equal(service.repositories[1].initialUpload.enabled, false);
    for (const entry of service.repositories) {
        assert.equal(entry.checkout.workspace, path.join(root, entry.directory));
        await fs.writeFile(path.join(entry.checkout.workspace, 'app.js'), entry.directory);
    }
    const stopping = service.stop('test');
    assert.equal(service.stop('test'), stopping);
    const results = await stopping;
    assert.equal(results.length, 2);
    for (const entry of service.repositories) {
        assert.equal((await exec('git', ['--git-dir', entry.url, 'show', 'dev:app.js'])).stdout, entry.directory);
    }
    assert.ok(repositories.every(entry => entry.initialUpload.completed === false), 'Checkout/checkpoint must not mark publication complete');
});

test('startup defaults to main when no branch is configured', async t => {
    const options = await setup(t);
    delete options.config.branch;
    const { workspace } = await initializeRepository(options);
    assert.equal(await fs.readFile(path.join(workspace, 'app.js'), 'utf8'), 'main');
    assert.equal((await exec('git', ['branch', '--show-current'], { cwd: workspace })).stdout.trim(), 'main');
});
