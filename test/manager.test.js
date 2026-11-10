import assert from 'node:assert/strict';
import test from 'node:test';
import Http from 'node:http';
import config from '@cocreate/config';
import { init } from '../src/manager.js';
import Fetch from '../src/fetch.js';

test('manager serializes only connection fields and directly sends the existing envelope', async t => {
    t.mock.method(config, 'get', () => undefined);
    t.mock.method(config, 'query', async () => undefined);
    const manager = await init({ isPrimary: true, fetch: Fetch });
    assert.equal(manager.organization_id, null);
    Object.assign(manager, { organization_id: 'parent', host: 'parent.test', apiKey: 'test-only-key' });
    assert.equal(typeof manager.send, 'function');
    assert.deepEqual(Object.keys(manager), ['organization_id', 'host', 'apiKey']);
    t.mock.method(globalThis, 'fetch', async (url, options) => {
        assert.equal(url, 'https://parent.test/manager');
        assert.equal(options.headers.Authorization, 'Bearer test-only-key');
        assert.deepEqual(JSON.parse(options.body), { method: 'object.update', array: 'servers', object: { _id: 'server-1', worker2: { status: 'running' } } });
        return Response.json({ object: [{ _id: 'server-1' }] });
    });
    const result = await manager.send({ method: 'object.update', array: 'servers', object: { _id: 'server-1', worker2: { status: 'running' } }, organization_id: 'tenant', host: 'tenant.test', apikey: 'excluded' });
    assert.equal(result.object[0]._id, 'server-1');
});

test('workers inherit manager connection without an inventory read', async t => {
    t.mock.method(config, 'get', () => undefined);
    t.mock.method(config, 'query', async () => undefined);
    const previous = process.env.SERVER_MANAGER;
    t.after(() => { if (previous === undefined) delete process.env.SERVER_MANAGER; else process.env.SERVER_MANAGER = previous; });
    process.env.SERVER_MANAGER = JSON.stringify({ organization_id: 'parent', host: 'parent.test', apiKey: 'test-only-key' });
    assert.equal((await init({ isPrimary: false })).organization_id, 'parent');
    assert.equal((await init({ isPrimary: true })).organization_id, null);
});

let moduleId = 0;
async function setupState(t, { primary = true, failedMirror = false } = {}) {
    const listeners = new Map(['SIGINT', 'SIGTERM', 'SIGBREAK'].map(signal => [signal, process.listeners(signal)]));
    t.after(() => {
        for (const [signal, previous] of listeners) for (const listener of process.listeners(signal)) {
            if (!previous.includes(listener)) process.removeListener(signal, listener);
        }
    });
    t.mock.method(config, 'get', async () => undefined);
    t.mock.method(config, 'query', async () => undefined);
    t.mock.method(Http, 'createServer', () => ({ listen() {}, close() {} }));
    const { default: State } = await import(`../src/state.js?manager-test=${++moduleId}`);
    const writes = [], mirrors = [], order = [];
    const server = {
        id: 'server-1', organization_id: 'tenant', host: 'tenant.test', isPrimary: primary,
        workerId: primary ? null : 2, isWorker: !primary, on() {}, workers: new Map(),
        manager: { organization_id: 'parent', send: async request => {
            order.push('mirror'); mirrors.push(structuredClone(request));
            if (failedMirror) throw new Error('offline');
        } },
        crud: { send: async request => {
            if (request.method === 'object.read') return { object: [{ _id: 'server-1', master: { status: 'running' } }] };
            order.push('local'); writes.push(structuredClone(request)); return { object: [request.object] };
        } }
    };
    server.state = await State.init(server);
    return { server, writes, mirrors, order };
}

for (const primary of [true, false]) {
    test(`${primary ? 'primary' : 'worker'} state save mirrors exactly its existing patch`, async t => {
        const { server, writes, mirrors, order } = await setupState(t, { primary });
        await server.state.save('running');
        assert.deepEqual(order, ['local', 'mirror']);
        assert.deepEqual(mirrors, writes);
        assert.equal(writes.length, 1);
        if (primary) assert.equal(writes[0].object.master.status, 'running');
        else assert.deepEqual(Object.keys(writes[0].object), ['_id', 'worker2']);
    });
}

test('failed manager delivery preserves successful local persistence without repeating the write', async t => {
    t.mock.method(console, 'error', () => {});
    const { server, writes, mirrors } = await setupState(t, { failedMirror: true });
    assert.ok((await server.state.save()).object);
    assert.equal(writes.length, 1);
    assert.equal(mirrors.length, 1);
});

test('manager failures log phase and node context without credentials or raw response errors', async t => {
    t.mock.method(config, 'get', () => undefined);
    t.mock.method(config, 'query', async () => undefined);
    const logs = [];
    t.mock.method(console, 'error', (...args) => logs.push(args));
    const manager = await init({ isPrimary: true, id: 'local-server', cluster_id: 'cluster-1', fetch: Fetch });
    Object.assign(manager, { organization_id: 'parent', host: 'parent.test', apiKey: 'private-manager-key' });
    const request = { method: 'post', endpoint: 'create-instance', data: { server_id: 'new-server', cluster_id: 'cluster-1' } };
    for (const [phase, respond] of [
        ['transport', () => { throw Object.assign(new TypeError('private-manager-key'), { cause: { code: 'ECONNRESET' } }); }],
        ['http-response', () => new Response('private-provider-error', { status: 503 })],
        ['http-response', () => new Response('private-provider-error', { status: 200 })],
        ['task-response', () => Response.json({ error: ['private-provider-error'] })]
    ]) {
        const fetch = t.mock.method(globalThis, 'fetch', respond);
        const before = logs.length;
        await assert.rejects(manager.send(request));
        assert.equal(fetch.mock.callCount(), 1);
        fetch.mock.restore();
        assert.equal(logs.length, before + 1);
        const entry = logs.at(-1)[1];
        assert.equal(entry.phase, phase);
        assert.equal(entry.server_id, 'new-server');
        assert.equal(entry.cluster_id, 'cluster-1');
        assert.equal(entry.endpoint, 'create-instance');
    }
    const output = JSON.stringify(logs);
    assert.equal(output.includes('private-manager-key'), false);
    assert.equal(output.includes('private-provider-error'), false);
});
