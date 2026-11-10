import assert from 'node:assert/strict';
import test from 'node:test';
import { Readable } from 'node:stream';
import { setImmediate } from 'node:timers/promises';
import Authorize from '../../authorize/src/index.js';
import { init as initApi } from '../../api/src/server.js';
import { init as initManager } from '../src/manager.js';

const serverId = 'aaaaaaaaaaaaaaaaaaaaaaaa';
const create = { api: 'cloud', endpoint: 'create-instance', method: 'post', data: { server_id: serverId } };
const update = id => ({ method: 'object.update', array: 'servers', object: { _id: id, master: { status: 'running' } } });

async function post(manager, input) {
    const req = Readable.from([JSON.stringify(input)]);
    Object.assign(req, { method: 'POST', url: '/manager', headers: {
        host: 'platform.test', 'content-type': 'application/json', authorization: 'Bearer test-key'
    } });
    const response = {};
    await manager.http(req, {
        setHeader() {},
        writeHead(status) { response.status = status; },
        end(body) { response.body = JSON.parse(body); }
    });
    return response;
}

async function setup(t, options = {}) {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const logs = [], reads = [], calls = [];
    t.mock.method(console, 'error', (...args) => logs.push(args));
    const records = new Map([[serverId, { _id: serverId, master: { status: 'provisioning' } }]]);
    const server = {
        organization_id: 'platform', host: 'platform.test',
        authorize: { ...Authorize, check: async () => true },
        crud: {
            getOrganization: async () => ({ _id: 'platform' }),
            send: async request => {
                if (request.method === 'object.read') {
                    reads.push(request);
                    if (options.failRead) return { success: false };
                    return { object: [records.get(request.filter.query._id)].filter(Boolean) };
                }
                if (request.array === 'clusters') return { object: [structuredClone(request.object)] };
                if (options.failUpdate) return { success: false, error: 'Write failed' };
                const record = { ...records.get(request.object._id), ...structuredClone(request.object) };
                records.set(record._id, record);
                return { object: [record] };
            }
        }
    };
    server.api = { ...await initApi(server), send: async request => {
        calls.push(request);
        if (options.beforeResponse) await options.beforeResponse(server);
        return options.providerResult ?? { data: { id: 'i-test' } };
    } };
    server.manager = await initManager(server);
    return { server, logs, reads, calls, records };
}

async function advance(t, milliseconds = 120000) {
    t.mock.timers.tick(milliseconds);
    await setImmediate();
}

test('successful creation waits two minutes for the created server update and never retries', async t => {
    const { server, logs, calls } = await setup(t);
    assert.equal((await post(server.manager, create)).status, 200);
    await advance(t, 119999);
    assert.equal(logs.length, 0);
    await advance(t, 1);
    assert.equal(logs.length, 1);
    assert.match(logs[0][0], /No managed startup update/);
    assert.equal(logs[0][1], serverId);
    await advance(t, 240000);
    assert.equal(logs.length, 1);
    assert.equal(calls.length, 1);
});

test('a persisted matching server update cancels startup observation', async t => {
    const { server, logs, reads } = await setup(t);
    await post(server.manager, create);
    assert.equal((await post(server.manager, update(serverId))).status, 200);
    await advance(t);
    assert.equal(logs.length, 0);
    assert.equal(reads.length, 0);
});

test('an unrelated server update does not confirm the created node', async t => {
    const { server, logs } = await setup(t);
    await post(server.manager, create);
    await post(server.manager, update('other-server'));
    await advance(t);
    assert.equal(logs.length, 1);
    assert.equal(logs[0][1], serverId);
});

test('a startup update arriving before the provider response prevents a false timeout', async t => {
    const { server, logs, reads } = await setup(t, { beforeResponse: server => post(server.manager, update(serverId)) });
    await post(server.manager, create);
    await advance(t);
    assert.equal(logs.length, 0);
    assert.equal(reads.length, 0);
});

test('an update received by another managing worker is recognized in persisted state', async t => {
    const { server, logs, reads } = await setup(t);
    await post(server.manager, create);
    const otherManager = await initManager(server);
    await post(otherManager, update(serverId));
    await advance(t);
    assert.equal(logs.length, 0);
    assert.equal(reads.length, 1);
});

for (const providerResult of [{ success: false, error: 'Rejected' }, { data: {} }]) {
    test(`no startup timer without a successful instance ID: ${JSON.stringify(providerResult)}`, async t => {
        const { server, logs, reads, calls } = await setup(t, { providerResult });
        await post(server.manager, create);
        await advance(t);
        assert.equal(logs.length, 0);
        assert.equal(reads.length, 0);
        assert.equal(calls.length, 1);
    });
}

test('a failed inventory write does not confirm startup', async t => {
    const { server, logs } = await setup(t, { failUpdate: true });
    await post(server.manager, create);
    assert.equal((await post(server.manager, update(serverId))).status, 400);
    await advance(t);
    assert.match(logs[0][0], /No managed startup update/);
});

test('a denied update does not confirm startup', async t => {
    const { server, logs } = await setup(t);
    await post(server.manager, create);
    server.authorize.check = async () => false;
    assert.equal((await post(server.manager, update(serverId))).status, 403);
    await advance(t);
    assert.match(logs[0][0], /No managed startup update/);
});

test('an unavailable inventory reports verification failure without declaring a missing update', async t => {
    const { server, logs } = await setup(t, { failRead: true });
    await post(server.manager, create);
    await advance(t);
    assert.equal(logs.length, 1);
    assert.match(logs[0][0], /Unable to verify/);
});

test('cluster metadata updates cannot confirm server startup', async t => {
    const { server, logs } = await setup(t);
    await post(server.manager, create);
    await post(server.manager, { ...update(serverId), array: 'clusters' });
    await advance(t);
    assert.match(logs[0][0], /No managed startup update/);
});

test('a thrown creation failure does not start the startup timer', async t => {
    const { server, logs, reads, calls } = await setup(t, { beforeResponse: () => { throw new TypeError('Transport failed'); } });
    assert.equal((await post(server.manager, create)).status, 500);
    await advance(t);
    assert.equal(logs.length, 0);
    assert.equal(reads.length, 0);
    assert.equal(calls.length, 1);
});
