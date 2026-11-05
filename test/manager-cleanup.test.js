import assert from 'node:assert/strict';
import test from 'node:test';
import { Readable } from 'node:stream';
import { setImmediate } from 'node:timers/promises';
import { init } from '../src/manager.js';

const id = 'server-1';
const update = status => ({
    method: 'object.update', array: 'servers', object: { _id: id, 'master.status': status }
});

async function setup(t, { authorized = true, failUpdate = false, failDelete = false } = {}) {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const calls = [], logs = [];
    t.mock.method(console, 'error', (...args) => logs.push(args));
    const record = { _id: id, cluster_id: 'cluster-1', host: 'tenant.test', master: { status: 'running' } };
    const copies = new Map([['platform', structuredClone(record)], ['tenant', structuredClone(record)]]);
    const server = {
        isPrimary: false, organization_id: 'platform', host: 'platform.test',
        authorize: { request: async request => authorized ? request : undefined },
        crud: {
            getOrganization: async () => ({ _id: 'platform' }),
            send: async request => {
                calls.push(structuredClone(request));
                if (request.method === 'object.read') {
                    assert.equal(request.array, 'clusters', 'cleanup must not reread server status');
                    assert.deepEqual(request.filter.query, { _id: 'cluster-1', type: 'organization' });
                    assert.equal(request.organization_id, 'platform');
                    return { object: [{ _id: 'cluster-1', type: 'organization', name: 'Production', manager: { organization_id: 'platform', host: 'platform.test', for: 'tenant' }, host: 'tenant.test' }] };
                }
                if (request.method === 'object.delete') {
                    if (failDelete) return { error: ['Deletion unavailable'] };
                    const record = copies.get(request.organization_id);
                    copies.delete(request.organization_id);
                    return { object: record ? [record] : [] };
                }
                if (failUpdate) return { success: false, error: ['Write unavailable'] };
                const saved = copies.get('platform');
                const status = request.object['master.status'] || request.object.master?.status;
                saved.master.status = status;
                return { object: [structuredClone(saved)] };
            }
        }
    };
    server.manager = await init(server);
    return { server, copies, calls, logs };
}

async function post(manager, input) {
    const req = Readable.from([JSON.stringify(input)]);
    Object.assign(req, { method: 'POST', headers: {
        host: 'platform.test', authorization: 'Bearer test-key', 'content-type': 'application/json'
    } });
    const response = {};
    await manager.http(req, {
        setHeader() {}, writeHead(status) { response.status = status; },
        end(body) { response.body = JSON.parse(body); }
    });
    return response;
}

async function advance(t, ms) {
    t.mock.timers.tick(ms);
    await setImmediate();
}

test('a persisted terminating update schedules both inventory deletions after exactly one minute', async t => {
    const { server, copies, calls } = await setup(t);
    assert.equal((await post(server.manager, update('terminating'))).status, 200);
    await advance(t, 59999);
    assert.equal(copies.size, 2);
    assert.equal(calls.length, 1);
    await advance(t, 1);
    assert.equal(copies.size, 0);
    assert.deepEqual(calls.map(call => call.method), ['object.update', 'object.read', 'object.delete', 'object.delete']);
    assert.deepEqual(calls.slice(-2).map(call => [call.organization_id, call.host]),
        [['tenant', 'tenant.test'], ['platform', 'platform.test']]);
});

test('duplicate terminating reports do not postpone or duplicate the pending cleanup', async t => {
    const { server, copies, calls } = await setup(t);
    await post(server.manager, update('terminating'));
    await advance(t, 30000);
    await post(server.manager, update('terminating'));
    await advance(t, 30000);
    assert.equal(copies.size, 0);
    await advance(t, 60000);
    assert.equal(calls.filter(call => call.method === 'object.delete').length, 2);
});

test('a persisted failure cancels pending cleanup and retains both inventories without another lookup', async t => {
    const { server, copies, calls } = await setup(t);
    await post(server.manager, update('terminating'));
    await advance(t, 30000);
    assert.equal((await post(server.manager, update('failed'))).status, 200);
    await advance(t, 600000);
    assert.equal(copies.size, 2);
    assert.equal(copies.get('platform').master.status, 'failed');
    assert.deepEqual(calls.map(call => call.method), ['object.update', 'object.update']);
});

test('a rejected failure update cannot cancel an authoritative cleanup timer', async t => {
    const { server, copies } = await setup(t);
    await post(server.manager, update('terminating'));
    server.authorize.request = async () => undefined;
    assert.equal((await post(server.manager, update('failed'))).status, 403);
    await advance(t, 60000);
    assert.equal(copies.size, 0);
});

test('AutoScaler cleanup can win the race with the Manager timer without causing errors', async t => {
    const { server, copies, calls, logs } = await setup(t);
    await post(server.manager, update('terminating'));
    copies.delete('tenant');
    await post(server.manager, { method: 'object.delete', array: 'servers', object: { _id: id } });
    await advance(t, 60000);
    assert.equal(copies.size, 0);
    assert.deepEqual(logs, []);
    assert.equal(calls.filter(call => call.method === 'object.delete').length, 3);
});

for (const status of ['failed', 'running', 'provisioning']) {
    test(`${status} reports do not schedule inventory cleanup`, async t => {
        const { server, copies, calls } = await setup(t);
        await post(server.manager, update(status));
        await advance(t, 600000);
        assert.equal(copies.size, 2);
        assert.equal(calls.length, 1);
    });
}

test('unsuccessful persistence does not schedule cleanup', async t => {
    const { server, copies, calls } = await setup(t, { failUpdate: true });
    assert.equal((await post(server.manager, update('terminating'))).status, 400);
    await advance(t, 60000);
    assert.equal(copies.size, 2);
    assert.equal(calls.length, 1);
});

test('Manager deletion is authorized, parent-scoped, idempotent and does not mirror back', async t => {
    const { server, copies, calls } = await setup(t);
    const request = { method: 'object.delete', array: 'servers', object: { _id: id },
        organization_id: 'forged', host: 'forged.test', upsert: true };
    for (let attempt = 0; attempt < 2; attempt++) assert.equal((await post(server.manager, request)).status, 200);
    assert.equal(copies.has('tenant'), true);
    assert.equal(copies.has('platform'), false);
    assert.equal(calls.length, 2);
    assert.ok(calls.every(call => call.organization_id === 'platform' && call.host === 'platform.test'));
    assert.deepEqual(calls[0].object, [{ _id: id }]);
    assert.equal(calls[0].upsert, undefined);
    assert.equal((await post(server.manager, { ...request, array: 'clusters' })).status, 400);
});

test('unauthorized Manager deletion does not reach CRUD', async t => {
    const { server, calls } = await setup(t, { authorized: false });
    assert.equal((await post(server.manager, { method: 'object.delete', array: 'servers', object: { _id: id } })).status, 403);
    assert.deepEqual(calls, []);
});

test('Manager reports actual deletion failure instead of treating it as an absent record', async t => {
    const { server, copies, logs } = await setup(t, { failDelete: true });
    await post(server.manager, update('terminating'));
    await advance(t, 60000);
    assert.equal(copies.size, 2);
    assert.equal(logs.length, 1);
    assert.match(logs[0][0], /Managed server inventory cleanup failed/);
    assert.equal((await post(server.manager, { method: 'object.delete', array: 'servers', object: { _id: id } })).status, 500);
});

test('management permission reference grants deletion only for server inventory', async () => {
    const { default: reference } = await import('../../../CoCreate-app/src/apps/api-keys/config/managed-cluster.js');
    assert.deepEqual(reference.object[0].methods.object.delete, { $array: ['servers'] });
});
