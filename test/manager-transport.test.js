import assert from 'node:assert/strict';
import test from 'node:test';
import config from '@cocreate/config';
import { init } from '../src/manager.js';
import Fetch from '../src/fetch.js';

test('manager sends in platform scope with a trusted managed organization reference', async t => {
    t.mock.method(config, 'get', () => ({
        organization_id: 'platform', host: 'platform.test', apiKey: 'bootstrap-managed-key'
    }));
    const server = { organization_id: 'tenant', fetch: Fetch };
    const service = await init(server);
    assert.equal(server.manager, service);
    assert.equal(server.manager.organization_id, 'platform');
    assert.equal(typeof server.manager.send, 'function');
    assert.equal(service?.createServer, undefined);
    const payload = {
        method: 'post', api: 'platform-cloud', endpoint: 'create-instance',
        organization_id: 'untrusted-scope', managedOrganization: 'untrusted-tenant',
        host: 'tenant.test', apikey: 'payload-key',
        socketId: 'socket-1', clientId: 'client-1', broadcastSender: true,
        data: { server_id: 'server-1', cluster_id: 'cluster-1', region: 'us-east-1' }
    };
    const expected = { ...structuredClone(payload), organization_id: 'platform', managedOrganization: 'tenant' };
    delete expected.apikey;
    const response = { ...payload, data: { id: 'vm-1' } };
    t.mock.method(globalThis, 'fetch', async (url, options) => {
        assert.equal(url, 'https://platform.test');
        assert.equal(options.method, 'POST');
        assert.equal(options.headers.Authorization, 'Bearer bootstrap-managed-key');
        assert.equal(options.headers['Content-Type'], 'application/json');
        assert.deepEqual(payload, expected);
        assert.deepEqual(JSON.parse(options.body), expected);
        return Response.json(response);
    });
    assert.deepEqual(await server.manager.send(payload), response);
    assert.deepEqual(payload, expected);
});

test('the platform has no upstream manager connection', async t => {
    t.mock.method(config, 'get', () => ({ organization_id: 'platform', host: 'platform.test' }));
    const server = { organization_id: 'platform' };
    const service = await init(server);
    assert.equal(server.manager, undefined);
    assert.equal(server.manager, service);
    assert.equal(service?.createServer, undefined);
});

test('manager connection is frozen after its send method is attached', async t => {
    const managerConfig = { organization_id: 'platform', host: 'platform.test', apiKey: 'tenant-bootstrap-key' };
    t.mock.method(config, 'get', () => managerConfig);
    const server = { organization_id: 'tenant', fetch: Fetch };
    await init(server);
    assert.ok(Object.isFrozen(server.manager));
    for (const field of ['organization_id', 'host', 'apiKey', 'send']) {
        assert.throws(() => { server.manager[field] = 'changed'; }, TypeError);
    }
    managerConfig.organization_id = 'changed';
    managerConfig.host = 'changed.test';
    managerConfig.apiKey = 'changed';
    assert.equal(server.manager.organization_id, 'platform');
    assert.equal(server.manager.host, 'platform.test');
    assert.equal(server.manager.apiKey, 'tenant-bootstrap-key');
    t.mock.method(globalThis, 'fetch', async (url, options) => {
        assert.equal(url, 'https://platform.test');
        assert.equal(options.headers.Authorization, 'Bearer tenant-bootstrap-key');
        const payload = JSON.parse(options.body);
        assert.equal(payload.organization_id, 'platform');
        assert.equal(payload.managedOrganization, 'tenant');
        return Response.json({ success: true });
    });
    assert.deepEqual(await server.manager.send({ method: 'post', api: 'aws', endpoint: 'create-instance' }), { success: true });
});


test('received error envelopes are returned to the caller', async t => {
    t.mock.method(config, 'get', () => ({ organization_id: 'platform', host: 'platform.test', apiKey: 'key' }));
    const server = { organization_id: 'tenant', fetch: Fetch };
    await init(server);
    const response = { error: ['provider unavailable'] };
    t.mock.method(globalThis, 'fetch', async () => Response.json(response));
    assert.deepEqual(await server.manager.send({ method: 'workflow.createOrganizationServer' }), response);
});

test('HTTP failures reject with the parsed response available', async t => {
    t.mock.method(config, 'get', () => ({ organization_id: 'platform', host: 'platform.test', apiKey: 'key' }));
    t.mock.method(console, 'error', () => {});
    const server = { organization_id: 'tenant', fetch: Fetch };
    await init(server);
    t.mock.method(globalThis, 'fetch', async () => Response.json({ error: ['forbidden'] }, { status: 403 }));
    await assert.rejects(server.manager.send({ method: 'workflow.createOrganizationServer' }), error => {
        assert.equal(error.response.status, 403);
        assert.deepEqual(error.response.data, { error: ['forbidden'] });
        return true;
    });
});
