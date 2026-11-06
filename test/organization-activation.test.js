import assert from 'node:assert/strict';
import test from 'node:test';
import { ServerMesh } from '../src/mesh/index.js';

const tenant = 'aaaaaaaaaaaaaaaaaaaaaaaa';
const flush = () => new Promise(resolve => setImmediate(resolve));

function setup({ records = [], response, pendingRead } = {}) {
    const mesh = new ServerMesh();
    const calls = [], reads = [], connections = [], announcements = [];
    mesh.server = {
        isPrimary: true, id: 'platform-node', organization_id: 'platform', host: 'platform.test',
        manager: { startCluster: id => { calls.push(id); } },
        crud: { send: async request => {
            reads.push(request);
            assert.equal(request.array, 'servers');
            assert.equal(request.organization_id, 'platform');
            assert.equal(request.host, 'platform.test');
            assert.deepEqual(request.filter.query, {
                _id: { $lt: 'platform-node' }, 'master.status': 'running',
                ...(request.filter.query.tenant_organization_id && { tenant_organization_id: tenant })
            });
            return pendingRead || response || { object: records };
        } }
    };
    mesh.outboundManager.server = mesh.server;
    mesh.outboundManager.connectPeer = (...args) => connections.push(args);
    mesh.announceOrganization = id => announcements.push(id);
    return { mesh, calls, reads, connections, announcements };
}

test('empty organization discovery notifies Manager using the same single server read', async () => {
    const { mesh, calls, reads } = setup();
    mesh.openOrganization(tenant);
    await flush();
    assert.deepEqual(calls, [tenant]);
    assert.equal(reads.length, 1);

});

test('one organization discovery result drives peer connections and avoids cluster startup', async () => {
    const peer = { _id: 'older-peer', ip: '192.0.2.1' };
    const { mesh, calls, reads, connections } = setup({ records: [peer] });
    mesh.outboundManager.peers = [{ _id: 'control-peer' }];
    mesh.openOrganization(tenant);
    await flush();
    assert.deepEqual(calls, []);
    assert.equal(reads.length, 1);
    assert.deepEqual(connections, [[peer, 1, tenant]]);
    assert.deepEqual(mesh.outboundManager.peers, [{ _id: 'control-peer' }]);
});

test('startup and activation call the same discovery function with their organization ID', async () => {
    const { mesh } = setup();
    const calls = [];
    mesh.outboundManager.getServers = id => { calls.push(id); };
    await mesh.outboundManager.init(mesh.server);
    mesh.openOrganization(tenant);
    mesh.openOrganization('platform');
    assert.deepEqual(calls, ['platform', tenant, 'platform']);
    assert.equal(mesh.checkOrganizationServers, undefined);
});

for (const response of [{ error: ['Unavailable'] }, { success: false }, {}, { object: null }]) {
    test(`failed discovery ${JSON.stringify(response)} does not trigger provisioning`, async t => {
        const { mesh, calls } = setup({ response });
        const logs = [];
        t.mock.method(console, 'error', (...args) => logs.push(args));
        mesh.openOrganization(tenant);
        await flush();
        assert.deepEqual(calls, []);
        assert.equal(logs.length, 1);
    });
}

test('organization activation announces immediately without waiting for discovery or provisioning', async () => {
    let resolve;
    const pendingRead = new Promise(done => { resolve = done; });
    const { mesh, calls, announcements } = setup({ pendingRead });
    mesh.server.manager.startCluster = id => {
        calls.push(id);
        return new Promise(() => {});
    };
    assert.equal(mesh.openOrganization(tenant), undefined);
    assert.deepEqual(announcements, [tenant]);
    assert.deepEqual(calls, []);
    resolve({ object: [] });
    await flush();
    assert.deepEqual(calls, [tenant]);
});

test('Mesh delegates policy to Manager and does not read manager settings', async () => {
    const { mesh, calls } = setup();
    Object.defineProperty(mesh.server.manager, 'config', { get() { throw new Error('Mesh must not read Manager policy'); } });
    mesh.openOrganization(tenant);
    await flush();
    assert.deepEqual(calls, [tenant]);
});

test('workers do not initiate organization discovery', async () => {
    const { mesh, calls, reads } = setup();
    mesh.server.isPrimary = false;
    mesh.openOrganization(tenant);
    await flush();
    assert.deepEqual(calls, []);
    assert.deepEqual(reads, []);
});


test('own-organization activation discovers and connects control peers without requesting startup', async () => {
    const peer = { _id: 'older-peer', ip: '192.0.2.1' };
    const { mesh, calls, reads, connections } = setup({ records: [peer] });
    mesh.openOrganization('platform');
    await flush();
    assert.deepEqual(calls, []);
    assert.equal(reads.length, 1);
    assert.equal(reads[0].filter.query.tenant_organization_id, undefined);
    assert.deepEqual(connections, [[peer, 1, undefined]]);
});

test('own-organization startup with no peers never requests another cluster', async () => {
    const { mesh, calls, reads } = setup();
    await mesh.outboundManager.init(mesh.server);
    assert.deepEqual(calls, []);
    assert.equal(reads.length, 1);
});
