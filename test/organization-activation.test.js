import assert from 'node:assert/strict';
import test from 'node:test';
import { ServerMesh } from '../src/mesh/index.js';

const tenant = 'aaaaaaaaaaaaaaaaaaaaaaaa';
const cluster = { _id: 'cluster-1', type: 'organization', name: 'Production', host: 'tenant.test', manager: { organization_id: 'platform', host: 'platform.test', for: tenant } };

function setup({ records = [], enabled = true, matchingCluster = true, failRead = false } = {}) {
    const mesh = new ServerMesh();
    const calls = [], reads = [];
    mesh.server = {
        isPrimary: true, id: 'platform-node', organization_id: 'platform', host: 'platform.test',
        manager: { config: { enabled }, createServer: async definition => {
            calls.push(definition);
            return { status: 'created' };
        } },
        crud: { send: async request => {
            reads.push(request);
            if (failRead) return { error: ['Unavailable'] };
            if (request.array === 'clusters') {
                assert.deepEqual(request.filter.query, { type: 'organization', 'manager.for': tenant, 'manager.organization_id': 'platform' });
                return { object: matchingCluster ? [cluster] : [] };
            }
            assert.equal(request.array, 'servers');
            assert.equal(request.organization_id, 'platform');
            assert.equal(request.host, 'platform.test');
            assert.deepEqual(request.filter.query, { cluster_id: cluster._id, 'master.status': 'running' });
            return { object: records.filter(record => record.cluster_id === cluster._id && record.master.status === 'running') };
        } }
    };
    return { mesh, calls, reads };
}

test('Mesh decides creation from the target cluster running inventory and passes the stored definition', async () => {
    const { mesh, calls, reads } = setup();
    assert.deepEqual(await mesh.checkOrganizationServers(tenant), { status: 'created' });
    assert.deepEqual(calls, [cluster]);
    assert.deepEqual(reads.map(read => read.array), ['clusters', 'servers']);
    assert.equal(reads[1].filter.query._id, undefined, 'creation is independent of older-peer ordering');
});

test('a running server blocks creation even when its ID is newer than the platform node', async () => {
    const { mesh, calls } = setup({ records: [{ _id: 'zzzzzzzz', cluster_id: cluster._id, master: { status: 'running' } }] });
    await mesh.checkOrganizationServers(tenant);
    assert.deepEqual(calls, []);
});

for (const status of ['failed', 'terminating', 'terminated', 'stopped', 'provisioning']) {
    test(`${status} inventory remains untouched and does not block a new deployment`, async () => {
        const record = { _id: 'existing', cluster_id: cluster._id, master: { status } };
        const { mesh, calls } = setup({ records: [record] });
        await mesh.checkOrganizationServers(tenant);
        assert.deepEqual(calls, [cluster]);
        assert.equal(record.master.status, status);
    });
}

test('another cluster running inventory does not block creation', async () => {
    const { mesh, calls } = setup({ records: [{ _id: 'other', cluster_id: 'other-cluster', master: { status: 'running' } }] });
    await mesh.checkOrganizationServers(tenant);
    assert.equal(calls.length, 1);
});

test('unmanaged organizations have no cluster creation decision', async () => {
    const { mesh, calls, reads } = setup({ matchingCluster: false });
    await mesh.checkOrganizationServers(tenant);
    assert.deepEqual(calls, []);
    assert.equal(reads.length, 1);
});

test('failed inventory reads do not count as an empty running inventory', async () => {
    const { mesh, calls } = setup({ failRead: true });
    await assert.rejects(mesh.checkOrganizationServers(tenant), /Unable to read/);
    assert.deepEqual(calls, []);
});

test('activation preserves immediate announcement while its creation decision completes asynchronously', async t => {
    const { mesh, calls } = setup();
    const events = [];
    let reject;
    mesh.server.manager.createServer = definition => {
        calls.push(definition);
        return new Promise((resolve, fail) => { reject = fail; });
    };
    mesh.announceOrganization = id => events.push(`announce:${id}`);
    t.mock.method(console, 'error', () => events.push('reported'));
    mesh.openOrganization(tenant);
    assert.deepEqual(events, [`announce:${tenant}`]);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(calls.length, 1);
    reject(new Error('Cloud operation failed'));
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(events.at(-1), 'reported');
});

test('disabled activation, own organization and workers do not start platform provisioning', async () => {
    const { mesh, calls, reads } = setup({ enabled: false });
    mesh.announceOrganization = () => {};
    mesh.openOrganization(tenant);
    mesh.server.manager.config.enabled = true;
    mesh.openOrganization('platform');
    mesh.server.isPrimary = false;
    mesh.openOrganization(tenant);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(calls, []);
    assert.deepEqual(reads, []);
});
