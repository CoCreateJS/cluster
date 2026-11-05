import assert from 'node:assert/strict';
import test from 'node:test';
import config from '@cocreate/config';
import { init } from '../src/manager.js';

const tenant = 'aaaaaaaaaaaaaaaaaaaaaaaa';
const clusterId = 'bbbbbbbbbbbbbbbbbbbbbbbb';
const platform = 'cccccccccccccccccccccccc';
const clone = value => structuredClone(value);

async function setup(t, options = {}) {
    const cluster = {
        _id: clusterId, name: 'Production', manager: { organization_id: platform, host: 'platform.test', for: tenant }, type: 'organization', host: 'tenant.test',
        initialDeployment: { cloud: 'cloud-api', region: 'example-region', vmType: 'suitable' }, storage: 'storage-api',
        binaryUrl: 'https://example.test/runtime.tar.gz', ...options.cluster
    };
    const requests = [], calls = [], inventory = new Map();
    if (options.existing) inventory.set(options.existing._id, clone(options.existing));
    t.mock.method(config, 'get', () => undefined);
    t.mock.method(config, 'query', async query => query.type === 'organization-connection'
        ? { enabled: options.enabled ?? true }
        : { 'clouds.aws': { regions: [{ region: 'example-region' }], instances: [
            { name: 'too-small', vCPU: 1, memoryGb: 1, default: true }, { name: 'suitable', vCPU: 2, memoryGb: 2 }
        ] }, startupScript: { $concat: [['#!/bin/bash\n# org=', '$payload.organization_id', '\n# storage=', '$storageBase64', '\n# config=', '$bootstrapConfigBase64', '\n']] } });
    const tenantRecords = new Map();
    function update(target, object) {
        const value = target.get(object._id) || {};
        for (const [key, entry] of Object.entries(clone(object))) {
            const segments = key.split('.');
            let cursor = value;
            for (const segment of segments.slice(0, -1)) cursor = cursor[segment] ||= {};
            cursor[segments.at(-1)] = entry;
        }
        target.set(object._id, value);
        return value;
    }
    const server = {
        id: '000000000000000000000001', isPrimary: true, organization_id: platform, host: 'platform.test',
        crud: { send: async request => {
            requests.push(clone(request));
            if (request.organization_id === tenant) {
                if (options.failProjection || (options.failFinalProjection && request.object['infrastructure.instanceId'])) return { error: ['projection failed'] };
                return { object: [clone(update(tenantRecords, request.object))] };
            }
            assert.equal(request.organization_id, platform);
            const query = request.filter?.query;
            if (request.method === 'object.read') {
                if (request.array === 'organizations') return { object: [{ _id: tenant, cluster_id: clusterId, bootstrapStorage: {
                    name: 'tenant-storage', provider: 'mongodb', connectionString: 'mongodb://tenant-only.test'
                } }] };
                if (request.array === 'clusters') return { object: [cluster] };
                if (request.array === 'keys') {
                    assert.equal(query.tenant_organization_id, tenant);
                    return { object: options.missingManagementKey ? [] : [{ key: 'test-only-management-key' }] };
                }
                if (request.array === 'apis') {
                    assert.deepEqual(query, { _id: 'cloud-api', type: 'cloud', enabled: true });
                    return { object: options.missingCloud ? [] : [{ _id: 'cloud-api', name: 'platform-cloud', provider: 'aws', token: 'platform-secret-must-stay-here' }] };
                }
                if (request.array === 'workflows') return { object: options.missingWorkflow ? [] : [{ name: 'create-organization-server' }] };
                if (request.array === 'servers') {
                    if (query?._id) return { object: inventory.has(query._id) ? [clone(inventory.get(query._id))] : [] };
                    assert.fail('Manager must not query cluster server inventory to decide creation');
                }
            }
            if (request.array === 'servers') return { object: [clone(update(inventory, request.object))] };
            throw new Error('Unexpected CRUD request');
        } },
        workflow: { run: async envelope => {
            calls.push(clone(envelope));
            assert.equal(envelope.organization_id, platform);
            const input = envelope['create-organization-server'][0];
            assert.ok(!JSON.stringify(input).includes('platform-secret-must-stay-here'));
            assert.ok(tenantRecords.has(input.server._id), 'Tenant inventory exists before VM boot');
            if (options.failProvider) return { error: ['provider result unknown'] };
            const tenantRecord = tenantRecords.get(input.server._id);
            tenantRecord.master.status = 'running'; // Simulate startup racing the provider response.
            tenantRecord.ip = '203.0.113.10';
            const patch = { _id: input.server._id, status: 'starting', 'infrastructure.instanceId': 'instance-1' };
            await server.crud.send({ method: 'object.update', array: 'servers', organization_id: platform, host: server.host, object: patch });
            return { 'create-organization-server': [{ patch, server: clone(inventory.get(patch._id)) }] };
        } }
    };
    return { handler: await init(server), server, cluster, inventory, tenantRecords, requests, calls };
}


test('createServer uses the supplied cluster and mirrors tenant bootstrap and provider identity', async t => {
    const { handler, cluster, calls, inventory, tenantRecords, requests } = await setup(t);
    const result = await handler.createServer(cluster);
    assert.equal(result.status, 'created');
    assert.equal(calls.length, 1);
    const input = calls[0]['create-organization-server'][0];
    const script = JSON.parse(input.startupScript).$concat[0][0];
    const bootstrap = JSON.parse(Buffer.from(script.match(/# config=(.*)/)[1], 'base64'));
    assert.equal(bootstrap.organization_id, tenant);
    assert.equal(bootstrap.cluster_id, clusterId);
    assert.equal(bootstrap.storage.connectionString, 'mongodb://tenant-only.test');
    assert.equal(bootstrap.apikey, undefined);
    assert.equal(inventory.get(input.server._id).infrastructure.vmType, 'suitable');
    assert.equal(inventory.get(input.server._id).infrastructure.instanceId, 'instance-1');
    assert.equal(tenantRecords.get(input.server._id).infrastructure.instanceId, 'instance-1');
    assert.equal(tenantRecords.get(input.server._id).master.status, 'running');
    assert.deepEqual(tenantRecords.get(input.server._id).manager, {
        organization_id: platform, host: 'platform.test', apiKey: 'test-only-management-key'
    });
    assert.equal(JSON.stringify(calls).includes('test-only-management-key'), false);
    assert.equal(requests.some(request => request.array === 'clusters'), false, 'supplied cluster is reused');
    assert.equal(requests.some(request => request.array === 'organization_server_reservations'), false);
    assert.equal(handler.ensureServer, undefined);
    assert.equal(Object.keys(handler).includes('config'), false);
});

for (const status of ['running', 'provisioning', 'failed', 'terminating', 'stopped']) {
    test(`createServer executes the command without reconsidering existing ${status} inventory`, async t => {
        const { handler, cluster, calls, inventory } = await setup(t, {
            existing: { _id: 'ffffffffffffffffffffffff', cluster_id: clusterId, master: { status } }
        });
        assert.equal((await handler.createServer(cluster)).status, 'created');
        assert.equal(calls.length, 1);
        assert.equal(inventory.size, 2);
        assert.equal(inventory.get('ffffffffffffffffffffffff').master.status, status);
    });
}

test('creation commands are not deduplicated or locked by Manager', async t => {
    const { handler, server, cluster, calls, inventory } = await setup(t);
    const other = await init(server);
    const results = await Promise.all([handler.createServer(cluster), other.createServer(cluster)]);
    assert.ok(results.every(result => result.status === 'created'));
    assert.equal(calls.length, 2);
    assert.equal(inventory.size, 2);
});

test('a missing management key fails before inventory creation', async t => {
    const { handler, cluster, inventory, calls } = await setup(t, { missingManagementKey: true });
    await assert.rejects(handler.createServer(cluster), /management API key/);
    assert.equal(inventory.size, 0);
    assert.equal(calls.length, 0);
});

for (const option of ['failProvider', 'failProjection', 'failFinalProjection', 'missingWorkflow', 'missingCloud']) {
    test(`${option} fails without an implicit retry or reservation`, async t => {
        const { handler, cluster, calls, requests } = await setup(t, { [option]: true });
        await assert.rejects(handler.createServer(cluster));
        assert.equal(calls.length, ['failProvider', 'failFinalProjection'].includes(option) ? 1 : 0);
        assert.equal(requests.some(request => request.array === 'organization_server_reservations'), false);
    });
}

test('Manager exposes its activation settings without serializing them', async t => {
    const { handler, requests } = await setup(t, { enabled: false });
    assert.equal(handler.config.enabled, false);
    assert.deepEqual(Object.keys(handler), ['organization_id', 'host', 'apiKey']);
    assert.deepEqual(requests, []);
});

for (const initial of [undefined, {}, { cloud: 'cloud-api', region: 'example-region', vmType: 'unknown' }]) {
    test(`invalid initialDeployment ${JSON.stringify(initial)} fails before inventory creation`, async t => {
        const { handler, cluster, inventory } = await setup(t, { cluster: { initialDeployment: initial } });
        await assert.rejects(handler.createServer(cluster));
        assert.equal(inventory.size, 0);
    });
}

test('initialDeployment controls placement despite different legacy fields', async t => {
    const { handler, cluster, inventory } = await setup(t, { cluster: {
        clouds: ['unrelated-cloud'], region: 'legacy-region', vmType: 'too-small', vertical: { min: { vCPU: 64, memoryGb: 256 } }
    } });
    await handler.createServer(cluster);
    const [record] = inventory.values();
    assert.equal(record.cloud, 'cloud-api');
    assert.equal(record.infrastructure.region, 'example-region');
    assert.equal(record.infrastructure.vmType, 'suitable');
});

test('creation retains its existing two-minute startup observation', async t => {
    const { handler, cluster, calls } = await setup(t);
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const logs = [];
    t.mock.method(console, 'error', (...args) => logs.push(args));
    const result = await handler.createServer(cluster);
    t.mock.timers.tick(119999);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(logs.length, 0);
    t.mock.timers.tick(1);
    await new Promise(resolve => setImmediate(resolve));
    assert.match(logs[0][0], /No managed startup update/);
    assert.equal(logs[0][1], result.server._id);
    assert.equal(calls.length, 1);
});
