import assert from 'node:assert/strict';
import test from 'node:test';
import cluster from 'node:cluster';
import { EventEmitter } from 'node:events';
import { setImmediate } from 'node:timers/promises';
import config from '@cocreate/config';
import { init as initAutoscaler } from '../../autoscaler/src/index.js';
import { SocketServer } from '../../socket-server/src/index.js';
import IPC from '../src/ipc.js';
import { ServerMesh } from '../src/mesh/index.js';

async function setup(t, { minimum = 0, enabled = true, workers = 1, telemetry = true } = {}) {
    t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
    t.mock.method(config, 'get', async () => undefined);
    t.mock.method(config, 'query', async () => undefined);
    t.mock.method(console, 'log', () => {});
    const logs = [];
    t.mock.method(console, 'error', (...args) => logs.push(args));
    const previous = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    t.after(() => {
        for (const [event, listeners] of previous) {
            for (const listener of cluster.listeners(event)) if (!listeners.includes(listener)) cluster.removeListener(event, listener);
        }
    });
    const record = { _id: 'node', cluster_id: 'cluster', cloud: 'cloud', master: { status: 'running' },
        infrastructure: { vmId: 'vm', region: 'us-east-1', vCPU: 2, memoryGb: 2 } };
    const calls = [];
    const server = Object.assign(new EventEmitter(), {
        id: 'node', organization_id: 'tenant', host: 'tenant.test', cluster_id: 'cluster',
        isPrimary: true, totalWorkers: 1, workers: new Map(),
        api: { send: async request => { calls.push(['terminate', request]); return { success: true }; } },
        state: {
            drain: async value => calls.push(['drain', value]),
            shutdown: payload => calls.push(['shutdown', payload])
        },
        crud: { send: async request => {
            if (request.method === 'object.update') {
                record.master.status = request.object['master.status'];
                calls.push(['status', record.master.status]);
                return { object: [structuredClone(record)] };
            }
            if (request.array === 'apis') return { object: [{ _id: 'cloud', name: 'aws', provider: 'aws' }] };
            if (request.array === 'clusters') return { object: [{ _id: 'cluster', clouds: ['cloud'],
                autoscaling: { enabled }, scaleIn: { minClusterSize: minimum, drainWaitSeconds: 1, decommissionStrategy: 'local',
                    cpuThreshold: 20, memoryThreshold: 20, consecutiveFlushes: 1 },
                scaleOut: { cpuThreshold: 90, memoryThreshold: 90, consecutiveFlushes: 1 }, endpoint: { delete: 'delete-instance' } }] };
            if (request.array === 'servers_telemetry') return { object: telemetry ? [{ serverId: 'node', workerId: 1,
                timestamp: new Date().toISOString(), cpu: { avg: 80 }, memory: { avg_used_pct: 80 } }] : [] };
            const query = request.filter.query;
            if (query._id) return { object: [structuredClone(record)] };
            const status = query['master.status'];
            return { object: (Array.isArray(status?.$in) ? status.$in.includes(record.master.status) : status === record.master.status)
                ? [structuredClone(record)] : [] };
        } }
    });
    const mesh = new ServerMesh();
    mesh.server = server;
    server.mesh = mesh;
    server.send = IPC.init(server, new Map());
    await initAutoscaler(server);
    const managers = Array.from({ length: workers }, (_, index) => {
        const manager = new EventEmitter();
        Object.setPrototypeOf(manager, SocketServer.prototype);
        Object.assign(manager, { server, workerId: index + 1, organizations: new Map(), organizationBalances: new Map(),
            clients: new Map(), sockets: new Map(), users: new Map(), removeSubscriptions() {} });
        return manager;
    });
    function connect(worker = 0, id = `client-${worker}`) {
        const socket = Object.assign(new EventEmitter(), { id, clientId: id, organization_id: 'tenant', host: 'tenant.test' });
        managers[worker].add(socket);
        return socket;
    }
    return { server, mesh, managers, calls, logs, connect };
}

async function advance(t, ms) { t.mock.timers.tick(ms); await setImmediate(); }

test('own-organization last-client grace triggers the existing termination lifecycle independently of low load', async t => {
    const { managers, connect, calls, logs } = await setup(t);
    const socket = connect();
    managers[0].delete(socket);
    await advance(t, 59999);
    assert.deepEqual(calls, []);
    await advance(t, 1);
    assert.ok(managers[0].organizations.has('tenant'), 'own organization stays loaded');
    assert.equal(managers[0].organizations.get('tenant').isRegistered, false);
    await advance(t, 1000);
    assert.deepEqual(calls.map(call => call[0]), ['status', 'drain', 'status', 'terminate', 'shutdown']);
    assert.equal(calls[2][1], 'terminating');
    assert.deepEqual(logs, []);
});

test('a reconnect during the grace period cancels scale-to-zero', async t => {
    const { managers, connect, calls, server } = await setup(t);
    managers[0].delete(connect());
    await advance(t, 30000);
    connect(0, 'replacement');
    await advance(t, 120000);
    assert.deepEqual(calls, []);
    assert.equal(server.organizationWorkers.has('tenant'), true);
});

test('low CPU and memory alone cannot remove the last node', async t => {
    const { server, connect, calls } = await setup(t);
    connect();
    await server.listeners('telemetry')[0]({ data: { serverId: 'node', workerId: 1,
        timestamp: new Date().toISOString(), interval_seconds: 1,
        cpu: { avg: 1 }, memory: { avg_used_pct: 1 } } });
    await advance(t, 2000);
    assert.deepEqual(calls, []);
});

test('confirmed organization removal can reach zero without CPU or memory samples', async t => {
    const { mesh, calls } = await setup(t, { telemetry: false });
    mesh.closeOrganization('tenant');
    await setImmediate();
    await advance(t, 1000);
    assert.equal(calls.filter(call => call[0] === 'terminate').length, 1);
});

test('another organization removal does not terminate this node', async t => {
    const { mesh, calls } = await setup(t);
    mesh.closeOrganization('other-tenant');
    await advance(t, 61000);
    assert.deepEqual(calls, []);
});

test('another worker with clients prevents organization inactivity', async t => {
    const { managers, connect, calls, server } = await setup(t, { workers: 2 });
    managers[0].delete(connect(0));
    connect(1);
    await advance(t, 61000);
    assert.deepEqual(calls, []);
    assert.equal(server.organizationWorkers.has('tenant'), true);
});

for (const options of [{ minimum: 1 }, { enabled: false }]) {
    test(`inactivity respects policy ${JSON.stringify(options)}`, async t => {
        const { managers, connect, calls } = await setup(t, options);
        managers[0].delete(connect());
        await advance(t, 61000);
        assert.deepEqual(calls, []);
    });
}

test('client return after idle deregistration registers the retained own organization again', async t => {
    const { managers, connect, calls, server } = await setup(t, { minimum: 1 });
    managers[0].delete(connect());
    await advance(t, 60000);
    assert.equal(server.organizationWorkers.has('tenant'), false);
    connect(0, 'returned');
    assert.equal(server.organizationWorkers.has('tenant'), true);
    assert.equal(managers[0].organizations.get('tenant').isRegistered, true);
    assert.deepEqual(calls, []);
});

test('existing Mesh organization removal notifies AutoScaler and uses the same termination path', async t => {
    const { mesh, calls } = await setup(t);
    mesh.setupProtocolListeners();
    const socket = { readyState: 1, serverId: 'platform', send() {} };
    mesh.addOrganizationSocket('tenant', socket);
    mesh.handleIncomingMessage(JSON.stringify({ method: 'mesh.orgDeleted', organization_id: 'tenant', serverId: 'platform' }), socket);
    await setImmediate();
    await advance(t, 1000);
    assert.equal(calls.filter(call => call[0] === 'terminate').length, 1);
    assert.equal(calls.at(-1)[0], 'shutdown');
});

test('closing an organization sends the existing removal message before closing its sockets', async t => {
    const { mesh } = await setup(t, { minimum: 1 });
    const order = [];
    mesh.addOrganizationSocket('tenant', { readyState: 1,
        send: body => order.push(JSON.parse(body).method), close: () => order.push('close') });
    mesh.closeOrganization('tenant');
    assert.deepEqual(order, ['mesh.orgDeleted', 'close']);
});
