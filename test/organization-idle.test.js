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
    const processPrevious = new Map(['orgConnected', 'orgDisconnected'].map(event => [event, process.listeners(event)]));
    const previous = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    t.after(() => {
        for (const [event, listeners] of processPrevious) {
            for (const listener of process.listeners(event)) if (!listeners.includes(listener)) process.removeListener(event, listener);
        }
        for (const [event, listeners] of previous) {
            for (const listener of cluster.listeners(event)) if (!listeners.includes(listener)) cluster.removeListener(event, listener);
        }
    });
    const record = { _id: 'node', cluster_id: 'cluster', cloud: 'cloud', master: { status: 'running' },
        infrastructure: { vmId: 'vm', region: 'us-east-1', vCPU: 2, memoryGb: 2 } };
    const calls = [];
    const server = Object.assign(new EventEmitter(), {
        id: 'node', organization_id: 'tenant', host: 'tenant.test', cluster_id: 'cluster',
        isPrimary: true, isWorker: true, workers: new Map(),
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
    mesh.setupServerListeners();
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
    assert.equal(managers[0].organizations.has('tenant'), false, 'own organization is cleaned up like any other');
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
    const { server, calls } = await setup(t, { telemetry: false });
    server.emit('orgDisconnected', { organization_id: 'tenant' });
    await setImmediate();
    await advance(t, 1000);
    assert.equal(calls.filter(call => call[0] === 'terminate').length, 1);
});

test('another organization removal does not terminate this node', async t => {
    const { server, calls } = await setup(t);
    server.emit('orgDisconnected', { organization_id: 'other-tenant' });
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

test('client return recreates the own organization after uniform idle cleanup', async t => {
    const { managers, connect, calls, server } = await setup(t, { minimum: 1 });
    managers[0].delete(connect());
    await advance(t, 60000);
    assert.equal(server.organizationWorkers.has('tenant'), false);
    connect(0, 'returned');
    assert.equal(server.organizationWorkers.has('tenant'), true);
    assert.equal(managers[0].organizations.get('tenant').isRegistered, true);
    assert.deepEqual(calls, []);
});

test('a remote Mesh departure removes peer presence without triggering local idle scale-in', async t => {
    const { mesh, calls } = await setup(t);
    const socket = { readyState: 1, serverId: 'platform', organization_id: 'tenant', send() {} };
    mesh.addOrganizationSocket('tenant', socket);
    mesh.removeSocketFromAllOrgServers(socket);
    await setImmediate();
    await advance(t, 1000);
    assert.deepEqual(calls, []);
    assert.equal(mesh.organizations.has('tenant'), false);
});

test('closing an organization uses socket closure without sending a removal message', async t => {
    const { mesh, server } = await setup(t, { minimum: 1 });
    const order = [];
    mesh.addOrganizationSocket('tenant', { readyState: 1,
        send: body => order.push(JSON.parse(body).method), close: () => order.push('close') });
    server.emit('orgDisconnected', { organization_id: 'tenant' });
    assert.deepEqual(order, ['close']);
});


test('Socket reports connected once per local cycle and IPC emits once across workers', async t => {
    const { server, managers, connect } = await setup(t, { minimum: 1, workers: 2 });
    const workerReports = [], transitions = [], localEvents = [];
    const onConnected = (organizationId, details) => {
        assert.equal(organizationId, 'tenant');
        const socketManager = managers.find(manager => manager.workerId === details.workerId);
        assert(socketManager.organizations.get(organizationId).clients.size > 0, 'local maps exist before notification');
        localEvents.push(details.workerId);
    };
    process.on('orgConnected', onConnected);
    t.after(() => process.removeListener('orgConnected', onConnected));
    const send = server.send;
    server.send = payload => {
        if (payload.method === 'orgConnected') workerReports.push(payload.workerId);
        return send(payload);
    };
    server.on('orgConnected', () => transitions.push('connected'));
    server.on('orgDisconnected', () => transitions.push('disconnected'));
    const first = connect(0, 'first');
    const second = connect(0, 'second');
    const third = connect(1, 'third');
    assert.deepEqual(workerReports, [], 'Socket Server does not directly send presence reports');
    assert.deepEqual(localEvents, [1, 2]);
    assert.deepEqual(transitions, ['connected']);
    managers[0].delete(first);
    managers[0].delete(second);
    await advance(t, 60000);
    assert.deepEqual(transitions, ['connected']);
    managers[1].delete(third);
    await advance(t, 60000);
    assert.deepEqual(transitions, ['connected', 'disconnected']);
    connect(0, 'returned');
    assert.deepEqual(workerReports, [], 'IPC consumes the process events independently');
    assert.deepEqual(localEvents, [1, 2, 1]);
    assert.deepEqual(transitions, ['connected', 'disconnected', 'connected']);
});


test('a worker reconnect during disconnect grace does not emit another local orgConnected', async t => {
    const { managers, connect } = await setup(t, { minimum: 1 });
    const events = [];
    const listener = id => events.push(id);
    process.on('orgConnected', listener);
    t.after(() => process.removeListener('orgConnected', listener));
    const socket = connect(0, 'first');
    managers[0].delete(socket);
    await advance(t, 30000);
    connect(0, 'returned');
    await advance(t, 60000);
    assert.deepEqual(events, ['tenant']);
});


test('forced tenant disconnection emits once and reaches IPC without Socket Server sending presence', async t => {
    const { server, managers } = await setup(t, { minimum: 1 });
    const transitions = [], local = [];
    const listener = (id, details) => { if (id === 'other') local.push(details); };
    process.on('orgDisconnected', listener);
    t.after(() => process.removeListener('orgDisconnected', listener));
    server.on('orgConnected', ({ organization_id }) => transitions.push(['connected', organization_id]));
    server.on('orgDisconnected', ({ organization_id }) => transitions.push(['disconnected', organization_id]));
    server.send = () => assert.fail('Socket Server must not send presence reports');
    const socket = Object.assign(new EventEmitter(), {
        id: 'other-socket', clientId: 'other-client', organization_id: 'other', host: 'other.test',
        readyState: 1, send() {}, terminate() {}
    });
    managers[0].add(socket);
    managers[0].destroyOrganizations('other');
    managers[0].destroyOrganizations('other');
    assert.deepEqual(transitions, [['connected', 'other'], ['disconnected', 'other']]);
    assert.equal(local.length, 1);
    assert.equal('retainResources' in local[0], false);
    assert.equal(server.organizationWorkers.has('other'), false);
    assert.equal(managers[0].organizations.has('other'), false);
});


test('forced own-organization cleanup uses the same disconnect event and reactivates on demand', async t => {
    const { server, managers, connect, calls } = await setup(t, { minimum: 1 });
    const transitions = [];
    server.on('orgConnected', () => transitions.push('connected'));
    server.on('orgDisconnected', () => transitions.push('disconnected'));
    const socket = connect();
    socket.readyState = 1;
    socket.send = () => {};
    socket.terminate = () => {};
    managers[0].destroyOrganizations('tenant');
    assert.equal(managers[0].organizations.has('tenant'), false);
    assert.equal(server.organizationWorkers.has('tenant'), false);
    connect(0, 'returned');
    assert.deepEqual(transitions, ['connected', 'disconnected', 'connected']);
    assert.deepEqual(calls, []);
});
