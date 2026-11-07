import assert from 'node:assert/strict';
import cluster from 'node:cluster';
import test from 'node:test';

import IPC from '../src/ipc.js';

function createWorker(id, received) {
    const worker = {
        id, isListening: true, isCordoned: false, isDraining: false, responses: [],
        process: {
            isConnected: () => true,
            send(payload) {
                if (payload.response) { worker.responses.push(payload); return; }
                received.push({ id, payload: { ...payload } });
                queueMicrotask(() => cluster.emit('message', worker, { ...payload, response: true }));
            }
        }
    };
    return worker;
}

// Minimal runtime surface: production CoCreateServer supplies these methods.
function initIPC(server, pending) {
    server.cluster ||= { send() {} };
    server.emitAsync ||= async (method, payload) => { server.emit(method, payload); return payload; };
    return IPC.init(server, pending);
}

function isolateClusterListeners(t) {
    const processPrevious = new Map(['message', 'orgConnected', 'orgDisconnected'].map(event => [event, process.listeners(event)]));
    const previous = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    t.after(() => {
        for (const [event, listeners] of processPrevious) {
            for (const listener of process.listeners(event)) if (!listeners.includes(listener)) process.removeListener(event, listener);
        }
        for (const [event, listeners] of previous) {
            for (const listener of cluster.listeners(event)) {
                if (!listeners.includes(listener)) cluster.removeListener(event, listener);
            }
        }
    });
}

test.beforeEach(t => {
    isolateClusterListeners(t);
    const descriptor = Object.getOwnPropertyDescriptor(process, 'send');
    Object.defineProperty(process, 'send', { configurable: true, value() {} });
    t.after(() => {
        if (descriptor) Object.defineProperty(process, 'send', descriptor);
        else delete process.send;
    });
});

test('targeted shutdown reaches the remote mesh without shutting down the issuing primary', async t => {
    const forwarded = [], events = [];
    const server = {
        id: 'issuing-server', cluster_id: 'cluster-1', isPrimary: true,
        workers: new Map(),
        cluster: { send: payload => forwarded.push({ ...payload }) },
        emit: (...args) => events.push(args)
    };
    const send = initIPC(server, new Map());
    await send({ method: 'shutdown', server: 'target-server', master: true, organization_id: 'tenant' });
    assert.equal(forwarded.length, 1);
    assert.equal(forwarded[0].server, 'target-server');
    assert.deepEqual(events, []);
});

test('targeted shutdown executes on the target primary', async t => {
    const events = [];
    const server = {
        id: 'target-server', cluster_id: 'cluster-1', isPrimary: true,
        workers: new Map(), emit: (...args) => events.push(args)
    };
    const send = initIPC(server, new Map());
    await send({ method: 'shutdown', server: 'target-server', master: true, organization_id: 'tenant', serverId: 'issuing-server' });
    assert.equal(events.length, 1);
    assert.equal(events[0][0], 'shutdown');
});

test('a primary serving as a worker stamps its origin before mesh delivery', async () => {
    const received = [];
    const server = {
        id: 'server-1',
        isPrimary: true,
        isWorker: true,
        workerId: 1,
        workers: new Map(),
        cluster: { send: payload => received.push({ ...payload }) },
        emit: () => {}
    };
    const send = initIPC(server, new Map());

    await send({ method: 'example.changed', organization_id: 'org-1' });
    assert.equal(received.length, 1);
    assert.equal(received[0].workerId, 1);
    assert.equal(received[0].serverId, 'server-1');

    const remote = await send({
        method: 'example.changed',
        organization_id: 'org-1',
        serverId: 'server-2',
        workerId: 7
    });
    assert.equal(remote.workerId, 7);
    assert.equal(remote.serverId, 'server-2');
});

test('server.send stores a Promise only at the UUID origin', async () => {
    const received = [];
    const emitted = [];
    const pendingPromises = new Map();
    const workers = new Map([
        [1, createWorker(1, received)],
        [2, createWorker(2, received)]
    ]);
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers,
        emit: (method, payload) => emitted.push({ method, payload })
    };
    const send = initIPC(server, pendingPromises);

    const responsePromise = send({
        method: 'example.get',
        worker: 2,
        master: false,
        broadcast: false,
        broadcastSender: true
    });

    assert.equal(pendingPromises.size, 1);
    assert.equal(received.length, 1);
    assert.equal(received[0].id, 2);
    assert.equal(emitted.length, 0);

    const request = received[0].payload;
    cluster.emit('message', workers.get(2), {
        ...request,
        response: true,
        success: true,
        result: 'value'
    });

    const response = await responsePromise;
    assert.equal(response.uuid, request.uuid);
    assert.equal(response.result, 'value');
    assert.equal(pendingPromises.size, 0);
    assert.equal(emitted.length, 0);
});

test('forwarded broadcasts await all workers and ignore broadcastSender', async () => {
    const received = [];
    const emitted = [];
    const pendingPromises = new Map();
    const workers = new Map([
        [1, createWorker(1, received)],
        [2, createWorker(2, received)]
    ]);
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers,
        emit: (method, payload) => emitted.push({ method, payload })
    };
    initIPC(server, pendingPromises);

    cluster.emit('message', workers.get(1), {
        uuid: 'existing-uuid', serverId: 'server-1', workerId: 1,
        method: 'example.changed',
        broadcast: true,
        broadcastSender: false,
        master: false
    });

    assert.equal(pendingPromises.size, 2);
    assert.equal(emitted.length, 0);
    assert.deepEqual(received.map(item => item.id), [1, 2]);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(pendingPromises.size, 0);
    assert.equal(workers.get(1).responses.length, 1);
    assert.equal(received[0].payload.workerId, 1);
    assert.equal(received[0].payload.serverId, 'server-1');
});

test('broadcastSender false still returns the completed worker envelope', async () => {
    const received = [];
    const pendingPromises = new Map();
    const workers = new Map([[1, createWorker(1, received)]]);
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers,
        emit() {}
    };
    const send = initIPC(server, pendingPromises);
    const payload = {
        method: 'example.fire-and-forget',
        broadcast: true,
        broadcastSender: false
    };

    const result = await send(payload);

    assert.equal(result.uuid, payload.uuid);
    assert.equal(result.response, true);
    assert.equal(typeof payload.uuid, 'string');
    assert.equal(pendingPromises.size, 0);
    assert.equal(received.length, 1);
});

test('sending an existing UUID routes the response to its source worker', async () => {
    const received = [];
    const pendingPromises = new Map();
    const workers = new Map([
        [1, createWorker(1, received)],
        [2, createWorker(2, received)]
    ]);
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers,
        emit: () => {
            throw new Error('a response must not execute again on the primary');
        }
    };
    const send = initIPC(server, pendingPromises);
    const response = {
        uuid: 'request-uuid',
        method: 'storage.get',
        serverId: 'server-1',
        workerId: 2,
        response: true,
        broadcastSender: true,
        success: true,
        result: 'stored-value'
    };

    await send(response);

    assert.equal(pendingPromises.size, 0);
    assert.equal(received.length, 0);
    assert.equal(workers.get(2).responses.length, 1);
    assert.equal(workers.get(2).responses[0].response, true);
});

test('server and cluster targets are handed to cluster.send by the primary', async () => {
    const meshMessages = [];
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers: new Map(),
        cluster: {
            send: payload => meshMessages.push(payload)
        },
        emit: () => {}
    };
    const send = initIPC(server, new Map());

    await send({
        method: 'configuration.refresh',
        server: ['server-2', 'server-3'],
        broadcastSender: false
    });
    await send({
        method: 'configuration.refresh',
        cluster: 'cluster-2',
        broadcastSender: false
    });

    assert.equal(meshMessages.length, 2);
    assert.deepEqual(meshMessages[0].server, ['server-2', 'server-3']);
    assert.equal(meshMessages[1].cluster, 'cluster-2');
});

test('worker-originated server targets are forwarded into the mesh', () => {
    const meshMessages = [];
    const sourceWorker = createWorker(1, []);
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers: new Map([[1, sourceWorker]]),
        cluster: {
            send: payload => meshMessages.push(payload)
        },
        emit: () => {}
    };
    initIPC(server, new Map());

    cluster.emit('message', sourceWorker, {
        uuid: 'worker-originated-uuid', serverId: 'server-1', workerId: 1,
        method: 'configuration.refresh',
        server: 'server-2',
        master: false,
        broadcastSender: false
    });

    assert.equal(meshMessages.length, 1);
    assert.equal(meshMessages[0].server, 'server-2');
    assert.equal(meshMessages[0].workerId, 1);
    assert.equal(meshMessages[0].serverId, 'server-1');
});

test('organization messages route only to registered workers and exclude their origin', async t => {
    const received = [];
    const meshMessages = [];
    const openedOrganizations = [];
    const closedOrganizations = [];
    const workers = new Map([
        [1, createWorker(1, received)],
        [2, createWorker(2, received)],
        [3, createWorker(3, received)]
    ]);
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers,
        cluster: {
            send: payload => meshMessages.push({ ...payload }),
            openOrganization: () => assert.fail('IPC must notify listeners, not call Mesh directly'),
            closeOrganization: () => assert.fail('IPC must notify listeners, not call Mesh directly')
        },
        emit: (event, payload) => {
            if (event === 'orgConnected') openedOrganizations.push(payload.organization_id);
            if (event === 'orgDisconnected') closedOrganizations.push(payload.organization_id);
        }
    };
    initIPC(server, new Map());

    cluster.emit('message', workers.get(1), {
        method: 'orgConnected', workerId: 1,
        organization_id: 'organization-1'
    });
    cluster.emit('message', workers.get(2), {
        method: 'orgConnected', workerId: 2,
        organization_id: 'organization-1'
    });
    cluster.emit('message', workers.get(1), {
        uuid: 'organization-message', serverId: 'server-1', workerId: 1,
        method: 'crud',
        organization_id: 'organization-1',
        broadcast: true
    });

    assert.deepEqual([...server.organizationWorkers.get('organization-1')], [1, 2]);
    assert.deepEqual(received.map(item => item.id), [2]);
    const deliveries = [];
    const previousListeners = new Set(process.listeners('message'));
    initIPC({
        isPrimary: false,
        socket: { send: payload => deliveries.push(payload) },
        emit() { assert.fail('Organization delivery must not execute the operation again'); }
    }, new Map());
    const workerListener = process.listeners('message').find(listener => !previousListeners.has(listener));
    t.after(() => process.removeListener('message', workerListener));
    await workerListener(received[0].payload);
    assert.deepEqual(deliveries, [received[0].payload]);
    assert.deepEqual(openedOrganizations, ['organization-1']);
    assert.equal(meshMessages.filter(item => item.uuid === 'organization-message').length, 1);

    cluster.emit('message', workers.get(1), {
        method: 'orgDisconnected', workerId: 1,
        organization_id: 'organization-1'
    });
    assert.deepEqual(closedOrganizations, []);

    cluster.emit('message', workers.get(2), {
        method: 'orgDisconnected', workerId: 2,
        organization_id: 'organization-1'
    });
    assert.deepEqual(closedOrganizations, ['organization-1']);
    cluster.emit('message', workers.get(2), {
        method: 'orgDisconnected', organization_id: 'organization-1'
    });
    assert.deepEqual(closedOrganizations, ['organization-1'], 'duplicate reports do not emit another transition');
});


test('last worker exit emits orgDisconnected locally even without Mesh installed', t => {
    const events = [];
    const server = { id: 'server-1', isPrimary: true, workers: new Map(),
        emit: (event, payload) => events.push([event, payload]) };
    const send = initIPC(server, new Map());
    send({ method: 'orgConnected', organization_id: 'tenant', workerId: 1 });
    send({ method: 'orgConnected', organization_id: 'tenant', workerId: 2 });
    assert.deepEqual(events, [['orgConnected', { organization_id: 'tenant' }]]);
    events.length = 0;
    cluster.emit('exit', { id: 1 });
    assert.deepEqual(events, []);
    cluster.emit('exit', { id: 2 });
    assert.deepEqual(events, [['orgDisconnected', { organization_id: 'tenant' }]]);
    assert.equal(server.organizationWorkers.has('tenant'), false);
    cluster.emit('exit', { id: 2 });
    assert.equal(events.length, 1);
});


test('connected reports emit once per organization until all worker registrations leave', t => {
    const events = [];
    const server = { id: 'node', isPrimary: true, workers: new Map(),
        emit: (event, payload) => events.push([event, payload.organization_id]) };
    const send = initIPC(server, new Map());
    for (const workerId of [1, 1, 2, 3]) send({ method: 'orgConnected', organization_id: 'tenant', workerId });
    send({ method: 'orgConnected', organization_id: 'another', workerId: 1 });
    assert.deepEqual(events, [['orgConnected', 'tenant'], ['orgConnected', 'another']]);
    for (const workerId of [1, 2]) send({ method: 'orgDisconnected', organization_id: 'tenant', workerId });
    assert.equal(events.length, 2);
    send({ method: 'orgDisconnected', organization_id: 'tenant', workerId: 3 });
    send({ method: 'orgConnected', organization_id: 'tenant', workerId: 2 });
    assert.deepEqual(events.slice(2), [['orgDisconnected', 'tenant'], ['orgConnected', 'tenant']]);
    assert.deepEqual([...server.organizationWorkers.get('another')], [1]);
});


test('worker IPC forwards local presence events once without reflecting server events', t => {
    const sent = [];
    const descriptors = new Map(['send', 'connected'].map(key => [key, Object.getOwnPropertyDescriptor(process, key)]));
    t.after(() => {
        for (const [key, descriptor] of descriptors) {
            if (descriptor) Object.defineProperty(process, key, descriptor);
            else delete process[key];
        }
    });
    Object.defineProperty(process, 'send', { configurable: true, value: payload => sent.push(payload) });
    Object.defineProperty(process, 'connected', { configurable: true, value: true });
    const server = { isPrimary: false, isWorker: true, workerId: 3, emit() {} };
    server.send = initIPC(server, new Map());
    process.emit('orgConnected', 'tenant', { workerId: 3 });
    process.emit('orgDisconnected', 'tenant', { workerId: 3 });
    assert.deepEqual(sent.map(({ method, organization_id, workerId }) => ({ method, organization_id, workerId })), [
        { method: 'orgConnected', organization_id: 'tenant', workerId: 3 },
        { method: 'orgDisconnected', organization_id: 'tenant', workerId: 3 }
    ]);
    assert.equal(sent.length, 2);
});

test('a primary serving clients aggregates local process events without sending or looping', t => {
    const events = [];
    const server = { isPrimary: true, isWorker: true, workerId: 1, workers: new Map(),
        emit: (event, payload) => events.push([event, payload.organization_id]) };
    server.send = () => assert.fail('Primary presence must aggregate directly');
    initIPC(server, new Map());
    process.emit('orgConnected', 'tenant', { workerId: 1 });
    process.emit('orgConnected', 'tenant', { workerId: 2 });
    process.emit('orgDisconnected', 'tenant', { workerId: 1 });
    assert.deepEqual(events, [['orgConnected', 'tenant']]);
    process.emit('orgDisconnected', 'tenant', { workerId: 2 });
    assert.deepEqual(events, [['orgConnected', 'tenant'], ['orgDisconnected', 'tenant']]);
});

// A blocked local socket must not hold up other workers or network routing.
test('send starts local, worker and cluster delivery independently and awaits local completion', async () => {
    const received = [], forwarded = [], local = [];
    const pending = new Map();
    let finishLocal;
    const localCompletion = new Promise(resolve => { finishLocal = resolve; });
    const server = {
        id: 'node', cluster_id: 'cluster', isPrimary: true, isWorker: true, workerId: 1,
        workers: new Map([[1, createWorker(1, received)], [2, createWorker(2, received)]]),
        organizationWorkers: new Map([['org', new Set([1, 2])]]),
        socket: { send: data => { local.push(data); return localCompletion; } },
        cluster: { send: data => forwarded.push(data) }, emit() {}
    };
    server.send = initIPC(server, pending);
    const socket = { purpose: 'ordinary payload' };
    const payload = { method: 'object.read', organization_id: 'org', clientId: 'client', socket,
        broadcastSender: true };
    let complete = false;
    const result = server.send(payload).then(value => { complete = true; return value; });
    assert.equal(forwarded.length, 1);
    assert.deepEqual(received.map(item => item.id), [2]);
    assert.equal(forwarded[0].socket, socket);
    assert.equal(received[0].payload.socket, socket);
    assert.doesNotThrow(() => JSON.stringify(forwarded[0]));
    assert.equal(pending.size, 1, 'The worker receipt uses the same correlation path as other requests');
    await Promise.resolve();
    assert.equal(local.length, 1);
    assert.equal(local[0].socket, socket);
    assert.equal(complete, false);
    assert.equal(pending.size, 0);
    finishLocal();
    assert.equal((await result).uuid, payload.uuid);
});

test('destination server, cluster and worker filters apply before local client delivery', async () => {
    const local = [], received = [];
    const server = { id: 'node', cluster_id: 'cluster', isPrimary: true, isWorker: true, workerId: 1,
        workers: new Map([[2, createWorker(2, received)]]),
        organizationWorkers: new Map([['org', new Set([2])]]),
        socket: { send: data => local.push(data) }, cluster: { send() {} }, emit() {} };
    const send = initIPC(server, new Map());
    for (const target of [{ server: 'other' }, { cluster: 'other' }, { worker: 2 }]) {
        await send({ method: 'object.read', organization_id: 'org', ...target });
    }
    assert.equal(local.length, 0);
    assert.equal(received.length, 1);
    assert.equal(received[0].id, 2);
});

test('local socket failure does not prevent forwarding and rejects the caller await', async () => {
    const received = [], forwarded = [];
    const server = { id: 'node', isPrimary: true, workers: new Map([[2, createWorker(2, received)]]),
        organizationWorkers: new Map([['org', new Set([2])]]),
        socket: { send() { throw new Error('local delivery failed'); } },
        cluster: { send: data => forwarded.push(data) }, emit() {} };
    const send = initIPC(server, new Map());
    await assert.rejects(send({ method: 'object.read', organization_id: 'org' }), /local delivery failed/);
    assert.equal(received.length, 1);
    assert.equal(forwarded.length, 1);
});

test('remote worker IDs do not exclude a same-numbered local worker', async () => {
    const received = [];
    const server = { id: 'node', isPrimary: true, workers: new Map([[1, createWorker(1, received)]]),
        organizationWorkers: new Map([['org', new Set([1])]]), emit() {} };
    await initIPC(server, new Map())({ method: 'object.read', organization_id: 'org',
        serverId: 'remote-node', workerId: 1, broadcastSender: false });
    assert.deepEqual(received.map(item => item.id), [1]);
});

test('a worker sends locally and upstream once; receiving another worker delivery stays local', async t => {
    const upstream = [], local = [], events = [];
    const descriptors = new Map(['send', 'connected'].map(key => [key, Object.getOwnPropertyDescriptor(process, key)]));
    t.after(() => {
        for (const [key, descriptor] of descriptors) {
            if (descriptor) Object.defineProperty(process, key, descriptor);
            else delete process[key];
        }
    });
    Object.defineProperty(process, 'send', { configurable: true, value: data => {
        upstream.push(data);
        if (!data.response) queueMicrotask(() => process.emit('message', { ...data, response: true }));
    } });
    Object.defineProperty(process, 'connected', { configurable: true, value: true });
    const previous = new Set(process.listeners('message'));
    const pending = new Map();
    const server = { id: 'node', isPrimary: false, workerId: 1,
        socket: { send: data => local.push(data) }, emit: (...args) => events.push(args) };
    server.send = initIPC(server, pending);
    const socket = { purpose: 'ordinary payload' };
    await server.send({ method: 'object.read', organization_id: 'org', socket, broadcastSender: true });
    assert.equal(local.length, 1);
    assert.equal(upstream.length, 1);
    assert.equal(upstream[0].serverId, 'node');
    assert.equal(upstream[0].workerId, 1);
    assert.equal(upstream[0].socket, socket);
    assert.equal(pending.size, 0);
    const receive = process.listeners('message').find(listener => !previous.has(listener));
    await receive({ method: 'object.read', organization_id: 'org', serverId: 'remote', workerId: 2 });
    assert.equal(local.length, 2);
    assert.equal(upstream.filter(data => !data.response).length, 1, 'Incoming delivery only sends its terminal reply');
    await receive({ method: 'object.read', organization_id: 'org', worker: 2 });
    assert.equal(local.length, 2, 'A delivery targeted to another worker must not reach this worker');
    assert.equal(events.length, 0);
});

test('a socket payload field cannot supply organization scope for routing', async () => {
    const local = [], forwarded = [];
    const server = { id: 'node', isPrimary: true, workers: new Map(),
        socket: { send: data => local.push(data) }, cluster: { send: data => forwarded.push(data) }, emit() {} };
    const payload = { method: 'workflow.complete', socket: { organization_id: 'org' }, broadcastSender: true };
    const pending = new Map();
    await initIPC(server, pending)(payload);
    assert.equal(payload.organization_id, undefined);
    assert.equal(local.length, 0);
    assert.equal(forwarded.length, 0);
    assert.equal(pending.size, 0);
});

test('local eviction cannot remove peer and worker routes before their sends are dispatched', async () => {
    const received = [], forwarded = [];
    const peers = new Set(['remote']);
    const server = { id: 'node', isPrimary: true, workers: new Map([[2, createWorker(2, received)]]),
        organizationWorkers: new Map([['org', new Set([2])]]),
        socket: { send() { peers.clear(); server.organizationWorkers.clear(); } },
        cluster: { send() { forwarded.push([...peers]); } }, emit() {} };
    await initIPC(server, new Map())({ method: 'organization.evicted', organization_id: 'org' });
    assert.deepEqual(forwarded, [['remote']]);
    assert.equal(received.length, 1);
    assert.equal(peers.size, 0);
});

test('worker exit rejects its unfinished request and clears correlation', async () => {
    const worker = { id: 2, process: { send() {} } };
    const pending = new Map();
    const server = { id: 'node', isPrimary: true, workers: new Map([[2, worker]]), emit() {} };
    const send = initIPC(server, pending);
    const failed = assert.rejects(send({ method: 'example.work', worker: 2, master: false }), /exited before replying/);
    assert.equal(pending.size, 1);
    cluster.emit('exit', worker);
    await failed;
    assert.equal(pending.size, 0);
});

test('unmatched responses never execute a method or generate another response', async () => {
    const received = [];
    const server = { id: 'node', isPrimary: true, workers: new Map(),
        emit() { assert.fail('An unmatched response must not execute'); } };
    initIPC(server, new Map());
    const worker = createWorker(1, received);
    cluster.emit('message', worker, { method: 'example.work', uuid: 'finished', response: true });
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(received, []);
    assert.deepEqual(worker.responses, []);
});

test('request correlation uses native worker IDs while presence uses the logical slot', async () => {
    const pending = new Map();
    const process = { id: 41, send(payload) {
        queueMicrotask(() => cluster.emit('message', process, { ...payload, response: true, result: 'ok' }));
    } };
    const server = { id: 'node', isPrimary: true,
        workers: new Map([[2, { id: 2, process }]]),
        organizationWorkers: new Map([['org', new Set([2])]]), emit() {} };
    const send = initIPC(server, pending);
    assert.equal((await send({ method: 'example.work', worker: 2, master: false })).result, 'ok');
    assert.equal(pending.size, 0);
    process.send = () => {};
    const failed = assert.rejects(send({ method: 'example.work', worker: 2, master: false }), /exited before replying/);
    cluster.emit('exit', process);
    await failed;
    assert.equal(pending.size, 0);
    assert.equal(server.organizationWorkers.has('org'), false);
});

test('a local send fills empty origin fields without changing destination fields', async () => {
    const forwarded = [];
    const server = { id: 'node', workerId: 1, cluster_id: 'origin-cluster', isPrimary: true, isWorker: true,
        workers: new Map(), cluster: { send: payload => forwarded.push(payload) }, emit() {} };
    const payload = { method: 'object.read', organization_id: 'org', uuid: 'existing-uuid',
        serverId: '', workerId: null, clusterId: '',
        server: 'destination-node', cluster: 'destination-cluster', worker: 2 };
    await initIPC(server, new Map())(payload);
    assert.equal(payload.uuid, 'existing-uuid');
    assert.equal(payload.serverId, 'node');
    assert.equal(payload.workerId, 1);
    assert.equal(payload.clusterId, 'origin-cluster');
    assert.equal(forwarded[0].clusterId, 'origin-cluster');
    assert.equal(payload.server, 'destination-node');
    assert.equal(payload.cluster, 'destination-cluster');
    assert.equal(payload.worker, 2);
});

test('unknown cluster context stays absent and a known local origin can fill its own missing fields', async () => {
    const server = { id: 'node', workerId: 1, isPrimary: true, workers: new Map(), emit() {} };
    const send = initIPC(server, new Map());
    const initial = await send({ method: 'example.local', serverId: 'node', workerId: 1 });
    assert.equal(initial.workerId, 1);
    assert.equal('clusterId' in initial, false);
    server.cluster_id = 'known-cluster';
    const existing = await send({ method: 'example.local', serverId: 'node', workerId: 1, clusterId: 'known-cluster' });
    assert.equal(existing.serverId, 'node');
    assert.equal(existing.workerId, 1);
    assert.equal(existing.clusterId, 'known-cluster');
});

for (const origin of [
    { serverId: 'remote-node', workerId: 4, clusterId: 'remote-cluster' },
    { serverId: 'remote-node' },
    { serverId: 'node', workerId: 4 },
    { serverId: 'node' },
    { serverId: 'node', clusterId: 'local-cluster' },
    { clusterId: 'remote-cluster' },
    { serverId: 'node', workerId: 0 }
]) {
    test(`relayed origin remains unchanged: ${JSON.stringify(origin)}`, async () => {
        const server = { id: 'node', workerId: 1, cluster_id: 'local-cluster', isPrimary: true,
            workers: new Map(), emit() {} };
        const payload = { method: 'object.read', organization_id: 'org', ...origin };
        await initIPC(server, new Map())(payload);
        for (const field of ['serverId', 'workerId', 'clusterId']) {
            assert.equal(payload[field], origin[field]);
            assert.equal(field in payload, field in origin);
        }
    });
}

test('receiving IPC traffic does not stamp the receiving node or worker as its origin', async () => {
    const server = { id: 'relay-node', workerId: 1, cluster_id: 'relay-cluster', isPrimary: true,
        workers: new Map(), emit() {} };
    initIPC(server, new Map());
    const source = createWorker(7, []);
    const receive = cluster.listeners('message').at(-1);
    await receive(source, { method: 'object.read', organization_id: 'org', uuid: 'relayed', serverId: 'original-node' });
    const reply = source.responses[0];
    assert.equal(reply.serverId, 'original-node');
    assert.equal('workerId' in reply, false);
    assert.equal('clusterId' in reply, false);
});
