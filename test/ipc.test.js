import assert from 'node:assert/strict';
import cluster from 'node:cluster';
import test from 'node:test';

import IPC from '../src/ipc.js';

function createWorker(id, received) {
    return {
        id,
        isListening: true,
        isCordoned: false,
        isDraining: false,
        process: {
            isConnected: () => true,
            send: payload => received.push({ id, payload: { ...payload } })
        }
    };
}

function isolateClusterListeners(t) {
    const previous = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    t.after(() => {
        for (const [event, listeners] of previous) {
            for (const listener of cluster.listeners(event)) {
                if (!listeners.includes(listener)) cluster.removeListener(event, listener);
            }
        }
    });
}

test('targeted shutdown reaches the remote mesh without shutting down the issuing primary', async t => {
    isolateClusterListeners(t);
    const forwarded = [], events = [];
    const server = {
        id: 'issuing-server', cluster_id: 'cluster-1', isPrimary: true,
        workers: new Map(),
        mesh: { send: payload => forwarded.push({ ...payload }) },
        emit: (...args) => events.push(args)
    };
    const send = IPC.init(server, new Map());
    await send({ method: 'shutdown', server: 'target-server', master: true, organization_id: 'tenant' });
    assert.equal(forwarded.length, 1);
    assert.equal(forwarded[0].server, 'target-server');
    assert.deepEqual(events, []);
});

test('targeted shutdown executes on the target primary', async t => {
    isolateClusterListeners(t);
    const events = [];
    const server = {
        id: 'target-server', cluster_id: 'cluster-1', isPrimary: true,
        workers: new Map(), emit: (...args) => events.push(args)
    };
    const send = IPC.init(server, new Map());
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
        mesh: { send: payload => received.push({ ...payload }) },
        emit: () => {}
    };
    const send = IPC.init(server, new Map());

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
    const send = IPC.init(server, pendingPromises);

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

test('forwarded messages do not create Promises and honor routing flags', () => {
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
    IPC.init(server, pendingPromises);

    cluster.emit('message', workers.get(1), {
        uuid: 'existing-uuid',
        method: 'example.changed',
        broadcast: true,
        broadcastSender: false,
        master: false
    });

    assert.equal(pendingPromises.size, 0);
    assert.equal(emitted.length, 0);
    assert.deepEqual(received.map(item => item.id), [2]);
    assert.equal(received[0].payload.workerId, 1);
    assert.equal(received[0].payload.serverId, 'server-1');
});

test('broadcastSender false resolves the origin immediately', async () => {
    const received = [];
    const pendingPromises = new Map();
    const workers = new Map([[1, createWorker(1, received)]]);
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers,
        emit: () => {
            throw new Error('originating primary should be excluded');
        }
    };
    const send = IPC.init(server, pendingPromises);
    const payload = {
        method: 'example.fire-and-forget',
        broadcast: true,
        broadcastSender: false
    };

    const result = await send(payload);

    assert.equal(result, payload);
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
    const send = IPC.init(server, pendingPromises);
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
    assert.equal(received.length, 1);
    assert.equal(received[0].id, 2);
    assert.equal(received[0].payload.response, true);
    assert.equal(received[0].payload.master, false);
});

test('server and cluster targets are handed to mesh.send by the primary', async () => {
    const meshMessages = [];
    const server = {
        id: 'server-1',
        isPrimary: true,
        workers: new Map(),
        mesh: {
            send: payload => meshMessages.push(payload)
        },
        emit: () => {}
    };
    const send = IPC.init(server, new Map());

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
        mesh: {
            send: payload => meshMessages.push(payload)
        },
        emit: () => {}
    };
    IPC.init(server, new Map());

    cluster.emit('message', sourceWorker, {
        uuid: 'worker-originated-uuid',
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
    isolateClusterListeners(t);
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
        mesh: {
            send: payload => meshMessages.push({ ...payload }),
            openOrganization: organizationId => openedOrganizations.push(organizationId),
            closeOrganization: organizationId => closedOrganizations.push(organizationId)
        },
        emit: () => {}
    };
    IPC.init(server, new Map());

    cluster.emit('message', workers.get(1), {
        method: 'registerOrg',
        organization_id: 'organization-1'
    });
    cluster.emit('message', workers.get(2), {
        method: 'registerOrg',
        organization_id: 'organization-1'
    });
    cluster.emit('message', workers.get(1), {
        uuid: 'organization-message',
        method: 'crud',
        organization_id: 'organization-1',
        broadcast: true
    });

    assert.deepEqual([...server.organizationWorkers.get('organization-1')], [1, 2]);
    assert.deepEqual(received.map(item => item.id), [2]);
    const deliveries = [];
    const previousListeners = new Set(process.listeners('message'));
    IPC.init({
        isPrimary: false,
        wsManager: { send: payload => deliveries.push(payload) },
        emit() { assert.fail('Organization delivery must not execute the operation again'); }
    }, new Map());
    const workerListener = process.listeners('message').find(listener => !previousListeners.has(listener));
    t.after(() => process.removeListener('message', workerListener));
    await workerListener(received[0].payload);
    assert.deepEqual(deliveries, [{ ...received[0].payload, serverSent: true }]);
    assert.deepEqual(openedOrganizations, ['organization-1']);
    assert.equal(meshMessages.filter(item => item.uuid === 'organization-message').length, 1);

    cluster.emit('message', workers.get(1), {
        method: 'deregisterOrg',
        organization_id: 'organization-1'
    });
    assert.deepEqual(closedOrganizations, []);

    cluster.emit('message', workers.get(2), {
        method: 'deregisterOrg',
        organization_id: 'organization-1'
    });
    assert.deepEqual(closedOrganizations, ['organization-1']);
});
