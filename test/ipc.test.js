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

test('organization messages route only to registered workers and exclude their origin', () => {
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
    assert.equal(received[0].payload.socketDelivery, true);
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
