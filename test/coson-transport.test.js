import assert from 'node:assert/strict';
import test from 'node:test';
import cluster from 'node:cluster';
import { EventEmitter } from 'node:events';
import { SegmentReceiver } from '@cocreate/utils/segments';
import { encode, decode } from '@cocreate/coson';
import IPC from '../src/send/ipc.js';
import { ServerMesh } from '../src/send/socket/index.js';

const frame = value => encode(value, { operators: false });
const prepared = value => decode(value, { operators: false });

test('Socket to IPC and IPC to Socket preserve CoSON operators and binary bytes', async t => {
    const clusterListeners = new Set(cluster.listeners('message'));
    const processEvents = ['message', 'orgConnected', 'orgDisconnected'];
    const processListeners = new Map(processEvents.map(event => [event, process.listeners(event)]));
    t.after(() => {
        for (const listener of cluster.listeners('message')) if (!clusterListeners.has(listener)) cluster.off('message', listener);
        for (const [event, listeners] of processListeners) {
            for (const listener of process.listeners(event)) if (!listeners.includes(listener)) process.off(event, listener);
        }
    });

    const receivedByWorker = [];
    const worker = { id: 2, isListening: true };
    worker.process = {
        id: 902,
        send(bytes) {
            const message = prepared(bytes);
            receivedByWorker.push(message);
            queueMicrotask(() => cluster.emit('message', worker, frame({ ...message, response: true })));
        }
    };
    const server = new EventEmitter();
    Object.assign(server, {
        id: 'local-node', cluster_id: 'cluster-a', isPrimary: true,
        workers: new Map([[worker.id, worker]]),
        organizationWorkers: new Map([['tenant-a', new Set([worker.id])]]),
        socket: { send: async payload => payload }
    });
    server.emitAsync = async (event, payload) => {
        const replies = await Promise.all(server.rawListeners(event).map(listener => listener.call(server, payload)));
        return replies.find(reply => reply?.uuid === payload.uuid) || payload;
    };
    server.send = IPC.init(server, new Map());

    const mesh = new ServerMesh();
    mesh.server = server;
    server.cluster = mesh;
    const receiver = new SegmentReceiver();
    const peerFrames = [];
    const peer = {
        serverId: 'peer-node', organization_id: 'tenant-a', readyState: 1,
        send(bytes) {
            const complete = receiver.push(bytes);
            if (!complete) return;
            try { peerFrames.push(prepared(complete)); }
            finally { receiver.release(complete); }
        }
    };
    mesh.organizations.set('tenant-a', [peer]);

    const socketMessage = {
        method: 'object.update', organization_id: 'tenant-a',
        serverId: 'remote-origin', workerId: 8, uuid: 'socket-to-ipc',
        object: {
            created: { $date: 1700000000123 }, label: '東京 🗼', data: 'binary[0]',
            binary: [{ $Uint8Array: new Uint8Array(96 * 1024).fill(13) }]
        }
    };
    mesh.handleIncomingMessage(frame(socketMessage), { serverId: 'remote-origin' });
    for (let i = 0; i < 20 && !receivedByWorker.length; i++) await new Promise(resolve => setImmediate(resolve));
    assert.equal(receivedByWorker[0]?.object.created.$date, 1700000000123);
    assert.equal(receivedByWorker[0]?.object.label, '東京 🗼');
    assert.equal(receivedByWorker[0]?.object.data, 'binary[0]');
    assert.deepEqual([...receivedByWorker[0].object.binary[0].$Uint8Array], new Array(96 * 1024).fill(13));

    const ipcMessage = {
        method: 'cache.refresh', server: 'peer-node', organization_id: 'tenant-a',
        serverId: 'local-node', workerId: 1, uuid: 'ipc-to-socket',
        created: { $date: 1700000000456 }, label: '日本語 🌱', data: 'binary[0]',
        binary: [{ $Uint8Array: new Uint8Array([2, 7, 1, 8]) }]
    };
    await server.send(ipcMessage);
    for (let i = 0; i < 20 && !peerFrames.length; i++) await new Promise(resolve => setImmediate(resolve));
    assert.equal(peerFrames[0]?.created.$date, 1700000000456);
    assert.equal(peerFrames[0]?.label, '日本語 🌱');
    assert.equal(peerFrames[0]?.data, 'binary[0]');
    assert.deepEqual([...peerFrames[0].binary[0].$Uint8Array], [2, 7, 1, 8]);
});
