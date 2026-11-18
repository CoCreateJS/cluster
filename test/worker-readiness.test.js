import assert from 'node:assert/strict';
import test from 'node:test';
import cluster from 'node:cluster';
import { EventEmitter } from 'node:events';
import Config from '@cocreate/config';
import IPC from '../src/send/ipc.js';
import State from '../src/state.js';
import { encode, decode } from '@cocreate/coson';

test('worker readiness travels through IPC and is applied by State', async t => {
    const clusterListeners = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    const processListeners = new Map(['SIGINT', 'SIGTERM', 'SIGBREAK'].map(event => [event, process.listeners(event)]));
    t.mock.method(Config, 'get', async () => undefined);
    t.after(() => {
        for (const [event, listeners] of clusterListeners) {
            for (const listener of cluster.listeners(event)) if (!listeners.includes(listener)) cluster.off(event, listener);
        }
        for (const [event, listeners] of processListeners) {
            for (const listener of process.listeners(event)) if (!listeners.includes(listener)) process.off(event, listener);
        }
    });

    const effects = [];
    const record = { id: 2, isListening: false };
    const workers = new Map([[2, record]]);
    workers.setListening = (workerId, state) => {
        const worker = workers.get(workerId);
        if (worker) worker.isListening = Boolean(state);
        return worker;
    };
    const server = new EventEmitter();
    Object.assign(server, {
        id: 'node-1', cluster_id: 'cluster-1', isPrimary: true,
        workers,
        repository: { publish: async () => effects.push('published') }
    });
    server.emitAsync = async (event, payload) => {
        const replies = await Promise.all(server.rawListeners(event).map(listener => listener.call(server, payload)));
        return replies.find(reply => reply?.uuid === payload.uuid) || payload;
    };
    server.state = await State.init(server);
    server.send = IPC.init(server, new Map());

    const responses = [];
    const worker = { id: 19, process: { id: 19, send: bytes => responses.push(decode(bytes, { operators: false })) } };
    cluster.emit('message', worker, encode({
        method: 'worker.listening', state: true, workerId: 2,
        serverId: 'node-1', uuid: 'readiness-1'
    }, { operators: false }));

    for (let i = 0; i < 10 && !responses.length; i++) await new Promise(resolve => setImmediate(resolve));
    assert.equal(record.isListening, true);
    assert.deepEqual(effects, ['published']);
    assert.equal(responses[0]?.response, true);
    assert.equal(responses[0]?.uuid, 'readiness-1');
});
