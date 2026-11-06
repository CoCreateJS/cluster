import test from 'node:test';
import assert from 'node:assert/strict';
import cluster from 'node:cluster';
import { EventEmitter, once } from 'node:events';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import IPC from '../src/ipc.js';
import Storage from '../src/storage.js';

const eventAPI = `
    server.emitAsync = async (event, payload) => {
        const replies = await Promise.all(server.rawListeners(event).map(listener => listener.call(server, payload)));
        return replies.find(reply => reply?.uuid === payload.uuid) || payload;
    };
`;

test('real workers reply consistently with async results, errors and ignored return values', { timeout: 10000 }, async t => {
    const previous = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    const processPrevious = new Map(['orgConnected', 'orgDisconnected'].map(event => [event, process.listeners(event)]));
    const directory = await mkdtemp(join(tmpdir(), 'coc-ipc-roundtrip-'));
    const filename = join(directory, 'worker.mjs');
    await writeFile(filename, `
        import cluster from 'node:cluster';
        import { EventEmitter } from 'node:events';
        import IPC from ${JSON.stringify(new URL('../src/ipc.js', import.meta.url).href)};
        import Storage from ${JSON.stringify(new URL('../src/storage.js', import.meta.url).href)};
        const server = new EventEmitter();
        Object.assign(server, { id: 'node', isPrimary: false, isWorker: true, workerId: cluster.worker.id });
        ${eventAPI}
        const pending = new Map();
        server.send = IPC.init(server, pending);
        const storage = Storage.init(server);
        let effects = 0;
        server.on('example.work', async payload => {
            await new Promise(resolve => setTimeout(resolve, 10));
            return { ...payload, result: payload.value * 2 };
        });
        server.on('example.fail', () => { throw new Error('expected operation error'); });
        server.on('example.sideEffect', payload => ({ ...payload, result: ++effects }));
        server.on('example.storage', async payload => {
            await storage.set('shared', payload.value);
            return { ...payload, result: await storage.get('shared'), pending: pending.size };
        });
        process.send({ ready: true });
    `);
    cluster.setupPrimary({ exec: filename, execArgv: [] });
    const children = [cluster.fork(), cluster.fork()];
    t.after(async () => {
        for (const worker of children) {
            if (!worker.isDead()) {
                const exited = once(worker, 'exit');
                worker.kill();
                await exited;
            }
        }
        for (const [event, listeners] of previous) {
            for (const listener of cluster.listeners(event)) if (!listeners.includes(listener)) cluster.off(event, listener);
        }
        for (const [event, listeners] of processPrevious) {
            for (const listener of process.listeners(event)) if (!listeners.includes(listener)) process.off(event, listener);
        }
        await rm(directory, { recursive: true, force: true });
    });
    await Promise.all(children.map(worker => once(worker, 'message')));
    const server = new EventEmitter();
    Object.assign(server, { id: 'node', isPrimary: true, workers: new Map(children.map(worker => [worker.id, worker])),
        cluster: { send() { assert.fail('This test only uses IPC'); } } });
    const pending = new Map();
    server.emitAsync = async (event, payload) => {
        const replies = await Promise.all(server.rawListeners(event).map(listener => listener.call(server, payload)));
        return replies.find(reply => reply?.uuid === payload.uuid) || payload;
    };
    server.send = IPC.init(server, pending);
    Storage.init(server);
    for (const broadcastSender of [undefined, false, true]) {
        const response = await server.send({ method: 'example.work', master: false,
            worker: children[0].id, value: 21, broadcastSender });
        assert.equal(response.result, 42);
        assert.equal(response.response, true);
        assert.equal(typeof response.uuid, 'string');
        assert.equal(pending.size, 0);
    }
    const failed = await server.send({ method: 'example.fail', master: false, worker: children[0].id });
    assert.equal(failed.success, false);
    assert.equal(failed.error, 'expected operation error');
    assert.equal(pending.size, 0);

    // Caller discards the send Promise; the transport still clears correlation.
    const received = once(children[0], 'message');
    server.send({ method: 'example.work', master: false, worker: children[0].id, value: 7 });
    assert.equal((await received)[0].result, 14);
    assert.equal(pending.size, 0);

    // Unknown/duplicate response envelopes must never execute their method.
    children[0].send({ method: 'example.sideEffect', uuid: 'late-response', response: true });
    const effect = await server.send({ method: 'example.sideEffect', master: false, worker: children[0].id });
    assert.equal(effect.result, 1);

    const stored = await server.send({ method: 'example.storage', master: false, worker: children[0].id, value: 'shared-value' });
    assert.equal(stored.result, 'shared-value');
    assert.equal(stored.pending, 0);
    assert.equal(pending.size, 0);

    const replies = new Set();
    const observe = (worker, payload) => { if (payload.uuid === 'broadcast-request') replies.add(worker.id); };
    cluster.on('message', observe);
    const broadcast = await server.send({ method: 'example.work', uuid: 'broadcast-request', master: false,
        broadcast: true, value: 3, broadcastSender: false });
    assert.equal(broadcast.uuid, 'broadcast-request');
    assert.equal(broadcast.result, 6);
    assert.equal(replies.size, 2, 'Await waits for both local worker replies');
    assert.equal(pending.size, 0);
});
