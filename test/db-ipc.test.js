import assert from 'node:assert/strict';
import cluster from 'node:cluster';
import { EventEmitter, once } from 'node:events';
import { access, mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import IPC from '../src/ipc.js';
import { initializeServerDB } from '@cocreate/db';

test('real worker IPC awaits primary CRUD, preserves concurrent updates, and returns errors', { timeout: 30000 }, async t => {
    const directory = await mkdtemp(join(tmpdir(), 'coc-db-real-ipc-'));
    const originalListeners = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    const processListeners = new Map(['orgConnected', 'orgDisconnected'].map(event => [event, process.listeners(event)]));
    const server = Object.assign(new EventEmitter(), {
        id: 'ipc-db-node', isPrimary: true, workers: new Map(), cluster: Object.assign(new EventEmitter(), { send() {} })
    });
    server.emitAsync = async (method, payload) => {
        const replies = await Promise.all(server.rawListeners(method).map(listener => listener(payload)));
        return replies[0] ?? payload;
    };
    const pending = new Map();
    server.send = IPC.init(server, pending);
    const db = initializeServerDB(server, { directory });
    cluster.setupPrimary({ exec: fileURLToPath(new URL('../fixtures/db-ipc-worker.mjs', import.meta.url)), silent: true });
    t.after(async () => {
        await db.close();
        await Promise.all([...server.workers.values()].map(async record => {
            const exited = once(record.process, 'exit');
            record.process.kill();
            await exited;
        }));
        for (const [event, listeners] of originalListeners)
            for (const listener of cluster.listeners(event)) if (!listeners.includes(listener)) cluster.removeListener(event, listener);
        for (const [event, listeners] of processListeners)
            for (const listener of process.listeners(event)) if (!listeners.includes(listener)) process.removeListener(event, listener);
        await rm(directory, { recursive: true, force: true });
    });
    for (const id of [1, 2]) {
        const worker = cluster.fork({ COCREATE_TEST_WORKER_ID: String(id), COCREATE_TEST_DB_DIRECTORY: join(directory, 'no-worker-db') });
        server.workers.set(id, { id, process: worker });
        const ready = await new Promise(resolve => worker.on('message', payload => { if (payload.fixtureReady) resolve(payload); }));
        assert.deepEqual(ready.driverKeys, ['send']);
    }
    const scope = { database: 'ipc-test', array: 'items', organization_id: '000000000000000000000001', storage: 'production' };
    const send = async (worker, data) => (await server.send({ method: 'fixture.crud', worker, master: false, broadcast: false, data })).result;
    const created = await send(1, { ...scope, method: 'object.create', object: { value: 0 } });
    assert.equal(created.success, true, created.error);
    const objectId = created.object[0]._id;
    const increment = { ...scope, method: 'object.update', object: { _id: objectId, $inc: { value: 1 } } };
    const replies = await Promise.all(Array.from({ length: 12 }, (_, i) => send(i % 2 + 1, structuredClone(increment))));
    assert.ok(replies.every(result => result.success));
    const read = { ...scope, method: 'object.read', object: { _id: objectId } };
    assert.equal((await send(2, read)).object[0].value, 12);
    const registeredRead = await send(2, { ...read, clientId: 'client-two' });
    const registrations = (await server.send({ method: 'fixture.distribution', worker: 2, master: false })).result;
    assert.deepEqual(registrations.at(-1), { organization_id: scope.organization_id, clientId: 'client-two', distribution: registeredRead.distribution });
    assert.deepEqual([...server.workers.distribution.get(registeredRead.distribution[0])].sort(), [1, 2]);

    const failed = await send(1, { ...increment, object: { _id: objectId, $inc: { value: 'invalid' } } });
    assert.equal(failed.success, false);
    assert.equal((await send(1, read)).object[0].value, 12);
    assert.equal(pending.size, 0);
    assert.ok(db.memoryStats().files > 0);
    await assert.rejects(access(join(directory, 'no-worker-db')), { code: 'ENOENT' });
});
