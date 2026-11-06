import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';

import Storage from '../src/storage.js';

function createServer({ isPrimary, send }) {
    const emitter = new EventEmitter();
    return {
        isPrimary,
        on: emitter.on.bind(emitter),
        emit: emitter.emit.bind(emitter),
        async emitAsync(method, payload) {
            const replies = await Promise.all(emitter.rawListeners(method).map(listener => listener.call(emitter, payload)));
            return replies.find(reply => reply?.uuid === payload.uuid) || payload;
        },
        send
    };
}

test('the primary owns and operates the storage Map', async () => {
    const server = createServer({
        isPrimary: true,
        send: async payload => payload
    });
    const storage = Storage.init(server);

    assert.equal(await storage.set('context', { ready: true }), true);
    assert.deepEqual(await storage.get('context'), { ready: true });
    assert.equal(await storage.size(), 1);
    assert.equal(await storage.delete('context'), true);
    assert.equal(await storage.size(), 0);
    assert.equal(await storage.clear(), true);
});

test('workers proxy every storage operation through server.send', async () => {
    const requests = [];
    const results = [true, 'value', true, 0, true];
    const server = createServer({
        isPrimary: false,
        send: async payload => {
            requests.push(payload);
            return { ...payload, result: results.shift() };
        }
    });
    const storage = Storage.init(server);

    assert.equal(await storage.set('key', 'value'), true);
    assert.equal(await storage.get('key'), 'value');
    assert.equal(await storage.delete('key'), true);
    assert.equal(await storage.size(), 0);
    assert.equal(await storage.clear(), true);

    assert.deepEqual(requests.map(request => request.method), [
        'storage.set',
        'storage.get',
        'storage.delete',
        'storage.size',
        'storage.clear'
    ]);
    for (const request of requests) {
        assert.equal(request.master, true);
        assert.equal(request.broadcast, false);
        assert.equal(request.broadcastSender, undefined);
    }
});

test('primary storage handlers return envelopes regardless of broadcastSender', async () => {
    const server = createServer({ isPrimary: true, send() { assert.fail('IPC owns response transmission'); } });
    Storage.init(server);
    const write = await server.emitAsync('storage.set', {
        method: 'storage.set', uuid: 'set-uuid', broadcastSender: false, key: 'shared', value: 42
    });
    const read = await server.emitAsync('storage.get', {
        method: 'storage.get', uuid: 'get-uuid', key: 'shared'
    });
    assert.equal(write.success, true);
    assert.equal(write.result, true);
    assert.equal(read.success, true);
    assert.equal(read.result, 42);
});
