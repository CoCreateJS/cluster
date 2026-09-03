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
        assert.equal(request.broadcastSender, true);
    }
});

test('primary storage events return results through server.send', async () => {
    const responses = [];
    const server = createServer({
        isPrimary: true,
        send: async payload => responses.push(payload)
    });
    Storage.init(server);

    server.emit('storage.set', {
        method: 'storage.set',
        uuid: 'set-uuid',
        sourceWorker: 2,
        broadcastSender: true,
        key: 'shared',
        value: 42
    });
    await new Promise(resolve => setImmediate(resolve));

    server.emit('storage.get', {
        method: 'storage.get',
        uuid: 'get-uuid',
        sourceWorker: 2,
        broadcastSender: true,
        key: 'shared'
    });
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(responses.length, 2);
    assert.equal(responses[0].success, true);
    assert.equal(responses[0].result, true);
    assert.equal(responses[1].success, true);
    assert.equal(responses[1].result, 42);
});
