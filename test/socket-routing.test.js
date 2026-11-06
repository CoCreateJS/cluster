import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createServer } from 'node:http';
import cluster from 'node:cluster';
import Config from '@cocreate/config';
import { SocketClient, SocketServer } from '@cocreate/socket';
import IPC from '../src/ipc.js';

test('a real socket response uses server.send once and fans out without socket forwarding', { timeout: 5000 }, async t => {
    const processEvents = ['message', 'orgConnected', 'orgDisconnected', 'socket.destroyAll', 'socket.destroyOrgs', 'organization.balance'];
    const previous = new Map(processEvents.map(event => [event, process.listeners(event)]));
    const clusterPrevious = new Map(['message', 'exit'].map(event => [event, cluster.listeners(event)]));
    t.after(() => {
        for (const [event, listeners] of previous) {
            for (const listener of process.listeners(event)) if (!listeners.includes(listener)) process.off(event, listener);
        }
        for (const [event, listeners] of clusterPrevious) {
            for (const listener of cluster.listeners(event)) if (!listeners.includes(listener)) cluster.off(event, listener);
        }
    });
    t.mock.method(Config, 'prompt', async () => ({}));
    const http = createServer();
    const peers = [], workers = [];
    const server = new EventEmitter();
    Object.assign(server, { id: 'node', cluster_id: 'cluster', isPrimary: true, isWorker: true, workerId: 1, http,
        workers: new Map([[2, { id: 2, process: { isConnected: () => true, send: data => { workers.push(data); queueMicrotask(() => cluster.emit('message', server.workers.get(2), { ...data, response: true })); } } }]]),
        organizationWorkers: new Map([['org', new Set([2])]]),
        cluster: { send: data => peers.push(data) },
        authorize: { check: async () => ({}) }, usage: { send() {} } });
    server.emitAsync = async (method, payload) => { await Promise.all(server.listeners(method).map(listener => listener(payload))); return payload; };
    const route = IPC.init(server, new Map());
    let calls = 0;
    server.send = data => { calls++; return route(data); };
    const manager = new SocketServer(server);
    server.socket = manager.collection;
    let completion;
    manager.on('object.read', data => {
        completion = server.send({ ...data, object: [{ _id: 'read-result' }] });
    });
    let url;
    t.after(async () => {
        const client = SocketClient.get(url);
        if (client) {
            client.onclose = null;
            client.onerror = null;
            client.terminate();
            SocketClient.delete(url);
        }
        for (const socket of manager.sockets.values()) {
            socket.bypassDeleteCascade = true;
            socket.terminate();
        }
        for (const organization of manager.organizations.values()) clearTimeout(organization.debounce);
        manager.close();
        await new Promise(resolve => http.close(resolve));
    });
    await new Promise(resolve => http.listen(0, '127.0.0.1', resolve));
    await SocketClient.init();
    const host = `ws://127.0.0.1:${http.address().port}`;
    url = `${host}/org`;
    const response = await SocketClient.send({ method: 'object.read', host, organization_id: 'org',
        array: 'local-test', broadcast: false, broadcastSender: true });
    await completion;
    assert.deepEqual(response.object, [{ _id: 'read-result' }]);
    assert.equal(calls, 1);
    assert.equal(peers.length, 1);
    assert.equal(workers.length, 1);
    assert.equal(peers[0].uuid, response.uuid);
    assert.equal(response.serverId, 'node');
    assert.equal(response.workerId, 1);
    assert.equal(response.clusterId, 'cluster');
    assert.equal(workers[0].clusterId, 'cluster');
    assert.equal(peers[0].clusterId, 'cluster');
    assert.equal(workers[0].uuid, response.uuid);
    assert.equal('socket' in peers[0], false);
    assert.equal('socket' in workers[0], false);
});
