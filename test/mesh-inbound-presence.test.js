import assert from 'node:assert/strict';
import test from 'node:test';
import { once } from 'node:events';
import { WebSocket } from 'ws';
import { ServerMesh } from '../src/mesh/index.js';

async function setup(t) {
    t.mock.method(console, 'log', () => {});
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const mesh = new ServerMesh();
    mesh.server = { isPrimary: true, id: 'server-z', organization_id: 'platform', organizationWorkers: new Map() };
    mesh.organizations.set('tenant', []);
    const inbound = mesh.inboundManager;
    inbound.meshPort = 0;
    await inbound.startServer();
    t.after(() => mesh.close());
    const port = inbound.secureMeshServer.address().port;
    async function connect(path = '/tenant', onMessage) {
        const accepted = once(inbound.meshWss, 'connection');
        const client = new WebSocket(`wss://127.0.0.1:${port}${path}`, [encodeURIComponent(JSON.stringify({ id: 'peer' }))], { rejectUnauthorized: false });
        client.on('error', () => {});
        if (onMessage) client.on('message', data => onMessage(JSON.parse(data)));
        await once(client, 'open');
        const [socket] = await accepted;
        return { client, socket };
    }
    async function disconnect(client, socket, normal = false) {
        const closed = once(socket, 'close');
        if (normal) client.close(1000);
        else client.terminate();
        await closed;
    }
    return { mesh, inbound, connect, disconnect };
}

test('inbound intentional closure immediately removes peer-only organization presence', async t => {
    const { mesh, connect, disconnect } = await setup(t);
    const { client, socket } = await connect();
    mesh.server.emit = () => assert.fail('Intentional closure must not emit peerUnavailable');
    mesh.server.workflow = { run() { assert.fail('Intentional closure must not report a failure'); } };
    await disconnect(client, socket, true);
    assert.equal(mesh.organizations.has('tenant'), false);
    assert.equal(mesh.activeConnections.size, 0);
    assert.equal(mesh.messageQueues.size, 0);
});

test('inbound unexpected closure waits for recovery and finalizes after its bounded window', async t => {
    const { mesh, inbound, connect, disconnect } = await setup(t);
    const events = [], calls = [];
    mesh.server.emit = (name, report) => events.push({ name, report });
    mesh.server.workflow = { async run(payload) { calls.push(payload); return payload; } };
    const { client, socket } = await connect();
    await disconnect(client, socket);
    mesh.send({ method: 'example', server: 'peer', organization_id: 'tenant' });
    assert.equal(mesh.messageQueues.size, 1);
    assert.equal(mesh.organizations.has('tenant'), true);
    assert.equal(inbound.reconnectTimers.size, 1);
    t.mock.timers.tick(mesh.outboundManager.reconnectWaitMs);
    assert.equal(inbound.reconnectTimers.size, 0);
    assert.equal(mesh.organizations.has('tenant'), false);
    assert.equal(mesh.activeConnections.size, 0);
    assert.equal(mesh.messageQueues.size, 0);
    await Promise.resolve();
    assert.equal(events.length, 1);
    assert.equal(events[0].name, 'peerUnavailable');
    assert.equal(calls[0].peerUnavailable[0].reason, 'reconnect window expired');
    assert.equal(mesh.pendingPeerFailures.size, 0);
});

test('inbound reconnection cancels final cleanup and retains only the replacement socket', async t => {
    const { mesh, inbound, connect, disconnect } = await setup(t);
    const first = await connect();
    await disconnect(first.client, first.socket);
    const second = await connect();
    assert.equal(inbound.reconnectTimers.size, 0);
    assert.deepEqual(mesh.organizations.get('tenant'), [second.socket]);
    t.mock.timers.tick(mesh.outboundManager.reconnectWaitMs);
    assert.deepEqual(mesh.organizations.get('tenant'), [second.socket]);
    await disconnect(second.client, second.socket, true);
    assert.equal(mesh.organizations.has('tenant'), false);
});


test('inbound replacement socket drains queued messages by stable peer ID', { timeout: 5000 }, async t => {
    const { mesh, connect, disconnect } = await setup(t);
    const first = await connect();
    await disconnect(first.client, first.socket);
    for (const value of [1, 2]) mesh.send({ method: 'example', server: 'peer', organization_id: 'tenant', value });
    const messages = [];
    let complete;
    const received = new Promise(resolve => { complete = resolve; });
    const second = await connect('/tenant', message => {
        messages.push(message.value);
        if (messages.length === 2) complete();
    });
    await received;
    assert.deepEqual(messages, [1, 2]);
    assert.equal(mesh.messageQueues.size, 0);
    await disconnect(second.client, second.socket, true);
});
