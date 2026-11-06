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
    async function connect(path = '/tenant') {
        const accepted = once(inbound.meshWss, 'connection');
        const client = new WebSocket(`wss://127.0.0.1:${port}${path}`, [encodeURIComponent(JSON.stringify({ id: 'peer' }))], { rejectUnauthorized: false });
        client.on('error', () => {});
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
    await disconnect(client, socket, true);
    assert.equal(mesh.organizations.has('tenant'), false);
    assert.equal(mesh.activeConnections.size, 0);
});

test('inbound unexpected closure waits for recovery and finalizes after its bounded window', async t => {
    const { mesh, inbound, connect, disconnect } = await setup(t);
    const { client, socket } = await connect();
    await disconnect(client, socket);
    assert.equal(mesh.organizations.has('tenant'), true);
    assert.equal(inbound.reconnectTimers.size, 1);
    t.mock.timers.tick(mesh.outboundManager.reconnectWaitMs);
    assert.equal(inbound.reconnectTimers.size, 0);
    assert.equal(mesh.organizations.has('tenant'), false);
    assert.equal(mesh.activeConnections.size, 0);
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
