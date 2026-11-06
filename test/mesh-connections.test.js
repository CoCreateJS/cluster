import assert from 'node:assert/strict';
import test from 'node:test';
import { EventEmitter, once } from 'node:events';
import { WebSocket, WebSocketServer } from 'ws';
import { ServerMesh } from '../src/mesh/index.js';
import { ServerMeshOutbound } from '../src/mesh/outbound.js';

function setup(t) {
    const sockets = [];
    class Socket extends EventEmitter {
        constructor(url) { super(); this.url = url; this.readyState = 1; sockets.push(this); }
        close(code) { this.readyState = 3; this.emit('close', code); }
    }
    const mesh = new ServerMesh();
    mesh.server = { isPrimary: true, id: 'server-z', ip: '192.0.2.10', organization_id: 'platform', host: 'platform.test' };
    const outbound = new ServerMeshOutbound(mesh, Socket);
    outbound.server = mesh.server;
    mesh.outboundManager = outbound;
    t.mock.method(console, 'log', () => {});
    t.mock.timers.enable({ apis: ['setTimeout'] });
    t.after(() => outbound.close());
    return { mesh, outbound, sockets };
}

test('peer discovery queries older running servers before initiating sockets', async t => {
    const { mesh, outbound, sockets } = setup(t);
    mesh.server.crud = { send: async request => {
        assert.deepEqual(request.filter.query, { _id: { $lt: 'server-z' }, 'master.status': 'running' });
        assert.equal(request.organization_id, 'platform');
        return { object: [{ _id: 'server-a', ip: '192.0.2.1' }, { _id: 'server-b', ip: '192.0.2.2' }] };
    } };
    await outbound.getServers();
    assert.deepEqual(sockets.map(socket => socket.url), ['wss://192.0.2.1:8090', 'wss://192.0.2.2:8090']);
    assert.equal(mesh.activeConnections.size, 2);
});

test('normal close stops retries and removes the socket', t => {
    const { mesh, outbound, sockets } = setup(t);
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' });
    sockets[0].close(1000);
    t.mock.timers.tick(100000);
    assert.equal(sockets.length, 1);
    assert.equal(mesh.activeConnections.size, 0);
    assert.equal(outbound.retryTimers.size, 0);
});

for (const code of [1006, 1012]) {
    test(`abnormal/restart close ${code} retains the existing retry behavior`, t => {
        const { outbound, sockets } = setup(t);
        outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' });
        sockets[0].close(code);
        t.mock.timers.tick(7000);
        assert.equal(sockets.length, 2);
    });
}

test('node shutdown/restart and organization closure send appropriate standard codes', async t => {
    const { mesh, outbound, sockets } = setup(t);
    mesh.organizations.set('tenant', []);
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' });
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' }, 1, 'tenant');
    let orgCode;
    sockets[1].on('close', code => { orgCode = code; });
    mesh.closeOrganization('tenant');
    assert.equal(orgCode, 1000);
    assert.equal(mesh.activeConnections.get('control:peer'), sockets[0]);
    for (const cause of ['OS_SHUTDOWN', 'API_RESTART', 'OS_REBOOT', 'SIGTERM']) {
        const node = new ServerMesh();
        const codes = [];
        node.activeConnections.set('peer', { readyState: 1, close: code => codes.push(code) });
        node.outboundManager.close = () => {};
        node.inboundManager.close = async () => {};
        await node.close(cause);
        assert.deepEqual(codes, [cause === 'OS_SHUTDOWN' ? 1000 : 1012]);
    }
});

test('standard close codes reach a real WebSocket peer', { timeout: 5000 }, async () => {
    for (const expected of [1000, 1012]) {
        const listener = new WebSocketServer({ host: '127.0.0.1', port: 0 });
        await once(listener, 'listening');
        listener.once('connection', socket => socket.close(expected));
        const client = new WebSocket(`ws://127.0.0.1:${listener.address().port}`);
        try { assert.equal((await once(client, 'close'))[0], expected); }
        finally {
            client.terminate();
            await new Promise(resolve => listener.close(resolve));
        }
    }
});


test('organization recovery retries are bounded and exhaustion clears peer-only presence', t => {
    const { mesh, outbound, sockets } = setup(t);
    mesh.organizations.set('tenant', []);
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' }, 1, 'tenant');
    for (let attempt = 0; attempt < 10; attempt++) {
        sockets.at(-1).close(1006);
        assert.equal(mesh.organizations.has('tenant'), true);
        assert.equal(outbound.retryTimers.size, 1);
        t.mock.timers.tick(7000);
        assert.equal(mesh.organizations.get('tenant').length, 1, 'retry replaces the previous socket reference');
    }
    assert.equal(sockets.length, 11);
    sockets.at(-1).close(1006);
    t.mock.timers.tick(100000);
    assert.equal(sockets.length, 11);
    assert.equal(outbound.retryTimers.size, 0);
    assert.equal(mesh.activeConnections.size, 0);
    assert.equal(mesh.organizations.has('tenant'), false);
});

test('a recovered organization socket replaces its failed reference and intentional closure finalizes it', t => {
    const { mesh, outbound, sockets } = setup(t);
    mesh.organizations.set('tenant', []);
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' }, 1, 'tenant');
    sockets[0].close(1006);
    t.mock.timers.tick(7000);
    sockets[1].emit('open');
    assert.deepEqual(mesh.organizations.get('tenant'), [sockets[1]]);
    assert.equal(outbound.retryTimers.size, 0);
    sockets[1].close(1000);
    assert.equal(mesh.organizations.has('tenant'), false);
    t.mock.timers.tick(100000);
    assert.equal(sockets.length, 2);
});

test('the last remote socket closing preserves an organization still used by local workers', t => {
    const { mesh, outbound, sockets } = setup(t);
    mesh.server.organizationWorkers = new Map([['tenant', new Set([1])]]);
    mesh.organizations.set('tenant', []);
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' }, 1, 'tenant');
    sockets[0].close(1000);
    assert.deepEqual(mesh.organizations.get('tenant'), []);
    assert.equal(mesh.server.organizationWorkers.get('tenant').size, 1);
});

test('own-organization messaging can close while node control connectivity remains', t => {
    const { mesh, outbound, sockets } = setup(t);
    mesh.organizations.set('platform', []);
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' });
    outbound.connectPeer({ _id: 'peer', ip: '192.0.2.1' }, 1, 'platform');
    mesh.closeOrganization('platform');
    assert.equal(mesh.organizations.has('platform'), false);
    assert.deepEqual(mesh.controlSockets, [sockets[0]]);
    assert.equal(mesh.activeConnections.get('control:peer'), sockets[0]);
    assert.equal(sockets[1].readyState, 3);
});

test('worker Mesh attaches as server.cluster and forwards direct sends through primary IPC', async () => {
    const server = new EventEmitter();
    const forwarded = [];
    Object.assign(server, { id: 'worker-node', workerId: 2, isPrimary: false,
        send: data => forwarded.push(data) });
    const mesh = new ServerMesh();
    mesh.inboundManager.init = () => assert.fail('Workers must not open the node listener');
    mesh.outboundManager.init = () => assert.fail('Workers must not open peer connections');
    await mesh.init(server);
    assert.equal(server.cluster, mesh);
    assert.equal(server.mesh, undefined);
    const payload = { method: 'cursor', organization_id: 'org' };
    server.cluster.send(payload);
    assert.equal(forwarded.length, 1);
    assert.equal(forwarded[0].method, 'mesh.outbound_mesh_relay');
    assert.equal(forwarded[0].payload, payload);
    assert.equal(payload.serverId, 'worker-node');
});
