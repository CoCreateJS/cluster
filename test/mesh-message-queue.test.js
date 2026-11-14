import assert from 'node:assert/strict';
import test from 'node:test';
import { EventEmitter } from 'node:events';
import { ServerMesh } from '../src/mesh/index.js';
import { ServerMeshOutbound } from '../src/mesh/outbound.js';

test('control peers announce connection and immediate disconnection without duplicate close events', () => {
    const mesh = new ServerMesh();
    const events = [];
    mesh.on('peerConnected', value => events.push(['connected', value.nodeId]));
    mesh.on('peerDisconnected', value => events.push(['disconnected', value.nodeId]));
    const first = { serverId: 'peer', isControlSocket: true, readyState: 1 };
    const second = { serverId: 'peer', isControlSocket: true, readyState: 1 };
    mesh.controlSockets.push(first, second);
    mesh.syncOrganizations(first);
    first.readyState = 3;
    mesh.peerDisconnected(first);
    assert.deepEqual(events, [['connected', 'peer']]);
    second.readyState = 3;
    mesh.peerDisconnected(second);
    mesh.peerDisconnected(second);
    assert.deepEqual(events, [['connected', 'peer'], ['disconnected', 'peer']]);
});

function setup(t) {
    const sockets = [];
    class Socket extends EventEmitter {
        constructor() { super(); this.readyState = 0; this.sent = []; sockets.push(this); }
        send(data) { this.sent.push(JSON.parse(data)); }
        open() { this.readyState = 1; this.emit('open'); }
        close(code) { this.readyState = 3; this.emit('close', code); }
    }
    const mesh = new ServerMesh();
    mesh.server = { isPrimary: true, id: 'self', ip: '192.0.2.10' };
    mesh.outboundManager = new ServerMeshOutbound(mesh, Socket);
    mesh.outboundManager.server = mesh.server;
    t.mock.method(console, 'log', () => {});
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'error', () => {});
    t.mock.timers.enable({ apis: ['setTimeout'] });
    t.after(() => mesh.outboundManager.close());
    mesh.outboundManager.connectPeer({ _id: 'peer', ip: '192.0.2.1' });
    const send = (value, extra = {}) => mesh.send({ method: 'example', server: 'peer', value, ...extra });
    return { mesh, sockets, send };
}

test('connecting and disconnected messages replay in FIFO order on the replacement server ID', t => {
    const { mesh, sockets, send } = setup(t);
    send(1);
    send(2);
    sockets[0].open();
    assert.deepEqual(sockets[0].sent.map(item => item.value), [1, 2]);
    assert.equal(mesh.messageQueues.size, 0);
    sockets[0].close(1006);
    send(3);
    send(4);
    t.mock.timers.tick(7000);
    send(5);
    assert.equal(sockets[1].serverId, 'peer');
    sockets[1].open();
    send(6);
    assert.deepEqual(sockets[1].sent.map(item => item.value), [3, 4, 5, 6]);
    assert.equal(mesh.messageQueues.size, 0);
});

test('a prepared replication frame is queued without serializing its value again', t => {
    const { mesh, sockets } = setup(t);
    let encodings = 0;
    const message = { method: 'db.partition.update', server: 'peer', uuid: 'prepared',
        value: { toJSON() { encodings++; return { entries: [[1, 'object']] }; } } };
    const encoded = JSON.stringify(message);
    mesh.send(message, encoded);
    sockets[0].open();
    assert.equal(encodings, 1);
    assert.deepEqual(sockets[0].sent[0].value.entries, [[1, 'object']]);
});

test('intentional closure discards pending messages and does not retry', t => {
    const { mesh, sockets, send } = setup(t);
    send(1);
    sockets[0].close(1000);
    assert.equal(mesh.messageQueues.size, 0);
    t.mock.timers.tick(100000);
    assert.equal(sockets.length, 1);
});

test('retry exhaustion releases queued messages and routing references', t => {
    const { mesh, sockets, send } = setup(t);
    send(1);
    for (let i = 0; i < 10; i++) {
        sockets.at(-1).close(1006);
        assert.equal(mesh.messageQueues.size, 1);
        t.mock.timers.tick(7000);
    }
    sockets.at(-1).close(1006);
    assert.equal(mesh.messageQueues.size, 0);
    assert.equal(mesh.activeConnections.size, 0);
    assert.equal(mesh.outboundManager.retryTimers.size, 0);
});

test('same server control and tenant queues remain isolated; tenant closure clears only its queue', t => {
    const { mesh, sockets, send } = setup(t);
    mesh.organizations.set('tenant', []);
    mesh.outboundManager.connectPeer({ _id: 'peer', ip: '192.0.2.1' }, 1, 'tenant');
    send('control');
    send('tenant', { organization_id: 'tenant' });
    assert.equal(mesh.messageQueues.size, 2);
    mesh.closeOrganization('tenant');
    assert.equal(mesh.messageQueues.size, 1);
    sockets[0].open();
    assert.deepEqual(sockets[0].sent.map(item => item.value), ['control']);
    assert.equal(mesh.messageQueues.size, 0);
});

test('a synchronous send failure retains the unsent message ahead of subsequent messages', t => {
    const { mesh, sockets, send } = setup(t);
    sockets[0].open();
    const transmit = sockets[0].send;
    sockets[0].send = () => { throw new Error('transport failed'); };
    send(1);
    sockets[0].send = transmit;
    send(2);
    assert.deepEqual(sockets[0].sent.map(item => item.value), [1, 2]);
    assert.equal(mesh.messageQueues.size, 0);
});

test('broadcast recipients each retain one independent queue and preserve the serialized envelope', t => {
    const { mesh, sockets } = setup(t);
    mesh.outboundManager.connectPeer({ _id: 'other', ip: '192.0.2.2' });
    const message = { method: 'example', value: 1 };
    mesh.send(message);
    message.value = 99;
    assert.equal(mesh.messageQueues.size, 2);
    sockets[0].open();
    sockets[1].open();
    assert.equal(sockets[0].sent[0].value, 1);
    assert.deepEqual(sockets[0].sent, sockets[1].sent);
    assert.equal(mesh.messageQueues.size, 0);
});

test('shutdown frees all pending messages and rejects later enqueueing', async t => {
    const { mesh, send } = setup(t);
    send(1);
    await mesh.close();
    send(2);
    assert.equal(mesh.messageQueues.size, 0);
});


function exhaust(t, sockets) {
    for (let i = 0; i < 10; i++) {
        sockets.at(-1).close(1006);
        t.mock.timers.tick(7000);
    }
    sockets.at(-1).close(1006);
}

test('retry exhaustion emits peerUnavailable and directly invokes its workflow with the queue', async t => {
    const { mesh, sockets, send } = setup(t);
    const events = [], invocations = [];
    mesh.server.emit = (name, report) => events.push({ name, report });
    mesh.server.workflow = { async run(payload, options) { invocations.push({ payload, options }); return payload; } };
    send(1);
    exhaust(t, sockets);
    await Promise.resolve();
    assert.equal(events.length, 1);
    assert.equal(events[0].name, 'peerUnavailable');
    assert.equal(invocations.length, 1);
    assert.equal(invocations[0].payload.method, 'workflow.peerUnavailable');
    assert.equal(invocations[0].options, undefined);
    const report = invocations[0].payload.peerUnavailable[0];
    assert.equal(report.serverId, 'peer');
    assert.equal(report.observerId, 'self');
    assert.equal(report.retries, 10);
    assert.equal(report.closeCode, 1006);
    assert.equal(report.queuedMessages[0].value, 1);
    assert.equal(report.peerReachable, false);
    assert.equal(mesh.messageQueues.size, 0);
    assert.equal(mesh.pendingPeerFailures.size, 0);
});

test('intentional close and shutdown never emit unavailability or invoke a failure workflow', async t => {
    const { mesh, sockets, send } = setup(t);
    mesh.server.emit = () => assert.fail('Intentional closure is not a failure');
    mesh.server.workflow = { run() { assert.fail('Intentional closure is not a failure'); } };
    send(1);
    sockets[0].close(1000);
    t.mock.timers.tick(100000);
    await mesh.close();
    assert.equal(mesh.pendingPeerFailures.size, 0);
});

test('workflow persistence failures retain the snapshot until a successful retry', async t => {
    const { mesh, sockets, send } = setup(t);
    mesh.server.workflow = { async run() { return { error: ['Storage offline'] }; } };
    send(1);
    exhaust(t, sockets);
    await Promise.resolve();
    assert.equal(mesh.messageQueues.size, 0);
    assert.equal(mesh.pendingPeerFailures.size, 1);
    const report = [...mesh.pendingPeerFailures.values()][0];
    assert.equal(report.queuedMessages[0].value, 1);
    mesh.server.workflow.run = async payload => payload;
    assert.equal(await mesh.persistPeerFailure(report), true);
    assert.equal(mesh.pendingPeerFailures.size, 0);
});

test('event listener failure does not prevent the workflow from saving the failure report', async t => {
    const { mesh, sockets, send } = setup(t);
    mesh.server.emit = () => { throw new Error('Broken observer'); };
    let called = false;
    mesh.server.workflow = { async run(payload) { called = true; return payload; } };
    send(1);
    exhaust(t, sockets);
    await Promise.resolve();
    assert.equal(called, true);
    assert.equal(mesh.pendingPeerFailures.size, 0);
});
