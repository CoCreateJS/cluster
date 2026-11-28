import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createServer as createNetServer, connect as connectTcp } from 'node:net';
import { createServer as createHttpsServer, request as httpsRequest } from 'node:https';
import test from 'node:test';
import { PassThrough } from 'node:stream';
import { once } from 'node:events';
import {
    parseClientHelloServerName,
    inspectOrganizationAffinity,
    readClientHelloServerName,
    resolveOrganizationForHost
} from '../src/tcp.js';

const uint16 = value => {
    const buffer = Buffer.alloc(2);
    buffer.writeUInt16BE(value);
    return buffer;
};

const record = handshake => Buffer.concat([
    Buffer.from([0x16, 0x03, 0x01]),
    uint16(handshake.length),
    handshake
]);

function clientHello(hostname) {
    const extensions = hostname
        ? (() => {
            const name = Buffer.from(hostname);
            const names = Buffer.concat([uint16(name.length + 3), Buffer.from([0]), uint16(name.length), name]);
            return Buffer.concat([uint16(0), uint16(names.length), names]);
        })()
        : Buffer.alloc(0);
    const body = Buffer.concat([
        Buffer.from([0x03, 0x03]),
        Buffer.alloc(32),
        Buffer.from([0]),
        uint16(2), Buffer.from([0x13, 0x01]),
        Buffer.from([1, 0]),
        uint16(extensions.length), extensions
    ]);
    const handshake = Buffer.alloc(4);
    handshake[0] = 1;
    handshake.writeUIntBE(body.length, 1, 3);
    return Buffer.concat([record(Buffer.concat([handshake, body]))]);
}

test('extracts and normalizes SNI from a TLS ClientHello', () => {
    assert.deepEqual(parseClientHelloServerName(clientHello('Tenant.Example.test.')),
        { complete: true, serverName: 'tenant.example.test' });
});

test('extracts SNI when the ClientHello handshake spans TLS records', () => {
    const hello = clientHello('tenant.example.test');
    const handshake = hello.subarray(5);
    const splitAt = 19;
    const split = Buffer.concat([record(handshake.subarray(0, splitAt)), record(handshake.subarray(splitAt))]);
    assert.deepEqual(parseClientHelloServerName(split),
        { complete: true, serverName: 'tenant.example.test' });
});

test('reports incomplete ClientHello data without claiming a hostname', () => {
    const hello = clientHello('tenant.example.test');
    assert.deepEqual(parseClientHelloServerName(hello.subarray(0, 12)),
        { complete: false, serverName: null });
});

test('peeks at SNI and restores all consumed bytes for the TLS server', async () => {
    const socket = new PassThrough();
    const hello = clientHello('tenant.example.test');
    const serverName = readClientHelloServerName(socket, { timeoutMs: 100 });
    socket.write(hello);

    assert.equal(await serverName, 'tenant.example.test');
    assert.deepEqual(socket.read(), hello);
    socket.destroy();
});

test('resolves the hostname through CRUD and returns organization identity', async () => {
    const calls = [];
    const server = { crud: { getOrganization: async query => {
        calls.push(query);
        return { object: [{ _id: 'org-1' }] };
    } } };

    assert.deepEqual(await resolveOrganizationForHost(server, 'tenant.example.test'), {
        host: 'tenant.example.test', organization_id: 'org-1'
    });
    assert.deepEqual(calls, [{ host: 'tenant.example.test' }]);
});

test('returns no organization when hostname or CRUD lookup is unavailable', async () => {
    assert.equal(await resolveOrganizationForHost({}, 'tenant.example.test'), null);
    assert.equal(await resolveOrganizationForHost({ crud: { getOrganization: async () => ({ _id: 'org-1' }) } }, ''), null);
    assert.equal(await resolveOrganizationForHost({ crud: { getOrganization: async () => ({}) } }, 'unknown.example.test'), null);
});

test('leaves organization lookup to the accepting node when affinity is enabled', async () => {
    const socket = new PassThrough();
    const hello = clientHello('tenant.example.test');
    const server = {
        state: { isProxying: false },
        tcp: { affinity: 'organization', organizationBySocket: new WeakMap() },
        crud: { getOrganization: async () => ({ _id: 'org-1' }) }
    };
    const pending = inspectOrganizationAffinity(server, socket);
    socket.write(hello);

    assert.deepEqual(await pending, { host: 'tenant.example.test', organization_id: 'org-1' });
    assert.deepEqual(server.tcp.organizationBySocket.get(socket), {
        host: 'tenant.example.test', organization_id: 'org-1'
    });
    assert.deepEqual(socket.read(), hello);
    socket.destroy();
});

test('peer-forwarding nodes bypass SNI inspection and organization lookup', async () => {
    const socket = new PassThrough();
    const hello = clientHello('tenant.example.test');
    let lookups = 0;
    const server = {
        state: { isProxying: true },
        tcp: { affinity: 'organization', organizationBySocket: new WeakMap() },
        crud: { getOrganization: async () => { lookups++; return { _id: 'org-1' }; } }
    };

    assert.equal(await inspectOrganizationAffinity(server, socket), null);
    socket.write(hello);
    assert.equal(lookups, 0);
    assert.deepEqual(socket.read(), hello);
    socket.destroy();
});

test('affinity-disabled nodes bypass SNI inspection and organization lookup', async () => {
    const socket = new PassThrough();
    let lookups = 0;
    const server = {
        state: { isProxying: false },
        tcp: { affinity: null, organizationBySocket: new WeakMap() },
        crud: { getOrganization: async () => { lookups++; return { _id: 'org-1' }; } }
    };

    assert.equal(await inspectOrganizationAffinity(server, socket), null);
    assert.equal(lookups, 0);
    socket.destroy();
});

test('peeks SNI, resolves organization, and forwards the original TLS stream', async t => {
    const key = readFileSync(new URL('./fixtures/tcp-affinity-test-key.key', import.meta.url));
    const cert = readFileSync(new URL('./fixtures/tcp-affinity-test-cert.crt', import.meta.url));
    const backend = createHttpsServer({ key, cert }, (request, response) => {
        response.end(JSON.stringify({
            host: request.headers.host,
            servername: request.socket.servername
        }));
    });
    backend.listen(0, '127.0.0.1');
    await once(backend, 'listening');

    const organizationBySocket = new WeakMap();
    const lookups = [];
    const server = {
        state: { isProxying: false },
        tcp: { affinity: 'organization', organizationBySocket },
        crud: { getOrganization: async query => {
            lookups.push(query);
            return { _id: 'org-1' };
        } }
    };
    let organizationAtForward;
    const proxy = createNetServer(socket => {
        void inspectOrganizationAffinity(server, socket).then(() => {
            organizationAtForward = organizationBySocket.get(socket);
            const upstream = connectTcp(backend.address().port, '127.0.0.1');
            socket.once('close', () => upstream.destroy());
            upstream.once('connect', () => {
                socket.pipe(upstream);
                upstream.pipe(socket);
            });
            upstream.on('error', error => socket.destroy(error));
        }, error => socket.destroy(error));
    });
    proxy.listen(0, '127.0.0.1');
    await once(proxy, 'listening');
    t.after(async () => {
        await Promise.all([
            new Promise(resolve => proxy.close(resolve)),
            new Promise(resolve => backend.close(resolve))
        ]);
    });

    const result = await new Promise((resolve, reject) => {
        const request = httpsRequest({
            hostname: '127.0.0.1',
            port: proxy.address().port,
            servername: 'tenant.example.test',
            path: '/',
            rejectUnauthorized: false,
            headers: { host: 'tenant.example.test' }
        }, response => {
            const chunks = [];
            response.on('data', chunk => chunks.push(chunk));
            response.on('end', () => resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))));
        });
        request.on('error', reject);
        request.end();
    });

    assert.deepEqual(lookups, [{ host: 'tenant.example.test' }]);
    assert.deepEqual(organizationAtForward, { host: 'tenant.example.test', organization_id: 'org-1' });
    assert.deepEqual(result, {
        host: 'tenant.example.test',
        servername: 'tenant.example.test'
    });
});
