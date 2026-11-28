import Net from 'node:net';
import Config from '@cocreate/config';

const HTTP_PORT = Number(process.env.HTTP_PORT || 80);
const HTTPS_PORT = Number(process.env.HTTPS_PORT || 443);
const BIND_IP = process.env.BIND_IP || '0.0.0.0';
const CONNECT_TIMEOUT = 5000;
const CLIENT_HELLO_TIMEOUT = 5000;
const CLIENT_HELLO_MAX_BYTES = 64 * 1024;

/** Read an unsigned 16-bit integer if the requested range is available. */
function readUInt16(buffer, offset) {
    return offset + 2 <= buffer.length ? buffer.readUInt16BE(offset) : null;
}

/** Parse the SNI name from a complete TLS ClientHello handshake message. */
function parseClientHello(handshake) {
    if (handshake.length < 4 || handshake[0] !== 1) return { complete: true, serverName: null };
    const bodyLength = handshake.readUIntBE(1, 3);
    if (handshake.length < 4 + bodyLength) return { complete: false, serverName: null };

    const body = handshake.subarray(4, 4 + bodyLength);
    let offset = 34; // legacy_version and random
    if (offset >= body.length) return { complete: true, serverName: null };

    const sessionIdLength = body[offset++];
    offset += sessionIdLength;
    const cipherLength = readUInt16(body, offset);
    if (cipherLength === null) return { complete: true, serverName: null };
    offset += 2 + cipherLength;

    if (offset >= body.length) return { complete: true, serverName: null };
    const compressionLength = body[offset++];
    offset += compressionLength;
    const extensionsLength = readUInt16(body, offset);
    if (extensionsLength === null) return { complete: true, serverName: null };
    offset += 2;
    const extensionsEnd = Math.min(offset + extensionsLength, body.length);

    while (offset + 4 <= extensionsEnd) {
        const type = readUInt16(body, offset);
        const length = readUInt16(body, offset + 2);
        if (type === null || length === null || offset + 4 + length > extensionsEnd) break;
        offset += 4;
        const extensionEnd = offset + length;

        if (type === 0 && offset + 2 <= extensionEnd) {
            const namesLength = readUInt16(body, offset);
            let nameOffset = offset + 2;
            const namesEnd = namesLength === null ? offset : Math.min(nameOffset + namesLength, extensionEnd);
            while (nameOffset + 3 <= namesEnd) {
                const nameType = body[nameOffset++];
                const nameLength = readUInt16(body, nameOffset);
                if (nameLength === null) break;
                nameOffset += 2;
                if (nameOffset + nameLength > namesEnd) break;
                if (nameType === 0 && nameLength > 0) {
                    const serverName = body.toString('utf8', nameOffset, nameOffset + nameLength)
                        .trim().toLowerCase().replace(/\.$/, '');
                    return { complete: true, serverName: serverName || null };
                }
                nameOffset += nameLength;
            }
        }

        offset = extensionEnd;
    }

    return { complete: true, serverName: null };
}

/** Extract SNI from a TLS record stream without terminating or changing TLS. */
export function parseClientHelloServerName(input) {
    const buffer = Buffer.isBuffer(input) ? input : Buffer.from(input);
    const handshakeChunks = [];
    let handshakeLength = 0;
    let offset = 0;

    while (offset < buffer.length) {
        if (buffer.length - offset < 5) return { complete: false, serverName: null };
        if (buffer[offset] !== 22) return { complete: true, serverName: null };
        const recordLength = readUInt16(buffer, offset + 3);
        if (recordLength === null || recordLength > 18432) return { complete: true, serverName: null };
        if (buffer.length - offset < 5 + recordLength) return { complete: false, serverName: null };

        const record = buffer.subarray(offset + 5, offset + 5 + recordLength);
        handshakeChunks.push(record);
        handshakeLength += record.length;
        if (handshakeLength > CLIENT_HELLO_MAX_BYTES) return { complete: true, serverName: null };
        const handshake = Buffer.concat(handshakeChunks, handshakeLength);
        if (handshake.length >= 4) {
            const parsed = parseClientHello(handshake);
            if (parsed.complete) return parsed;
        }
        offset += 5 + recordLength;
    }

    return { complete: false, serverName: null };
}

/** Peek at the ClientHello and put every consumed byte back for the HTTPS server. */
export function readClientHelloServerName(socket, { timeoutMs = CLIENT_HELLO_TIMEOUT, maxBytes = CLIENT_HELLO_MAX_BYTES } = {}) {
    socket.pause();
    return new Promise(resolve => {
        const chunks = [];
        let size = 0;
        let settled = false;
        const timer = setTimeout(() => finish(null), timeoutMs);
        timer.unref?.();

        const cleanup = () => {
            clearTimeout(timer);
            socket.off('data', onData);
            socket.off('error', onEnd);
            socket.off('close', onEnd);
        };
        const finish = serverName => {
            if (settled) return;
            settled = true;
            cleanup();
            socket.pause();
            const initialBytes = Buffer.concat(chunks, size);
            if (initialBytes.length && !socket.destroyed) socket.unshift(initialBytes);
            resolve(serverName);
        };
        const onEnd = () => finish(null);
        const onData = chunk => {
            const bytes = Buffer.from(chunk);
            chunks.push(bytes);
            size += bytes.length;
            if (size > maxBytes) return finish(null);
            const parsed = parseClientHelloServerName(Buffer.concat(chunks, size));
            if (parsed.complete) finish(parsed.serverName);
        };

        socket.on('data', onData);
        socket.once('error', onEnd);
        socket.once('close', onEnd);
        socket.resume();
    });
}

/** Resolve an SNI hostname through the existing organization CRUD helper. */
export async function resolveOrganizationForHost(server, host) {
    if (!host || typeof server?.crud?.getOrganization !== 'function') return null;
    try {
        let organization = await server.crud.getOrganization({ host });
        if (organization?.object) {
            organization = Array.isArray(organization.object) ? organization.object[0] : organization.object;
        } else if (Array.isArray(organization)) {
            organization = organization[0];
        }
        return organization?._id ? { host, organization_id: String(organization._id) } : null;
    } catch (error) {
        console.error('[@cocreate/cluster] Failed to resolve organization for TCP affinity:', error.message);
        return null;
    }
}

/** Resolve organization context only for locally accepted affinity connections. */
export async function inspectOrganizationAffinity(server, socket) {
    if (server.state?.isProxying || server.tcp?.affinity !== 'organization') return null;
    const host = await readClientHelloServerName(socket);
    if (!host) return null;
    const organization = await resolveOrganizationForHost(server, host);
    if (organization) server.tcp.organizationBySocket.set(socket, organization);
    return organization;
}

async function loadPeers(server) {
    try {
        const response = await server.crud.send({
            method: 'object.read',
            array: 'servers',
            filter: {
                query: { 'primary.status': 'running' },
                sort: { _id: 1 },
                limit: 0
            },
            organization_id: server.organization_id,
            host: server.host
        });
        const hasErrors = Array.isArray(response?.error)
            ? response.error.length > 0
            : Boolean(response?.error);
        const error = Array.isArray(response?.error) ? response.error[0] : response?.error;
        if (response?.success === false || hasErrors || !Array.isArray(response?.object)) {
            throw new Error(error || 'Peer server inventory is unavailable.');
        }
        server.tcp.peers = response.object
            .filter(peer => peer?._id && String(peer._id) !== String(server.id) && peer.ip)
            .map(({ _id, ip }) => ({ _id, ip }));
    } catch (error) {
        server.tcp.peers = [];
        console.error('[@cocreate/cluster] Failed to load TCP peers:', error.message);
    }
}

function forwardConnection(server, client, port) {
    const peers = Array.isArray(server.tcp.peers) ? server.tcp.peers : [];
    let index = 0;
    let activeUpstream;

    client.once('close', () => activeUpstream?.destroy());
    client.on('error', () => activeUpstream?.destroy());

    const tryNextPeer = () => {
        if (client.destroyed) return;
        const peer = peers[index++];
        if (!peer) {
            client.destroy();
            return;
        }

        const upstream = Net.connect(port, peer.ip);
        activeUpstream = upstream;
        let connected = false;
        let failed = false;
        const timeout = setTimeout(() => upstream.destroy(new Error('Peer connection timed out.')), CONNECT_TIMEOUT);
        timeout.unref?.();

        const failAttempt = () => {
            if (connected || failed) return;
            failed = true;
            clearTimeout(timeout);
            upstream.destroy();
            tryNextPeer();
        };

        upstream.once('connect', () => {
            connected = true;
            clearTimeout(timeout);
            if (client.destroyed) {
                upstream.destroy();
                return;
            }
            client.pipe(upstream);
            upstream.pipe(client);
        });

        upstream.on('error', () => {
            clearTimeout(timeout);
            if (!connected) {
                failAttempt();
            } else {
                client.destroy();
            }
        });

        upstream.once('close', () => {
            clearTimeout(timeout);
            if (!connected && !client.destroyed) failAttempt();
        });

    };

    tryNextPeer();
}

function createListener(server, application, port) {
    return Net.createServer(socket => {
        if (server.state?.isProxying) {
            forwardConnection(server, socket, port);
            return;
        }
        if (port === HTTPS_PORT && server.tcp.affinity === 'organization') {
            void inspectOrganizationAffinity(server, socket)
                .catch(error => console.error('[@cocreate/cluster] TCP affinity inspection failed:', error.message))
                .finally(() => application.emit('connection', socket));
            return;
        }
        application.emit('connection', socket);
    });
}

/** Creates public TCP listeners and routes accepted sockets locally or to peers. */
async function init(server) {
    if (!server) throw new TypeError('TCP.init requires a server instance.');
    if (server.isPrimary) throw new Error('TCP listeners can only be initialized in worker processes.');

    server.tcp = {
        peers: [], // Active server records are loaded before the TCP listeners start.
        affinity: Config.get('tcp.affinity') === 'organization' ? 'organization' : null,
        organizationBySocket: new WeakMap(),
        http: null,
        https: null,
        listen,
        close
    };
    if (!server.http || !server.https) {
        throw new Error('TCP.init requires initialized HTTP and HTTPS application servers.');
    }

    server.tcp.http = createListener(server, server.http, HTTP_PORT);
    server.tcp.https = createListener(server, server.https, HTTPS_PORT);

    await loadPeers(server);

    const reportReadiness = () => {
        const listening = server.tcp.http.listening && server.tcp.https.listening;
        if (listening === server.tcp.reportedListening) return;
        server.tcp.reportedListening = listening;
        if (process.connected && server.send) {
            Promise.resolve(server.send({ method: 'worker.listening', state: listening }))
                .catch(error => console.error('[@cocreate/cluster] Failed to report worker listener state:', error.message));
        }
    };
    server.tcp.http.on('listening', reportReadiness);
    server.tcp.https.on('listening', reportReadiness);
    server.tcp.http.on('close', reportReadiness);
    server.tcp.https.on('close', reportReadiness);
    server.tcp.http.on('error', error => console.error('[@cocreate/cluster] HTTP TCP listener error:', error.message));
    server.tcp.https.on('error', error => console.error('[@cocreate/cluster] HTTPS TCP listener error:', error.message));

    if (!server.state?.isCordon && !server.state?.isDraining && !server.state?.isShuttingDown) {
        await listen();
    }
    return server.tcp;

    function listen() {
        const listeners = [server.tcp.http, server.tcp.https];
        return Promise.all(listeners.map(listener => {
            if (listener.listening) return Promise.resolve();
            return new Promise((resolve, reject) => {
                const onError = error => {
                    listener.off('listening', onListening);
                    reject(error);
                };
                const onListening = () => {
                    listener.off('error', onError);
                    resolve();
                };
                listener.once('error', onError);
                listener.once('listening', onListening);
                listener.listen(listener === server.tcp.http ? HTTP_PORT : HTTPS_PORT, BIND_IP);
            });
        })).then(() => server.tcp);
    }

    function close() {
        for (const listener of [server.tcp.http, server.tcp.https]) {
            if (listener?.listening) listener.close();
        }
    }
}

export default { init };
