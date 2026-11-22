import Net from 'node:net';

const HTTP_PORT = Number(process.env.HTTP_PORT || 80);
const HTTPS_PORT = Number(process.env.HTTPS_PORT || 443);
const BIND_IP = process.env.BIND_IP || '0.0.0.0';
const CONNECT_TIMEOUT = 5000;

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
        application.emit('connection', socket);
    });
}

/** Creates public TCP listeners and routes accepted sockets locally or to peers. */
async function init(server) {
    if (!server) throw new TypeError('TCP.init requires a server instance.');
    if (server.isPrimary) throw new Error('TCP listeners can only be initialized in worker processes.');

    server.tcp = {
        peers: [], // Active server records are loaded before the TCP listeners start.
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
