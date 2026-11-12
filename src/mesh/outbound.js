/********************************************************************************
 * Copyright (C) 2023 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in commercials or open source under GNU AGPLv3.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 ********************************************************************************/

import { WebSocket } from 'ws';

/**
 * ServerMeshOutbound manages outbound peer discovery, connection establishment,
 * and server events requiring outbound dispatch.
 */
export class ServerMeshOutbound {
    constructor(mesh, WebSocketClass = WebSocket) {
        this.mesh = mesh;
        this.WebSocket = WebSocketClass;
        this.server = null;
        this.meshPort = process.env.MESH_PORT || 8090;
        this.retryTimers = new Map();
        this.pruneTimers = new Map();
        this.peers = [];
        this.reconnectWaitMs = 120000; // Ten attempts: up to 7s backoff plus a 5s handshake.
    }

    closeOrganization(organization_id) {
        for (const [key, timerId] of this.retryTimers) {
            if (!key.startsWith(`${organization_id}:`)) continue;
            clearTimeout(timerId);
            this.retryTimers.delete(key);
        }
    }

    /**
     * Initializes the outbound transport component with host server context and registers IPC handlers.
     * 
     * @param {Object} server - CoCreateServer instance context
     * @returns {Promise<void>}
     */
    async init(server) {
        this.server = server;

        if (this.server && typeof this.server.on === 'function') {
            this.server.on('mesh.outbound.reconnect', (payload) => {
                if (payload && payload.peerIp) {
                    this.connectPeer({ ip: payload.peerIp });
                }
            });

            // Fallback listener for legacy event name
            this.server.on('mesh.client.reconnect', (payload) => {
                if (payload && payload.peerIp) {
                    this.connectPeer({ ip: payload.peerIp });
                }
            });

            this.server.on('mesh.outbound_mesh_relay', (payload) => {
                if (payload) {
                    this.mesh.send(payload.payload ? payload.payload : payload);
                }
            });
        }

        await this.getServers(this.server.organization_id);
    }

    /**
     * Discovers an organization's older running peers and connects them.
     * The peer results are emitted for interested server components.
     */
    async getServers(organization_id = this.server?.organization_id) {
        if (!this.server?.crud || !organization_id) return;
        const ownOrganization = organization_id === this.server.organization_id;

        const readPayload = {
            method: 'object.read',
            array: 'servers',
            filter: {
                query: {
                    _id: { $lt: this.server.id }, 'master.status': 'running',
                    ...(!ownOrganization && { tenant_organization_id: organization_id })
                },
                sort: { _id: 1 },
                limit: 0
            },
            organization_id: this.server.organization_id,
            host: this.server.host
        };

        try {
            const readResult = await this.server.crud.send(readPayload);
            if (!readResult || readResult.success === false ||
                (Array.isArray(readResult.error) ? readResult.error.length : readResult.error) ||
                !(Array.isArray(readResult) || Array.isArray(readResult.object) || Array.isArray(readResult.data))) {
                throw new Error('Peer server inventory is unavailable.');
            }
            const peers = this.normalizePeersList(readResult);
            if (ownOrganization) this.peers = peers;
            this.server.emit?.('organizationPeers', { organization_id, peers });
            for (const targetPeer of peers) {
                this.connectPeer(targetPeer, peers.length, ownOrganization ? undefined : organization_id);
            }
        } catch (err) {
            console.error("[@cocreate/server-mesh/outbound] Failed to discover peers:", err.message);
        }
    }

    /**
     * Initiates secure outbound WebSocket transport link to a remote peer.
     * 
     * @param {Object|string} targetPeer - Peer metadata object or IP string
     * @param {number} totalPeers - Count of peers for calculating eviction delay
     */
    connectPeer(targetPeer, totalPeers = 1, organization_id) {
        const targetIp = typeof targetPeer === 'string' ? targetPeer : targetPeer?.ip;
        const targetId = typeof targetPeer === 'object' ? targetPeer._id : targetIp;
        const connectionKey = `${organization_id || 'control'}:${targetId || targetIp}`;

        if (!targetIp || targetIp === this.server.ip || this.mesh.activeConnections.has(connectionKey)) return;

        console.log(`[@cocreate/server-mesh/outbound] Initiating outbound link to peer: ${targetIp}`);

        let retryCount = 0;
        const maxRetries = 10;

        const connectToPeer = () => {
            if (this.mesh.isClosed) return;
            if (organization_id && !this.mesh.organizations.has(organization_id)) return;

            const path = organization_id
                ? `/${encodeURIComponent(organization_id)}`
                : '';
            const targetUrl = `wss://${targetIp}:${this.meshPort}${path}`;
            const previousSocket = this.mesh.activeConnections.get(connectionKey);
            const clientSocket = new this.WebSocket(targetUrl, [
                encodeURIComponent(JSON.stringify({
                    type: 'peer',
                    ip: this.server.ip,
                    id: this.server.id,
                    token: process.env.CLUSTER_SECRET,
                    sniHost: this.server.host
                }))
            ], {
                servername: this.server.host,
                rejectUnauthorized: false,
                handshakeTimeout: 5000
            });

            clientSocket.isAlive = true;
            clientSocket.remoteAddress = targetIp;
            clientSocket.serverId = targetId || targetIp;
            clientSocket.organization_id = organization_id;
            clientSocket.intentionalClose = false;
            this.mesh.activeConnections.set(connectionKey, clientSocket);
            if (!organization_id) {
                this.mesh.addControlSocket(clientSocket);
            } else {
                this.mesh.addOrganizationSocket(organization_id, clientSocket);
            }
            if (previousSocket) this.mesh.removeSocketFromAllOrgServers(previousSocket);
            this.retryTimers.delete(connectionKey);
            clientSocket.on('pong', () => { clientSocket.isAlive = true; });
            clientSocket.on('ping', () => { clientSocket.pong(); });

            clientSocket.on('open', () => {
                console.log(`[@cocreate/server-mesh/outbound] Outbound connection complete to peer: ${targetIp}`);
                retryCount = 0;

                if (this.retryTimers.has(connectionKey)) {
                    clearTimeout(this.retryTimers.get(connectionKey));
                }
                if (this.pruneTimers.has(targetIp)) clearTimeout(this.pruneTimers.get(targetIp));

                this.retryTimers.delete(connectionKey);
                this.pruneTimers.delete(targetIp);

                this.mesh.flushMessageQueue(clientSocket);
                clientSocket.isPeer = true;
                clientSocket.isControlSocket = !organization_id;
                if (clientSocket.isControlSocket) {
                    this.mesh.syncOrganizations(clientSocket);
                }

            });

            clientSocket.on('close', (code) => {
                if (this.mesh.activeConnections.get(connectionKey) !== clientSocket) return;
                if (this.mesh.isClosed || clientSocket.intentionalClose || code === 1000 ||
                    (organization_id && !this.mesh.organizations.has(organization_id)) || retryCount >= maxRetries) {
                    this.retryTimers.delete(connectionKey);
                    const exhausted = !this.mesh.isClosed && !clientSocket.intentionalClose && code !== 1000 &&
                        (!organization_id || this.mesh.organizations.has(organization_id)) && retryCount >= maxRetries;
                    if (exhausted) {
                        void this.mesh.peerUnavailable(clientSocket, { reason: 'retries exhausted', retries: retryCount, closeCode: code });
                    } else {
                        this.mesh.discardMessageQueue(clientSocket, 'connection closed');
                    }
                    this.mesh.removeSocketFromAllOrgServers(clientSocket);
                    return;
                }
                retryCount++;
                const delay = 5000 + Math.floor(Math.random() * 2000);
                this.retryTimers.set(connectionKey, setTimeout(connectToPeer, delay));
            });

            clientSocket.on('message', (rawData) => {
                if (this.mesh && typeof this.mesh.handleIncomingMessage === 'function') {
                    this.mesh.handleIncomingMessage(rawData, clientSocket);
                }
            });

            clientSocket.on('error', (err) => {
                console.log(`[@cocreate/server-mesh/outbound] Socket transport failure (${targetIp}): ${err.message}`);
            });
        };

        connectToPeer();
    }

    /**
     * Extracts raw dataset arrays from standard database responses.
     */
    normalizePeersList(result) {
        if (!result) return [];
        if (Array.isArray(result)) return result;
        if (Array.isArray(result.object)) return result.object;
        if (Array.isArray(result.data)) return result.data;
        return [];
    }

    /**
     * Clears outbound retry and prune timers on teardown.
     */
    close() {
        for (const timerId of this.retryTimers.values()) clearTimeout(timerId);
        for (const timerId of this.pruneTimers.values()) clearTimeout(timerId);
        this.retryTimers.clear();
        this.pruneTimers.clear();
    }
}
