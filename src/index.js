/********************************************************************************
 * Copyright (C) 2023 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 ********************************************************************************/

import { EventEmitter } from 'node:events';
import Http from 'node:http';
import Https from 'node:https';

import TLS from '@cocreate/tls';
import ServerMesh from './mesh/index.js';
import ServerAutoscaler from '@cocreate/autoscaler';
import ServerTelemetry from '@cocreate/telemetry';
import Config from '@cocreate/config';
import authenticate from '@cocreate/authenticate';
import authorize from '@cocreate/authorize';

import { SocketServer } from '@cocreate/socket';
import CrudServer from '@cocreate/crud-server';
import FileServer from '@cocreate/file/server';
import ApiServer from '@cocreate/api';
import Workflow from '@cocreate/workflows';
import Usage from '@cocreate/usage';

import { ObjectId, getClientInfo } from '@cocreate/utils';

import getInfrastructure from './getInfrastructure.js';
import Manager from './manager.js';
import IPC from './ipc.js';
import Storage from './storage.js';
import State from './state.js';
import Log from './log.js';
import Routing from './routing.js';
import ApiKey from './apikey.js';
import Repository from './repository/index.js';
import Workers from './workers.js';

export { Workers as ClusterManager } from './workers.js';

/**
 * CoCreateServer orchestrates socket handling, API endpoint dispatching,
 * SSL certificates with SNI support, and mesh cluster synchronization.
 * @public
 * @environment server
 */
export class CoCreateServer {
    #emitter = new EventEmitter();

    constructor() {
        this.id = process.env.SERVER_ID || null;
        this.isInitialized = false;

        // Dynamic route registry (Map key: route path, value: request handler callback)
        this.routes = new Map();

        // The local worker collection also owns its process lifecycle.
        this.workers = new Workers(this);
        this.cluster = ServerMesh;

        // Directly bind cluster instance properties to parent server instance
        this.isPrimary = this.workers.isPrimary;

        // Initialize reactive cluster storage bound to cluster IPC promises
        this.storage = Storage.init(this, this.workers.pendingPromises);
        this.authenticate = authenticate.init(this);
        this.authorize = authorize;
    }

    /** Public Event API delegates (keeps prototype clean from standard EventEmitter bloat)  * @private
 * @environment server
 */
    on(event, listener) { this.#emitter.on(event, listener); return this; }
    once(event, listener) { this.#emitter.once(event, listener); return this; }
    off(event, listener) { this.#emitter.off(event, listener); return this; }
    emit(event, ...args) { return this.#emitter.emit(event, ...args); }
    async emitAsync(event, payload) {
        const replies = await Promise.all(this.#emitter.rawListeners(event)
            .map(listener => listener.call(this.#emitter, payload)));
        return replies.find(reply => reply?.uuid === payload.uuid) || payload;
    }


    #closeEdgeListeners() {
        if (this.isPrimary) return;
        if (this.http && this.http.listening) this.http.close();
        if (this.https && this.https.listening) this.https.close();
    }

    closeEdgeListeners() {
        return this.#closeEdgeListeners();
    }

    #restoreEdgeListeners() {
        const isCordoned = this.state?.isCordon || this.state?.isCordoned;
        if (this.isPrimary || this.state?.isDraining || isCordoned || this.state?.isShuttingDown) return;
        const HTTP_PORT = process.env.HTTP_PORT || 80;
        const HTTPS_PORT = process.env.HTTPS_PORT || 443;
        const BIND_IP = process.env.BIND_IP || '0.0.0.0';

        if (this.http && !this.http.listening) this.http.listen(HTTP_PORT, BIND_IP);
        if (this.https && !this.https.listening) this.https.listen(HTTPS_PORT, BIND_IP);
    }

    restoreEdgeListeners() {
        return this.#restoreEdgeListeners();
    }

    async init() {
        if (this.isInitialized) {
            return this;
        }

        this.isInitialized = true;

        this.log = await Log.init(this);
        this.state = await State.init(this);
        this.send = IPC.init(this, this.workers.pendingPromises);

        const config = await Config.prompt({
            'organization_id': '',
            'host': '',
            'workers': '',
            'server_id': ''
        });

        this.organization_id = config.organization_id || null;
        this.host = config.host || null;

        if (this.isPrimary) {
            if (config.server_id) {
                this.id = config.server_id;
            } else {
                this.id = process.env.SERVER_ID || ObjectId().toString();
                await Config.prompt({ server_id: { value: this.id } });
                console.log(`[@cocreate/cluster] [MASTER] Generated & persisted new Server ID: ${this.id}`);
            }
            process.env.SERVER_ID = this.id || "";
        } else {
            this.id = process.env.SERVER_ID;
        }

        // Configure the local worker collection
        this.workers.configureWorkers(config.workers);

        this.crud = await CrudServer.init(this);
        try {
            await ApiKey.init(this);
        } catch {
            console.error('[@cocreate/cluster] Unable to initialize the node organization API key.');
        }
        this.authenticate = authenticate.init(this);
        this.api = await ApiServer.init(this);
        this.workflow = await Workflow.init(this);
        this.manager = await Manager.init(this);
        if (!this.isPrimary) await this.cluster.init(this);

        if (this.isPrimary) {
            console.log(`[@cocreate/cluster] [MASTER] Orchestrator running with PID: ${process.pid} (Server ID: ${this.id})`);
            this.location = await getClientInfo();
            this.ip = this.location.ip;
            this.infrastructure = await getInfrastructure(this.id);
            await this.cluster.init(this);
            this.autoscaler = await ServerAutoscaler.init(this);

            // Reset logs array for a fresh logging session on master startup
            await this.crud.send(this.state.getServerObject({ logs: [] }));

            // Replace the persisted worker profile before the new workers start.
            let persistedServer;

            try {
                const response = await this.crud.send({
                    method: 'object.read',
                    array: 'servers',
                    filter: { query: { _id: this.id }, limit: 1 },
                    organization_id: this.organization_id,
                    host: this.host
                });
                if (response?.error?.length || (response?.error && !Array.isArray(response.error)) ||
                    !Array.isArray(response?.object))
                    throw new Error('Unable to read the previous worker profile.');

                persistedServer = response.object[0];
                if (persistedServer?.manager) {
                    const { organization_id, host, apiKey } = persistedServer.manager;
                    Object.assign(this.manager, { organization_id, host, apiKey });
                }
            } catch (error) {
                console.warn('[@cocreate/cluster] Could not clear unused worker fields:', error);
            }

            const initialWorkerStates = this.workers.initialState(persistedServer);
            const workerResponse = await this.crud.send(this.state.getServerObject(initialWorkerStates));
            if (workerResponse?.error?.length || (workerResponse?.error && !Array.isArray(workerResponse.error)))
                console.warn('[@cocreate/cluster] Could not update the worker profile:', workerResponse.error);
            await this.state.save('running');

            this.routing = await Routing.init(this);
            this.repository = await Repository.init(this);

            if (await this.workers.setupMasterCluster()) {
                return this;
            }
        }

        this.isWorker = this.workers.isWorker;

        if (this.isWorker) {
            this.workerId = this.workers.workerId;

            if (!this.isPrimary) {
                console.log(`[@cocreate/cluster] [WORKER ${this.workerId}] running with PID: ${process.pid} (Server ID: ${this.id})`);
            }

            this.telemetry = await ServerTelemetry.init(this);
            this.usage = await Usage.init(this);

            if (!this.ip) {
                this.ip = process.env.SERVER_IP || (await getClientInfo()).ip;
            }

            if (!this.infrastructure) {
                try {
                    this.infrastructure = process.env.SERVER_INFRASTRUCTURE
                        ? JSON.parse(process.env.SERVER_INFRASTRUCTURE)
                        : await getInfrastructure(this.id);
                } catch (error) {
                    this.infrastructure = await getInfrastructure(this.id);
                }
            }

            if (!this.location) {
                try {
                    this.location = process.env.SERVER_LOCATION
                        ? JSON.parse(process.env.SERVER_LOCATION)
                        : await getClientInfo(this.id);
                } catch (error) {
                    this.location = await getClientInfo(this.id);
                }
            }

            // Initialize certificates module on worker
            this.tls = await TLS.init(this);

            // Create HTTPS options with dynamic SNICallback for multi-tenant domains
            const httpsOptions = this.tls.createHttpsOptions(this);
            this.https = Https.createServer(httpsOptions, (req, res) => this.#handleEdgeRequest(req, res));

            const HTTP_PORT = process.env.HTTP_PORT || 80;
            const HTTPS_PORT = process.env.HTTPS_PORT || 443;
            const BIND_IP = process.env.BIND_IP || '0.0.0.0';

            this.http = Http.createServer((req, res) => {
                const requestUrl = req.url || '/';

                // 1. Allow ACME challenges to pass through over unencrypted HTTP
                if (requestUrl.startsWith('/.well-known/acme-challenge/')) {
                    return this.#handleEdgeRequest(req, res);
                }

                // 2. Force a 301 redirect to HTTPS for all traffic (preserving non-standard ports)
                const rawHost = req.headers.host || 'localhost';
                const host = rawHost.split(':')[0];
                const portSuffix = (HTTPS_PORT && String(HTTPS_PORT) !== '443') ? `:${HTTPS_PORT}` : '';

                res.writeHead(301, { "Location": `https://${host}${portSuffix}${requestUrl}` });
                res.end();
            });

            this.wsManager = new SocketServer(this);
            this.socket = this.wsManager.collection;
            this.wsManager.on('api', this.api.send);
            this.wsManager.on('workflow', this.workflow.run);
            this.files = await FileServer.init(this);

            const reportListening = (state) => {
                if (process.connected) {
                    process.send({ type: 'listening_state', state });
                }
            };

            this.http.on('listening', () => reportListening(true));
            this.http.on('close', () => reportListening(false));
            this.http.listen(HTTP_PORT, BIND_IP, () => {
                console.log(`[@cocreate/cluster] [WORKER ${this.workerId}] HTTP listening on ${BIND_IP}:${HTTP_PORT}`);
            });

            this.https.listen(HTTPS_PORT, BIND_IP, () => {
                console.log(`[@cocreate/cluster] [WORKER ${this.workerId}] HTTPS listening on ${BIND_IP}:${HTTPS_PORT}`);
            });

            // Publish worker active running state object to DB upon creation/startup
            await this.state.save('running');
        }

        if (this.isPrimary && this.routing) {
            await Promise.all([this.http, this.https].map(listener => listener.listening
                ? Promise.resolve()
                : new Promise(resolve => listener.once('listening', resolve))));
            try {
                await this.routing.add();
            } catch (error) {
                console.error('[@cocreate/cluster] Failed to register node routing:', error.message);
            }
        }

        if (this.isPrimary) {
            try { await this.repository?.publish(); }
            catch { console.error('[@cocreate/cluster] Initial repository publication failed; inspect its reservation and retry on a later startup.'); }
        }

        return this;
    }

    /**
     * Internal private request handler that evaluates registered user routes,
     * matching exact paths or wildcard directory patterns from this.routes Map.
     * @param {Http.IncomingMessage} req 
     * @param {Http.ServerResponse} res 
     */
    async #handleEdgeRequest(req, res) {
        try {
            const rawUrl = req.url || '/';
            const [reqPath] = rawUrl.split('?');

            if (
                !reqPath.startsWith('/.well-known/acme-challenge/') &&
                this.tls?.checkOrganizationAccess
            ) {
                const access = await this.tls.checkOrganizationAccess(req.headers.host);
                if (access.allowed === false) {
                    res.writeHead(402, {
                        'Content-Type': 'application/json; charset=utf-8',
                        'Cache-Control': 'no-store',
                        'Account-Balance': 'false'
                    });
                    res.end(JSON.stringify({
                        error: 'Organization balance depleted',
                        organizationBalance: false,
                        balance: access.balance,
                        currency: access.currency
                    }));
                    return;
                }
            }

            // 1. Direct O(1) exact match lookup or directory/wildcard pattern matching in this.routes Map
            let handler = this.routes.get(reqPath);

            if (!handler) {
                for (const [routePath, routeHandler] of this.routes.entries()) {
                    if (routePath === '*' || (routePath.endsWith('*') && reqPath.startsWith(routePath.slice(0, -1)))) {
                        handler = routeHandler;
                        break;
                    }
                }
            }

            if (handler && typeof handler === 'function') {
                await handler(req, res);

                // If handler responded or started sending headers, conclude routing cycle
                if (res.writableEnded || res.headersSent) {
                    return;
                }
            }

            if (reqPath === '/manager' || reqPath.startsWith('/manager/')) {
                return this.manager.http(req, res);
            }
            if (reqPath === '/api' || reqPath.startsWith('/api/')) {
                return this.api.http(req, res);
            }
            if (reqPath.startsWith('/workflows/')) {
                return this.workflow.http(req, res);
            }

            // 2. Default route handling: API webhooks
            if (reqPath.startsWith("/webhooks/")) {
                return this.api.webhook(req, res);
            }

            // 3. Default route handling: Static file server
            return this.files.send(req, res);
        } catch (error) {
            console.error('[@cocreate/cluster] Edge request routing error:', error);
            if (!res.headersSent) {
                res.writeHead(500, { "Content-Type": "text/plain" });
            }
            res.end("Internal Error: Request routing failed");
        }
    }
}

export default new CoCreateServer();

/**
 *
 * @name default
 * @kind member
 * @public
 * @environment server
 */

/**
 * A secure, stateless, clustered headless server built for high-performance multi-tenancy, real-time collaboration, and sandboxed AST execution.
 *
 * @module server
 * @environment server
 */
