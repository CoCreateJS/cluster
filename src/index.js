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
import cluster from 'node:cluster';
import os from 'node:os';

import TLS from '@cocreate/tls';
import ServerMesh from './mesh/index.js';
import ServerAutoscaler from '@cocreate/autoscaler';
import ServerTelemetry from '@cocreate/telemetry';
import Config from '@cocreate/config';
import authenticate from '@cocreate/authenticate';
import authorize from '@cocreate/authorize';

import SocketServer from '@cocreate/socket-server';
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

/**
 * ClusterManager handles Node.js cluster process lifecycle, worker forks,
 * worker exit respawning, IPC signal routing, and fallback master web servers.
 * @public
 * @environment server
 */
export class ClusterManager {
    #emitter = new EventEmitter();

    /**
     * Creates an instance of ClusterManager.
     * @param {CoCreateServer} server - Reference to parent server instance.
 * @public
 * @environment server
 */
    constructor(server) {
        this.server = server;
        this.isPrimary = cluster.isPrimary;
        this.workers = new Map();
        this.pendingPromises = new Map();
        this.totalWorkers = 0;
        this.workerId = this.isPrimary ? null : (parseInt(process.env.WORKER_ID, 10) || 1);
    }

    /** Delegate event listeners without polluting top-level prototype  * @private
 * @environment server
 */
    on(event, listener) { this.#emitter.on(event, listener); return this; }
    once(event, listener) { this.#emitter.once(event, listener); return this; }
    off(event, listener) { this.#emitter.off(event, listener); return this; }
    emit(event, ...args) { return this.#emitter.emit(event, ...args); }

    /**
     * Resolves worker count configuration and assigns worker identity.
     * @param {string|number|boolean} configuredWorkers - Worker configuration value.
 * @private
 * @environment server
 */
    configureWorkers(configuredWorkers) {
        if (configuredWorkers !== false && configuredWorkers !== 'false') {
            const parsedWorkers = parseInt(configuredWorkers, 10);
            if (parsedWorkers === -1) {
                this.totalWorkers = Math.max(1, os.cpus().length - 1);
            } else {
                this.totalWorkers = parsedWorkers || os.cpus().length;
            }
        }

        if (this.isPrimary && this.totalWorkers <= 1) {
            this.workerId = 1;
        }

        // Sync resolved worker identity and total worker count to parent server
        this.server.totalWorkers = this.totalWorkers;
        this.server.workerId = this.workerId;
    }

    /**
     * Initializes master cluster forks and listens for exit signals.
 * @private
 * @environment server
 */
    async setupMasterCluster() {
        if (!this.server.state)
            this.server.state = await State.init(this.server);
        if (!this.isPrimary || this.totalWorkers <= 1) return;

        console.log(`[@cocreate/cluster] [MASTER] Orchestrator creating ${this.totalWorkers} workers.`);

        const generateWorkerEnv = (wId) => ({
            WORKER_ID: String(wId),
            SERVER_ID: String(this.server.id || ''),
            SERVER_IP: String(this.server.ip || '127.0.0.1'),
            SERVER_INFRASTRUCTURE: JSON.stringify(this.server.infrastructure || {}),
            SERVER_LOCATION: JSON.stringify(this.server.location || {}),
            SERVER_MANAGER: JSON.stringify(this.server.manager || {})
        });

        const handleWorkerListeningMsg = (workerId, msg) => {
            if (msg && msg.type === 'listening_state') {
                const w = this.workers.get(workerId);
                if (w) {
                    w.isListening = Boolean(msg.state);
                    if (this.server.state?.evaluateMasterFallback) {
                        this.server.state.evaluateMasterFallback();
                    }

                    const workersReady = this.workers.size === this.totalWorkers &&
                        [...this.workers.values()].every((item) => item.isListening);
                    if (workersReady && this.server.state?.save) {
                        this.server.state.save('running').then(async () => {
                            try { await this.server.routing?.add(); }
                            catch { console.error('[@cocreate/cluster] Failed to register node routing.'); }
                            await this.server.repository?.publish();
                        }).catch((error) => {
                            console.error(
                                '[@cocreate/cluster] Failed to publish master readiness or initial repository data:',
                                error.message
                            );
                        });
                    }
                }
            }
        };

        for (let i = 0; i < this.totalWorkers; i++) {
            const currentWorkerId = i + 1;
            const worker = cluster.fork(generateWorkerEnv(currentWorkerId));

            worker.on('error', (err) => {
                console.error(`[@cocreate/cluster] [MASTER] Error on Worker ${currentWorkerId}:`, err.message);
            });

            worker.on('message', (msg) => handleWorkerListeningMsg(currentWorkerId, msg));

            this.workers.set(currentWorkerId, {
                id: currentWorkerId,
                pid: worker.process.pid,
                process: worker,
                isListening: false
            });
        }

        cluster.on('exit', async (worker, code, signal) => {
            console.log(`[@cocreate/cluster] [MASTER] Worker ${worker.process.pid} exited (Code: ${code}, Signal: ${signal})`);

            let deadWorkerId = null;
            for (let [id, w] of this.workers.entries()) {
                if (w.pid === worker.process.pid) {
                    deadWorkerId = id;
                    this.workers.delete(id);
                    break;
                }
            }

            if (this.server.state?.evaluateMasterFallback) {
                this.server.state.evaluateMasterFallback();
            }

            if (deadWorkerId && this.server.crud) {
                await this.server.crud.send(this.server.state.getServerObject({
                    [`worker${deadWorkerId}`]: {
                        pid: null,
                        status: 'stopped',
                        isCordoned: false,
                        isShedding: false,
                        isDraining: false
                    }
                }));
            }

            const state = this.server.state;
            const isCordoned = state?.isCordon || state?.isCordoned;

            if (state?.isShuttingDown || state?.isDraining || isCordoned) {
                console.log(`[@cocreate/cluster] [MASTER] Worker ${worker.process.pid} removed. Active workers remaining: ${this.workers.size}`);

                if (this.workers.size === 0) {
                    console.log("[@cocreate/cluster] [MASTER] All workers successfully drained.");
                    if (state?.isShuttingDown) await state.finalizeShutdown?.();
                    if (typeof this.server.state.shutdownCompletionCallback === 'function') {
                        this.server.state.shutdownCompletionCallback();
                    } else if (state?.isShuttingDown) {
                        process.exit(0);
                    }
                }
                return;
            }

            const targetId = deadWorkerId || (this.workers.size + 1);

            setTimeout(() => {
                console.log(`[@cocreate/cluster] [MASTER] Respawning Worker ${targetId}...`);
                const newWorker = cluster.fork(generateWorkerEnv(targetId));

                newWorker.on('error', (err) => {
                    console.error(`[@cocreate/cluster] [MASTER] Error on replacement Worker ${targetId}:`, err.message);
                });

                newWorker.on('message', (msg) => handleWorkerListeningMsg(targetId, msg));

                this.workers.set(targetId, {
                    id: targetId,
                    pid: newWorker.process.pid,
                    process: newWorker,
                    isListening: false
                });
            }, 2000);
        });
    }
}

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

        // Encapsulate cluster management cleanly under dedicated property
        this.cluster = new ClusterManager(this);

        // Directly bind cluster instance properties to parent server instance
        this.isPrimary = this.cluster.isPrimary;
        this.totalWorkers = this.cluster.totalWorkers;

        // Initialize reactive cluster storage bound to cluster IPC promises
        this.storage = Storage.init(this, this.cluster.pendingPromises);
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
        this.send = IPC.init(this, this.cluster.pendingPromises);

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

        // Configure workers in ClusterManager
        this.cluster.configureWorkers(config.workers);

        this.crud = await CrudServer.init(this);
        try {
            await ApiKey.init(this);
        } catch {
            console.error('[@cocreate/cluster] Unable to initialize the node organization API key.');
        }
        this.authenticate = authenticate.init(this);
        this.mesh = ServerMesh;
        this.api = await ApiServer.init(this);
        this.workflow = await Workflow.init(this);
        this.manager = await Manager.init(this);

        if (this.isPrimary) {
            console.log(`[@cocreate/cluster] [MASTER] Orchestrator running with PID: ${process.pid} (Server ID: ${this.id})`);
            this.location = await getClientInfo();
            this.ip = this.location.ip;
            this.infrastructure = await getInfrastructure(this.id);
            await this.mesh.init(this);
            this.autoscaler = await ServerAutoscaler.init(this);

            // Reset logs array for a fresh logging session on master startup
            await this.crud.send(this.state.getServerObject({ logs: [] }));

            // Replace the persisted worker profile before the new workers start.
            const workerCount = Math.max(1, this.totalWorkers);
            const initialWorkerStates = { totalWorkers: this.totalWorkers };
            for (let i = 1; i <= workerCount; i++) {
                initialWorkerStates[`worker${i}`] = {
                    pid: null,
                    status: 'stopped',
                    isCordoned: false,
                    isShedding: false,
                    isDraining: false
                };
            }

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

                const persistedServer = response.object[0];
                if (persistedServer?.manager) {
                    const { organization_id, host, apiKey } = persistedServer.manager;
                    Object.assign(this.manager, { organization_id, host, apiKey });
                }

                for (const field of Object.keys(response?.object?.[0] || {})) {
                    const match = /^worker([1-9]\d*)$/.exec(field);
                    if (match && Number(match[1]) > workerCount)
                        initialWorkerStates[`$unset.${field}`] = true;
                }
            } catch (error) {
                console.warn('[@cocreate/cluster] Could not clear unused worker fields:', error);
            }

            const workerResponse = await this.crud.send(this.state.getServerObject(initialWorkerStates));
            if (workerResponse?.error?.length || (workerResponse?.error && !Array.isArray(workerResponse.error)))
                console.warn('[@cocreate/cluster] Could not update the worker profile:', workerResponse.error);
            await this.state.save('running');

            // Route mesh commands to IPC
            const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];
            for (const eventName of systemEvents) {
                this.mesh.on(eventName, (payload) => {
                    console.log(`[@cocreate/cluster] [MASTER] Received remote mesh command: "${eventName}"`);
                    this.send(payload);
                });
            }

            this.workers = this.cluster.workers; // Direct reference to the workers Map instance
            this.routing = await Routing.init(this);
            this.repository = await Repository.init(this);

            if (this.totalWorkers > 1) {
                await this.cluster.setupMasterCluster();
                return this;
            }
        }

        this.isWorker = (!this.isPrimary || this.totalWorkers <= 1);

        if (this.isWorker) {
            this.workerId = this.cluster.workerId;

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
