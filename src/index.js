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
import { exec } from 'node:child_process';

import Acme from '@cocreate/acme';
import ServerMesh from '@cocreate/server-mesh';
import ServerAutoscaler from '@cocreate/server-autoscaler';
import ServerTelemetry from '@cocreate/server-telemetry';
import Config from '@cocreate/config';
import authenticate from '@cocreate/authenticate';
import authorize from '@cocreate/authorize';

import SocketServer from '@cocreate/socket-server';
import CrudServer from '@cocreate/crud-server';
import LazyLoader from '@cocreate/lazy-loader';
import FileServer from '@cocreate/file-server';
import ApiServer from '@cocreate/api';

import { ObjectId, uuid } from '@cocreate/utils';

// Dynamic environmental resolvers
import resolveInfrastructure from './getInfrastructure.js';
import getIP from './getIp.js';

const isPrimary = cluster.isPrimary !== undefined ? cluster.isPrimary : cluster.isMaster;

const pendingPromises = new Map();
const activeWorkers = new Map();

// Track overall system state during shutdowns to prevent concurrent signal re-evaluation
let isPrimaryShuttingDown = false;
let isMeshClosed = false; // Dual-lock flag to guarantee DB prunes finish before process exits
let primaryShutdownTimeout = null;
let shutdownCompletionCallback = null;

class ClusterStateManager extends EventEmitter {
    constructor(serverCtx) {
        super();
        this.serverCtx = serverCtx;
        this.store = new Map();
        
        // Listen for top-down state broadcasts from Master
        if (!this.serverCtx.isPrimary) {
            this.serverCtx.on('cluster_state_sync', (payload) => {
                if (payload && payload.key !== undefined) {
                    if (payload.value === undefined) {
                        this.store.delete(payload.key);
                        this.emit(`delete:${payload.key}`);
                        this.emit('delete', { key: payload.key });
                    } else {
                        this.store.set(payload.key, payload.value);
                        this.emit(`change:${payload.key}`, payload.value);
                        this.emit('change', { key: payload.key, value: payload.value });
                    }
                } else if (payload && payload.clear) {
                    this.store.clear();
                    this.emit('clear');
                }
            });
        }
    }

    async set(key, value) {
        if (this.serverCtx.isPrimary) {
            this.store.set(key, value);
            this.emit(`change:${key}`, value);
            this.emit('change', { key, value });
            
            this.serverCtx.send({ 
                method: 'cluster_state_sync', key, value, broadcast: true, master: false 
            });
            return true;
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                pendingPromises.set(messageId, { resolve, reject });
                process.send({ id: this.serverCtx.id, messageId, method: 'cluster_state_set', key, value });
            });
        }
    }

    async get(key) {
        if (this.serverCtx.isPrimary) {
            return this.store.get(key);
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                pendingPromises.set(messageId, { resolve, reject });
                process.send({ id: this.serverCtx.id, messageId, method: 'cluster_state_get', key });
            });
        }
    }

    async delete(key) {
        if (this.serverCtx.isPrimary) {
            const result = this.store.delete(key);
            this.emit(`delete:${key}`);
            this.emit('delete', { key });
            this.serverCtx.send({ method: 'cluster_state_sync', key, value: undefined, broadcast: true, master: false });
            return result;
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                pendingPromises.set(messageId, { resolve, reject });
                process.send({ id: this.serverCtx.id, messageId, method: 'cluster_state_delete', key });
            });
        }
    }

    async size() {
        if (this.serverCtx.isPrimary) return this.store.size;
        return new Promise((resolve, reject) => {
            const messageId = uuid();
            pendingPromises.set(messageId, { resolve, reject });
            process.send({ id: this.serverCtx.id, messageId, method: 'cluster_state_size' });
        });
    }

    async clear() {
        if (this.serverCtx.isPrimary) {
            this.store.clear();
            this.emit('clear');
            this.serverCtx.send({ method: 'cluster_state_sync', clear: true, broadcast: true, master: false });
            return true;
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                pendingPromises.set(messageId, { resolve, reject });
                process.send({ id: this.serverCtx.id, messageId, method: 'cluster_state_clear' });
            });
        }
    }
}

export class CoCreateServer extends EventEmitter {
    constructor() {
        super();
        this.id = process.env.SERVER_ID || null; // Will be properly initialized via Config in init()
        this.isPrimary = isPrimary;
        
        if (isPrimary) {
            this.workers = activeWorkers;
        } else {
            this.workerId = parseInt(process.env.WORKER_ID, 10) || 1;
        }
        
        this.totalWorkers = 0;
        this.workerStorage = null;
        this.isInitialized = false;
        
        this.organization_id = null;
        this.host = null;
        this.ip = null;
        this.infrastructure = null;

        this.state = {
            status: 'INITIALIZING', // INITIALIZING, ACTIVE, CORDONED, DRAINING, SHUTTING_DOWN
            isCordon: false,
            isShedding: false,
            isDraining: false,
            sheddingOrgs: new Set()
        };
        
        // Initialize robust reactive cluster storage
        this.store = new ClusterStateManager(this);
        // Backwards compatibility for legacy workerStorage references
        this.workerStorage = this.store;

        // Fallback servers for the Primary node
        this.fallbackHttpServer = null;
        this.fallbackHttpsServer = null;

        this.on('cordon', (payload) => this.cordon(payload, true));
        this.on('shed', (payload) => this.shed(payload, true));
        this.on('drain', (payload) => this.drain(payload, true));
        this.on('restart', (payload) => this.restart(payload, true));
        this.on('reboot', (payload) => this.reboot(payload, true));
        this.on('shutdown', (payload) => this.shutdown(payload, true));

        this.http = Http.createServer((req, res) => {
            const requestUrl = req.url || '/';
            
            // 1. Allow ACME challenges to pass through over unencrypted HTTP
            if (requestUrl.startsWith('/.well-known/acme-challenge/')) {
                return this.handleEdgeRequest(req, res);
            }
            
            const host = req.headers.host ? req.headers.host.split(':')[0] : 'localhost';

            // 2. Intercept with a 307 Temporary Redirect if cordon is active (but ports are still open)
            if (this.state.isCordon) {
                res.writeHead(307, { "Location": `https://${host}${requestUrl}` });
                return res.end();
            }

            // 3. Force a 301 redirect to HTTPS for all other traffic
            res.writeHead(301, { "Location": `https://${host}${requestUrl}` });
            res.end();
        });
        
        this.https = null;
        this.mesh = null;
        this.autoscaler = null;
        this.authenticate = authenticate;
        this.authorize = authorize;
        this.wsManager = null;
        this.crud = null;
        this.telemetry = null;
        this.files = null;
        this.loader = null;
    }

    /**
     * Programmatically cordons or un-cordons the server node.
     * Rejects new incoming connections or triggers 307 redirects when active.
     * 
     * @param {boolean|Object} [arg=true] - Boolean flag or detailed payload object
     * @param {boolean} [fromEvent=false] - Internal flag to prevent propagation loops
     */
    cordon(arg = true, fromEvent = false) {
        if (!fromEvent) {
            const payload = (arg && typeof arg === 'object') ? arg : { value: arg };
            payload.method = 'cordon';
            this.send(payload);
            return; // Let the event bus handle local execution via the listener
        }

        const isCordon = typeof arg === 'boolean' ? arg : (arg && arg.value !== undefined ? arg.value : true);
        
        if (!isCordon && (this.state.isShedding || this.state.isDraining)) {
            console.warn(`[@cocreate/server] Blocked un-cordon request. Overriding active states: isShedding=${this.state.isShedding}, isDraining=${this.state.isDraining}`);
            return;
        }
        
        this.state.isCordon = isCordon;
        this.updateLifecycleStatus();
        
        const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isCordon updated to: ${this.state.isCordon}`);
    }

    /**
     * Programmatically sheds tenant workloads from active memory and sockets.
     * Automatically cordons the node. Passing an empty array or false turns shedding off.
     * 
     * @param {string|string[]|Object} [arg=[]] - Org IDs to evict, or detailed payload
     * @param {boolean} [fromEvent=false] - Internal flag to prevent propagation loops
     */
    shed(arg = [], fromEvent = false) {
        if (!fromEvent) {
            const payload = (arg && typeof arg === 'object' && !Array.isArray(arg)) 
                ? arg 
                : { organizations: arg === false ? false : (Array.isArray(arg) ? arg : (arg ? [arg] : [])) };
            payload.method = 'shed';
            this.send(payload);
            return;
        }

        let organizations = [];
        let isShedding = false;

        if (arg === false) {
            isShedding = false;
        } else if (arg && arg.organizations !== undefined) {
            if (arg.organizations === false) {
                isShedding = false;
            } else {
                organizations = Array.isArray(arg.organizations) ? arg.organizations : [arg.organizations];
                isShedding = organizations.length > 0;
            }
        } else if (arg) {
            organizations = Array.isArray(arg) ? arg : (typeof arg === 'string' ? [arg] : []);
            isShedding = organizations.length > 0;
        }

        this.state.isShedding = isShedding;
        this.state.sheddingOrgs = new Set(organizations);
        
        if (this.state.isShedding) {
            this.state.isCordon = true;
            
            if (organizations.length > 0 && this.wsManager && typeof this.wsManager.destroyOrganizations === 'function') {
                console.log(`[@cocreate/server] Shedding event triggered. Selective eviction of org IDs:`, organizations);
                this.wsManager.destroyOrganizations(organizations);
            }

            this.closeEdgeListeners();
        } else {
            this.restoreEdgeListeners();
        }
        
        this.updateLifecycleStatus();

        const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isShedding updated to: ${this.state.isShedding} (isCordon: ${this.state.isCordon})`);
    }

    /**
     * Programmatically drains active client socket connections prior to restart or shutdown.
     * Automatically cordons the node.
     * 
     * @param {boolean|Object} [arg=true] - Boolean flag or detailed payload object
     * @param {boolean} [fromEvent=false] - Internal flag to prevent propagation loops
     */
    drain(arg = true, fromEvent = false) {
        if (!fromEvent) {
            const payload = (arg && typeof arg === 'object') ? arg : { value: arg };
            payload.method = 'drain';
            this.send(payload);
            return;
        }

        const isDraining = typeof arg === 'boolean' ? arg : (arg && arg.value !== undefined ? arg.value : true);
        this.state.isDraining = isDraining;
        
        if (this.state.isDraining) {
            this.state.isCordon = true;
            
            if (this.wsManager && typeof this.wsManager.destroyAll === 'function') {
                console.log(`[@cocreate/server] Worker-level drain event triggered. Purging all tenant configurations.`);
                this.wsManager.destroyAll();
            }

            this.closeEdgeListeners();
        }
        
        this.updateLifecycleStatus();

        const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isDraining updated to: ${this.state.isDraining} (isCordon: ${this.state.isCordon})`);
    }

    /**
     * Programmatically initiates a process restart for the context in which it is invoked.
     * Takes zero arguments and acts directly on the local worker or primary process.
     * 
     * @param {Object} [payload={}] - Configuration payload for the restart action
     * @param {boolean} [fromEvent=false] - Internal flag to prevent propagation loops
     */
    restart(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'restart';
            this.send(sendPayload);
            return;
        }

        if (this.isPrimary) {
            if (payload && (payload.isPrimary || payload.master)) {
                console.log(`[@cocreate/server] [PRIMARY] App restart commanded. Triggering global graceful shutdown...`);
                if (this.triggerGlobalShutdown) this.triggerGlobalShutdown('API_RESTART');
            }
        } else {
            if (payload && (payload.isPrimary || payload.master)) return;

            console.log(`[@cocreate/server] [WORKER ${this.workerId}] Restart triggered. Commencing drain...`);
            this.drain(true, true); // Direct internal execution of drain sequence
            
            setTimeout(() => {
                console.log(`[@cocreate/server] [WORKER ${this.workerId}] Drain complete. Exiting for worker restart.`);
                process.exit(0);
            }, 3000); 
        }
    }

    /**
     * Programmatically initiates a full Operating System reboot.
     * Drains all connections, closes mesh database bindings, and executes `sudo reboot`.
     * 
     * @param {Object} [payload={}] - Configuration payload for the reboot action
     * @param {boolean} [fromEvent=false] - Internal flag to prevent propagation loops
     */
    reboot(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'reboot';
            if (!this.isPrimary) sendPayload.master = true;
            this.send(sendPayload);
            return;
        }

        if (!this.isPrimary) return;

        console.log(`[@cocreate/server] [PRIMARY] OS Reboot requested. Commencing graceful shutdown before kernel reboot...`);
        if (this.triggerGlobalShutdown) {
            this.triggerGlobalShutdown('OS_REBOOT', () => {
                console.log(`[@cocreate/server] [PRIMARY] Executing OS Reboot: sudo reboot`);
                exec('sudo reboot', (err) => {
                    if (err) {
                        console.error("[@cocreate/server] Failed to execute sudo reboot:", err.message);
                        process.exit(1);
                    }
                });
            });
        }
    }

    /**
     * Programmatically initiates a full Operating System poweroff/shutdown.
     * Drains all connections, closes mesh database bindings, and executes `sudo poweroff`.
     * 
     * @param {Object} [payload={}] - Configuration payload for the shutdown action
     * @param {boolean} [fromEvent=false] - Internal flag to prevent propagation loops
     */
    shutdown(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'shutdown';
            if (!this.isPrimary) sendPayload.master = true;
            this.send(sendPayload);
            return;
        }

        if (!this.isPrimary) return;

        console.log(`[@cocreate/server] [PRIMARY] OS Shutdown requested. Commencing graceful shutdown before VM poweroff...`);
        if (this.triggerGlobalShutdown) {
            this.triggerGlobalShutdown('OS_SHUTDOWN', () => {
                console.log(`[@cocreate/server] [PRIMARY] Executing OS Poweroff: sudo poweroff`);
                exec('sudo poweroff', (err) => {
                    if (err) {
                        console.error("[@cocreate/server] Failed to execute sudo poweroff:", err.message);
                        process.exit(1);
                    }
                });
            });
        }
    }

    updateLifecycleStatus() {
        let newStatus = 'ACTIVE';
        if (isPrimaryShuttingDown) {
            newStatus = 'SHUTTING_DOWN';
        } else if (this.state.isDraining) {
            newStatus = 'DRAINING';
        } else if (this.state.isCordon) {
            newStatus = 'CORDONED';
        }

        if (this.state.status !== newStatus) {
            const oldStatus = this.state.status;
            this.state.status = newStatus;
            this.emit('status_change', { oldStatus, newStatus });
            
            if (this.isPrimary && this.mesh && typeof this.mesh.send === 'function' && newStatus !== 'INITIALIZING') {
                // Broadcast state to mesh peers for distributed state awareness
                try {
                    this.mesh.send({ method: 'mesh.node_status_update', server_id: this.id, status: newStatus });
                } catch (err) {}
            }
        }
    }

    closeEdgeListeners() {
        if (this.isPrimary) return;
        if (this.http && this.http.listening) try { this.http.close(); } catch {}
        if (this.https && this.https.listening) try { this.https.close(); } catch {}
    }

    restoreEdgeListeners() {
        if (this.isPrimary || this.state.isDraining) return;
        const HTTP_PORT = process.env.HTTP_PORT || 80;
        const HTTPS_PORT = process.env.HTTPS_PORT || 443;
        const BIND_IP = process.env.BIND_IP || '0.0.0.0';

        if (this.http && !this.http.listening) this.http.listen(HTTP_PORT, BIND_IP);
        if (this.https && !this.https.listening) this.https.listen(HTTPS_PORT, BIND_IP);
    }

    send(payload) {
        if (!payload || !payload.method) {
            console.error("[@cocreate/server] Cannot route IPC send without a 'method' definition.");
            return;
        }

        if (this.isPrimary) {
            if (payload.master !== false) {
                this.emit(payload.method, payload);
            }

            let targetIds = [];
            if (payload.broadcast === true) {
                targetIds = Array.from(this.workers.keys());
            } else if (Array.isArray(payload.workers)) {
                targetIds = payload.workers.map(id => parseInt(id, 10));
            } else if (payload.worker !== undefined) {
                targetIds = [parseInt(payload.worker, 10)];
            }

            for (const wId of targetIds) {
                const worker = this.workers.get(wId);
                if (worker?.process && typeof worker.process.send === 'function' && worker.process.isConnected()) {
                    try {
                        worker.process.send({ ipcPayload: true, payload });
                    } catch (err) {
                        console.error(`[@cocreate/server] Failed sending to worker ${wId}:`, err);
                    }
                }
            }
        } else {
            this.emit(payload.method, payload);

            if (typeof process.send === 'function') {
                try {
                    process.send({ ipcPayload: true, payload });
                } catch (err) {
                    console.error("[@cocreate/server] Failed sending upstream IPC payload to Master:", err);
                }
            }
        }
    }

    evaluatePrimaryFallback() {
        if (!this.isPrimary) return;

        const allWorkersDown = Array.from(this.workers.values()).every(w => w.isListening === false);

        if (allWorkersDown) {
            if (!this.fallbackHttpServer) {
                const HTTP_PORT = process.env.HTTP_PORT || 80;
                const HTTPS_PORT = process.env.HTTPS_PORT || 443;
                const BIND_IP = process.env.BIND_IP || '0.0.0.0';

                const handleRedirect = (req, res) => {
                    const host = req.headers.host ? req.headers.host.split(':')[0] : 'localhost';
                    res.writeHead(307, { "Location": `https://${host}${req.url || '/'}`, "Retry-After": "5" });
                    res.end();
                };

                this.fallbackHttpServer = Http.createServer(handleRedirect);
                this.fallbackHttpServer.listen(HTTP_PORT, BIND_IP);

                if (this.httpsOptions) {
                    this.fallbackHttpsServer = Https.createServer(this.httpsOptions, handleRedirect);
                    this.fallbackHttpsServer.listen(HTTPS_PORT, BIND_IP);
                }
                console.log(`[@cocreate/server] [PRIMARY] All workers cordoned/down. 307 Redirect Fallback active on ${BIND_IP}.`);
            }
        } else {
            if (this.fallbackHttpServer) {
                try { this.fallbackHttpServer.close(); } catch {}
                this.fallbackHttpServer = null;
                if (this.fallbackHttpsServer) {
                    try { this.fallbackHttpsServer.close(); } catch {}
                    this.fallbackHttpsServer = null;
                }
                console.log(`[@cocreate/server] [PRIMARY] Workers active. 307 Redirect Fallback disabled.`);
            }
        }
    }

    async init() {
        if (this.isInitialized) {
            return this;
        }

        this.isInitialized = true;
        setupIPC(this);

        const config = await Config.request({
            'organization_id': '',
            'host': '',
            'workers': '',
            'server_id': ''
        });

        this.organization_id = config.organization_id || null;
        this.host = config.host || null;

        // Resolve Persistent Server ID Identity
        if (this.isPrimary) {
            if (config.server_id) {
                this.id = config.server_id;
            } else {
                // If not found in config, generate and dynamically persist to global config state
                this.id = process.env.SERVER_ID || ObjectId().toString();
                await Config.request({ server_id: { value: this.id } });
                console.log(`[@cocreate/server] [PRIMARY] Generated & persisted new Server ID: ${this.id}`);
            }
            process.env.SERVER_ID = this.id; // Bridge for child worker forks
        } else {
            this.id = process.env.SERVER_ID;
        }

        let configuredWorkers = config.workers;
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

        this.crud = await CrudServer.init(this);
        this.mesh = ServerMesh;
        this.autoscaler = ServerAutoscaler;

        if (this.isPrimary) {
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator running with PID: ${process.pid} (Server ID: ${this.id})`);            
            this.ip = await getIP();
            this.infrastructure = await resolveInfrastructure(this.id);
            await this.mesh.init(this);
            await this.autoscaler.init(this);

            // Bridge remote Mesh network commands directly into local CoCreateServer event bus
            const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];
            for (const eventName of systemEvents) {
                this.mesh.on(eventName, (payload) => {
                    console.log(`[@cocreate/server] [PRIMARY] Received remote mesh cluster command: "${eventName}"`);
                    this.send(payload);
                });
            }

            setupPrimaryShutdownHooks(this);
            
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator creating ${this.totalWorkers} workers.`);

            if (this.totalWorkers > 1) {
                for (let i = 0; i < this.totalWorkers; i++) {
                    const currentWorkerId = i + 1;
                    const worker = cluster.fork({ 
                        WORKER_ID: currentWorkerId,
                        SERVER_ID: this.id,
                        SERVER_IP: this.ip,
                        SERVER_INFRASTRUCTURE: JSON.stringify(this.infrastructure)
                    });

                    worker.on('error', (err) => {
                        console.error(`[@cocreate/server] [PRIMARY] Error on Worker ${currentWorkerId}:`, err.message);
                    });

                    activeWorkers.set(currentWorkerId, {
                        id: currentWorkerId,
                        pid: worker.process.pid,
                        process: worker,
                        isListening: false
                    });
                }

                cluster.on('exit', (worker, code, signal) => {
                    console.log(`[@cocreate/server] [PRIMARY] Worker ${worker.process.pid} exited (Code: ${code}, Signal: ${signal})`);
                    
                    let deadWorkerId = null;
                    for (let [id, w] of activeWorkers.entries()) {
                        if (w.pid === worker.process.pid) {
                            deadWorkerId = id;
                            activeWorkers.delete(id);
                            break;
                        }
                    }
                    this.evaluatePrimaryFallback();

                    if (isPrimaryShuttingDown || this.state.isDraining) {
                        console.log(`[@cocreate/server] [PRIMARY] Worker ${worker.process.pid} removed. Draining active workers: ${activeWorkers.size} remaining.`);
                        
                        if (activeWorkers.size === 0 && isMeshClosed) {
                            console.log("[@cocreate/server] [PRIMARY] All workers successfully drained and Mesh closed.");
                            if (primaryShutdownTimeout) clearTimeout(primaryShutdownTimeout);
                            if (typeof shutdownCompletionCallback === 'function') {
                                shutdownCompletionCallback();
                            } else {
                                process.exit(0);
                            }
                        }
                        return;
                    }

                    const targetId = deadWorkerId || (activeWorkers.size + 1);
                    
                    const newWorker = cluster.fork({ 
                        WORKER_ID: targetId,
                        SERVER_ID: this.id, 
                        SERVER_IP: this.ip,
                        SERVER_INFRASTRUCTURE: JSON.stringify(this.infrastructure)
                    });

                    newWorker.on('error', (err) => {
                        console.error(`[@cocreate/server] [PRIMARY] Error on replacement Worker ${targetId}:`, err.message);
                    });

                    activeWorkers.set(targetId, {
                        id: targetId,
                        pid: newWorker.process.pid,
                        process: newWorker,
                        isListening: false
                    });
                });

                if (this.totalWorkers > 1) {
                    return this;
                }
            }
        } else {
            console.log(`[@cocreate/server] [WORKER ${this.workerId}] running with PID: ${process.pid} (Server ID: ${this.id})`);
        }

        this.telemetry = await ServerTelemetry.init(this);
        this.ip = process.env.SERVER_IP || await getIP();
        
        try {
            this.infrastructure = process.env.SERVER_INFRASTRUCTURE 
                ? JSON.parse(process.env.SERVER_INFRASTRUCTURE) 
                : await resolveInfrastructure(this.id);
        } catch (error) {
            this.infrastructure = await resolveInfrastructure(this.id);
        }

        this.certificate = Acme;
        this.httpsOptions = {
            SNICallback: (domain, cb) => this.certificate.sniCallback(domain, cb)
        };
        this.https = Https.createServer(this.httpsOptions, (req, res) => this.handleEdgeRequest(req, res));
        await this.certificate.init(this);

        this.wsManager = new SocketServer(this);
        this.api = await ApiServer.init(this);
        this.files = FileServer.init(this);
        this.loader = await LazyLoader.init(this);

        const HTTP_PORT = process.env.HTTP_PORT || 80;
        const HTTPS_PORT = process.env.HTTPS_PORT || 443;
        const BIND_IP = process.env.BIND_IP || '0.0.0.0';

        const reportListening = (state) => {
            if (typeof process.send === 'function') process.send({ type: 'listening_state', state });
        };

        if (this.http) {
            this.http.on('listening', () => reportListening(true));
            this.http.on('close', () => reportListening(false));
            this.http.listen(HTTP_PORT, BIND_IP, () => {
                console.log(`[@cocreate/server] [WORKER ${this.workerId}] HTTP listening on ${BIND_IP}:${HTTP_PORT}`);
            });
        }

        if (this.https) {
            this.https.listen(HTTPS_PORT, BIND_IP, () => {
                console.log(`[@cocreate/server] [WORKER ${this.workerId}] HTTPS listening on ${BIND_IP}:${HTTPS_PORT}`);
            });
        }

        this.updateLifecycleStatus();

        return this;
    }

    async handleEdgeRequest(req, res) {
        try {
            const requestUrl = req.url || '/';

            if (requestUrl.startsWith("/webhooks/")) {
                if (this.loader) return this.loader.request(req, res);
            }
            if (this.files) return this.files.send(req, res);

            res.writeHead(500, { "Content-Type": "text/plain" });
            res.end("Internal Error: Files component missing");
        } catch (error) {
            res.writeHead(400, { "Content-Type": "text/plain" });
            res.end("Invalid request routing context");
        }
    }
}

function setupPrimaryShutdownHooks(serverCtx) {
    serverCtx.triggerGlobalShutdown = async (signal = 'MANUAL_RESTART', completionCb = null) => {
        if (isPrimaryShuttingDown) return;
        isPrimaryShuttingDown = true;
        serverCtx.state.isCordon = true;
        serverCtx.state.isDraining = true;
        serverCtx.updateLifecycleStatus();
        shutdownCompletionCallback = completionCb;

        console.log(`\n[@cocreate/server] [PRIMARY] Process termination: ${signal}. Commencing graceful shutdown...`);

        if (serverCtx.mesh && typeof serverCtx.mesh.close === 'function') {
            try {
                await serverCtx.mesh.close();
                console.log("[@cocreate/server] [PRIMARY] Success: Server Mesh closed.");
            } catch (err) {
                console.error("[@cocreate/server] [PRIMARY] Failed to close Server Mesh:", err);
            }
        }
        isMeshClosed = true;

        const finalizeExecution = () => {
            if (primaryShutdownTimeout) clearTimeout(primaryShutdownTimeout);
            if (typeof shutdownCompletionCallback === 'function') {
                shutdownCompletionCallback();
            } else {
                process.exit(0);
            }
        };

        if (activeWorkers.size === 0) {
            console.log("[@cocreate/server] [PRIMARY] All workers successfully drained. Terminating master process cleanly.");
            finalizeExecution();
            return;
        }

        console.log("[@cocreate/server] [PRIMARY] Informing worker processes to start local connection drains...");
        for (const [wId, worker] of activeWorkers.entries()) {
            const workerInstance = worker.process;
            if (workerInstance && typeof workerInstance.send === 'function' && workerInstance.isConnected()) {
                try {
                    workerInstance.send({ type: 'shutdown' });
                } catch (err) {
                    console.error(`[@cocreate/server] Failed sending shutdown IPC instruction to worker ${wId}:`, err);
                }
            }
        }

        primaryShutdownTimeout = setTimeout(() => {
            console.warn("[@cocreate/server] [PRIMARY] Safety grace window expired! Force-terminating remaining processes...");
            for (const [wId, worker] of activeWorkers.entries()) {
                if (worker?.process) {
                    try { worker.process.kill('SIGKILL'); } catch (err) {}
                }
            }
            finalizeExecution();
        }, 5000);
    };

    process.on('SIGTERM', () => serverCtx.triggerGlobalShutdown('SIGTERM'));
    process.on('SIGINT', () => serverCtx.triggerGlobalShutdown('SIGINT'));
    process.on('SIGBREAK', () => serverCtx.triggerGlobalShutdown('SIGBREAK'));
}

function setupIPC(serverCtx) {
    if (serverCtx.isPrimary) {
        cluster.on('message', (worker, message) => {
            if (message && message.type === 'listening_state') {
                for (let w of serverCtx.workers.values()) {
                    if (w.pid === worker.process.pid) {
                        w.isListening = message.state;
                        serverCtx.evaluatePrimaryFallback();
                        break;
                    }
                }
            }
            else if (message && message.id === serverCtx.id && message.method && message.method.startsWith('cluster_state_')) {
                try {
                    const action = message.method.replace('cluster_state_', '');
                    if (action === 'set') {
                        serverCtx.store.set(message.key, message.value).then(val => {
                            message.value = val; worker.send(message);
                        });
                    } else if (action === 'get') {
                        serverCtx.store.get(message.key).then(val => {
                            message.value = val; worker.send(message);
                        });
                    } else if (action === 'delete') {
                        serverCtx.store.delete(message.key).then(val => {
                            message.value = val; worker.send(message);
                        });
                    } else if (action === 'size') {
                        serverCtx.store.size().then(val => {
                            message.value = val; worker.send(message);
                        });
                    } else if (action === 'clear') {
                        serverCtx.store.clear().then(val => {
                            message.value = val; worker.send(message);
                        });
                    }
                } catch (error) {
                    message.error = error.message || error.error;
                    worker.send(message);
                }
            }
            else if (message && message.ipcPayload && message.payload) {
                const { payload } = message;
                let senderWorkerId = null;
                for (const [wId, w] of serverCtx.workers.entries()) {
                    if (w.pid === worker.process.pid) {
                        senderWorkerId = wId;
                        break;
                    }
                }
                payload.senderWorkerId = senderWorkerId;
                serverCtx.send(payload);
            }
        });
    } else {
        process.on('message', (message) => {
            if (message && message.type === 'shutdown') {
                console.log(`[@cocreate/server] [WORKER ${process.pid}] Received master coordinated shutdown signal. Commencing local drain...`);
                serverCtx.emit('drain', true);
                
                setTimeout(() => process.exit(0), 3000);
            }
            else if (message && message.id === serverCtx.id) {
                const pending = pendingPromises.get(message.messageId);
                if (pending) {
                    pendingPromises.delete(message.messageId);
                    if (message.error) {
                        pending.reject(new Error(message.error));
                    } else {
                        pending.resolve(message.value);
                    }
                }
            }
            else if (message && message.ipcPayload && message.payload) {
                serverCtx.emit(message.payload.method, message.payload);
            }
            else if (message && message.method === 'cluster_state_sync') {
                serverCtx.emit('cluster_state_sync', message);
            }
        });
    }
}

export default new CoCreateServer();