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
const id = process.env.SERVER_ID || ObjectId().toString();

const sharedMap = new Map();
const pendingPromises = new Map();
const activeWorkers = new Map();

// Track overall system state during shutdowns to prevent concurrent signal re-evaluation
let isPrimaryShuttingDown = false;
let isMeshClosed = false; // Dual-lock flag to guarantee DB prunes finish before process exits
let primaryShutdownTimeout = null;

export class CoCreateServer extends EventEmitter {
    constructor() {
        super();
        this.id = id;
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

        // Ingress state management flags
        this.isCordon = false;
        this.isShedding = false;
        this.isDraining = false;

        // Fallback servers for the Primary node
        this.fallbackHttpServer = null;
        this.fallbackHttpsServer = null;

        this.on('cordon', (payload) => {
            const active = payload?.active !== false;
            
            // Core Logic: Telemetry or callers cannot uncordon the node if shedding or draining is active
            if (!active && (this.isShedding || this.isDraining)) {
                console.warn(`[@cocreate/server] Blocked un-cordon request. Overriding active states: isShedding=${this.isShedding}, isDraining=${this.isDraining}`);
                return;
            }
            
            this.isCordon = active;
            const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;
            console.log(`[@cocreate/server] ${processLabel} isCordon updated to: ${this.isCordon}`);
        });

        this.on('shed', (payload) => {
            this.isShedding = payload?.active !== false;
            
            // Shedding state activation automatically triggers a network cordon
            if (this.isShedding) {
                this.isCordon = true;
                
                // Selective Shedding Integration: Target specified organizations inside the socket server
                if (payload?.organizations && this.wsManager && typeof this.wsManager.destroyOrganizations === 'function') {
                    const orgList = Array.isArray(payload.organizations) ? payload.organizations : [payload.organizations];
                    console.log(`[@cocreate/server] Shedding event triggered. Selective eviction of org IDs:`, orgList);
                    this.wsManager.destroyOrganizations(orgList);
                }

                // If shedding, proactively close edge listeners to refuse new connections
                this.closeEdgeListeners();
            } else {
                this.restoreEdgeListeners();
            }

            const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;
            console.log(`[@cocreate/server] ${processLabel} isShedding updated to: ${this.isShedding} (isCordon: ${this.isCordon})`);
        });

        this.on('drain', (payload) => {
            this.isDraining = payload?.active !== false;
            
            // Draining state activation automatically triggers a network cordon
            if (this.isDraining) {
                this.isCordon = true;
                
                // Complete Worker Drain Integration: Instantly purge all sockets and loaded maps
                if (this.wsManager && typeof this.wsManager.destroyAll === 'function') {
                    console.log(`[@cocreate/server] Worker-level drain event triggered. Purging all tenant configurations.`);
                    this.wsManager.destroyAll();
                }

                // Disconnect edge completely
                this.closeEdgeListeners();
            }

            const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;
            console.log(`[@cocreate/server] ${processLabel} isDraining updated to: ${this.isDraining} (isCordon: ${this.isCordon})`);
        });

        this.http = Http.createServer((req, res) => {
            const requestUrl = req.url || '/';
            
            // 1. Allow ACME challenges to pass through over unencrypted HTTP
            if (requestUrl.startsWith('/.well-known/acme-challenge/')) {
                return this.handleEdgeRequest(req, res);
            }
            
            const host = req.headers.host ? req.headers.host.split(':')[0] : 'localhost';

            // 2. Intercept with a 307 Temporary Redirect if cordon is active (but ports are still open)
            if (this.isCordon) {
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

    closeEdgeListeners() {
        if (this.isPrimary) return;
        if (this.http && this.http.listening) try { this.http.close(); } catch {}
        if (this.https && this.https.listening) try { this.https.close(); } catch {}
    }

    restoreEdgeListeners() {
        if (this.isPrimary || this.isDraining) return; // Cannot restore if draining
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
            // --- MASTER SEND ROUTING PATHWAY ---
            // 1. Dispatch locally on Master process unless explicitly bypassed
            if (payload.master !== false) {
                this.emit(payload.method, payload);
            }

            // 2. Compile target worker list dynamically using a flat ternary condition.
            let targetIds = [];
            if (payload.broadcast === true) {
                targetIds = Array.from(this.workers.keys());
            } else if (Array.isArray(payload.workers)) {
                targetIds = payload.workers.map(id => parseInt(id, 10));
            } else if (payload.worker !== undefined) {
                targetIds = [parseInt(payload.worker, 10)];
            }

            // 3. Dispatch to all compiled destination targets in a single unified loop.
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
            // --- WORKER SEND ROUTING PATHWAY ---
            // 1. Emit locally on this worker so native listeners trigger immediately
            this.emit(payload.method, payload);

            // 2. Relay the message upstream to the Primary Orchestrator Process
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

        if (allWorkersDown && !isPrimaryShuttingDown) {
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
            // If at least one worker comes back online, tear down the fallback
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

        this.workerStorage = {
            set: (key, value) => workerStorageManager(this, 'set', key, value),
            get: (key) => workerStorageManager(this, 'get', key),
            delete: (key) => workerStorageManager(this, 'delete', key),
            size: () => workerStorageManager(this, 'size'),
            clear: () => workerStorageManager(this, 'clear')
        };

        const config = await Config.request({
            'organization_id': '',
            'host': '',
            'workers': ''
        });

        this.organization_id = config.organization_id || null;
        this.host = config.host || null;

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
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator running with PID: ${process.pid}`);            
            this.ip = await getIP();
            this.infrastructure = await resolveInfrastructure(this.id);
            await this.mesh.init(this);
            await this.autoscaler.init(this);

            // Setup Primary process Graceful Signal Listeners
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

                    // Protect against unhandled 'error' events on the cluster worker wrapper
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

                // Supervise and automatically reboot dead or crashed worker processes
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
                    this.evaluatePrimaryFallback(); // Re-evaluate in case this was the last active worker

                    // SAFETY: If Primary is shutting down or draining, DO NOT fork a replacement worker process!
                    if (isPrimaryShuttingDown || this.isDraining) {
                        console.log(`[@cocreate/server] [PRIMARY] Worker ${worker.process.pid} removed. Draining active workers: ${activeWorkers.size} remaining.`);
                        
                        // Dual-Lock Guard: Only exit Master if workers are dead AND mesh has completed the MongoDB prune
                        if (activeWorkers.size === 0 && isMeshClosed) {
                            console.log("[@cocreate/server] [PRIMARY] All workers successfully drained and Mesh closed. Terminating master process cleanly.");
                            if (primaryShutdownTimeout) clearTimeout(primaryShutdownTimeout);
                            process.exit(0);
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
            console.log(`[@cocreate/server] [WORKER ${this.workerId}] running with PID: ${process.pid}`);
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

function executeLocalWorkerStorageOperation(method, key, value) {
    switch (method) {
        case 'set': sharedMap.set(key, value); return true;
        case 'get': return sharedMap.get(key);
        case 'delete': return sharedMap.delete(key);
        case 'size': return sharedMap.size;
        case 'clear': sharedMap.clear(); return true;
        default: throw new Error(`Unsupported storage key operation: "${method}"`);
    }
}

function workerStorageManager(serverCtx, method, key, value) {
    return new Promise((resolve, reject) => {
        try {
            if (serverCtx.isPrimary) {
                resolve(executeLocalWorkerStorageOperation(method, key, value));
            } else {
                const messageId = uuid();
                pendingPromises.set(messageId, { resolve, reject });
                process.send({ id: serverCtx.id, messageId, method, key, value });
            }
        } catch (error) {
            reject(error);
        }
    });
}

function setupPrimaryShutdownHooks(serverCtx) {
    const handleSignal = async (signal) => {
        if (isPrimaryShuttingDown) return;
        isPrimaryShuttingDown = true;
        serverCtx.isCordon = true;
        serverCtx.isDraining = true;

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

        if (activeWorkers.size === 0) {
            console.log("[@cocreate/server] [PRIMARY] All workers successfully drained. Terminating master process cleanly.");
            if (primaryShutdownTimeout) clearTimeout(primaryShutdownTimeout);
            process.exit(0);
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
            process.exit(0);
        }, 5000);
    };

    process.on('SIGTERM', () => handleSignal('SIGTERM'));
    process.on('SIGINT', () => handleSignal('SIGINT'));
    process.on('SIGBREAK', () => handleSignal('SIGBREAK'));
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
            else if (message && message.id === serverCtx.id) {
                try {
                    message.value = executeLocalWorkerStorageOperation(message.method, message.key, message.value);
                    worker.send(message);
                } catch (error) {
                    message.error = error.error;
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
                serverCtx.emit('drain', { active: true });
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
        });
    }
}

export default new CoCreateServer();