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
import Acme from '@cocreate/acme';
import ServerMesh from '@cocreate/server-mesh';
import ServerAutoscaler from '@cocreate/server-autoscaler';
import ServerTelemetry from '@cocreate/server-telemetry';

import cluster from 'node:cluster';
import os from 'node:os';
import uid from '@cocreate/uuid';
import Config from '@cocreate/config';
import authenticate from '@cocreate/authenticate';
import authorize from '@cocreate/authorize';

import SocketServer from '@cocreate/socket-server';
import CrudServer from '@cocreate/crud-server';
import LazyLoader from '@cocreate/lazy-loader';
import FileServer from '@cocreate/file-server';
import ApiServer from '@cocreate/api';

import { ObjectId } from '@cocreate/utils';

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

            // 2. Intercept with a 307 Temporary Redirect if cordon is active
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
            const targetIds = payload.broadcast === true
                ? Array.from(this.workers.keys())
                : (Array.isArray(payload.workers) ? payload.workers : []);

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
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator starting...`);
            
            this.ip = await getIP();
            this.infrastructure = await resolveInfrastructure(this.id);
            await this.mesh.init(this);
            await this.autoscaler.init(this);

            // Setup Primary process Graceful Signal Listeners
            setupPrimaryShutdownHooks(this);

            if (this.totalWorkers > 1) {
                console.log(`[@cocreate/server] [PRIMARY] Orchestrator running with PID: ${process.pid}`);
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
                        process: worker
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

                    // SAFETY: If Primary is shutting down or draining, DO NOT fork a replacement worker process!
                    if (isPrimaryShuttingDown || this.isDraining) {
                        console.log(`[@cocreate/server] [PRIMARY] Worker ${worker.process.pid} removed. Draining active workers: ${activeWorkers.size} remaining.`);
                        
                        // Dual-Lock Guard: Only exit Master if workers are dead AND mesh has completed the MongoDB prune
                        if (activeWorkers.size === 0 && isMeshClosed) {
                            console.log("[@cocreate/server] [PRIMARY] All workers successfully drained and Mesh closed. Terminating master process cleanly.");
                            if (primaryShutdownTimeout) {
                                clearTimeout(primaryShutdownTimeout);
                            }
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
                        process: newWorker
                    });
                });

                if (this.totalWorkers > 1) {
                    return this;
                }
            }
        }
        
        this.ip = process.env.SERVER_IP || await getIP();
        
        try {
            this.infrastructure = process.env.SERVER_INFRASTRUCTURE 
                ? JSON.parse(process.env.SERVER_INFRASTRUCTURE) 
                : await resolveInfrastructure(this.id);
        } catch (error) {
            console.error(`[@cocreate/server] Worker ${process.pid} failed to parse pre-resolved infrastructure. Falling back...`, error);
            this.infrastructure = await resolveInfrastructure(this.id);
        }

        this.certificate = Acme;
        this.https = Https.createServer({
            SNICallback: (domain, cb) => this.certificate.sniCallback(domain, cb)
        }, (req, res) => this.handleEdgeRequest(req, res));
        await this.certificate.init(this);

        this.wsManager = new SocketServer(this);
        this.api = await ApiServer.init(this);
        this.files = FileServer.init(this);
        this.loader = await LazyLoader.init(this);

        this.telemetry = await ServerTelemetry.init(this);

        const HTTP_PORT = process.env.HTTP_PORT || 80;
        const HTTPS_PORT = process.env.HTTPS_PORT || 443;
        const BIND_IP = process.env.BIND_IP || '0.0.0.0';
        const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;

        if (this.http) {
            this.http.listen(HTTP_PORT, BIND_IP, () => {
                console.log(`[@cocreate/server] ${processLabel} HTTP listening on ${BIND_IP}:${HTTP_PORT} (Redirects & ACME)`);
            });
        }

        if (this.https) {
            this.https.listen(HTTPS_PORT, BIND_IP, () => {
                console.log(`[@cocreate/server] ${processLabel} HTTPS listening on ${BIND_IP}:${HTTPS_PORT} (Secure Edge & WSS)`);
            });
        }

        return this;
    }

    async handleEdgeRequest(req, res) {
        try {
            const requestUrl = req.url || '/';

            // Check if cordon is active (exclude critical ACME challenges from loop-breaking)
            if (this.isCordon && !requestUrl.startsWith('/.well-known/acme-challenge/')) {
                const host = req.headers.host ? req.headers.host.split(':')[0] : 'localhost';
                res.writeHead(307, { "Location": `https://${host}${requestUrl}` });
                return res.end();
            }

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
                const messageId = uid.generate();
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

        console.log(`\n[@cocreate/server] [PRIMARY] Intercepted process termination signal: ${signal}. Commencing cluster-wide graceful shutdown...`);

        // STEP 1: Close Mesh FIRST. Wipes the server record profile from the database while CRUD connection pools are completely active.
        if (serverCtx.mesh && typeof serverCtx.mesh.close === 'function') {
            try {
                console.log("[@cocreate/server] [PRIMARY] Step 1: Initiating Server Mesh close & server record database removal...");
                await serverCtx.mesh.close();
                console.log("[@cocreate/server] [PRIMARY] Success: Server Mesh closed and server record cleanly wiped from database.");
            } catch (err) {
                console.error("[@cocreate/server] [PRIMARY] Failed to close Server Mesh or wipe server profile cleanly:", err);
            }
        }
        isMeshClosed = true;

        // Dual-Lock Resolution: If all workers finished exiting while the database prune was running,
        // terminate the Master process cleanly right now instead of waiting for the safety timeout.
        if (activeWorkers.size === 0) {
            console.log("[@cocreate/server] [PRIMARY] All workers successfully drained and Mesh closed. Terminating master process cleanly.");
            if (primaryShutdownTimeout) {
                clearTimeout(primaryShutdownTimeout);
            }
            process.exit(0);
        }

        // STEP 2: Now that the database profile is clean, tell all worker processes over IPC to begin local graceful drains.
        console.log("[@cocreate/server] [PRIMARY] Step 2: Informing worker processes to start local connection drains...");
        for (const [wId, worker] of activeWorkers.entries()) {
            const workerInstance = worker.process;
            if (workerInstance && typeof workerInstance.send === 'function' && workerInstance.isConnected()) {
                console.log(`[@cocreate/server] [PRIMARY] Sending 'shutdown' instruction to Worker ${wId} (PID: ${worker.pid})`);
                try {
                    workerInstance.send({ type: 'shutdown' });
                } catch (err) {
                    console.error(`[@cocreate/server] Failed sending shutdown IPC instruction to worker ${wId}:`, err);
                }
            } else {
                console.log(`[@cocreate/server] [PRIMARY] Skipping disconnected Worker ${wId} (PID: ${worker.pid})`);
            }
        }

        // STEP 3: Set Cooperative Grace Fallback Timer (5 Seconds)
        // If workers finish early, the exit event triggers process exit immediately. If they hang, we force-kill.
        primaryShutdownTimeout = setTimeout(() => {
            console.warn("[@cocreate/server] [PRIMARY] Safety grace window expired! Force-terminating remaining processes...");
            
            for (const [wId, worker] of activeWorkers.entries()) {
                if (worker?.process) {
                    try {
                        console.log(`[@cocreate/server] [PRIMARY] Force-killing hanging Worker ${wId} (PID: ${worker.pid}).`);
                        worker.process.kill('SIGKILL');
                    } catch (err) {
                        console.error(`[@cocreate/server] Error force-killing Worker ${wId}:`, err);
                    }
                }
            }
            
            console.log("[@cocreate/server] [PRIMARY] All worker processes forcefully reaped. Terminating Master process.");
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
            if (message && message.id === serverCtx.id) {
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
            // Intercept Master-directed shutdown instruction over the worker's IPC channel
            if (message && message.type === 'shutdown') {
                console.log(`[@cocreate/server] [Worker ${process.pid}] Received master coordinated shutdown signal. Commencing local drain...`);
                
                // Immediately cordon worker traffic and trigger the internal SocketServer destroyAll logic
                serverCtx.isCordon = true;
                serverCtx.isDraining = true;
                
                if (serverCtx.wsManager && typeof serverCtx.wsManager.destroyAll === 'function') {
                    serverCtx.wsManager.destroyAll();
                }
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