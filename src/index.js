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

import { ObjectId } from '@cocreate/utils';

// Dynamic environmental resolvers & state manager
import resolveInfrastructure from './getInfrastructure.js';
import getIP from './getIp.js';
import IPC from './ipc.js';
import Storage from './storage.js';
import State from './state.js';

const isPrimary = cluster.isPrimary !== undefined ? cluster.isPrimary : cluster.isMaster;

const pendingPromises = new Map();
const activeWorkers = new Map();

// Track overall system state during shutdowns to prevent concurrent signal re-evaluation
let isPrimaryShuttingDown = false;
let isMeshClosed = false; // Dual-lock flag to guarantee DB prunes finish before process exits
let primaryShutdownTimeout = null;
let shutdownCompletionCallback = null;

export class CoCreateServer extends EventEmitter {
    constructor() {
        super();
        this.id = process.env.SERVER_ID || null;
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
        this.isPrimaryShuttingDown = false;

        // Initialize reactive cluster storage
        this.storage = new Storage(this, pendingPromises);
        this.store = this.storage;
        this.workerStorage = this.storage;

        // Initialize state manager module (attaches cordon, shed, drain, publishState, etc.)
        State.init(this);

        // Fallback servers for the Primary node
        this.fallbackHttpServer = null;
        this.fallbackHttpsServer = null;

        this.send = null;

        this.http = Http.createServer((req, res) => {
            const requestUrl = req.url || '/';
            
            // 1. Allow ACME challenges to pass through over unencrypted HTTP
            if (requestUrl.startsWith('/.well-known/acme-challenge/')) {
                return this.handleEdgeRequest(req, res);
            }
            
            const host = req.headers.host ? req.headers.host.split(':')[0] : 'localhost';

            // 2. Intercept with a 307 Temporary Redirect if cordon is active
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
        this.send = IPC.init(this, pendingPromises);

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
                this.id = process.env.SERVER_ID || ObjectId().toString();
                await Config.request({ server_id: { value: this.id } });
                console.log(`[@cocreate/server] [PRIMARY] Generated & persisted new Server ID: ${this.id}`);
            }
            process.env.SERVER_ID = this.id;
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

        // Setup process signals and graceful shutdown hooks
        setupProcessShutdownHooks(this);

        if (this.isPrimary) {
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator running with PID: ${process.pid} (Server ID: ${this.id})`);            
            this.ip = await getIP();
            this.infrastructure = await resolveInfrastructure(this.id);
            await this.mesh.init(this);
            await this.autoscaler.init(this);

            // Register initial running profile to DB
            this.publishState('running');
            
            // Zero-out worker states in DB
            if (this.totalWorkers > 0) {
                const initialWorkerStates = {};
                for (let i = 1; i <= this.totalWorkers; i++) {
                    initialWorkerStates[`worker${i}`] = {
                        pid: null,
                        status: 'stopped',
                        isCordoned: false,
                        isShedding: false,
                        isDraining: false
                    };
                }

                if (this.crud && this.id) {
                    try {
                        await this.crud.send(this.getServerObject(initialWorkerStates));
                        console.log(`[@cocreate/server] [PRIMARY] Initialized ${this.totalWorkers} worker states in DB`);
                    } catch (err) {
                        console.error(`[@cocreate/server] [PRIMARY] Failed to initialize worker states in DB:`, err.message);
                    }
                }
            }

            // Route mesh commands to IPC
            const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];
            for (const eventName of systemEvents) {
                this.mesh.on(eventName, (payload) => {
                    console.log(`[@cocreate/server] [PRIMARY] Received remote mesh command: "${eventName}"`);
                    this.send(payload);
                });
            }

            console.log(`[@cocreate/server] [PRIMARY] Orchestrator creating ${this.totalWorkers} workers.`);

            const generateWorkerEnv = (wId) => ({
                WORKER_ID: String(wId),
                SERVER_ID: String(this.id || ''),
                SERVER_IP: String(this.ip || '127.0.0.1'),
                SERVER_INFRASTRUCTURE: JSON.stringify(this.infrastructure || {})
            });

            if (this.totalWorkers > 1) {
                for (let i = 0; i < this.totalWorkers; i++) {
                    const currentWorkerId = i + 1;
                    const worker = cluster.fork(generateWorkerEnv(currentWorkerId));

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

                cluster.on('exit', async (worker, code, signal) => {
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
                    
                    if (deadWorkerId && this.crud && this.id) {
                        try {
                            await this.crud.send(this.getServerObject({
                                [`worker${deadWorkerId}`]: {
                                    pid: null,
                                    status: 'stopped',
                                    isCordoned: false,
                                    isShedding: false,
                                    isDraining: false
                                }
                            }));
                            console.log(`[@cocreate/server] [PRIMARY] Reset worker${deadWorkerId} state in DB to stopped`);
                        } catch (err) {
                            console.error(`[@cocreate/server] [PRIMARY] Failed to reset worker${deadWorkerId} in DB:`, err.message);
                        }
                    }

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
                    
                    setTimeout(() => {
                        console.log(`[@cocreate/server] [PRIMARY] Respawning Worker ${targetId}...`);
                        const newWorker = cluster.fork(generateWorkerEnv(targetId));

                        newWorker.on('error', (err) => {
                            console.error(`[@cocreate/server] [PRIMARY] Error on replacement Worker ${targetId}:`, err.message);
                        });

                        activeWorkers.set(targetId, {
                            id: targetId,
                            pid: newWorker.process.pid,
                            process: newWorker,
                            isListening: false
                        });
                    }, 2000);
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
            if (typeof process.send === 'function' && process.connected) {
                try {
                    process.send({ type: 'listening_state', state });
                } catch (e) {}
            }
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

function setupProcessShutdownHooks(serverCtx) {
    if (serverCtx.isPrimary) {
        setupPrimaryShutdownHooks(serverCtx);
    } else {
        setupWorkerShutdownHooks(serverCtx);
    }
}

function setupPrimaryShutdownHooks(serverCtx) {
    serverCtx.triggerGlobalShutdown = async (signal = 'MANUAL_RESTART', completionCb = null) => {
        if (isPrimaryShuttingDown) return;
        isPrimaryShuttingDown = true;
        serverCtx.isPrimaryShuttingDown = true;
        serverCtx.state.isCordon = true;
        serverCtx.state.isDraining = true;
        serverCtx.updateLifecycleStatus();
        shutdownCompletionCallback = completionCb;

        const finalStatus = signal === 'OS_REBOOT' ? 'rebooting' : 'stopped';
        await serverCtx.publishState(finalStatus);

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

function setupWorkerShutdownHooks(serverCtx) {
    let isWorkerShuttingDown = false;

    const handleWorkerShutdown = async (reason) => {
        if (isWorkerShuttingDown) return;
        isWorkerShuttingDown = true;
        
        console.log(`[@cocreate/server] [WORKER ${process.pid}] Caught terminal signal/shutdown event (${reason}). Initiating local drain...`);

        try {
            if (typeof serverCtx.drain === 'function') {
                await serverCtx.drain(true, true);
            } else {
                serverCtx.emit('drain', true);
            }
        } catch (err) {
            console.error(`[@cocreate/server] [WORKER ${process.pid}] Error during ${reason} drain:`, err.message);
        }

        setTimeout(() => {
            console.log(`[@cocreate/server] [WORKER ${process.pid}] Drain complete after ${reason}. Exiting process.`);
            process.exit(0);
        }, 1000);
    };

    serverCtx.on('shutdown', () => handleWorkerShutdown('master_shutdown_signal'));
    process.on('SIGINT', () => handleWorkerShutdown('SIGINT'));
    process.on('SIGTERM', () => handleWorkerShutdown('SIGTERM'));
    process.on('SIGBREAK', () => handleWorkerShutdown('SIGBREAK'));
}

export default new CoCreateServer();