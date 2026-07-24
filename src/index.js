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

import resolveInfrastructure from './getInfrastructure.js';
import getIP from './getIp.js';
import IPC from './ipc.js';
import Storage from './storage.js';
import State from './state.js';
import LogManager from './log.js';

const isMaster = cluster.isMaster;

const pendingPromises = new Map();
const activeWorkers = new Map();

/**
 * CoCreateServer orchestrates cluster process management, socket handling,
 * API endpoint dispatching, SSL certificates, and mesh cluster synchronization.
 */
export class CoCreateServer extends EventEmitter {
    constructor() {
        super();
        this.id = process.env.SERVER_ID || null;
        
        this.isMaster = isMaster;
        
        if (this.isMaster) {
            this.workers = activeWorkers;
        } else {
            this.workerId = parseInt(process.env.WORKER_ID, 10) || 1;
        }
        
        this.totalWorkers = 0;
        this.isInitialized = false;

        // Initialize reactive cluster storage
        this.storage = new Storage(this, pendingPromises);
        this.store = this.storage;
        this.workerStorage = this.storage;

        // Attach State Manager module (attaches cordon, shed, drain, publishState, process signals)
        State.init(this);

        // Attach Log Manager module
        this.logManager = LogManager.init(this);

        // Master unencrypted HTTP edge listener for ACME and redirect handling
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

        this.authenticate = authenticate;
        this.authorize = authorize;
    }

    closeEdgeListeners() {
        if (this.isMaster) return;
        if (this.http && this.http.listening) this.http.close();
        if (this.https && this.https.listening) this.https.close();
    }

    restoreEdgeListeners() {
        if (this.isMaster || this.state.isDraining) return;
        const HTTP_PORT = process.env.HTTP_PORT || 80;
        const HTTPS_PORT = process.env.HTTPS_PORT || 443;
        const BIND_IP = process.env.BIND_IP || '0.0.0.0';

        if (!this.http.listening) this.http.listen(HTTP_PORT, BIND_IP);
        if (this.https && !this.https.listening) this.https.listen(HTTPS_PORT, BIND_IP);
    }

    evaluateMasterFallback() {
        if (!this.isMaster) return;

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
                console.log(`[@cocreate/server] [MASTER] All workers cordoned/down. 307 Redirect Fallback active on ${BIND_IP}.`);
            }
        } else {
            if (this.fallbackHttpServer) {
                this.fallbackHttpServer.close();
                this.fallbackHttpServer = null;
                if (this.fallbackHttpsServer) {
                    this.fallbackHttpsServer.close();
                    this.fallbackHttpsServer = null;
                }
                console.log(`[@cocreate/server] [MASTER] Workers active. 307 Redirect Fallback disabled.`);
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

        if (this.isMaster) {
            if (config.server_id) {
                this.id = config.server_id;
            } else {
                this.id = process.env.SERVER_ID || ObjectId().toString();
                await Config.request({ server_id: { value: this.id } });
                console.log(`[@cocreate/server] [MASTER] Generated & persisted new Server ID: ${this.id}`);
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

        if (this.isMaster && this.totalWorkers <= 1) {
            this.workerId = 1;
        }

        this.crud = await CrudServer.init(this);
        this.mesh = ServerMesh;
        this.autoscaler = ServerAutoscaler;

        if (this.isMaster) {
            console.log(`[@cocreate/server] [MASTER] Orchestrator running with PID: ${process.pid} (Server ID: ${this.id})`);            
            this.ip = await getIP();
            this.infrastructure = await resolveInfrastructure(this.id);
            await this.mesh.init(this);
            await this.autoscaler.init(this);

            // Reset logs array for a fresh logging session on master startup
            await this.crud.send(this.getServerObject({ logs: [] }));

            // Register initial running profile to DB for Master
            await this.publishState();
            
            // Zero-out worker states in DB upon master initialization
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

                await this.crud.send(this.getServerObject(initialWorkerStates));
            }

            // Route mesh commands to IPC
            const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];
            for (const eventName of systemEvents) {
                this.mesh.on(eventName, (payload) => {
                    console.log(`[@cocreate/server] [MASTER] Received remote mesh command: "${eventName}"`);
                    this.send(payload);
                });
            }

            if (this.totalWorkers > 1) {
                console.log(`[@cocreate/server] [MASTER] Orchestrator creating ${this.totalWorkers} workers.`);

                const generateWorkerEnv = (wId) => ({
                    WORKER_ID: String(wId),
                    SERVER_ID: String(this.id || ''),
                    SERVER_IP: String(this.ip || '127.0.0.1'),
                    SERVER_INFRASTRUCTURE: JSON.stringify(this.infrastructure || {})
                });

                for (let i = 0; i < this.totalWorkers; i++) {
                    const currentWorkerId = i + 1;
                    const worker = cluster.fork(generateWorkerEnv(currentWorkerId));

                    worker.on('error', (err) => {
                        console.error(`[@cocreate/server] [MASTER] Error on Worker ${currentWorkerId}:`, err.message);
                    });

                    activeWorkers.set(currentWorkerId, {
                        id: currentWorkerId,
                        pid: worker.process.pid,
                        process: worker,
                        isListening: false
                    });
                }

                cluster.on('exit', async (worker, code, signal) => {
                    console.log(`[@cocreate/server] [MASTER] Worker ${worker.process.pid} exited (Code: ${code}, Signal: ${signal})`);
                    
                    let deadWorkerId = null;
                    for (let [id, w] of activeWorkers.entries()) {
                        if (w.pid === worker.process.pid) {
                            deadWorkerId = id;
                            activeWorkers.delete(id);
                            break;
                        }
                    }
                    this.evaluateMasterFallback();
                    
                    if (deadWorkerId) {
                        await this.crud.send(this.getServerObject({
                            [`worker${deadWorkerId}`]: {
                                pid: null,
                                status: 'stopped',
                                isCordoned: false,
                                isShedding: false,
                                isDraining: false
                            }
                        }));
                        console.log(`[@cocreate/server] [MASTER] Reset worker${deadWorkerId} state in DB to stopped`);
                    }

                    if (this.state.isShuttingDown || this.state.isDraining) {
                        console.log(`[@cocreate/server] [MASTER] Worker ${worker.process.pid} removed. Active workers remaining: ${activeWorkers.size}`);
                        
                        if (activeWorkers.size === 0) {
                            console.log("[@cocreate/server] [MASTER] All workers successfully drained.");
                            if (this.state.masterShutdownTimeout) clearTimeout(this.state.masterShutdownTimeout);
                            if (typeof this.state.shutdownCompletionCallback === 'function') {
                                this.state.shutdownCompletionCallback();
                            } else {
                                process.exit(0);
                            }
                        }
                        return;
                    }

                    const targetId = deadWorkerId || (activeWorkers.size + 1);
                    
                    setTimeout(() => {
                        console.log(`[@cocreate/server] [MASTER] Respawning Worker ${targetId}...`);
                        const newWorker = cluster.fork(generateWorkerEnv(targetId));

                        newWorker.on('error', (err) => {
                            console.error(`[@cocreate/server] [MASTER] Error on replacement Worker ${targetId}:`, err.message);
                        });

                        activeWorkers.set(targetId, {
                            id: targetId,
                            pid: newWorker.process.pid,
                            process: newWorker,
                            isListening: false
                        });
                    }, 2000);
                });

                return this;
            }
        }

        if (!this.isMaster || this.totalWorkers <= 1) {
            if (!this.isMaster) {
                console.log(`[@cocreate/server] [WORKER ${this.workerId}] running with PID: ${process.pid} (Server ID: ${this.id})`);
            }

            this.telemetry = await ServerTelemetry.init(this);

            if (!this.ip) {
                this.ip = process.env.SERVER_IP || await getIP();
            }

            if (!this.infrastructure) {
                try {
                    this.infrastructure = process.env.SERVER_INFRASTRUCTURE 
                        ? JSON.parse(process.env.SERVER_INFRASTRUCTURE) 
                        : await resolveInfrastructure(this.id);
                } catch (error) {
                    this.infrastructure = await resolveInfrastructure(this.id);
                }
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
                if (process.connected) {
                    process.send({ type: 'listening_state', state });
                }
            };

            this.http.on('listening', () => reportListening(true));
            this.http.on('close', () => reportListening(false));
            this.http.listen(HTTP_PORT, BIND_IP, () => {
                console.log(`[@cocreate/server] [WORKER ${this.workerId}] HTTP listening on ${BIND_IP}:${HTTP_PORT}`);
            });

            this.https.listen(HTTPS_PORT, BIND_IP, () => {
                console.log(`[@cocreate/server] [WORKER ${this.workerId}] HTTPS listening on ${BIND_IP}:${HTTPS_PORT}`);
            });

            // Publish worker active running state object to DB upon creation/startup
            await this.publishState('running');
        }

        return this;
    }

    async handleEdgeRequest(req, res) {
        try {
            const requestUrl = req.url || '/';

            if (requestUrl.startsWith("/webhooks/")) {
                return this.loader.request(req, res);
            }
            return this.files.send(req, res);
        } catch (error) {
            res.writeHead(500, { "Content-Type": "text/plain" });
            res.end("Internal Error: Request routing failed");
        }
    }
}

export default new CoCreateServer();