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
 * ********************************************************************************/

import Http from 'node:http';
import Https from 'node:https';
import Acme from '@cocreate/acme';
import SocketMesh from '@cocreate/socket-mesh';
import Metrics from '@cocreate/metrics';

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

// Importing our dynamic environmental resolvers
import resolveInfrastructure from './getInfrastructure.js';
import getIP from './getIp.js';

const isPrimary = cluster.isPrimary !== undefined ? cluster.isPrimary : cluster.isMaster;
const id = process.env.SERVER_ID || ObjectId().toString();

const sharedMap = new Map();
const pendingPromises = new Map();
const activeWorkers = new Map();

export class CoCreateServer {
    constructor() {
        this.id = id;
        this.isPrimary = isPrimary;
        
        if (isPrimary) {
            this.workers = activeWorkers;
        } else {
            this.workerId = parseInt(process.env.WORKER_ID, 10) || 1;
        }
        
        this.totalWorkers = 1;
        this.workerStorage = null;
        this.isInitialized = false;
        
        this.organization_id = null;
        this.host = null;
        this.ip = null;
        this.infrastructure = null;

        this.certificate = null;
        // this.ingress = null;
        
        this.http = Http.createServer((req, res) => {
            const requestUrl = req.url || '/';
            // 1. Allow ACME challenges to pass through over unencrypted HTTP
            if (requestUrl.startsWith('/.well-known/acme-challenge/')) {
                return this.handleEdgeRequest(req, res);
            }
            // 2. Force a 301 redirect to HTTPS for all other traffic (Replacing Nginx)
            const host = req.headers.host ? req.headers.host.split(':')[0] : 'localhost';
            res.writeHead(301, { "Location": `https://${host}${requestUrl}` });
            res.end();
        });
        
        this.https = null;
        this.authenticate = authenticate;
        this.authorize = authorize;
        this.wsManager = null;
        this.mesh = null;
        this.crud = null;
        this.metrics = null;
        this.files = null;
        this.loader = null;
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

        const config = await Config({
            'organization_id': '',
            'host': '',
            'workers': ''
        });

        this.organization_id = config.organization_id || null;
        this.host = config.host || null;

        let configuredWorkers = config.workers;

        if (configuredWorkers === false || configuredWorkers === 'false') {
            this.totalWorkers = 1;
        } else {
            this.totalWorkers = parseInt(configuredWorkers, 10) || os.cpus().length;
        }

        if (this.isPrimary && this.totalWorkers > 1) {
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator resolving environmental context...`);
            
            this.ip = await getIP();
            this.infrastructure = await resolveInfrastructure(this.id);

            console.log(`[@cocreate/server] [PRIMARY] Orchestrator running with PID: ${process.pid}`);
            for (let i = 0; i < this.totalWorkers; i++) {
                const currentWorkerId = i + 1;
                const worker = cluster.fork({ 
                    WORKER_ID: currentWorkerId,
                    SERVER_ID: this.id,
                    SERVER_IP: this.ip,
                    SERVER_INFRASTRUCTURE: JSON.stringify(this.infrastructure)
                });
                activeWorkers.set(currentWorkerId, {
                    id: currentWorkerId,
                    pid: worker.process.pid,
                    process: worker
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
                const targetId = deadWorkerId || (activeWorkers.size + 1);
                
                const newWorker = cluster.fork({ 
                    WORKER_ID: targetId,
                    SERVER_ID: this.id, 
                    SERVER_IP: this.ip,
                    SERVER_INFRASTRUCTURE: JSON.stringify(this.infrastructure)
                });
                activeWorkers.set(targetId, {
                    id: targetId,
                    pid: newWorker.process.pid,
                    process: newWorker
                });
            });

            return this;
        }

        const processLabel = this.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} adopting primary environmental context...`);
        
        this.ip = process.env.SERVER_IP || await getIP();
        
        try {
            this.infrastructure = process.env.SERVER_INFRASTRUCTURE 
                ? JSON.parse(process.env.SERVER_INFRASTRUCTURE) 
                : await resolveInfrastructure(this.id);
        } catch (error) {
            console.error(`[@cocreate/server] Worker ${process.pid} failed to parse pre-resolved infrastructure. Falling back...`, error);
            this.infrastructure = await resolveInfrastructure(this.id);
        }

        // 1. Initialize HTTPS & Certificate via Acme
        this.certificate = Acme;
        this.https = Https.createServer({
            SNICallback: (domain, cb) => this.certificate.sniCallback(domain, cb)
        }, (req, res) => this.handleEdgeRequest(req, res));
        await this.certificate.init(this);

        // Standard Core Services
        this.crud = await CrudServer.init(this);
        this.wsManager = new SocketServer(this);
        this.api = await ApiServer.init(this);
        this.files = FileServer.init(this);
        this.loader = await LazyLoader.init(this);

        // 3. Initialize Mesh Synchronization
        this.mesh = SocketMesh;
        if (typeof this.mesh.init === 'function') {
            await this.mesh.init(this);
        } else {
            console.error(`[@cocreate/server] Error: Loaded mesh module does not export an "init" function.`);
        }

        // 4. Initialize Performance Metrics
        this.metrics = await Metrics.init(this);

        // Bind directly to standard web ports (Requires setcap or root privileges)
        const HTTP_PORT = process.env.HTTP_PORT || 80;
        const HTTPS_PORT = process.env.HTTPS_PORT || 443;
        const BIND_IP = process.env.BIND_IP || '0.0.0.0'; // Accept traffic from any network interface

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
        });
    } else {
        process.on('message', (message) => {
            if (message && message.id === serverCtx.id) {
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
        });
    }
}

export default new CoCreateServer();