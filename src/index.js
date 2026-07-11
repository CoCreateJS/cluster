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

// Commercial Licensing Information:
// For commercial use of this software without the copyleft provisions of the AGPLv3,
// you must obtain a commercial license from CoCreate LLC.
// For details, visit <https://cocreate.app/licenses/> or contact us at sales@cocreate.app.

import Http from 'node:http';
import cluster from 'node:cluster';
import os from 'node:os';
import uid from '@cocreate/uuid';
import SocketServer from '@cocreate/socket-server';
import Config from '@cocreate/config';
import CrudServer from '@cocreate/crud-server';
import LazyLoader from '@cocreate/lazy-loader';
import FileServer from '@cocreate/file-server';
import authenticate from '@cocreate/authenticate';
import authorize from '@cocreate/authorize';

const isPrimary = cluster.isPrimary !== undefined ? cluster.isPrimary : cluster.isMaster;
const instanceId = process.env.INSTANCE_ID || uid.generate();

const sharedMap = new Map();
const pendingPromises = new Map();
const activeWorkers = new Map();

export class CoCreateServer {
    constructor() {
        // Core lifecycle and process identity context
        this.isPrimary = isPrimary;
        this.workerId = isPrimary ? 0 : parseInt(process.env.WORKER_ID, 10) || 1;
        this.totalWorkers = 1;
        this.workerStorage = null;
        this.isInitialized = false;
        this.instanceId = instanceId;
        this.config = {};
        // Directly defined properties, visible immediately to consumers
        this.certificate = null;
        this.http = Http.createServer((req, res) => this.handleEdgeRequest(req, res));
        this.https = null;
        this.wsManager = null;
        this.ingress = null;
        this.crud = null;
        this.files = null;
        this.loader = null;
        this.authenticate = authenticate;
        this.authorize = authorize;

        if (isPrimary) {
            this.workers = activeWorkers;
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

        const config = await Config({
            'workers': {
                prompt: 'Enter number of worker processes (or "false" for standalone): '
            },
            'ingress.provider': {
                prompt: 'Enter your ingress provider (e.g., nginx, haproxy, or none): '
            },
            'certificate.provider': {
                prompt: 'Enter your certificate provider (e.g., acme, self-signed, or none): '
            }
        });

        const ingressProvider = config?.ingress?.provider || 'nginx';
        const certProvider = config?.certificate?.provider || 'acme';
        let configuredWorkers = config?.workers;

        if (!configuredWorkers || configuredWorkers === 'false') {
            this.totalWorkers = 1;
        } else {
            this.totalWorkers = parseInt(configuredWorkers, 10) || os.cpus().length;
        }

        if (this.isPrimary && this.totalWorkers > 1) {
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator process running with PID: ${process.pid}`);
            console.log(`[@cocreate/server] [PRIMARY] Forking ${this.totalWorkers} vertical worker instances...`);

            for (let i = 0; i < this.totalWorkers; i++) {
                const currentWorkerId = i + 1;
                const worker = cluster.fork({ 
                    WORKER_ID: currentWorkerId,
                    INSTANCE_ID: this.instanceId 
                });
                
                activeWorkers.set(currentWorkerId, {
                    id: currentWorkerId,
                    pid: worker.process.pid,
                    process: worker
                });
            }

            cluster.on('exit', (worker, code, signal) => {
                console.log(`[@cocreate/server] [PRIMARY] Worker ${worker.process.pid} terminated (Code: ${code}, Signal: ${signal})`);
                
                let deadWorkerId = null;
                for (let [id, w] of activeWorkers.entries()) {
                    if (w.pid === worker.process.pid) {
                        deadWorkerId = id;
                        activeWorkers.delete(id);
                        break;
                    }
                }

                const targetId = deadWorkerId || (activeWorkers.size + 1);
                console.log(`[@cocreate/server] [PRIMARY] Respawning replacement for Worker ID: ${targetId}...`);
                
                const newWorker = cluster.fork({ 
                    WORKER_ID: targetId,
                    INSTANCE_ID: this.instanceId 
                });
                activeWorkers.set(targetId, {
                    id: targetId,
                    pid: newWorker.process.pid,
                    process: newWorker
                });
            });

            return this;
        }

        this.crud = await CrudServer.init(this);
        this.wsManager = new SocketServer(this);
        this.files = FileServer.init(this);
        this.loader = LazyLoader.init(this);

        const ingressImportPath = config?.ingress?.import || `@cocreate/${ingressProvider}`;
        const certImportPath = config?.certificate?.import || `@cocreate/${certProvider}`;

        if (certProvider !== 'none' && certProvider !== 'false') {
            const loadedCertModule = await importModule(certImportPath);
            
            if (loadedCertModule) {
                this.https = (await import('node:https')).createServer({
                    SNICallback: (domain, cb) => loadedCertModule.sniCallback(domain, cb)
                }, (req, res) => this.handleEdgeRequest(req, res));
                this.certificate = loadedCertModule.init(this);
            }
        }

        if (ingressProvider !== 'none' && ingressProvider !== 'false') {
            const IngressClass = await importModule(ingressImportPath);
            
            if (IngressClass) {
                const clusterProxy = new Proxy(cluster, {
                    get(target, prop) {
                        if (prop === 'worker') {
                            return target.worker || { id: 1 };
                        }
                        return Reflect.get(target, prop);
                    }
                });

                this.ingress = new IngressClass(clusterProxy);
            }
        }

        const HTTP_PORT = 8080;
        const HTTPS_PORT = 8443;

        if (this.http) {
            this.http.listen(HTTP_PORT, () => {
                console.log(`[@cocreate/server] Worker ${process.pid} (ID: ${this.workerId}) HTTP listening on PORT ${HTTP_PORT}`);
            });
        }

        if (this.https) {
            this.https.listen(HTTPS_PORT, () => {
                console.log(`[@cocreate/server] Worker ${process.pid} (ID: ${this.workerId}) HTTPS listening on PORT ${HTTPS_PORT}`);
            });
        }

        return this;
    }

    async handleEdgeRequest(req, res) {
        try {
            const requestUrl = req.url || '/';
            const isWebhookRoute = requestUrl.startsWith("/webhooks/");

            if (isWebhookRoute) {
                if (this.loader) {
                    return this.loader.request(req, res);
                }
            }

            if (this.files) {
                return this.files.send(req, res);
            }

            res.writeHead(500, { "Content-Type": "text/plain" });
            res.end("Internal Server Error: Files server not found");
        } catch (error) {
            console.error("[@cocreate/server] Error routing request:", error);
            res.writeHead(400, { "Content-Type": "text/plain" });
            res.end("Invalid edge request format");
        }
    }
}

function executeLocalWorkerStorageOperation(method, key, value) {
    switch (method) {
        case 'set':
            sharedMap.set(key, value);
            return true;
        case 'get':
            return sharedMap.get(key);
        case 'delete':
            return sharedMap.delete(key);
        case 'size':
            return sharedMap.size;
        case 'clear':
            sharedMap.clear();
            return true;
        default:
            throw new Error(`[@cocreate/server] Unsupported worker storage operation: "${method}"`);
    }
}

function workerStorageManager(serverCtx, method, key, value) {
    return new Promise((resolve, reject) => {
        try {
            if (serverCtx.isPrimary) {
                const result = executeLocalWorkerStorageOperation(method, key, value);
                resolve(result);
            } else {
                const messageId = uid.generate();
                pendingPromises.set(messageId, { resolve, reject });
                process.send({
                    instanceId: serverCtx.instanceId,
                    messageId,
                    method,
                    key,
                    value
                });
            }
        } catch (error) {
            reject(new Error(`[@cocreate/server] ${method} worker storage operation failed: ${error.message}`));
        }
    });
}

function setupIPC(serverCtx) {
    if (serverCtx.isPrimary) {
        cluster.on('message', (worker, message) => {
            if (message && message.instanceId === serverCtx.instanceId) {
                try {
                    const result = executeLocalWorkerStorageOperation(message.method, message.key, message.value);
                    message.value = result;
                    worker.send(message);
                } catch (error) {
                    message.error = error.message;
                    worker.send(message);
                }
            }
        });
    } else {
        process.on('message', (message) => {
            if (message && message.instanceId === serverCtx.instanceId) {
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

async function importModule(importPath) {
    try {
        const modulePayload = await import(importPath);
        return modulePayload && modulePayload.default !== undefined ? modulePayload.default : modulePayload;
    } catch (err) {
        console.error(`[@cocreate/server] Failed to load "${importPath}":`, err);
        return null;
    }
}

const coCreateServer = new CoCreateServer();
export default coCreateServer;