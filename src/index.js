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

/**
 * Robust helper to retrieve configuration variables.
 * Resolves both flat dot-notation keys (e.g., obj["mesh.provider"]) 
 * and standard nested properties (e.g., obj.mesh.provider).
 */
function getVal(obj, path) {
    if (!obj) return undefined;
    if (obj[path] !== undefined) return obj[path];
    return path.split('.').reduce((acc, part) => acc && acc[part], obj);
}

export class CoCreateServer {
    constructor() {
        this.id = id;
        this.isPrimary = isPrimary;
        if (isPrimary) {
            this.workers = activeWorkers;
        }
        this.workerId = isPrimary ? 0 : parseInt(process.env.WORKER_ID, 10) || 1;
        this.totalWorkers = 1;
        this.workerStorage = null;
        this.isInitialized = false;
        
        // Dynamic host parameters resolved on boot
        this.organization_id = null;
        this.host = null;
        this.ip = null;
        this.infrastructure = null;

        // Core systems
        this.certificate = null;
        this.ingress = null;
        this.http = Http.createServer((req, res) => this.handleEdgeRequest(req, res));
        this.https = null;
        this.authenticate = authenticate;
        this.authorize = authorize;
        this.wsManager = null;
        this.mesh = null;
        this.crud = null;
        this.metrics = null
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
            'workers': '',
            'ingress': '',
            'certificate': '',
            'mesh': '',
            'metrics': ''
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
            console.log(`[@cocreate/server] [PRIMARY] Orchestrator running with PID: ${process.pid}`);
            for (let i = 0; i < this.totalWorkers; i++) {
                const currentWorkerId = i + 1;
                const worker = cluster.fork({ 
                    WORKER_ID: currentWorkerId,
                    SERVER_ID: this.id 
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
                    SERVER_ID: this.id 
                });
                activeWorkers.set(targetId, {
                    id: targetId,
                    pid: newWorker.process.pid,
                    process: newWorker
                });
            });

            return this;
        }

        console.log(`[@cocreate/server] Worker ${process.pid} (ID: ${this.workerId}) resolving environment...`);
        this.ip = await getIP();
        this.infrastructure = await resolveInfrastructure(this.id);

        const certProvider = getVal(config, 'certificate.provider') || 'acme';
        const certImportPath = getVal(config, 'certificate.import') || `@cocreate/${certProvider}`;
        if (certProvider !== 'none' && certProvider !== 'false') {
            const loadedCertModule = await importModule(certImportPath);
            if (loadedCertModule) {
                this.https = (await import('node:https')).createServer({
                    SNICallback: (domain, cb) => loadedCertModule.sniCallback(domain, cb)
                }, (req, res) => this.handleEdgeRequest(req, res));
                this.certificate = loadedCertModule;
                await this.certificate.init(this);
            }
        }

        const ingressProvider = getVal(config, 'ingress.provider') || 'nginx';
        const ingressImportPath = getVal(config, 'ingress.import') || `@cocreate/${ingressProvider}`;
        if (ingressProvider !== 'none' && ingressProvider !== 'false') {
            const IngressClass = await importModule(ingressImportPath);
            if (IngressClass) {
                const clusterProxy = new Proxy(cluster, {
                    get(target, prop) {
                        if (prop === 'worker') return target.worker || { id: 1 };
                        return Reflect.get(target, prop);
                    }
                });
                this.ingress = new IngressClass(clusterProxy);
                await this.ingress.init();
            }
        }

        this.crud = await CrudServer.init(this);
        this.wsManager = new SocketServer(this);
        this.api = await ApiServer.init(this);
        this.files = FileServer.init(this);
        this.loader = await LazyLoader.init(this);

        const meshProvider = getVal(config, 'mesh.provider') || 'socket-mesh';
        const meshImportPath = getVal(config, 'mesh.import') || `@cocreate/${meshProvider}`;
        if (meshProvider !== 'none' && meshProvider !== 'false') {
            const MeshModule = await importModule(meshImportPath);
            if (MeshModule) {
                this.mesh = MeshModule;
                if (typeof this.mesh.init === 'function') {
                    await this.mesh.init(this);
                } else {
                    console.error(`[@cocreate/server] Error: Loaded mesh module from "${meshImportPath}" but it does not export an "init" function.`);
                }
            } else {
                console.error(`[@cocreate/server] Error: Mesh module could not be imported from "${meshImportPath}". Check package dependencies or spelling.`);
            }
        }

        const metricsProvider = getVal(config, 'metrics.provider') || 'metrics';
        const metricsImportPath = getVal(config, 'metrics.import') || `@cocreate/${metricsProvider}`;
        if (metricsProvider !== 'none' && metricsProvider !== 'false') {
            const MetricsModule = await importModule(metricsImportPath);
            if (MetricsModule) {
                this.metrics = await MetricsModule.init(this);
            } else {
                console.error(`[@cocreate/server] Error: Metrics module could not be imported from "${meshImportPath}". Check package dependencies or spelling.`);
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

async function importModule(importPath) {
    try {
        const modulePayload = await import(importPath);
        return modulePayload && modulePayload.default !== undefined ? modulePayload.default : modulePayload;
    } catch (err) {
        console.error(`[@cocreate/server] Failed to load module at "${importPath}":`, err.message);
        return null;
    }
}

export default new CoCreateServer();
