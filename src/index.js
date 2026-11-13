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

import TLS from '@cocreate/tls';
import ServerMesh from './mesh/index.js';
import ServerAutoscaler from '@cocreate/autoscaler';
import ServerTelemetry from '@cocreate/telemetry';
import Config from '@cocreate/config';
import authenticate from '@cocreate/authenticate';
import authorize from '@cocreate/authorize';

import { SocketServer } from '@cocreate/socket';
import MCP from '@cocreate/mcp';
import { crudServer } from '@cocreate/crud';
import FileServer from '@cocreate/file/server';
import ApiServer from '@cocreate/api';
import Workflow from '@cocreate/workflows';
import Usage from '@cocreate/usage';

import { ObjectId, getClientInfo } from '@cocreate/utils';

import getDeployment from './getDeployment.js';
import Manager from './manager.js';
import Fetch from './fetch.js';
import http from './http.js';
import IPC from './ipc.js';
import Storage from './storage.js';
import State from './state.js';
import Log from './log.js';
import ApiKey from './apikey.js';
import Repository from './repository/index.js';
import Workers from './workers.js';
import { CoCreateDB } from '@cocreate/db';

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
        this.fetch = Fetch;
        this.responseListeners = new Map();

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

        // CoCreateDB owns its directory configuration and attaches itself to this server.
        await CoCreateDB.init(this);

        // Configure the local worker collection
        this.workers.configureWorkers(Config.get('workers'));

        this.crud = await crudServer.init(this);
        try {
            await ApiKey.init(this);
        } catch {
            console.error('[@cocreate/cluster] Unable to initialize the node organization API key.');
        }
        this.authenticate = authenticate.init(this);
        this.api = await ApiServer.init(this);
        this.workflow = await Workflow.init(this);
        await Manager.init(this);
        if (!this.isPrimary) await this.cluster.init(this);

        if (this.isPrimary) {
            console.log(`[@cocreate/cluster] [MASTER] Orchestrator running with PID: ${process.pid} (Server ID: ${this.id})`);
            const response = await this.crud.send({
                method: 'object.read', array: 'servers',
                filter: { query: { _id: this.id }, limit: 1 },
                organization_id: this.organization_id, host: this.host
            });
            if (!response || response.success === false ||
                (Array.isArray(response.error) ? response.error.length : response.error) ||
                !Array.isArray(response.object)) {
                throw new Error('Unable to read the saved server deployment.');
            }
            const persistedServer = response.object[0];
            const location = await getClientInfo();
            this.ip = location.ip;
            this.deployment = await getDeployment(this.id, {
                ...persistedServer?.deployment,
                ...this.deployment,
                location: { ...persistedServer?.deployment?.location, ...location }
            });
            this.autoscaler = await ServerAutoscaler.init(this);
            await this.cluster.init(this);

            // Reset logs array for a fresh logging session on master startup
            await this.crud.send(this.state.getServerObject({ logs: [] }));

            // Replace the persisted worker profile before the new workers start.
            const initialWorkerStates = this.workers.initialState(persistedServer);
            const workerResponse = await this.crud.send(this.state.getServerObject(initialWorkerStates));
            if (workerResponse?.error?.length || (workerResponse?.error && !Array.isArray(workerResponse.error)))
                console.warn('[@cocreate/cluster] Could not update the worker profile:', workerResponse.error);
            await this.state.save('running');

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

            if (!this.deployment) {
                try {
                    this.deployment = process.env.SERVER_DEPLOYMENT
                        ? JSON.parse(process.env.SERVER_DEPLOYMENT)
                        : await getDeployment(this.id);
                } catch (error) {
                    this.deployment = await getDeployment(this.id);
                }
            }
            if (!this.deployment.location) {
                this.deployment.location = await getClientInfo(this.id);
            }

            // Initialize certificates module on worker
            this.tls = await TLS.init(this);

            this.files = await FileServer.init(this);
            this.mcp = await MCP.init(this);
            await http.init(this);
            this.socket = new SocketServer(this);

            // Publish worker active running state object to DB upon creation/startup
            await this.state.save('running');
        }

        if (this.isPrimary) {
            try { await this.repository?.publish(); }
            catch { console.error('[@cocreate/cluster] Initial repository publication failed; inspect its reservation and retry on a later startup.'); }
        }

        return this;
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
