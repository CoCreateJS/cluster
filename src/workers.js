/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
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
import cluster from 'node:cluster';
import os from 'node:os';
import State from './state.js';

/**
 * Tracks local Node.js worker processes by logical ID and owns fork, readiness,
 * respawn, and drain coordination.
 * @public
 * @environment server
 */
export class Workers extends Map {
    #emitter = new EventEmitter();
    #exitListener;
    #config = { workers: 0 };
    #deleting = new Set();
    #respawns = new Map();
    #environments = new Map();

    /**
     * Creates the local worker collection and lifecycle interface.
     * @param {CoCreateServer} server - Reference to parent server instance.
 * @public
 * @environment server
 */
    constructor(server) {
        super();
        this.server = server;
        this.isPrimary = cluster.isPrimary;
        this.pendingPromises = new Map();
        this.workerId = this.isPrimary ? null : (parseInt(process.env.WORKER_ID, 10) || 1);
    }

    get isWorker() { return !this.isPrimary || this.#config.workers === 0; }

    /** Compatibility collection view for the former ClusterManager interface. */
    get workers() { return this; }

    /** Delegate event listeners without polluting top-level prototype  * @private
 * @environment server
 */
    on(event, listener) { this.#emitter.on(event, listener); return this; }
    once(event, listener) { this.#emitter.once(event, listener); return this; }
    off(event, listener) { this.#emitter.off(event, listener); return this; }
    emit(event, ...args) { return this.#emitter.emit(event, ...args); }

    /**
     * Resolves worker count configuration and assigns worker identity.
     * @param {string|number|boolean} [configuredWorkers] - Fork count; unset uses CPU count, zero runs work in the primary, and negative values reserve CPUs.
 * @private
 * @environment server
 */
    configureWorkers(configuredWorkers) {
        const cpuCount = os.cpus().length;
        if (configuredWorkers === undefined || configuredWorkers === null || configuredWorkers === '') {
            this.#config.workers = cpuCount;
        } else if (configuredWorkers === false || configuredWorkers === 'false') {
            this.#config.workers = 0;
        } else {
            const parsedWorkers = Number(configuredWorkers);
            if (!Number.isInteger(parsedWorkers)) {
                throw new TypeError('Worker count must be an integer, false, or unset.');
            }
            this.#config.workers = parsedWorkers < 0
                ? Math.max(0, cpuCount + parsedWorkers)
                : parsedWorkers;
        }

        if (this.isPrimary && this.#config.workers === 0) {
            this.workerId = 1;
        }

        this.server.workerId = this.workerId;
        this.server.isWorker = this.isWorker;
    }

    #handleWorkerListeningMsg(workerId, msg) {
        if (msg && msg.type === 'listening_state') {
            const w = this.get(workerId);
            if (w && !this.#deleting.has(workerId)) {
                w.isListening = Boolean(msg.state);
                if (this.server.state?.evaluateMasterFallback) {
                    this.server.state.evaluateMasterFallback();
                }

                const workersReady = this.size > 0 &&
                    [...this.values()].every((item) => item.isListening);
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
    }

    async #handleWorkerExit(worker, code, signal) {
        console.log(`[@cocreate/cluster] [MASTER] Worker ${worker.process.pid} exited (Code: ${code}, Signal: ${signal})`);

        let deadWorkerId = null;
        for (let [id, w] of this.entries()) {
            if (w.pid === worker.process.pid) {
                deadWorkerId = id;
                super.delete(id);
                break;
            }
        }

        if (deadWorkerId === null) return;

        if (this.server.state?.evaluateMasterFallback) {
            this.server.state.evaluateMasterFallback();
        }

        if (deadWorkerId && this.server.crud) {
            await this.server.crud.send(this.server.state.getServerObject({
                totalWorkers: this.size,
                [`worker${deadWorkerId}`]: {
                    pid: null,
                    status: 'stopped',
                    isCordoned: false,
                    isShedding: false,
                    isDraining: false
                }
            }));
        }

        const explicitlyDeleted = this.#deleting.delete(deadWorkerId);
        if (explicitlyDeleted) this.#environments.delete(deadWorkerId);
        const state = this.server.state;
        const isCordoned = state?.isCordon || state?.isCordoned;

        if (state?.isShuttingDown || state?.isDraining || isCordoned) {
            this.#environments.delete(deadWorkerId);
            console.log(`[@cocreate/cluster] [MASTER] Worker ${worker.process.pid} removed. Active workers remaining: ${this.size}`);

            if (this.size === 0) {
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

        if (explicitlyDeleted) return;
        const targetId = deadWorkerId;

        this.#respawns.set(targetId, setTimeout(() => {
            this.#respawns.delete(targetId);
            if (this.server.state?.isShuttingDown || this.server.state?.isDraining ||
                this.server.state?.isCordon || this.server.state?.isCordoned) {
                this.#environments.delete(targetId);
                return;
            }
            console.log(`[@cocreate/cluster] [MASTER] Respawning Worker ${targetId}...`);
            this.#spawn(targetId, this.#environments.get(targetId));
        }, 2000));
    }

    /** Create a worker and return its tracked record. Options are fork environment variables. */
    add(options = {}) {
        if (!options || typeof options !== 'object' || Array.isArray(options)) {
            throw new TypeError('Worker options must be an environment object.');
        }
        let workerId = this.isWorker && this.isPrimary ? this.workerId + 1 : 1;
        while (this.has(workerId) || this.#respawns.has(workerId)) workerId++;
        return this.#spawn(workerId, options);
    }

    /** Patch metadata in place; process identity and readiness remain lifecycle-owned. */
    set(workerId, updates) {
        const record = this.get(workerId);
        if (!record) throw new Error(`Worker ${workerId} is not tracked.`);
        if (!updates || typeof updates !== 'object' || Array.isArray(updates)) {
            throw new TypeError('Worker updates must be an object.');
        }
        for (const [key, value] of Object.entries(updates)) {
            if (['id', 'pid', 'process', 'isListening', '__proto__', 'constructor', 'prototype'].includes(key)) continue;
            record[key] = value;
        }
    }

    /** Request graceful shutdown; retain the record until exit and suppress replacement. */
    delete(workerId) {
        if (this.#respawns.has(workerId)) {
            clearTimeout(this.#respawns.get(workerId));
            this.#respawns.delete(workerId);
            this.#environments.delete(workerId);
        }
        const record = this.get(workerId);
        if (!record) {
            if (this.#environments.has(workerId)) this.#deleting.add(workerId);
            return;
        }
        if (this.#deleting.has(workerId)) return;
        this.#deleting.add(workerId);
        record.isListening = false;
        this.server.state?.evaluateMasterFallback?.();
        const stop = () => record.process.kill('SIGTERM');
        try {
            record.process.send({ method: 'shutdown', master: false, worker: workerId, server: this.server.id }, error => {
                if (error) stop();
            });
        } catch {
            stop();
        }
    }

    /** Clear the managed collection through the same worker shutdown lifecycle. */
    clear() {
        for (const id of new Set([...this.keys(), ...this.#respawns.keys()])) this.delete(id);
    }

    #spawn(workerId, options = {}) {
        if (!this.isPrimary) throw new Error('Only the primary can add workers.');
        if (this.has(workerId)) throw new Error(`Worker ${workerId} is already tracked.`);
        if (!this.#exitListener) {
            this.#exitListener = (worker, code, signal) => this.#handleWorkerExit(worker, code, signal);
            cluster.on('exit', this.#exitListener);
        }
        const worker = cluster.fork({
            ...options,
            WORKER_ID: String(workerId),
            SERVER_ID: String(this.server.id || ''),
            SERVER_IP: String(this.server.ip || '127.0.0.1'),
            SERVER_INFRASTRUCTURE: JSON.stringify(this.server.infrastructure || {}),
            SERVER_LOCATION: JSON.stringify(this.server.location || {}),
        });
        worker.on('error', error => {
            console.error(`[@cocreate/cluster] [MASTER] Error on Worker ${workerId}:`, error.message);
        });
        worker.on('message', message => this.#handleWorkerListeningMsg(workerId, message));
        const record = {
            id: workerId,
            pid: worker.process.pid,
            process: worker,
            isListening: false
        };
        super.set(workerId, record);
        this.#environments.set(workerId, { ...options });
        return record;
    }

    initialState(persistedServer = {}) {
        const state = { totalWorkers: this.size };
        for (let id = 1; id <= Math.max(1, this.#config.workers); id++) {
            state[`worker${id}`] = { pid: null, status: 'stopped', isCordoned: false, isShedding: false, isDraining: false };
        }
        for (const field of Object.keys(persistedServer || {})) {
            if (/^worker[1-9]\d*$/.test(field) && !Object.hasOwn(state, field)) {
                state[`$unset.${field}`] = true;
            }
        }
        return state;
    }

    /**
     * Initializes master cluster forks and listens for exit signals.
 * @private
 * @environment server
 */
    async setupMasterCluster() {
        if (!this.server.state)
            this.server.state = await State.init(this.server);
        if (!this.isPrimary || this.isWorker) return false;

        console.log(`[@cocreate/cluster] [MASTER] Orchestrator creating ${this.#config.workers} workers.`);

        for (let i = 0; i < this.#config.workers; i++) this.#spawn(i + 1);
        return true;
    }
}

export default Workers;
