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

import cluster from 'node:cluster';
import { uuid } from '@cocreate/utils';

function resolvePending(pendingPromises, message) {
    const messageId = message.uuid || message.messageId;
    if (!messageId || !pendingPromises.has(messageId)) return false;

    const promiseHandler = pendingPromises.get(messageId);
    pendingPromises.delete(messageId);

    if (message.error || message.success === false) {
        promiseHandler.reject(new Error(message.error || 'IPC request failed'));
    } else {
        promiseHandler.resolve(
            message.uuid || message.result === undefined
                ? message
                : message.result
        );
    }

    return true;
}

function normalizeBroadcast(payload) {
    if (payload.broadcast === 'false') payload.broadcast = false;
}

function isOrganizationDelivery(payload) {
    return Boolean(
        payload.organization_id &&
        payload.response !== true
    );
}

function includesTarget(target, value) {
    if (target === undefined) return true;
    const targets = Array.isArray(target) ? target : [target];
    return targets.some(item => String(item) === String(value));
}

/**
 * Initializes Inter-Process Communication (IPC) transport routing between 
 * the Master orchestrator and clustered Worker processes.
 * 
 * @param {Object} Server - The CoCreateServer instance reference
 * @param {Map} pendingPromises - Tracking map for async worker RPC promises
 * @returns {Function} Context-aware send function for dispatching cluster messages
 */
export function init(Server, pendingPromises) {
    const organizationWorkers = Server.organizationWorkers || new Map();
    Server.organizationWorkers = organizationWorkers;

    function updateOrganizationWorker(payload, remove = false) {
        const organizationId = payload.organization_id;
        const workerId = payload.workerId;
        if (!organizationId || workerId == null) return;

        let workers = organizationWorkers.get(organizationId);
        const wasActive = Boolean(workers && workers.size);

        if (remove) {
            if (!workers) return;
            workers.delete(workerId);
            if (!workers.size) organizationWorkers.delete(organizationId);
        } else {
            if (!workers) {
                workers = new Set();
                organizationWorkers.set(organizationId, workers);
            }
            workers.add(workerId);
        }

        const isActive = organizationWorkers.has(organizationId);
        if (wasActive === isActive || !Server.mesh) return;

        if (isActive) {
            Server.mesh.openOrganization?.(organizationId);
        } else {
            Server.mesh.closeOrganization?.(organizationId);
        }
    }

    if (Server.isPrimary) {
        cluster.on('exit', (worker) => {
            for (const [organizationId, workers] of organizationWorkers) {
                if (!workers.delete(worker.id)) continue;
                if (workers.size) continue;

                organizationWorkers.delete(organizationId);
                Server.mesh?.closeOrganization?.(organizationId);
            }
        });

        cluster.on('message', async (worker, message) => {
            if (!message || typeof message !== 'object') return;
            normalizeBroadcast(message);

            // Only the process that originated this UUID can own its Promise.
            if (resolvePending(pendingPromises, message)) return;

            if (message.workerId === undefined) {
                message.workerId = worker?.id;
            }
            if (message.serverId === undefined) {
                message.serverId = Server.id;
            }
            message.status = 'sent';

            if (message.method === 'registerOrg') {
                updateOrganizationWorker(message);
                return;
            }
            if (message.method === 'deregisterOrg') {
                updateOrganizationWorker(message, true);
                return;
            }

            // Responses are routed to their origin and are not executed again.
            if (
                message.method &&
                !message.organization_id &&
                message.master !== false &&
                message.response !== true
            ) {
                Server.emit(message.method, message);
            }

            // Relay without creating another Promise.
            if (
                message.broadcast ||
                message.worker ||
                message.server !== undefined ||
                message.cluster !== undefined ||
                message.organization_id ||
                message.response === true
            ) {
                route(message);
            }
        });

        function route(payload = {}) {
            if (!payload || typeof payload !== 'object') return;
            normalizeBroadcast(payload);

            const organizationDelivery = isOrganizationDelivery(payload);
            const targetsCurrentServer =
                includesTarget(payload.server, Server.id) &&
                includesTarget(payload.cluster, Server.cluster_id);
            const targetsRemoteServer =
                payload.server === undefined ||
                (Array.isArray(payload.server)
                    ? payload.server
                    : [payload.server]
                ).some(serverId => String(serverId) !== String(Server.id));

            if (
                payload.serverId === Server.id &&
                targetsRemoteServer &&
                (organizationDelivery ||
                    payload.server !== undefined ||
                    payload.cluster !== undefined) &&
                Server.mesh &&
                typeof Server.mesh.send === 'function'
            ) {
                Server.mesh.send(payload);
            }

            const isMasterControl =
                payload.method === 'restart' ||
                payload.method === 'reboot' ||
                payload.method === 'shutdown';
            if (
                isMasterControl &&
                payload.master === true &&
                Server.isPrimary &&
                targetsCurrentServer
            ) {
                Server.emit(payload.method, payload);
                return;
            }

            const isWorkerControl =
                payload.method === 'cordon' ||
                payload.method === 'shed' ||
                payload.method === 'drain';
            if (
                isWorkerControl &&
                Server.isWorker &&
                targetsCurrentServer &&
                includesTarget(payload.worker, Server.workerId)
            ) {
                Server.emit(payload.method, payload);
            }

            const targetWorkers = [];

            if (targetsCurrentServer && payload.worker) {
                const organizationWorkerIds = payload.organization_id
                    ? organizationWorkers.get(payload.organization_id) || new Set()
                    : null;
                if (Array.isArray(payload.worker)) {
                    for (const wId of payload.worker) {
                        if (
                            organizationWorkerIds &&
                            !includesTarget([...organizationWorkerIds], wId)
                        ) continue;
                        const w = Server.workers.get(wId);
                        if (w) targetWorkers.push(w);
                    }
                } else {
                    if (
                        organizationWorkerIds &&
                        !includesTarget(
                            [...organizationWorkerIds],
                            payload.worker
                        )
                    ) return;
                    const w = Server.workers.get(payload.worker);
                    if (w) targetWorkers.push(w);
                }
            } else if (targetsCurrentServer && organizationDelivery) {
                const workerIds = organizationWorkers.get(
                    payload.organization_id
                ) || [];
                for (const workerId of workerIds) {
                    const worker = Server.workers.get(workerId);
                    if (worker) targetWorkers.push(worker);
                }
            } else if (targetsCurrentServer && payload.broadcast) {
                for (const w of Server.workers.values()) {
                    targetWorkers.push(w);
                }
            }

            const filteredWorkers = targetWorkers.filter((workerObj) => {
                if (
                    organizationDelivery &&
                    payload.serverId === Server.id &&
                    payload.workerId === workerObj.id
                ) return false;

                return payload.broadcastSender !== false ||
                    workerObj.id !== payload.workerId;
            });

            // Check edge listener fallback conditions for cordon/drain commands
            if (payload.method === 'cordon' || payload.method === 'drain') {
                const isEnabling = typeof payload.value === 'boolean' ? payload.value : (payload.value !== false);
                if (isEnabling) {
                    const targetedIds = new Set(targetWorkers.map(w => w.id));
                    let activeRemaining = 0;
                    
                    for (const w of Server.workers.values()) {
                        if (!targetedIds.has(w.id) && w.isListening && !w.isCordoned && !w.isDraining) {
                            activeRemaining++;
                        }
                    }
                    
                    if (activeRemaining === 0) {
                        Server.state.evaluateMasterFallback();
                    }
                }
            }

            for (const workerObj of filteredWorkers) {
                const workerProcess = workerObj.process || workerObj;
                if (workerProcess && typeof workerProcess.send === 'function' && workerProcess.isConnected()) {
                    try {
                        workerProcess.send(payload);
                    } catch (err) {
                        console.error(`[@cocreate/cluster] [MASTER] Failed to send IPC payload to worker ${workerObj.id}:`, err.message);
                    }
                }
            }

            if (
                organizationDelivery &&
                Server.isWorker &&
                Server.wsManager &&
                !(payload.serverId === Server.id &&
                    payload.workerId === Server.workerId)
            ) {
                const data = { ...payload, serverSent: true };
                Server.wsManager.send(data);
            }

        }

        return function send(payload = {}) {
            if (!payload || typeof payload !== 'object') return Promise.resolve(payload);
            normalizeBroadcast(payload);

            if (!payload.uuid) payload.uuid = uuid();
            if (
                Server.isWorker &&
                payload.serverId === undefined &&
                payload.workerId === undefined
            ) {
                payload.workerId = Server.workerId;
            }
            if (payload.serverId === undefined) {
                payload.serverId = Server.id;
                payload.status = 'sent';
            }

            if (payload.method === 'registerOrg') {
                updateOrganizationWorker(payload);
                return Promise.resolve(payload);
            }
            if (payload.method === 'deregisterOrg') {
                updateOrganizationWorker(payload, true);
                return Promise.resolve(payload);
            }

            const isResponse = payload.response === true;
            if (isResponse) {
                payload.worker = payload.workerId;
                payload.master = payload.workerId == null;
            }

            const response = new Promise((resolve, reject) => {
                if (
                    !isResponse &&
                    !payload.broadcast &&
                    payload.broadcastSender === true
                ) {
                    pendingPromises.set(payload.uuid, { resolve, reject });
                } else {
                    resolve(payload);
                }
            });

            route(payload);

            if (
                payload.method &&
                !payload.organization_id &&
                payload.master !== false &&
                !isResponse &&
                payload.broadcastSender !== false
            ) {
                Server.emit(payload.method, payload);
            }

            return response;
        };
    } else {
        process.on('message', async (message) => {
            if (!message || typeof message !== 'object') return;

            if (resolvePending(pendingPromises, message)) return;

            if (
                message.method === 'cordon' ||
                message.method === 'shed' ||
                message.method === 'drain'
            ) {
                Server.emit(message.method, message);
                return;
            }

            if (isOrganizationDelivery(message) && Server.wsManager) {
                const data = { ...message, serverSent: true };
                Server.wsManager.send(data);
                return;
            }

            if (message.method) {
                Server.emit(message.method, message);
            }
        });

        return function send(payload = {}) {
            if (!payload || typeof payload !== 'object') return Promise.resolve(payload);
            normalizeBroadcast(payload);

            if (!payload.uuid) payload.uuid = uuid();
            if (
                payload.serverId === undefined &&
                payload.workerId === undefined
            ) {
                payload.workerId = Server.workerId;
            }

            const isResponse = payload.response === true;
            if (isResponse) {
                payload.worker = payload.workerId;
                payload.master = payload.workerId == null;
            }
            const response = new Promise((resolve, reject) => {
                if (
                    !isResponse &&
                    !payload.broadcast &&
                    payload.broadcastSender === true
                ) {
                    pendingPromises.set(payload.uuid, { resolve, reject });
                } else {
                    resolve(payload);
                }
            });

            if (typeof process.send === 'function' && process.connected) {
                try {
                    process.send(payload);
                } catch (err) {
                    console.error(`[@cocreate/cluster] [WORKER ${process.pid}] Failed to dispatch IPC message to master:`, err.message);
                }
            } else {
                if (payload.method) {
                    Server.emit(payload.method, payload);
                }
            }

            return response;
        };
    }
}

export default { init };
