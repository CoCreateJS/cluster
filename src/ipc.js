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

/**
 * Initializes Inter-Process Communication (IPC) transport routing between 
 * the Primary orchestrator and clustered Worker processes.
 * 
 * @param {Object} serverCtx - The CoCreateServer instance reference
 * @param {Map} pendingPromises - Tracking map for async worker RPC promises
 * @returns {Function} Context-aware send function for dispatching cluster messages
 */
export function init(serverCtx, pendingPromises) {
    if (serverCtx.isPrimary) {
        cluster.on('message', async (worker, message) => {
            if (!message || typeof message !== 'object') return;

            if (message.type === 'listening_state') {
                for (let w of serverCtx.workers.values()) {
                    if (w.pid === worker.process.pid) {
                        w.isListening = message.state;
                        serverCtx.evaluatePrimaryFallback();
                        if (typeof serverCtx.publishState === 'function') {
                            serverCtx.publishState();
                        }
                        break;
                    }
                }
                return;
            }

            if (message.id === serverCtx.id && message.method && message.method.startsWith('cluster_state_')) {
                const { messageId, method, key, value } = message;
                try {
                    let result;
                    if (method === 'cluster_state_set') {
                        result = await serverCtx.storage.set(key, value);
                    } else if (method === 'cluster_state_get') {
                        result = await serverCtx.storage.get(key);
                    } else if (method === 'cluster_state_delete') {
                        result = await serverCtx.storage.delete(key);
                    } else if (method === 'cluster_state_size') {
                        result = await serverCtx.storage.size();
                    } else if (method === 'cluster_state_clear') {
                        result = await serverCtx.storage.clear();
                    }
                    
                    if (messageId && worker.send && worker.isConnected()) {
                        worker.send({ messageId, result, success: true });
                    }
                } catch (err) {
                    if (messageId && worker.send && worker.isConnected()) {
                        worker.send({ messageId, error: err.message, success: false });
                    }
                }
                return;
            }

            if (message.id === serverCtx.id && message.method) {
                // Relaying or broadcasting worker commands across the cluster if requested
                if (message.broadcast || message.workers || message.worker) {
                    serverCtx.send(message);
                } else if (typeof serverCtx[message.method] === 'function') {
                    serverCtx[message.method](message, true);
                } else {
                    serverCtx.emit(message.method, message, true);
                }
            }
        });

        return function send(payload = {}) {
            if (!payload || typeof payload !== 'object') return;
            payload.id = serverCtx.id;

            const targetWorkers = [];

            if (payload.worker) {
                const w = serverCtx.workers.get(payload.worker);
                if (w) targetWorkers.push(w);
            } else if (Array.isArray(payload.workers)) {
                for (const wId of payload.workers) {
                    const w = serverCtx.workers.get(wId);
                    if (w) targetWorkers.push(w);
                }
            } else if (payload.broadcast) {
                for (const w of serverCtx.workers.values()) {
                    targetWorkers.push(w);
                }
            } else {
                for (const w of serverCtx.workers.values()) {
                    targetWorkers.push(w);
                }
            }

            for (const workerObj of targetWorkers) {
                const workerProcess = workerObj.process || workerObj;
                if (workerProcess && typeof workerProcess.send === 'function' && workerProcess.isConnected()) {
                    try {
                        workerProcess.send(payload);
                    } catch (err) {
                        console.error(`[@cocreate/server] [PRIMARY] Failed to send IPC payload to worker ${workerObj.id}:`, err.message);
                    }
                }
            }

            // Execute locally on primary context if master flag is true or omitted
            if (payload.master !== false && payload.method) {
                if (typeof serverCtx[payload.method] === 'function') {
                    serverCtx[payload.method](payload, true);
                } else {
                    serverCtx.emit(payload.method, payload, true);
                }
            }
        };
    } else {
        process.on('message', async (message) => {
            if (!message || typeof message !== 'object') return;

            if (message.type === 'shutdown') {
                console.log(`[@cocreate/server] [WORKER ${process.pid}] Received master coordinated shutdown signal. Commencing local drain...`);
                if (typeof serverCtx.drain === 'function') {
                    await serverCtx.drain(true, true);
                } else {
                    serverCtx.emit('drain', true);
                }
                
                setTimeout(() => process.exit(0), 1000);
                return;
            }

            if (message.messageId && pendingPromises.has(message.messageId)) {
                const promiseHandler = pendingPromises.get(message.messageId);
                pendingPromises.delete(message.messageId);
                
                if (message.success) {
                    promiseHandler.resolve(message.result);
                } else {
                    promiseHandler.reject(new Error(message.error || 'IPC RPC Call Failed'));
                }
                return;
            }

            if (message.id === serverCtx.id) {
                if (message.method === 'cluster_state_sync') {
                    serverCtx.emit('cluster_state_sync', message);
                } else if (message.method) {
                    if (typeof serverCtx[message.method] === 'function') {
                        serverCtx[message.method](message, true);
                    } else {
                        serverCtx.emit(message.method, message, true);
                    }
                }
            }
        });

        return function send(payload = {}) {
            if (!payload || typeof payload !== 'object') return;
            payload.id = serverCtx.id;

            if (typeof process.send === 'function' && process.connected) {
                try {
                    process.send(payload);
                } catch (err) {
                    console.error(`[@cocreate/server] [WORKER ${process.pid}] Failed to dispatch IPC message to primary:`, err.message);
                }
            } else {
                // If not connected to primary IPC, execute locally if method exists
                if (payload.method) {
                    if (typeof serverCtx[payload.method] === 'function') {
                        serverCtx[payload.method](payload, true);
                    } else {
                        serverCtx.emit(payload.method, payload, true);
                    }
                }
            }
        };
    }
}

export default { init };