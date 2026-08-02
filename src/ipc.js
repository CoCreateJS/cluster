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
 * the Master orchestrator and clustered Worker processes.
 * 
 * @param {Object} Server - The CoCreateServer instance reference
 * @param {Map} pendingPromises - Tracking map for async worker RPC promises
 * @returns {Function} Context-aware send function for dispatching cluster messages
 */
export function init(Server, pendingPromises) {
    if (Server.isMaster) {
        cluster.on('message', async (worker, message) => {
            if (!message || typeof message !== 'object') return;

            // 1. Resolve pending RPC promise if messageId matches
            if (message.messageId && pendingPromises.has(message.messageId)) {
                const promiseHandler = pendingPromises.get(message.messageId);
                pendingPromises.delete(message.messageId);
                if (message.success) {
                    promiseHandler.resolve(message.result);
                } else {
                    promiseHandler.reject(new Error(message.error || 'IPC RPC Request Failed'));
                }
                return;
            }

            // 2. Emit event locally on Master for subscribers (e.g. log, state, storage)
            if (message.method) {
                Server.emit(message.method, message);
            }

            // 3. Relay broadcast or targeted messages to other workers
            if (message.broadcast || message.worker) {
                send(message);
            }
        });

        function send(payload = {}) {
            if (!payload || typeof payload !== 'object') return;
            payload.id = Server.id;

            const targetWorkers = [];

            if (payload.worker) {
                if (Array.isArray(payload.worker)) {
                    for (const wId of payload.worker) {
                        const w = Server.workers.get(wId);
                        if (w) targetWorkers.push(w);
                    }
                } else {
                    const w = Server.workers.get(payload.worker);
                    if (w) targetWorkers.push(w);
                }
            } else {
                // Broadcast to all workers by default if no specific worker specified
                for (const w of Server.workers.values()) {
                    targetWorkers.push(w);
                }
            }

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

            for (const workerObj of targetWorkers) {
                const workerProcess = workerObj.process || workerObj;
                if (workerProcess && typeof workerProcess.send === 'function' && workerProcess.isConnected()) {
                    try {
                        workerProcess.send(payload);
                    } catch (err) {
                        console.error(`[@cocreate/server] [MASTER] Failed to send IPC payload to worker ${workerObj.id}:`, err.message);
                    }
                }
            }

            // Fire event locally on Master process unless explicitly turned off via master: false
            if (payload.master !== false && payload.method) {
                Server.emit(payload.method, payload);
            }
        }

        return send;
    } else {
        process.on('message', async (message) => {
            if (!message || typeof message !== 'object') return;

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

            if (message.method) {
                Server.emit(message.method, message);
            }
        });

        return function send(payload = {}) {
            if (!payload || typeof payload !== 'object') return;
            payload.id = Server.id;

            if (typeof process.send === 'function' && process.connected) {
                try {
                    process.send(payload);
                } catch (err) {
                    console.error(`[@cocreate/server] [WORKER ${process.pid}] Failed to dispatch IPC message to master:`, err.message);
                }
            } else {
                if (payload.method) {
                    Server.emit(payload.method, payload);
                }
            }
        };
    }
}

export default { init };