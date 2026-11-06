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

import Http from 'node:http';
import Https from 'node:https';
import { exec } from 'node:child_process';
import config from '@cocreate/config';
import { dotNotationToObject } from '@cocreate/utils';

let server = null;
let fallbackHttpServer = null;
let fallbackHttpsServer = null;

let stateConfig = {
    shutdownTimeoutMs: 15000,
    shutdownHandlersTimeoutMs: 10000,
    meshCloseDelayMs: 1500
};

/** Give process-owned services a bounded cleanup window before this process exits. */
export function runShutdownHandlers(reason) {
    if (server.state?.shutdownHandlersPromise) return server.state.shutdownHandlersPromise;
    const handlers = [...(server.shutdownHandlers || [])];
    if (!handlers.length) return Promise.resolve();
    const controller = new AbortController();
    const timeoutMs = Math.max(1, Number(stateConfig.shutdownHandlersTimeoutMs) || 10000);
    let timer;
    const deadline = new Promise((_, reject) => {
        timer = setTimeout(() => {
            const error = new Error('Service shutdown deadline exceeded.');
            controller.abort(error);
            reject(error);
        }, timeoutMs);
    });
    const hooks = Promise.allSettled(handlers.map(handler => {
        try { return Promise.resolve(handler(reason, controller.signal)); }
        catch (error) { return Promise.reject(error); }
    })).then(results => {
        for (const result of results) {
            if (result.status === 'rejected')
                console.error('[@cocreate/cluster] Service shutdown handler failed:', result.reason?.message || 'Unknown error');
        }
    });
    const pending = Promise.race([hooks, deadline]).catch(error => {
        console.error('[@cocreate/cluster] Service shutdown handler failed:', error.message);
    }).finally(() => clearTimeout(timer));
    if (server.state) server.state.shutdownHandlersPromise = pending;
    return pending;
}

/** Worker signals, application restarts, and master shutdown messages share cleanup. */
export function shutdownWorker(reason) {
    if (server.state?.workerShutdownPromise) return server.state.workerShutdownPromise;
    if (server.state) server.state.isShuttingDown = true;
    const pending = (async () => {
        // Start services before drain tears down sockets and organization context.
        const services = runShutdownHandlers(reason);
        await drain(true, true);
        await services;
        await save('stopped');
        process.exit(0);
    })();
    if (server.state) server.state.workerShutdownPromise = pending;
    return pending;
}

/**
 * Loads state configuration using @cocreate/config and queries database overrides
 * for 'state' or 'stateManager' configurations.
 * 
 * @returns {Promise<Object>} Resolved state configuration object
 */
export async function loadStateConfig() {
    const sysConfig = await config.get('stateManager') || await config.get('state');

    let dbConfig = null;
    if (server && server.organization_id) {
        dbConfig = await config.query({
            type: 'state',
            organization_id: server.organization_id,
            server: server.host,
            cluster: server.cluster_id
        });
    }

    if (sysConfig) {
        dotNotationToObject(stateConfig, sysConfig);
    }

    if (dbConfig) {
        dotNotationToObject(stateConfig, dbConfig);
    }

    return stateConfig;
}

/**
 * Registers process signal handlers (SIGINT, SIGTERM, SIGBREAK) and master/worker IPC event bindings.
 */
function setupSignalHandlers() {
    if (server?.isPrimary) {
        ['SIGINT', 'SIGTERM', 'SIGBREAK'].forEach((signal) => {
            process.on(signal, () => {
                console.log(`\n[@cocreate/cluster] [MASTER] Received ${signal}. Orchestrating graceful shutdown...`);
                triggerGlobalShutdown(signal);
            });
        });
    } else {
        ['SIGINT', 'SIGTERM', 'SIGBREAK'].forEach((signal) => {
            process.on(signal, async () => {
                console.log(`\n[@cocreate/cluster] [WORKER ${server?.workerId}] Received ${signal}. Draining local connections...`);
                await shutdownWorker(signal);
            });
        });

        server?.on('shutdown', async () => {
            console.log(`[@cocreate/cluster] [WORKER ${server?.workerId}] Instructed by master to shutdown. Draining...`);
            await shutdownWorker('MASTER_SHUTDOWN');
        });
    }
}

/**
 * Formats a CRUD update payload targeting the server document in the database.
 * 
 * @param {Object} [data={}] - Specific properties to update on the server document
 * @returns {Object} Database payload object
 */
export function getServerObject(data = {}) {
    return {
        method: 'object.update',
        array: 'servers',
        object: {
            _id: server.id,
            ...data
        },
        upsert: true,
        broadcast: true,
        organization_id: server.organization_id,
        host: server.host
    };
}

const wait = (milliseconds) => new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
);

async function persistLifecycle(payload, attempts = 5) {
    let lastError;

    for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
            const response = await server.crud.send(payload);
            const errors = Array.isArray(response?.error)
                ? response.error
                : response?.error
                    ? [response.error]
                    : [];

            if (!errors.length) {
                if (server.manager?.organization_id) {
                    try { await server.manager.send(payload, { timeoutMs: 3000 }); }
                    catch { console.error('[@cocreate/cluster] Unable to mirror server state to its manager.'); }
                }
                return response;
            }
            lastError = new Error(errors
                .map((error) => error?.message || String(error))
                .join('; '));
        } catch (error) {
            lastError = error;
        }

        if (attempt < attempts) await wait(250 * (2 ** (attempt - 1)));
    }

    console.error(
        `[@cocreate/cluster] Failed to persist lifecycle state after ${attempts} attempts:`,
        lastError?.message || lastError
    );
    return null;
}

/**
 * Synchronizes process status and active cluster state flags to the database server object.
 * 
 * @param {string} [overrideStatus] - Optional status override (e.g. 'stopped', 'restarting')
 */
export async function save(overrideStatus) {
    if (server?.crud) {
        const current = await server.crud.send({
            method: 'object.read',
            array: 'servers',
            filter: { query: { _id: server.id }, limit: 1 },
            organization_id: server.organization_id,
            host: server.host
        });
        if (current.success === false || !Array.isArray(current.object) ||
            (Array.isArray(current.error) ? current.error.length : current.error)) {
            throw new Error('Unable to read server status before saving state.');
        }
        if (['failed', 'terminating'].includes(current.object?.[0]?.master?.status)) return;
    }
    if (server?.isPrimary) {
        const masterStatus = overrideStatus || (server.state?.isShuttingDown ? 'stopped' : 'running');
        const isPrimaryStopped = masterStatus === 'stopped';

        const payloadData = {
            ip: server.ip,
            host: server.host,
            status: (server.state?.status || 'RUNNING').toLowerCase(), // OS level status: 'running', 'rebooting', 'stopped'
            totalWorkers: server.workers.size,
            infrastructure: server.infrastructure,
            location: server.location,
            master: isPrimaryStopped ? {
                pid: null,
                status: 'stopped'
            } : {
                pid: process.pid,
                status: masterStatus
            }
        };

        // If running in single-process mode, also record worker1 state in DB
        if (server.isWorker) {
            payloadData.worker1 = isPrimaryStopped ? {
                pid: null,
                status: 'stopped',
                isCordoned: false,
                isShedding: false,
                isDraining: false
            } : {
                pid: process.pid,
                status: masterStatus,
                isCordoned: server.state?.isCordon || server.state?.isCordoned || false,
                isShedding: server.state?.isShedding || false,
                isDraining: server.state?.isDraining || false
            };
        }

        const payload = getServerObject(payloadData);

        if (server.crud) return await persistLifecycle(payload);
    } else if (server) {
        const dbStatus = overrideStatus || (server.state?.isShuttingDown ? 'stopped' : 'running');
        const isStopped = dbStatus === 'stopped';

        const payload = getServerObject({
            [`worker${server.workerId}`]: isStopped ? {
                pid: null,
                status: 'stopped',
                isCordoned: false,
                isShedding: false,
                isDraining: false
            } : {
                pid: process.pid,
                status: dbStatus,
                isCordoned: server.state?.isCordon || server.state?.isCordoned || false,
                isShedding: server.state?.isShedding || false,
                isDraining: server.state?.isDraining || false
            }
        });

        if (server.crud) return await persistLifecycle(payload);
    }
}

/**
 * Sets process cordon flag and opens/closes listener sockets on worker instances.
 * 
 * @param {boolean|Object} [value=true] - Cordon state toggle value
 * @param {boolean} [internalCall=false] - Flag indicating internal cascade call
 */
export async function cordon(value = true, internalCall = false) {
    const isCordon = typeof value === 'boolean' ? value : (value && value.value !== undefined ? Boolean(value.value) : true);
    
    // Prevent un-cordoning if a higher operational state is active
    if (!isCordon && (server?.state?.isShedding || server?.state?.isDraining || server?.state?.isShuttingDown)) {
        console.warn(`[@cocreate/cluster] Blocked un-cordon request. Active states: isShedding=${server?.state?.isShedding}, isDraining=${server?.state?.isDraining}, isShuttingDown=${server?.state?.isShuttingDown}`);
        return;
    }

    if (server?.state?.isCordon === isCordon) return;
    if (server?.state) {
        server.state.isCordon = isCordon;
    }

    if (!server.isPrimary) {
        if (isCordon) {
            server.closeEdgeListeners();
        } else {
            server.restoreEdgeListeners();
        }
        if (server.send) {
            server.send({ method: 'worker_state_change', isCordoned: isCordon });
        }
    } else {
        evaluateMasterFallback();
    }

    if (server.isPrimary && !internalCall) {
        await save();
    }
    
    const processLabel = server.isPrimary ? `[MASTER] (${process.pid})` : `[WORKER] ${process.pid} (ID: ${server.workerId})`;
    console.log(`[@cocreate/cluster] ${processLabel} isCordon updated to: ${server?.state?.isCordon}`);
}

/**
 * Manages traffic shedding for specific tenant organization IDs.
 * 
 * @param {boolean|Array|Object} arg - Shedding configuration or organization list
 * @param {boolean} [internalCall=false] - Flag indicating internal cascade call
 */
export async function shed(arg, internalCall = false) {
    let organizations = [];
    let isShedding = false;

    if (arg === false || (arg && arg.organizations === false)) {
        isShedding = false;
    } else if (arg && arg.organizations !== undefined) {
        organizations = Array.isArray(arg.organizations) ? arg.organizations : [arg.organizations];
        isShedding = organizations.length > 0;
    } else if (arg) {
        organizations = Array.isArray(arg) ? arg : (typeof arg === 'string' ? [arg] : []);
        isShedding = organizations.length > 0;
    }

    // Prevent un-shedding if drain or shutdown is active
    if (!isShedding && (server?.state?.isDraining || server?.state?.isShuttingDown)) {
        console.warn(`[@cocreate/cluster] Blocked un-shed request while drain or shutdown is active.`);
        return;
    }

    if (server?.state?.isShedding === isShedding && organizations.length === 0) return;

    if (server?.state) {
        server.state.isShedding = isShedding;
        server.state.sheddingOrgs = new Set(organizations);
    }

    if (isShedding) {
        await cordon(true, true);
        if (!server.isPrimary && organizations.length > 0 && server.wsManager) {
            console.log(`[@cocreate/cluster] Selective eviction of org IDs:`, organizations);
            server.wsManager.destroyOrganizations(organizations);
        }
    } else {
        await cordon(false, true);
    }

    if (!server.isPrimary && server.send) {
        server.send({ method: 'worker_state_change', isShedding: server?.state?.isShedding });
    }

    if (server.isPrimary && !internalCall) {
        await save();
    }

    const processLabel = server.isPrimary ? `Master (${process.pid})` : `Worker ${process.pid} (ID: ${server.workerId})`;
    console.log(`[@cocreate/cluster] ${processLabel} isShedding updated to: ${server?.state?.isShedding}`);
}

/**
 * Drains active WebSocket connections and cordons process from accepting new requests.
 * 
 * @param {boolean|Object} [value=true] - Drain state toggle value
 * @param {boolean} [internalCall=false] - Flag indicating internal cascade call
 */
export async function drain(value = true, internalCall = false) {
    const isDraining = typeof value === 'boolean' ? value : (value && value.value !== undefined ? Boolean(value.value) : true);

    // Prevent un-draining if shutdown is active
    if (!isDraining && server?.state?.isShuttingDown) {
        console.warn(`[@cocreate/cluster] Blocked un-drain request while shutdown is active.`);
        return;
    }

    if (server?.state?.isDraining === isDraining) return;

    if (server?.state) {
        server.state.isDraining = isDraining;
    }

    if (isDraining) {
        await cordon(true, true);
        if (!server.isPrimary && server.wsManager) {
            console.log(`[@cocreate/cluster] Worker drain triggered. Purging websocket connections.`);
            server.wsManager.destroyAll();
        }
    } else {
        await cordon(false, true);
    }

    if (!server.isPrimary && server.send) {
        server.send({ method: 'worker_state_change', isDraining: server?.state?.isDraining });
    }

    if (server.isPrimary && !internalCall) {
        await save();
    }

    const processLabel = server.isPrimary ? `Master (${process.pid})` : `Worker ${process.pid} (ID: ${server.workerId})`;
    console.log(`[@cocreate/cluster] ${processLabel} isDraining updated to: ${server?.state?.isDraining}`);
}

/**
 * Triggers a process restart across master or worker processes.
 * 
 * @param {Object} [payload={}] - Payload options
 * @param {boolean} [fromEvent=false] - Event trigger flag
 */
export async function restart(payload = {}, fromEvent = false) {
    if (!fromEvent) {
        const sendPayload = (payload && typeof payload === 'object') ? payload : {};
        sendPayload.method = 'restart';
        if (server.send) server.send(sendPayload);
        return;
    }

    if (server.isPrimary) {
        if (payload && (payload.isPrimary || payload.master)) {
            console.log(`[@cocreate/cluster] [MASTER] App restart commanded. Triggering global graceful shutdown...`);
            triggerGlobalShutdown('API_RESTART');
        }
    } else {
        if (payload && (payload.isPrimary || payload.master)) return;

        console.log(`[@cocreate/cluster] [WORKER ${server.workerId}] Restart triggered. Commencing drain...`);
        await save('restarting');
        await shutdownWorker('API_RESTART');
    }
}

/**
 * Triggers an OS kernel reboot sequence on the master node.
 * 
 * @param {Object} [payload={}] - Payload options
 * @param {boolean} [fromEvent=false] - Event trigger flag
 */
export function reboot(payload = {}, fromEvent = false) {
    if (!fromEvent) {
        const sendPayload = (payload && typeof payload === 'object') ? payload : {};
        sendPayload.method = 'reboot';
        if (!server.isPrimary) sendPayload.master = true;
        if (server.send) server.send(sendPayload);
        return;
    }

    if (!server.isPrimary) return;

    console.log(`[@cocreate/cluster] [MASTER] OS Reboot requested. Commencing graceful shutdown before kernel reboot...`);
    if (server.state) server.state.status = 'REBOOTING';
    
    if (server.mesh) {
        server.mesh.send({ method: 'mesh.node_status_update', server_id: server.id, status: 'REBOOTING' });
    }
    save();

    triggerGlobalShutdown('OS_REBOOT', () => {
        console.log(`[@cocreate/cluster] [MASTER] Executing OS Reboot: sudo reboot`);
        exec('sudo reboot', (err) => {
            if (err) {
                console.error("[@cocreate/cluster] Failed to execute sudo reboot:", err.message);
                process.exit(1);
            }
        });
    });
}

/**
 * Triggers an OS poweroff sequence on the master node.
 * 
 * @param {Object} [payload={}] - Payload options
 * @param {boolean} [fromEvent=false] - Event trigger flag
 */
export function shutdown(payload = {}, fromEvent = false) {
    if (!fromEvent) {
        const sendPayload = (payload && typeof payload === 'object') ? payload : {};
        sendPayload.method = 'shutdown';
        if (!server.isPrimary) sendPayload.master = true;
        if (server.send) server.send(sendPayload);
        return;
    }

    if (!server.isPrimary) return;

    console.log(`[@cocreate/cluster] [MASTER] OS Shutdown requested. Commencing graceful shutdown before VM poweroff...`);
    if (server.state) server.state.status = 'STOPPED';

    triggerGlobalShutdown('OS_SHUTDOWN', () => {
        console.log(`[@cocreate/cluster] [MASTER] Executing OS Poweroff: sudo poweroff`);
        exec('sudo poweroff', (err) => {
            if (err) {
                console.error("[@cocreate/cluster] Failed to execute sudo poweroff:", err.message);
                process.exit(1);
            }
        });
    });
}

/**
 * Orchestrates global cluster shutdown sequence across workers and mesh nodes.
 * 
 * @param {string} reason - Cause for shutdown sequence initiation
 * @param {Function} [completionCallback] - Completion callback function
 */
export async function triggerGlobalShutdown(reason, completionCallback) {
    if (!server.isPrimary || server.state?.isShuttingDown) return;
    const shutdownDeadline = Date.now() + stateConfig.shutdownTimeoutMs;

    console.log(`\n[@cocreate/cluster] [MASTER] Commencing Global Shutdown Sequence (Reason: ${reason})...`);
    
    if (server.state) {
        server.state.isShuttingDown = true;
        server.state.shutdownCompletionCallback = completionCallback;

        // Update top-level status only if this is an explicit OS-level reboot or poweroff
        if (reason === 'OS_SHUTDOWN') {
            server.state.status = 'STOPPED';
        } else if (reason === 'OS_REBOOT') {
            server.state.status = 'REBOOTING';
        }
    }

    // Run node-owned routing cleanup while CRUD/API services are still available.
    try {
        await server.routing?.remove(reason);
    } catch (error) {
        console.error('[@cocreate/cluster] Failed to withdraw node routing:', error.message);
    }

    // Publish stopped state unless the server already has a failed/terminating outcome.
    await save('stopped');

    // Step 1: Execute primary drain
    await drain(true, true);

    // Step 2: Notify mesh cluster
    if (server.mesh) {
        server.mesh.send({
            method: 'mesh.node_status_update',
            server_id: server.id,
            status: (server.state?.status || 'STOPPED').toLowerCase(),
            reason: reason
        });
        
        setTimeout(() => {
            if (server.mesh && typeof server.mesh.close === 'function') {
                server.mesh.close(reason);
            }
            if (server.state) server.state.isMeshClosed = true;
            console.log("[@cocreate/cluster] [MASTER] Mesh closed successfully.");
        }, stateConfig.meshCloseDelayMs);
    }

    let finalizing;
    const finalizeShutdown = () => {
        if (finalizing) return finalizing;
        finalizing = (async () => {
            if (server.state?.primaryShutdownTimeout) {
                clearTimeout(server.state.primaryShutdownTimeout);
                server.state.primaryShutdownTimeout = null;
            }
            // In single-process mode consumers such as AI live on the primary.
            if (server.isWorker) await runShutdownHandlers(reason);
            const remaining = shutdownDeadline - Date.now();
            if (remaining > 0) {
                try { await server.repository?.stop(reason, { timeoutMs: remaining }); }
                catch { console.error('[@cocreate/cluster] Repository shutdown checkpoint failed or timed out.'); }
            }
            await save('stopped');
        })();
        return finalizing;
    };

    if (server.state) server.state.finalizeShutdown = finalizeShutdown;

    // Step 3: Safety timeout
    if (server.state) {
        server.state.primaryShutdownTimeout = setTimeout(async () => {
            console.error(`[@cocreate/cluster] [MASTER] Shutdown Timeout exceeded (${stateConfig.shutdownTimeoutMs / 1000}s). Forcing process exit.`);
            await finalizeShutdown();
            if (typeof completionCallback === 'function') completionCallback();
            else process.exit(1);
        }, stateConfig.shutdownTimeoutMs);
    }

    // Step 4: Command worker processes to shut down
    if (server.send) {
        server.send({ method: 'shutdown', broadcast: true });
    }
    
    if (server.workers && server.workers.size === 0) {
        console.log("[@cocreate/cluster] [MASTER] No active workers detected. Concluding shutdown.");
        await finalizeShutdown();
        if (typeof completionCallback === 'function') setTimeout(completionCallback, 1000);
        else setTimeout(() => process.exit(0), 1000);
    }
}

/**
 * Evaluates active worker states and activates/deactivates emergency master fallback HTTP server.
 */
export function evaluateMasterFallback() {
    if (!server?.isPrimary) return;

    const workersList = server.workers ? Array.from(server.workers.values()) : [];
    const allWorkersDown = workersList.length === 0 || workersList.every(w => w.isListening === false);
    const isCordoned = Boolean(server.state?.isCordon || server.state?.isCordoned);

    if (allWorkersDown || isCordoned) {
        if (!fallbackHttpServer) {
            const HTTP_PORT = process.env.HTTP_PORT || 80;
            const HTTPS_PORT = process.env.HTTPS_PORT || 443;
            const BIND_IP = process.env.BIND_IP || '0.0.0.0';

            const handleFallback = (req, res) => {
                res.writeHead(503, {
                    "Content-Type": "text/plain",
                    "Retry-After": "5"
                });
                res.end("Service Unavailable: All workers cordoned or inactive");
            };
            
            fallbackHttpServer = Http.createServer(handleFallback);
            fallbackHttpServer.listen(HTTP_PORT, BIND_IP);

            if (server.tls && typeof server.tls.createHttpsOptions === 'function') {
                const httpsOptions = server.tls.createHttpsOptions(server);
                fallbackHttpsServer = Https.createServer(httpsOptions, handleFallback);
                fallbackHttpsServer.listen(HTTPS_PORT, BIND_IP);
            }
            
            console.log(`[@cocreate/cluster] [MASTER] All workers cordoned/down. Master fallback active (500 mode).`);
        }
    } else {
        if (fallbackHttpServer) {
            fallbackHttpServer.close();
            fallbackHttpServer = null;
            if (fallbackHttpsServer) {
                fallbackHttpsServer.close();
                fallbackHttpsServer = null;
            }
            console.log(`[@cocreate/cluster] [MASTER] Workers active. Deactivating Master fallback.`);
        }
    }
}

/**
 * Initializes the StateManager service.
 * 
 * @param {Object} serverInstance - Core server instance
 * @returns {Promise<Object>} State object containing initial state flags, configuration, and control methods
 */
export async function init(serverInstance) {
    server = serverInstance;

    await loadStateConfig();
    setupSignalHandlers();

    server.on('cordon', payload => cordon(payload));
    server.on('shed', payload => shed(payload));
    server.on('drain', payload => drain(payload));
    server.on('restart', payload => restart(payload, true));
    if (server.isPrimary) {
        server.on('reboot', payload => reboot(payload, true));
        server.on('shutdown', payload => shutdown(payload, true));
    }

    return {
        config: stateConfig,
        status: 'RUNNING',
        isCordon: false,
        get isCordoned() { return this.isCordon; },
        set isCordoned(val) { this.isCordon = val; },
        isShedding: false,
        isDraining: false,
        isShuttingDown: false,
        sheddingOrgs: new Set(),
        getServerObject,
        save,
        cordon,
        shed,
        drain,
        restart,
        reboot,
        shutdown,
        triggerGlobalShutdown,
        evaluateMasterFallback
    };
}

export default {
    init
};
