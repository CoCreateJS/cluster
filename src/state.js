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
    meshCloseDelayMs: 1500
};

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
    if (server?.isMaster) {
        ['SIGINT', 'SIGTERM', 'SIGBREAK'].forEach((signal) => {
            process.on(signal, () => {
                console.log(`\n[@cocreate/server] [MASTER] Received ${signal}. Orchestrating graceful shutdown...`);
                triggerGlobalShutdown(signal);
            });
        });
    } else {
        ['SIGINT', 'SIGTERM', 'SIGBREAK'].forEach((signal) => {
            process.on(signal, async () => {
                console.log(`\n[@cocreate/server] [WORKER ${server?.workerId}] Received ${signal}. Draining local connections...`);
                await drain(true, true);
                await save('stopped');
                process.exit(0);
            });
        });

        server?.on('shutdown', async () => {
            console.log(`[@cocreate/server] [WORKER ${server?.workerId}] Instructed by master to shutdown. Draining...`);
            await drain(true, true);
            await save('stopped');
            process.exit(0);
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

/**
 * Synchronizes process status and active cluster state flags to the database server object.
 * 
 * @param {string} [overrideStatus] - Optional status override (e.g. 'stopped', 'restarting')
 */
export async function save(overrideStatus) {
    if (server?.isMaster) {
        const masterStatus = overrideStatus || (server.state?.isShuttingDown ? 'stopped' : 'running');
        const isMasterStopped = masterStatus === 'stopped';

        const payloadData = {
            ip: server.ip,
            host: server.host,
            status: (server.state?.status || 'RUNNING').toLowerCase(), // OS level status: 'running', 'rebooting', 'stopped'
            totalWorkers: server.totalWorkers,
            infrastructure: server.infrastructure,
            location: server.location,
            master: isMasterStopped ? {
                pid: null,
                status: 'stopped'
            } : {
                pid: process.pid,
                status: masterStatus
            }
        };

        // If running in single-process mode, also record worker1 state in DB
        if (server.totalWorkers <= 1) {
            payloadData.worker1 = isMasterStopped ? {
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

        if (server.crud) {
            await server.crud.send(payload);
        }
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

        if (server.crud) {
            await server.crud.send(payload);
        }
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
        console.warn(`[@cocreate/server] Blocked un-cordon request. Active states: isShedding=${server?.state?.isShedding}, isDraining=${server?.state?.isDraining}, isShuttingDown=${server?.state?.isShuttingDown}`);
        return;
    }

    if (server?.state?.isCordon === isCordon) return;
    if (server?.state) {
        server.state.isCordon = isCordon;
    }

    if (!server.isMaster) {
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

    if (server.isMaster && !internalCall) {
        await save();
    }
    
    const processLabel = server.isMaster ? `[MASTER] (${process.pid})` : `[WORKER] ${process.pid} (ID: ${server.workerId})`;
    console.log(`[@cocreate/server] ${processLabel} isCordon updated to: ${server?.state?.isCordon}`);
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
        console.warn(`[@cocreate/server] Blocked un-shed request while drain or shutdown is active.`);
        return;
    }

    if (server?.state?.isShedding === isShedding && organizations.length === 0) return;

    if (server?.state) {
        server.state.isShedding = isShedding;
        server.state.sheddingOrgs = new Set(organizations);
    }

    if (isShedding) {
        await cordon(true, true);
        if (!server.isMaster && organizations.length > 0 && server.wsManager) {
            console.log(`[@cocreate/server] Selective eviction of org IDs:`, organizations);
            server.wsManager.destroyOrganizations(organizations);
        }
    } else {
        await cordon(false, true);
    }

    if (!server.isMaster && server.send) {
        server.send({ method: 'worker_state_change', isShedding: server?.state?.isShedding });
    }

    if (server.isMaster && !internalCall) {
        await save();
    }

    const processLabel = server.isMaster ? `Master (${process.pid})` : `Worker ${process.pid} (ID: ${server.workerId})`;
    console.log(`[@cocreate/server] ${processLabel} isShedding updated to: ${server?.state?.isShedding}`);
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
        console.warn(`[@cocreate/server] Blocked un-drain request while shutdown is active.`);
        return;
    }

    if (server?.state?.isDraining === isDraining) return;

    if (server?.state) {
        server.state.isDraining = isDraining;
    }

    if (isDraining) {
        await cordon(true, true);
        if (!server.isMaster && server.wsManager) {
            console.log(`[@cocreate/server] Worker drain triggered. Purging websocket connections.`);
            server.wsManager.destroyAll();
        }
    } else {
        await cordon(false, true);
    }

    if (!server.isMaster && server.send) {
        server.send({ method: 'worker_state_change', isDraining: server?.state?.isDraining });
    }

    if (server.isMaster && !internalCall) {
        await save();
    }

    const processLabel = server.isMaster ? `Master (${process.pid})` : `Worker ${process.pid} (ID: ${server.workerId})`;
    console.log(`[@cocreate/server] ${processLabel} isDraining updated to: ${server?.state?.isDraining}`);
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

    if (server.isMaster) {
        if (payload && (payload.isMaster || payload.master)) {
            console.log(`[@cocreate/server] [MASTER] App restart commanded. Triggering global graceful shutdown...`);
            triggerGlobalShutdown('API_RESTART');
        }
    } else {
        if (payload && (payload.isMaster || payload.master)) return;

        console.log(`[@cocreate/server] [WORKER ${server.workerId}] Restart triggered. Commencing drain...`);
        await save('restarting');
        await drain(true, true);
        await save('stopped');
        process.exit(0);
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
        if (!server.isMaster) sendPayload.master = true;
        if (server.send) server.send(sendPayload);
        return;
    }

    if (!server.isMaster) return;

    console.log(`[@cocreate/server] [MASTER] OS Reboot requested. Commencing graceful shutdown before kernel reboot...`);
    if (server.state) server.state.status = 'REBOOTING';
    
    if (server.mesh) {
        server.mesh.send({ method: 'mesh.node_status_update', server_id: server.id, status: 'REBOOTING' });
    }
    save();

    triggerGlobalShutdown('OS_REBOOT', () => {
        console.log(`[@cocreate/server] [MASTER] Executing OS Reboot: sudo reboot`);
        exec('sudo reboot', (err) => {
            if (err) {
                console.error("[@cocreate/server] Failed to execute sudo reboot:", err.message);
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
        if (!server.isMaster) sendPayload.master = true;
        if (server.send) server.send(sendPayload);
        return;
    }

    if (!server.isMaster) return;

    console.log(`[@cocreate/server] [MASTER] OS Shutdown requested. Commencing graceful shutdown before VM poweroff...`);
    if (server.state) server.state.status = 'STOPPED';

    triggerGlobalShutdown('OS_SHUTDOWN', () => {
        console.log(`[@cocreate/server] [MASTER] Executing OS Poweroff: sudo poweroff`);
        exec('sudo poweroff', (err) => {
            if (err) {
                console.error("[@cocreate/server] Failed to execute sudo poweroff:", err.message);
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
    if (!server.isMaster || server.state?.isShuttingDown) return;

    console.log(`\n[@cocreate/server] [MASTER] Commencing Global Shutdown Sequence (Reason: ${reason})...`);
    
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

    // Immediately publish stopped state for master (clears PID and sets master.status = 'stopped')
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
                server.mesh.close();
            }
            if (server.state) server.state.isMeshClosed = true;
            console.log("[@cocreate/server] [MASTER] Mesh closed successfully.");
        }, stateConfig.meshCloseDelayMs);
    }

    const finalizeShutdown = async () => {
        if (server.state?.primaryShutdownTimeout) {
            clearTimeout(server.state.primaryShutdownTimeout);
            server.state.primaryShutdownTimeout = null;
        }

        await save('stopped');
    };

    if (server.state) server.state.finalizeShutdown = finalizeShutdown;

    // Step 3: Safety timeout
    if (server.state) {
        server.state.primaryShutdownTimeout = setTimeout(async () => {
            console.error(`[@cocreate/server] [MASTER] Shutdown Timeout exceeded (${stateConfig.shutdownTimeoutMs / 1000}s). Forcing process exit.`);
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
        console.log("[@cocreate/server] [MASTER] No active workers detected. Concluding shutdown.");
        await finalizeShutdown();
        if (typeof completionCallback === 'function') setTimeout(completionCallback, 1000);
        else setTimeout(() => process.exit(0), 1000);
    }
}

/**
 * Evaluates active worker states and activates/deactivates emergency master fallback HTTP server.
 */
export function evaluateMasterFallback() {
    if (!server?.isMaster) return;

    const workersList = server.workers ? Array.from(server.workers.values()) : [];
    const allWorkersDown = workersList.length === 0 || workersList.every(w => w.isListening === false);
    const isCordoned = Boolean(server.state?.isCordon || server.state?.isCordoned);

    if (allWorkersDown || isCordoned) {
        if (!fallbackHttpServer) {
            const HTTP_PORT = process.env.HTTP_PORT || 80;
            const HTTPS_PORT = process.env.HTTPS_PORT || 443;
            const BIND_IP = process.env.BIND_IP || '0.0.0.0';

            const handleFallback = (req, res) => {
                res.writeHead(500, { 
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
            
            console.log(`[@cocreate/server] [MASTER] All workers cordoned/down. Master fallback active (500 mode).`);
        }
    } else {
        if (fallbackHttpServer) {
            fallbackHttpServer.close();
            fallbackHttpServer = null;
            if (fallbackHttpsServer) {
                fallbackHttpsServer.close();
                fallbackHttpsServer = null;
            }
            console.log(`[@cocreate/server] [MASTER] Workers active. Deactivating Master fallback.`);
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