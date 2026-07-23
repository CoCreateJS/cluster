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

import { exec } from 'node:child_process';

/**
 * Manages process-level state transitions, edge listener lifecycle (cordon/drain/shed),
 * system signal handling, and synchronizing state to the database and mesh network.
 */
export class StateManager {
    constructor(serverCtx) {
        this.server = serverCtx;

        // Top-level status reflects OS/VM status: RUNNING, REBOOTING, STOPPED
        this.server.state = {
            status: 'RUNNING',
            isCordon: false,
            isShedding: false,
            isDraining: false,
            isShuttingDown: false,
            sheddingOrgs: new Set()
        };

        this.bindMethods();
        this.setupSignalHandlers();
    }

    bindMethods() {
        this.server.getServerObject = this.getServerObject.bind(this);
        this.server.publishState = this.publishState.bind(this);
        this.server.cordon = this.cordon.bind(this);
        this.server.shed = this.shed.bind(this);
        this.server.drain = this.drain.bind(this);
        this.server.restart = this.restart.bind(this);
        this.server.reboot = this.reboot.bind(this);
        this.server.shutdown = this.shutdown.bind(this);
        this.server.triggerGlobalShutdown = this.triggerGlobalShutdown.bind(this);
    }

    setupSignalHandlers() {
        if (this.server.isPrimary) {
            ['SIGINT', 'SIGTERM', 'SIGBREAK'].forEach((signal) => {
                process.on(signal, () => {
                    console.log(`\n[@cocreate/server] [PRIMARY] Received ${signal}. Orchestrating graceful shutdown...`);
                    this.triggerGlobalShutdown(signal);
                });
            });
        } else {
            ['SIGINT', 'SIGTERM', 'SIGBREAK'].forEach((signal) => {
                process.on(signal, async () => {
                    console.log(`\n[@cocreate/server] [WORKER ${this.server.workerId}] Received ${signal}. Draining local connections...`);
                    await this.drain(true, true);
                    await this.publishState('stopped');
                    process.exit(0);
                });
            });

            this.server.on('shutdown', async () => {
                console.log(`[@cocreate/server] [WORKER ${this.server.workerId}] Instructed by master to shutdown. Draining...`);
                await this.drain(true, true);
                await this.publishState('stopped');
                process.exit(0);
            });
        }
    }

    getServerObject(data = {}) {
        return {
            method: 'object.update',
            array: 'servers',
            object: {
                _id: this.server.id,
                ...data
            },
            upsert: true,
            broadcast: true,
            organization_id: this.server.organization_id,
            host: this.server.host
        };
    }

    async publishState(overrideStatus) {
        if (this.server.isPrimary) {
            const masterStatus = overrideStatus || (this.server.state.isShuttingDown ? 'stopped' : 'running');
            const isMasterStopped = masterStatus === 'stopped';

            const payload = this.getServerObject({
                ip: this.server.ip,
                host: this.server.host,
                status: this.server.state.status.toLowerCase(), // OS level status: 'running', 'rebooting', 'stopped'
                totalWorkers: this.server.totalWorkers,
                infrastructure: this.server.infrastructure,
                master: isMasterStopped ? {
                    pid: null,
                    status: 'stopped'
                } : {
                    pid: process.pid,
                    status: masterStatus
                }
            });

            if (this.server.crud) {
                await this.server.crud.send(payload);
                console.log(`[@cocreate/server] [PRIMARY] Published server state (OS status: ${this.server.state.status.toLowerCase()}, master status: ${masterStatus}) to DB`);
            }
        } else {
            const dbStatus = overrideStatus || (this.server.state.isShuttingDown ? 'stopped' : 'running');
            const isStopped = dbStatus === 'stopped';

            const payload = this.getServerObject({
                [`worker${this.server.workerId}`]: isStopped ? {
                    pid: null,
                    status: 'stopped',
                    isCordoned: false,
                    isShedding: false,
                    isDraining: false
                } : {
                    pid: process.pid,
                    status: dbStatus,
                    isCordoned: this.server.state.isCordon,
                    isShedding: this.server.state.isShedding,
                    isDraining: this.server.state.isDraining
                }
            });

            if (this.server.crud) {
                await this.server.crud.send(payload);
                console.log(`[@cocreate/server] [WORKER ${this.server.workerId}] Published worker state '${dbStatus}' to DB`);
            }
        }
    }

    async cordon(value = true, internalCall = false) {
        const isCordon = typeof value === 'boolean' ? value : (value && value.value !== undefined ? Boolean(value.value) : true);
        
        // Prevent un-cordoning if a higher operational state is active
        if (!isCordon && (this.server.state.isShedding || this.server.state.isDraining || this.server.state.isShuttingDown)) {
            console.warn(`[@cocreate/server] Blocked un-cordon request. Active states: isShedding=${this.server.state.isShedding}, isDraining=${this.server.state.isDraining}, isShuttingDown=${this.server.state.isShuttingDown}`);
            return;
        }

        if (this.server.state.isCordon === isCordon) return;
        this.server.state.isCordon = isCordon;

        if (!this.server.isPrimary) {
            if (isCordon) {
                this.server.closeEdgeListeners();
            } else {
                this.server.restoreEdgeListeners();
            }
            if (process.send) {
                process.send({ type: 'worker_state_change', isCordoned: isCordon });
            }
        } else {
            this.server.evaluatePrimaryFallback();
        }

        if (this.server.isPrimary && !internalCall) {
            await this.publishState();
        }
        
        const processLabel = this.server.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.server.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isCordon updated to: ${this.server.state.isCordon}`);
    }

    async shed(arg, internalCall = false) {
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
        if (!isShedding && (this.server.state.isDraining || this.server.state.isShuttingDown)) {
            console.warn(`[@cocreate/server] Blocked un-shed request while drain or shutdown is active.`);
            return;
        }

        if (this.server.state.isShedding === isShedding && organizations.length === 0) return;

        this.server.state.isShedding = isShedding;
        this.server.state.sheddingOrgs = new Set(organizations);

        if (isShedding) {
            await this.cordon(true, true);
            if (!this.server.isPrimary && organizations.length > 0 && this.server.wsManager) {
                console.log(`[@cocreate/server] Selective eviction of org IDs:`, organizations);
                this.server.wsManager.destroyOrganizations(organizations);
            }
        } else {
            await this.cordon(false, true);
        }

        if (!this.server.isPrimary && process.send) {
            process.send({ type: 'worker_state_change', isShedding: this.server.state.isShedding });
        }

        if (this.server.isPrimary && !internalCall) {
            await this.publishState();
        }

        const processLabel = this.server.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.server.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isShedding updated to: ${this.server.state.isShedding}`);
    }

    async drain(value = true, internalCall = false) {
        const isDraining = typeof value === 'boolean' ? value : (value && value.value !== undefined ? Boolean(value.value) : true);

        // Prevent un-draining if shutdown is active
        if (!isDraining && this.server.state.isShuttingDown) {
            console.warn(`[@cocreate/server] Blocked un-drain request while shutdown is active.`);
            return;
        }

        if (this.server.state.isDraining === isDraining) return;

        this.server.state.isDraining = isDraining;

        if (isDraining) {
            await this.cordon(true, true);
            if (!this.server.isPrimary && this.server.wsManager) {
                console.log(`[@cocreate/server] Worker drain triggered. Purging websocket connections.`);
                this.server.wsManager.destroyAll();
            }
        } else {
            await this.cordon(false, true);
        }

        if (!this.server.isPrimary && process.send) {
            process.send({ type: 'worker_state_change', isDraining: this.server.state.isDraining });
        }

        if (this.server.isPrimary && !internalCall) {
            await this.publishState();
        }

        const processLabel = this.server.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.server.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isDraining updated to: ${this.server.state.isDraining}`);
    }

    async restart(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'restart';
            if (this.server.send) this.server.send(sendPayload);
            return;
        }

        if (this.server.isPrimary) {
            if (payload && (payload.isPrimary || payload.master)) {
                console.log(`[@cocreate/server] [PRIMARY] App restart commanded. Triggering global graceful shutdown...`);
                this.triggerGlobalShutdown('API_RESTART');
            }
        } else {
            if (payload && (payload.isPrimary || payload.master)) return;

            console.log(`[@cocreate/server] [WORKER ${this.server.workerId}] Restart triggered. Commencing drain...`);
            await this.publishState('restarting');
            await this.drain(true, true);
            await this.publishState('stopped');
            process.exit(0);
        }
    }

    reboot(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'reboot';
            if (!this.server.isPrimary) sendPayload.master = true;
            if (this.server.send) this.server.send(sendPayload);
            return;
        }

        if (!this.server.isPrimary) return;

        console.log(`[@cocreate/server] [PRIMARY] OS Reboot requested. Commencing graceful shutdown before kernel reboot...`);
        this.server.state.status = 'REBOOTING';
        
        if (this.server.mesh) {
            this.server.mesh.send({ method: 'mesh.node_status_update', server_id: this.server.id, status: 'REBOOTING' });
        }
        this.publishState();

        this.triggerGlobalShutdown('OS_REBOOT', () => {
            console.log(`[@cocreate/server] [PRIMARY] Executing OS Reboot: sudo reboot`);
            exec('sudo reboot', (err) => {
                if (err) {
                    console.error("[@cocreate/server] Failed to execute sudo reboot:", err.message);
                    process.exit(1);
                }
            });
        });
    }

    shutdown(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'shutdown';
            if (!this.server.isPrimary) sendPayload.master = true;
            if (this.server.send) this.server.send(sendPayload);
            return;
        }

        if (!this.server.isPrimary) return;

        console.log(`[@cocreate/server] [PRIMARY] OS Shutdown requested. Commencing graceful shutdown before VM poweroff...`);
        this.server.state.status = 'STOPPED';

        this.triggerGlobalShutdown('OS_SHUTDOWN', () => {
            console.log(`[@cocreate/server] [PRIMARY] Executing OS Poweroff: sudo poweroff`);
            exec('sudo poweroff', (err) => {
                if (err) {
                    console.error("[@cocreate/server] Failed to execute sudo poweroff:", err.message);
                    process.exit(1);
                }
            });
        });
    }

    async triggerGlobalShutdown(reason, completionCallback) {
        if (!this.server.isPrimary || this.server.state.isShuttingDown) return;

        console.log(`\n[@cocreate/server] [PRIMARY] Commencing Global Shutdown Sequence (Reason: ${reason})...`);
        
        this.server.state.isShuttingDown = true;
        this.server.state.shutdownCompletionCallback = completionCallback;

        // Update top-level status only if this is an explicit OS-level reboot or poweroff
        if (reason === 'OS_SHUTDOWN') {
            this.server.state.status = 'STOPPED';
        } else if (reason === 'OS_REBOOT') {
            this.server.state.status = 'REBOOTING';
        }

        // Immediately publish stopped state for master (clears PID and sets master.status = 'stopped')
        await this.publishState('stopped');

        // Step 1: Execute primary drain
        await this.drain(true, true);

        // Step 2: Notify mesh cluster
        if (this.server.mesh) {
            this.server.mesh.send({
                method: 'mesh.node_status_update',
                server_id: this.server.id,
                status: this.server.state.status.toLowerCase(),
                reason: reason
            });
            
            setTimeout(() => {
                if (this.server.mesh && typeof this.server.mesh.close === 'function') {
                    this.server.mesh.close();
                }
                this.server.state.isMeshClosed = true;
                console.log("[@cocreate/server] [PRIMARY] Mesh closed successfully.");
            }, 1500);
        }

        const finalizeShutdown = async () => {
            if (this.server.state.primaryShutdownTimeout) {
                clearTimeout(this.server.state.primaryShutdownTimeout);
                this.server.state.primaryShutdownTimeout = null;
            }

            await this.publishState('stopped');
        };

        this.server.state.finalizeShutdown = finalizeShutdown;

        // Step 3: Safety timeout
        this.server.state.primaryShutdownTimeout = setTimeout(async () => {
            console.error("[@cocreate/server] [PRIMARY] Shutdown Timeout exceeded (15s). Forcing process exit.");
            await finalizeShutdown();
            if (typeof completionCallback === 'function') completionCallback();
            else process.exit(1);
        }, 15000);

        // Step 4: Command worker processes to shut down
        if (this.server.send) {
            this.server.send({ type: 'shutdown', method: 'shutdown', broadcast: true });
        }
        
        if (this.server.workers && this.server.workers.size === 0) {
            console.log("[@cocreate/server] [PRIMARY] No active workers detected. Concluding shutdown.");
            await finalizeShutdown();
            if (typeof completionCallback === 'function') setTimeout(completionCallback, 1000);
            else setTimeout(() => process.exit(0), 1000);
        }
    }
}

export default {
    init(serverCtx) {
        return new StateManager(serverCtx);
    }
};