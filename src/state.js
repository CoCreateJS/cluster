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

export class StateManager {
    constructor(serverCtx) {
        this.server = serverCtx;

        // Initialize state object on server context
        this.server.state = {
            status: 'RUNNING', // RUNNING, CORDONED, DRAINING, SHUTTING_DOWN, REBOOTING
            isCordon: false,
            isShedding: false,
            isDraining: false,
            sheddingOrgs: new Set()
        };

        // Attach state methods to server instance
        this.bindMethods();
        
        // Bind internal event listeners for state commands
        this.server.on('cordon', (payload) => this.server.cordon(payload, true));
        this.server.on('shed', (payload) => this.server.shed(payload, true));
        this.server.on('drain', (payload) => this.server.drain(payload, true));
        this.server.on('restart', (payload) => this.server.restart(payload, true));
        this.server.on('reboot', (payload) => this.server.reboot(payload, true));
        this.server.on('shutdown', (payload) => this.server.shutdown(payload, true));
    }

    bindMethods() {
        this.server.getServerObject = this.getServerObject.bind(this);
        this.server.publishState = this.publishState.bind(this);
        this.server.updateLifecycleStatus = this.updateLifecycleStatus.bind(this);
        this.server.cordon = this.cordon.bind(this);
        this.server.shed = this.shed.bind(this);
        this.server.drain = this.drain.bind(this);
        this.server.restart = this.restart.bind(this);
        this.server.reboot = this.reboot.bind(this);
        this.server.shutdown = this.shutdown.bind(this);
    }

    /**
     * Generates standard DB payload structure for server document updates.
     */
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

    /**
     * Publishes current state (Primary or Worker) directly to DB.
     */
    async publishState(overrideStatus) {
        if (!this.server.crud || !this.server.id) return;

        if (this.server.isPrimary) {
            let dbStatus = overrideStatus || 'running';
            if (!overrideStatus) {
                if (this.server.state.status === 'SHUTTING_DOWN') dbStatus = 'stopped';
                else if (this.server.state.status === 'REBOOTING') dbStatus = 'rebooting';
                else if (this.server.state.status === 'CORDONED') dbStatus = 'cordoned';
                else if (this.server.state.status === 'DRAINING') dbStatus = 'draining';
            }

            const payload = this.getServerObject({
                ip: this.server.ip,
                host: this.server.host,
                status: dbStatus,
                totalWorkers: this.server.totalWorkers,
                infrastructure: this.server.infrastructure
            });

            try {
                await this.server.crud.send(payload);
                console.log(`[@cocreate/server] [PRIMARY] Published server state '${dbStatus}' to DB`);
            } catch (err) {
                console.error(`[@cocreate/server] [PRIMARY] Failed to publish server state to DB:`, err.message);
            }
        } else {
            let dbStatus = overrideStatus || this.server.state.status.toLowerCase();
            
            const payload = this.getServerObject({
                [`worker${this.server.workerId}`]: {
                    pid: process.pid,
                    status: dbStatus,
                    isCordoned: this.server.state.isCordon,
                    isShedding: this.server.state.isShedding,
                    isDraining: this.server.state.isDraining
                }
            });

            try {
                await this.server.crud.send(payload);
                console.log(`[@cocreate/server] [WORKER ${this.server.workerId}] Published worker state '${dbStatus}' to DB`);
            } catch (err) {
                console.error(`[@cocreate/server] [WORKER ${this.server.workerId}] Failed to publish worker state to DB:`, err.message);
            }
        }
    }

    /**
     * Cordon/un-cordon node and sync state to DB.
     */
    async cordon(arg = true, fromEvent = false) {
        if (!fromEvent) {
            const payload = (arg && typeof arg === 'object') ? arg : { value: arg };
            payload.method = 'cordon';
            this.server.send(payload);
            return;
        }

        const isCordon = typeof arg === 'boolean' ? arg : (arg && arg.value !== undefined ? arg.value : true);
        
        if (!isCordon && (this.server.state.isShedding || this.server.state.isDraining)) {
            console.warn(`[@cocreate/server] Blocked un-cordon request. Overriding active states: isShedding=${this.server.state.isShedding}, isDraining=${this.server.state.isDraining}`);
            return;
        }
        
        this.server.state.isCordon = isCordon;
        this.updateLifecycleStatus();
        
        const processLabel = this.server.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.server.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isCordon updated to: ${this.server.state.isCordon}`);

        if (this.server.crud && this.server.id) {
            let dbStatus = this.server.state.status.toLowerCase();
            const updateFields = this.server.isPrimary ? {
                status: dbStatus,
                isCordon: this.server.state.isCordon
            } : {
                [`worker${this.server.workerId}.isCordoned`]: this.server.state.isCordon,
                [`worker${this.server.workerId}.status`]: dbStatus
            };

            try {
                await this.server.crud.send(this.getServerObject(updateFields));
                console.log(`[@cocreate/server] ${processLabel} Synced cordon state to DB`);
            } catch (err) {
                console.error(`[@cocreate/server] ${processLabel} Failed to sync cordon state to DB:`, err.message);
            }
        }
    }

    /**
     * Shed tenant workloads and sync state to DB.
     */
    async shed(arg = [], fromEvent = false) {
        if (!fromEvent) {
            const payload = (arg && typeof arg === 'object' && !Array.isArray(arg)) 
                ? arg 
                : { organizations: arg === false ? false : (Array.isArray(arg) ? arg : (arg ? [arg] : [])) };
            payload.method = 'shed';
            this.server.send(payload);
            return;
        }

        let organizations = [];
        let isShedding = false;

        if (arg === false) {
            isShedding = false;
        } else if (arg && arg.organizations !== undefined) {
            if (arg.organizations === false) {
                isShedding = false;
            } else {
                organizations = Array.isArray(arg.organizations) ? arg.organizations : [arg.organizations];
                isShedding = organizations.length > 0;
            }
        } else if (arg) {
            organizations = Array.isArray(arg) ? arg : (typeof arg === 'string' ? [arg] : []);
            isShedding = organizations.length > 0;
        }

        this.server.state.isShedding = isShedding;
        this.server.state.sheddingOrgs = new Set(organizations);
        
        if (this.server.state.isShedding) {
            await this.cordon(true, true);
            
            if (organizations.length > 0 && this.server.wsManager && typeof this.server.wsManager.destroyOrganizations === 'function') {
                console.log(`[@cocreate/server] Shedding event triggered. Selective eviction of org IDs:`, organizations);
                this.server.wsManager.destroyOrganizations(organizations);
            }

            this.server.closeEdgeListeners();
        } else {
            this.server.restoreEdgeListeners();
        }
        
        this.updateLifecycleStatus();

        const processLabel = this.server.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.server.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isShedding updated to: ${this.server.state.isShedding} (isCordon: ${this.server.state.isCordon})`);

        if (this.server.crud && this.server.id) {
            let dbStatus = this.server.state.status.toLowerCase();
            const updateFields = this.server.isPrimary ? {
                status: dbStatus,
                isShedding: this.server.state.isShedding,
                isCordon: this.server.state.isCordon
            } : {
                [`worker${this.server.workerId}.isShedding`]: this.server.state.isShedding,
                [`worker${this.server.workerId}.isCordoned`]: this.server.state.isCordon,
                [`worker${this.server.workerId}.status`]: dbStatus
            };

            try {
                await this.server.crud.send(this.getServerObject(updateFields));
                console.log(`[@cocreate/server] ${processLabel} Synced shed state to DB`);
            } catch (err) {
                console.error(`[@cocreate/server] ${processLabel} Failed to sync shed state to DB:`, err.message);
            }
        }
    }

    /**
     * Drain active connections and sync state to DB.
     */
    async drain(arg = true, fromEvent = false) {
        if (!fromEvent) {
            const payload = (arg && typeof arg === 'object') ? arg : { value: arg };
            payload.method = 'drain';
            this.server.send(payload);
            return;
        }

        const isDraining = typeof arg === 'boolean' ? arg : (arg && arg.value !== undefined ? arg.value : true);
        this.server.state.isDraining = isDraining;
        
        if (this.server.state.isDraining) {
            await this.cordon(true, true);
            
            if (this.server.wsManager && typeof this.server.wsManager.destroyAll === 'function') {
                console.log(`[@cocreate/server] Worker-level drain event triggered. Purging all tenant configurations.`);
                this.server.wsManager.destroyAll();
            }

            this.server.closeEdgeListeners();
        } else {
            this.server.restoreEdgeListeners();
        }
        
        this.updateLifecycleStatus();

        const processLabel = this.server.isPrimary ? `Primary (${process.pid})` : `Worker ${process.pid} (ID: ${this.server.workerId})`;
        console.log(`[@cocreate/server] ${processLabel} isDraining updated to: ${this.server.state.isDraining} (isCordon: ${this.server.state.isCordon})`);

        if (this.server.crud && this.server.id) {
            let dbStatus = this.server.state.status.toLowerCase();
            const updateFields = this.server.isPrimary ? {
                status: dbStatus,
                isDraining: this.server.state.isDraining,
                isCordon: this.server.state.isCordon
            } : {
                [`worker${this.server.workerId}.isDraining`]: this.server.state.isDraining,
                [`worker${this.server.workerId}.isCordoned`]: this.server.state.isCordon,
                [`worker${this.server.workerId}.status`]: dbStatus
            };

            try {
                await this.server.crud.send(this.getServerObject(updateFields));
                console.log(`[@cocreate/server] ${processLabel} Synced drain state to DB`);
            } catch (err) {
                console.error(`[@cocreate/server] ${processLabel} Failed to sync drain state to DB:`, err.message);
            }
        }
    }

    /**
     * Programmatically restart process context.
     */
    restart(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'restart';
            this.server.send(sendPayload);
            return;
        }

        if (this.server.isPrimary) {
            if (payload && (payload.isPrimary || payload.master)) {
                console.log(`[@cocreate/server] [PRIMARY] App restart commanded. Triggering global graceful shutdown...`);
                if (this.server.triggerGlobalShutdown) this.server.triggerGlobalShutdown('API_RESTART');
            }
        } else {
            if (payload && (payload.isPrimary || payload.master)) return;

            console.log(`[@cocreate/server] [WORKER ${this.server.workerId}] Restart triggered. Commencing drain...`);
            this.publishState('restarting');
            this.drain(true, true);
            
            setTimeout(() => {
                console.log(`[@cocreate/server] [WORKER ${this.server.workerId}] Drain complete. Exiting for worker restart.`);
                process.exit(0);
            }, 3000); 
        }
    }

    /**
     * Trigger full OS reboot.
     */
    reboot(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'reboot';
            if (!this.server.isPrimary) sendPayload.master = true;
            this.server.send(sendPayload);
            return;
        }

        if (!this.server.isPrimary) return;

        console.log(`[@cocreate/server] [PRIMARY] OS Reboot requested. Commencing graceful shutdown before kernel reboot...`);
        this.server.state.status = 'REBOOTING';
        this.publishState('rebooting');

        if (this.server.triggerGlobalShutdown) {
            this.server.triggerGlobalShutdown('OS_REBOOT', () => {
                console.log(`[@cocreate/server] [PRIMARY] Executing OS Reboot: sudo reboot`);
                exec('sudo reboot', (err) => {
                    if (err) {
                        console.error("[@cocreate/server] Failed to execute sudo reboot:", err.message);
                        process.exit(1);
                    }
                });
            });
        }
    }

    /**
     * Trigger full OS shutdown.
     */
    shutdown(payload = {}, fromEvent = false) {
        if (!fromEvent) {
            const sendPayload = (payload && typeof payload === 'object') ? payload : {};
            sendPayload.method = 'shutdown';
            if (!this.server.isPrimary) sendPayload.master = true;
            this.server.send(sendPayload);
            return;
        }

        if (!this.server.isPrimary) return;

        console.log(`[@cocreate/server] [PRIMARY] OS Shutdown requested. Commencing graceful shutdown before VM poweroff...`);
        this.server.state.status = 'SHUTTING_DOWN';
        this.publishState('stopped');

        if (this.server.triggerGlobalShutdown) {
            this.server.triggerGlobalShutdown('OS_SHUTDOWN', () => {
                console.log(`[@cocreate/server] [PRIMARY] Executing OS Poweroff: sudo poweroff`);
                exec('sudo poweroff', (err) => {
                    if (err) {
                        console.error("[@cocreate/server] Failed to execute sudo poweroff:", err.message);
                        process.exit(1);
                    }
                });
            });
        }
    }

    /**
     * Updates internal lifecycle status and emits status_change event.
     */
    updateLifecycleStatus() {
        let newStatus = 'RUNNING';
        if (this.server.isPrimaryShuttingDown) {
            newStatus = 'SHUTTING_DOWN';
        } else if (this.server.state.isDraining) {
            newStatus = 'DRAINING';
        } else if (this.server.state.isCordon) {
            newStatus = 'CORDONED';
        }

        if (this.server.state.status !== newStatus) {
            const oldStatus = this.server.state.status;
            this.server.state.status = newStatus;
            this.server.emit('status_change', { oldStatus, newStatus });
            
            if (this.server.isPrimary) {
                if (this.server.mesh && typeof this.server.mesh.send === 'function') {
                    try {
                        this.server.mesh.send({ method: 'mesh.node_status_update', server_id: this.server.id, status: newStatus });
                    } catch (err) {}
                }
            }
        }
        
        this.publishState();
    }
}

export default {
    init(serverCtx) {
        return new StateManager(serverCtx);
    }
};