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

const presenceEvents = ['orgConnected', 'orgDisconnected'];
const masterControls = ['restart', 'reboot', 'shutdown'];
const workerControls = ['cordon', 'shed', 'drain'];
const includesTarget = (target, id) => target === undefined ||
    (Array.isArray(target) ? target : [target]).some(value => String(value) === String(id));
const organizationDelivery = payload => Boolean(payload.organization_id &&
    !presenceEvents.includes(payload.method) && !masterControls.includes(payload.method) &&
    !workerControls.includes(payload.method));

/** IPC owns request/reply correlation; callers always receive the envelope. */
export function init(server, pendingPromises) {
    const organizationWorkers = server.organizationWorkers ||= new Map();

    function transportEnvelope({ socket, wsManager, ...payload }) { return payload; }

    function request(send, payload, key, workerId) {
        return new Promise((resolve, reject) => {
            pendingPromises.set(key, { resolve, reject, workerId });
            try { send(transportEnvelope(payload)); }
            catch (error) { pendingPromises.delete(key); reject(error); }
        });
    }

    function resolveResponse(payload, key) {
        const pending = pendingPromises.get(key);
        pendingPromises.delete(key);
        // Late and duplicate replies are terminal, never executable events.
        pending?.resolve(payload);
    }

    function updateOrganizationWorker(payload) {
        const { organization_id, workerId, method } = payload;
        const workers = organizationWorkers.get(organization_id) || new Set();
        const wasActive = workers.size > 0;
        if (method === 'orgDisconnected') workers.delete(workerId);
        else workers.add(workerId);
        if (workers.size) organizationWorkers.set(organization_id, workers);
        else organizationWorkers.delete(organization_id);
        if (wasActive !== Boolean(workers.size)) {
            server.emit(workers.size ? 'orgConnected' : 'orgDisconnected', { organization_id });
        }
    }

    function deliverLocal(payload, execute) {
        if (!includesTarget(payload.server, server.id) ||
            !includesTarget(payload.cluster, server.cluster_id)) return payload;
        if (organizationDelivery(payload)) {
            if (server.socket && includesTarget(payload.worker, server.workerId)) {
                return Promise.resolve(server.socket.send({ ...payload })).then(() => payload);
            }
        } else if (execute && payload.method &&
            (!server.isPrimary || payload.master !== false)) {
            return server.emitAsync(payload.method, payload);
        }
        return payload;
    }

    async function route(payload, fromWorker = false) {
        if (presenceEvents.includes(payload.method)) {
            updateOrganizationWorker(payload);
            return payload;
        }
        // Queue local work: synchronous eviction must not remove remote routes first.
        const local = Promise.resolve().then(() =>
            fromWorker && payload.serverId === server.id && payload.workerId === server.workerId
                ? payload : deliverLocal(payload, true));
        const targetsThisNode = includesTarget(payload.server, server.id) &&
            includesTarget(payload.cluster, server.cluster_id);
        const targetsOtherNodes = payload.server === undefined ||
            (Array.isArray(payload.server) ? payload.server : [payload.server])
                .some(id => String(id) !== String(server.id));
        if (payload.serverId === server.id && targetsOtherNodes &&
            (payload.organization_id || payload.server !== undefined || payload.cluster !== undefined)) {
            server.cluster.send(transportEnvelope(payload));
        }

        const workers = targetsThisNode && !(payload.master === true && masterControls.includes(payload.method))
            ? [...server.workers.values()].filter(worker => {
                if (!includesTarget(payload.worker, worker.id)) return false;
                if (organizationDelivery(payload)) {
                    return organizationWorkers.get(payload.organization_id)?.has(worker.id) &&
                        !(payload.serverId === server.id && payload.workerId === worker.id);
                }
                return payload.worker !== undefined || payload.broadcast;
            }) : [];

        if (['cordon', 'drain'].includes(payload.method) && payload.value !== false) {
            const targeted = new Set(workers.map(worker => worker.id));
            const active = [...server.workers.values()].some(worker => !targeted.has(worker.id) &&
                worker.isListening && !worker.isCordoned && !worker.isDraining);
            if (!active) server.state.evaluateMasterFallback();
        }

        const replies = await Promise.all([local, ...workers.map(worker => {
            const process = worker.process || worker;
            const nativeId = process.id ?? worker.id;
            return request(message => process.send(message), payload, `${payload.uuid}:${nativeId}`, nativeId);
        })]);
        return replies.find(reply => reply.error || reply.success === false) || replies[1] || replies[0];
    }

    async function receive(payload, reply, worker) {
        if (payload.response === true) {
            resolveResponse(payload, server.isPrimary ? `${payload.uuid}:${worker.id}` : payload.uuid);
            return;
        }
        payload.uuid ||= uuid();
        payload.workerId ??= worker?.id;
        payload.serverId ??= server.id;
        if (payload.broadcast === 'false') payload.broadcast = false;
        let result;
        try {
            result = server.isPrimary ? await route(payload, true) : await deliverLocal(payload, true);
        } catch (error) {
            result = { ...payload, success: false, error: error.message };
        }
        reply({ ...transportEnvelope(result), response: true });
    }

    for (const method of presenceEvents) {
        process.on(method, (organization_id, details = {}) => {
            const payload = { method, organization_id, workerId: details.workerId ?? server.workerId };
            if (server.isPrimary) updateOrganizationWorker(payload);
            else server.send(payload);
        });
    }
    if (server.isPrimary) {
        cluster.on('message', (worker, payload) =>
            receive(payload, reply => (worker.process || worker).send(reply), worker));
        cluster.on('exit', worker => {
            const id = [...server.workers.values()].find(record => record.process === worker)?.id ?? worker.id;
            for (const organization_id of organizationWorkers.keys()) {
                updateOrganizationWorker({ method: 'orgDisconnected', organization_id, workerId: id });
            }
            for (const [key, pending] of pendingPromises) {
                if (pending.workerId === worker.id) {
                    pendingPromises.delete(key);
                    pending.reject(new Error(`Worker ${worker.id} exited before replying`));
                }
            }
        });
    } else {
        process.on('message', payload => receive(payload, reply => process.send(reply)));
    }

    return function send(payload = {}) {
        payload.uuid ||= uuid();
        payload.organization_id ||= payload.socket?.organization_id;
        if (payload.broadcast === 'false') payload.broadcast = false;
        if (payload.serverId === undefined && payload.workerId === undefined && (!server.isPrimary || server.isWorker)) {
            payload.workerId = server.workerId;
        }
        payload.serverId ??= server.id;
        payload.status ||= 'sent';
        if (payload.response === true) {
            if (server.isPrimary) {
                if (payload.workerId == null) resolveResponse(payload, payload.uuid);
                else {
                    const worker = server.workers.get(payload.workerId);
                    (worker.process || worker).send(transportEnvelope(payload));
                }
            } else process.send(transportEnvelope(payload));
            return Promise.resolve(payload);
        }
        if (server.isPrimary) return route(payload);
        const local = Promise.resolve().then(() => deliverLocal(payload, false));
        const response = request(message => process.send(message), payload, payload.uuid);
        return Promise.all([local, response]).then(([, envelope]) => envelope);
    };
}

export default { init };
