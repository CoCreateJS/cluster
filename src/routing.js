/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
 *
 * Licensed under GNU AGPLv3.
 ********************************************************************************/

import config from '@cocreate/config';
import { dotNotationToObject } from '@cocreate/utils';

/**
 * Creates node-owned routing workflow hooks after CRUD, workflows, and node
 * identity have been initialized. Only the primary registers the node.
 * @param {Object} server - Initialized server context.
 * @returns {Promise<Object|undefined>} Routing configuration and add/remove hooks.
 * @public
 * @environment server
 */
export async function init(server) {
    if (!server.isPrimary) return;

    const settings = {
        enabled: true,
        workflow: 'configure-cluster-routing'
    };
    const systemConfig = await config.get('routing');
    const databaseConfig = await config.query({
        type: 'routing',
        organization_id: server.organization_id,
        server: server.host,
        cluster: server.cluster_id
    }, server.crud);

    for (const source of [systemConfig, databaseConfig]) {
        const normalized = dotNotationToObject(source || {});
        Object.assign(settings, normalized);
    }

    let adding;
    let removing;
    let stopping = false;
    let removalRequest;

    function checkResult(result) {
        const errors = Array.isArray(result?.error) ? result.error : result?.error ? [result.error] : [];
        if (errors.length) {
            throw new Error(errors.map(error => error?.message || String(error)).join('; '));
        }
        if (result?.success === false) throw new Error('The routing API rejected the request.');
        return result;
    }

    async function initialize() {
        const name = settings.workflow;
        const result = await server.workflow.run({
            method: `workflow.${name}`,
            organization_id: server.organization_id,
            host: server.host,
            [name]: [{
                server_id: server.id,
                cluster_id: server.cluster_id,
                ip: server.ip,
                host: server.host,
                infrastructure: server.infrastructure,
                location: server.location,
            }]
        });
        removalRequest = result?.[name]?.[0]?.remove || removalRequest;
        return checkResult(result);
    }

    return {
        config: settings,
        /** Register this node once after its listeners are ready. */
        add() {
            if (!settings.enabled || stopping || server.state?.isShuttingDown) return Promise.resolve();
            if (!adding) {
                adding = initialize().catch(error => {
                    adding = undefined;
                    throw error;
                });
            }
            return adding;
        },
        /** Withdraw this node before coordinated shutdown tears down its services. */
        remove(reason) {
            if (!settings.enabled) return Promise.resolve();
            stopping = true;
            if (!removing) {
                removing = (async () => {
                    // An in-flight registration must finish before withdrawal.
                    try { await adding; } catch {}
                    if (!removalRequest) return;
                    const result = await server.api.send({
                        ...removalRequest,
                        organization_id: server.organization_id,
                        host: server.host,
                        data: { ...removalRequest.data, ...(reason && { reason }) }
                    });
                    return checkResult(result);
                })();
            }
            return removing;
        }
    };
}

export default { init };
