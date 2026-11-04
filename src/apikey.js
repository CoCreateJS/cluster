/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
 *
 * Licensed under GNU AGPLv3.
 ********************************************************************************/

import config from '@cocreate/config';

/**
 * Loads this node organization's upload key after CRUD is initialized and saves
 * it through Config for subsequent CLI processes. Workers inherit the primary's
 * environment; credentials are never returned or logged by this handler.
 * @param {Object} server - Initialized server context.
 * @returns {Promise<void>}
 * @public
 * @environment server
 */
export async function init(server) {
    if (!server.isPrimary) return;

    let response;
    try {
        response = await server.crud.send({
            method: 'object.read',
            organization_id: server.organization_id,
            host: server.host,
            array: 'keys',
            filter: {
                query: { type: 'key', admin: { $in: [true, 'true', 1, '1'] } },
                key: { key: 1 },
                sort: { _id: 1 },
                limit: 1
            }
        });
    } catch {
        throw new Error('Unable to read the node organization API key.');
    }

    const errors = Array.isArray(response?.error) ? response.error.length : response?.error;
    if (!response || errors || response.success === false) {
        throw new Error('Unable to read the node organization API key.');
    }

    const apikey = response.object?.[0]?.key;
    if (apikey === undefined) return;
    if (typeof apikey !== 'string' || !apikey.trim()) {
        throw new Error('The node organization API key is invalid.');
    }

    await config.prompt({ apikey: { value: apikey } });
}

export default { init };
