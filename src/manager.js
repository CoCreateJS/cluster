import config from '@cocreate/config';

/** Frozen platform connection; sends standard envelopes over HTTP and awaits their responses. */
export async function init(server) {
    const managerConfig = Object.freeze({ ...config.get('manager') });
    const managerOrganizationId = managerConfig.organization_id || null;
    const isManagerServer = Boolean(managerOrganizationId && managerOrganizationId === server.organization_id);
    const connection = isManagerServer ? null : {
        organization_id: managerOrganizationId,
        host: managerConfig.host || null,
        apiKey: managerConfig.apiKey || null
    };
    const manager = {};
    if (connection?.organization_id) {
        Object.assign(manager, connection);
        Object.defineProperty(manager, 'send', { value: async (payload, { timeoutMs = 15000 } = {}) => {
            let phase = 'configuration';
            let httpStatus;
            try {
                if (!manager.organization_id) throw new Error('This server has no manager.');
                if (!/^[a-zA-Z0-9.-]+(?::\d+)?$/.test(manager.host || '') || typeof manager.apiKey !== 'string' || !manager.apiKey.trim()) {
                    throw new Error('Server manager requires a host and API key.');
                }
                payload.organization_id = manager.organization_id;
                payload.managedOrganization = server.organization_id;
                delete payload.apikey;
                phase = 'transport';
                const response = await server.fetch(`https://${manager.host}`, {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${manager.apiKey}`, 'Content-Type': 'application/json', Accept: 'application/json' },
                    body: JSON.stringify(payload), timeout: timeoutMs, throwHttpErrors: true
                });
                // TODO: Keep transport failures distinct from received task failures so callers
                // can retry only network errors and notify the platform for failed responses.
                phase = 'http-response';
                httpStatus = response.status;
                const result = await response.json();
                phase = 'task-response';
                if (!result || typeof result !== 'object' || Array.isArray(result)) {
                    throw new Error('Server manager request failed.');
                }
                return result;
            } catch (error) {
                httpStatus = error.response?.status || httpStatus;
                if (error.response) phase = 'http-response';
                console.error('[@cocreate/cluster] Manager request failed; platform notification pending:', {
                    phase, httpStatus, method: payload?.method, endpoint: payload?.endpoint,
                    server_id: payload?.data?.server_id || payload?.object?._id || server.id,
                    cluster_id: payload?.data?.cluster_id || server.cluster_id,
                    error_code: error?.cause?.code || error?.code || error?.name
                });
                throw error;
            }
        } });
        server.manager = Object.freeze(manager);
    } else {
        delete server.manager;
    }
    return server.manager;
}

export default { init };
