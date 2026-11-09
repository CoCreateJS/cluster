import config from '@cocreate/config';
import { ObjectId, astAsync, dotNotationToObject } from '@cocreate/utils';

const errors = result => !result || result.success === false ||
    (Array.isArray(result.error) ? result.error.length : result.error);
const copyRecord = record => Object.fromEntries(Object.entries(structuredClone(record))
    .filter(([key]) => key !== 'organization_id' && !key.startsWith('$')));
const shellQuote = value => `'${String(value).replaceAll("'", "'\\''")}'`;
const renderStartupScript = (template, values) => {
    const rendered = template.replace(/{{([a-zA-Z0-9]+)}}/g, (marker, key) => {
        if (!(key in values)) throw new Error(`Missing startup script value: ${key}`);
        return shellQuote(values[key]);
    });
    if (/{{[^}]+}}/.test(rendered)) throw new Error('Unresolved startup script template marker.');
    return rendered;
};

/** Parent connection and platform-owned initial server provisioning. */
export async function init(server) {
    const managerConfig = config.get('manager') || {};
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
            const { organization_id, host, socket, apikey, ...request } = payload;
            phase = 'transport';
            const response = await server.fetch(`https://${manager.host}`, {
                method: 'POST',
                headers: { Authorization: `Bearer ${manager.apiKey}`, 'Content-Type': 'application/json', Accept: 'application/json' },
                body: JSON.stringify(request), timeout: timeoutMs, throwHttpErrors: true
            });
            // TODO: Keep transport failures distinct from received task failures so callers
            // can retry only network errors and notify the platform for failed responses.
            phase = 'http-response';
            httpStatus = response.status;
            const result = await response.json();
            phase = 'task-response';
            const errors = Array.isArray(result?.error) ? result.error.length : result?.error;
            if (!result || typeof result !== 'object' || Array.isArray(result) || result.success === false || errors) {
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
        server.manager = manager;
    } else {
        delete server.manager;
    }
    const startupMonitor = createStartupMonitor(server);
    const managerService = {};
    const settings = {
        enabled: isManagerServer && managerConfig.enabled === true,
        workflow: managerConfig.workflow || 'createOrganizationServer'
    };
    Object.defineProperty(managerService, 'config', { value: settings });

    async function crud(request, organization_id = server.organization_id, host = server.host) {
        const result = await server.crud.send({ ...request, organization_id, host });
        if (errors(result)) throw new Error(`Organization server ${request.method} failed for ${request.array}.`);
        return result;
    }
    async function read(array, query, key) {
        const result = await crud({ method: 'object.read', array, filter: { query, ...(key && { key }), limit: 0 } });
        if (!Array.isArray(result.object)) throw new Error(`Organization server inventory read failed for ${array}.`);
        return result.object;
    }
    async function mirror(record, organizationId, host) {
        const result = await crud({
            method: 'object.update', array: 'servers', upsert: true, object: copyRecord(record)
        }, organizationId, host);
        if (!result.object?.length) throw new Error('Tenant server projection was not persisted.');
    }
    async function saveServer(cluster, patch) {
        const result = await crud({ method: 'object.update', array: 'servers', upsert: true, object: copyRecord(patch) });
        await mirror(patch, patch.tenant_organization_id || cluster.managedOrganization, cluster.host);
        return result;
    }
    function startCluster(organizationId) {
        if (!server.isPrimary || !settings.enabled || !organizationId || organizationId === server.organization_id) return;
        read('clusters', { type: 'organization', managedOrganization: organizationId }).then(([cluster]) => {
            if (cluster) return createServer(cluster);
        }).catch(error => {
            console.error('[@cocreate/cluster] Organization cluster startup failed:', {
                organization_id: organizationId,
                error_code: error?.cause?.code || error?.code || error?.name
            });
        });
    }

    async function createServer(cluster, deployment = cluster.initialDeployment) {
        const organizationId = cluster?.managedOrganization;
        if (!organizationId) return;
        const organization = (await read('organizations', { _id: organizationId }, { _id: 1, bootstrapStorage: 1 }))[0];
        const workflows = await read('workflows', { name: settings.workflow, enabled: true });
        if (workflows.length !== 1) throw new Error('The organization server workflow is unavailable or ambiguous.');
        if (![deployment?.cloud, deployment?.region, deployment?.vmType].every(value => typeof value === 'string' && value.trim())) {
            throw new Error('Server deployment requires cloud, region and VM type.');
        }
        if (typeof cluster.storage !== 'string' || !cluster.storage.trim()) {
            throw new Error('Organization cluster requires a storage API reference.');
        }
        const bootstrap = organization.bootstrapStorage;
        if (!bootstrap?.name || !bootstrap?.provider) throw new Error('Tenant database bootstrap is unavailable.');
        const resolved = dotNotationToObject(await config.query({
            type: ['startupscript', 'autoscaler', 'servers_config'], organization_id: server.organization_id,
            server: cluster.host, cluster: cluster._id
        }, server.crud) || {});
        const cloud = (await read('apis', { _id: deployment.cloud, type: 'cloud', enabled: true }, { _id: 1, name: 1, provider: 1 }))[0];
        if (!cloud?.name || !cloud.provider) throw new Error('The deployment cloud API is unavailable.');
        if (deployment.api && deployment.api !== cloud.name) throw new Error('The deployment cloud does not match the authorized API.');
        const profile = resolved.clouds?.[cloud.provider];
        const region = profile?.regions?.find(item => item.region === deployment.region);
        const instance = profile?.instances?.find(item => item.name === deployment.vmType);
        if (!region || ![instance?.vCPU, instance?.memoryGb].every(value => Number.isFinite(Number(value)) && Number(value) > 0)) {
            throw new Error('The deployment region or VM type is unavailable.');
        }
        const binaryUrl = cluster.binaryUrl || resolved.binaryUrl;
        if (typeof binaryUrl !== 'string' || !/^https:\/\/[^\s\\"'`$]+$/.test(binaryUrl) ||
            typeof cluster.host !== 'string' || !/^[a-zA-Z0-9.-]+$/.test(cluster.host)) {
            throw new Error('Organization deployment requires a safe binary URL and hostname.');
        }
        const startup = cluster.startupScript || resolved.clouds?.[cloud.provider]?.startupScript ||
            server.startupScriptTemplate || resolved.startupScript;
        if (!startup) throw new Error('Organization deployment requires a configured startup script.');
        if (!managerConfig.organization_id || !managerConfig.host || !managerConfig.apiKey) {
            throw new Error('Manager config requires organization_id, host and apiKey to create a managed server.');
        }
        const serverId = deployment.server_id || ObjectId().toString();
        const record = {
            _id: serverId, name: deployment.name || `org-${organizationId}-initial-${serverId.slice(-6)}`,
            cluster_id: cluster._id, tenant_organization_id: organizationId,
            cloud: cloud._id, storage: cluster.storage, host: cluster.host,
            status: 'provisioning', master: { status: 'provisioning' }, totalWorkers: 0,
            infrastructure: {
                provider: cloud.provider, region: region.region,
                vmType: instance.name, vCPU: Number(instance.vCPU), memoryGb: Number(instance.memoryGb)
            }
        };
        const environment = `server_id=${serverId}\ncluster_id=${cluster._id}`;
        const nodeConfig = {
            organization_id: organizationId, server_id: serverId, cluster_id: cluster._id,
            host: cluster.host, storage: bootstrap,
            manager: {
                organization_id: managerConfig.organization_id,
                host: managerConfig.host,
                apiKey: managerConfig.apiKey,
                enabled: managerConfig.enabled === true,
                workflow: managerConfig.workflow || 'createOrganizationServer'
            }
        };
        const sandbox = {
            $payload: { organization_id: organizationId, data: { binaryUrl, host: cluster.host, environment } },
            $storageBase64: Buffer.from(JSON.stringify(bootstrap)).toString('base64'),
            $bootstrapConfigBase64: Buffer.from(JSON.stringify(nodeConfig)).toString('base64')
        };
        // Render the existing script with the tenant's bootstrap before cloud endpoint processing.
        const startupScript = startup === server.startupScriptTemplate
            ? renderStartupScript(startup, {
                binaryUrl, organizationId, serverId, clusterId: cluster._id, host: cluster.host,
                storageBase64: sandbox.$storageBase64,
                bootstrapConfigBase64: sandbox.$bootstrapConfigBase64
            })
            : await astAsync({ executeAST: startup, sandbox });
        if (typeof startupScript !== 'string' || !startupScript.trim()) throw new Error('Tenant startup script preparation failed.');
        const created = await crud({ method: 'object.create', array: 'servers', object: copyRecord(record) });
        if (!created.object?.length) throw new Error('Platform server inventory was not persisted.');
        try {
            // Populate tenant inventory before boot so its runtime can resolve the cluster ID immediately.
            await mirror(record, organizationId, cluster.host);
            const name = settings.workflow;
            const result = await startupMonitor.track(record._id, () => server.workflow.run({
                method: `workflow.${name}`, organization_id: server.organization_id, host: server.host,
                data: { ...cluster, initialDeployment: deployment },
                server: { _id: record._id },
                startupScript: JSON.stringify({ $concat: [[startupScript]] }), binaryUrl, environment
            }), result => {
                const patch = result?.data?.patch;
                return patch?._id === record._id ? patch['infrastructure.instanceId'] : undefined;
            });
            if (errors(result)) {
                console.error('[@cocreate/cluster] Initial VM creation failed; platform notification pending:', {
                    organization_id: organizationId, cluster_id: cluster._id, server_id: record._id
                });
                throw new Error('Organization VM creation did not complete; inspect its server inventory.');
            }
            const output = result?.data;
            if (!output?.patch || output.patch._id !== record._id || !output.patch['infrastructure.instanceId']) {
                console.error('[@cocreate/cluster] Initial VM response has no matching instance ID; platform notification pending:', {
                    organization_id: organizationId, cluster_id: cluster._id, server_id: record._id
                });
                throw new Error('Organization VM result did not include a provider instance ID.');
            }
            // Apply the same provider patch in tenant scope without overwriting node-owned readiness fields.
            await mirror(output.patch, organizationId, cluster.host);
            return { status: 'created', server: output.server, data: output.data || {
                instanceId: output.patch['infrastructure.instanceId'],
                privateIpAddress: output.patch['infrastructure.privateIpAddress']
            } };
        } catch (error) {
            await saveServer(cluster, { _id: record._id, cluster_id: cluster._id, 'master.status': 'failed' });
            throw error;
        }
    }
    // Methods stay out of persisted manager connection data and worker configuration.
    Object.defineProperty(managerService, 'startCluster', { value: startCluster });
    Object.defineProperty(managerService, 'createServer', { value: createServer });
    if (server.isPrimary && isManagerServer && typeof server.on === 'function') {
        server.on('organizationPeers', ({ organization_id, peers } = {}) => {
            if (Array.isArray(peers) && peers.length === 0) startCluster(organization_id);
        });
    }
    server.managerService = managerService;
    return managerService;
}

const STARTUP_UPDATE_TIMEOUT_MS = 120000;

/** A provider instance ID starts the wait for the node's first managed state update. */
function createStartupMonitor(server) {
    const pending = new Map();
    const hasReportedState = record => record && (
        (typeof record.master?.status === 'string' && record.master.status !== 'provisioning') ||
        Object.entries(record).some(([key, value]) => /^worker\d+$/.test(key) && typeof value?.status === 'string')
    );
    return {
        async track(serverId, create, instanceIdOf) {
            // Register before dispatch so a fast node can report before the provider responds.
            const entry = {};
            pending.set(serverId, entry);
            let watching = false;
            try {
                const result = await create();
                const instanceId = instanceIdOf(result);
                if (errors(result) || typeof instanceId !== 'string' || !instanceId.trim() || pending.get(serverId) !== entry) return result;
                entry.timer = setTimeout(async () => {
                    if (pending.get(serverId) !== entry) return;
                    pending.delete(serverId);
                    if (server.state?.isShuttingDown) return;
                    try {
                        // HTTP updates may reach another managing worker/node. Read their
                        // persisted state before deciding that this node never reported.
                        const response = await server.crud.send({
                            method: 'object.read', array: 'servers',
                            organization_id: server.organization_id, host: server.host,
                            filter: { query: { _id: serverId }, limit: 1 }
                        });
                        if (errors(response) || !Array.isArray(response.object)) throw new Error('Unable to read startup state.');
                        if (hasReportedState(response.object.find(record => String(record._id) === serverId))) return;
                        // TODO: Notify the platform about the missing startup update.
                        // Potentially remove this instance and try one replacement only;
                        // a second startup failure must notify, with no third launch.
                        console.error('[@cocreate/cluster] No managed startup update received within two minutes:', serverId);
                    } catch {
                        // TODO: Notify the platform that startup confirmation could not be checked.
                        console.error('[@cocreate/cluster] Unable to verify managed startup update:', serverId);
                    }
                }, STARTUP_UPDATE_TIMEOUT_MS);
                entry.timer.unref?.();
                watching = true;
                return result;
            } finally {
                if (!watching && pending.get(serverId) === entry) pending.delete(serverId);
            }
        }
    };
}

export default { init };
