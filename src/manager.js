import config from '@cocreate/config';
import { ObjectId, astAsync, dotNotationToObject } from '@cocreate/utils';
import { deleteServer } from '@cocreate/autoscaler';

const errors = result => !result || result.success === false ||
    (Array.isArray(result.error) ? result.error.length : result.error);
const copyRecord = record => Object.fromEntries(Object.entries(structuredClone(record))
    .filter(([key]) => key !== 'organization_id' && !key.startsWith('$')));

/** Parent connection and platform-owned server provisioning and termination. */
export async function init(server) {
    let connection = {};
    if (!server.isPrimary && process.env.SERVER_MANAGER) {
        try { connection = JSON.parse(process.env.SERVER_MANAGER); }
        catch { throw new Error('Invalid server manager configuration.'); }
    }
    const manager = {
        organization_id: connection.organization_id || null,
        host: connection.host || null,
        apiKey: connection.apiKey || null
    };
    // Only connection data is serialized into server inventory or worker configuration.
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
            const response = await fetch(`https://${manager.host}/manager`, {
                method: 'POST',
                headers: { Authorization: `Bearer ${manager.apiKey}`, 'Content-Type': 'application/json', Accept: 'application/json' },
                body: JSON.stringify(request), signal: AbortSignal.timeout(timeoutMs)
            });
            // TODO: Keep transport failures distinct from received task failures so callers
            // can retry only network errors and notify the platform for failed responses.
            phase = 'http-response';
            httpStatus = response.status;
            if (!response.ok) throw new Error(`Server manager request failed (HTTP ${response.status}).`);
            const result = await response.json();
            phase = 'task-response';
            const errors = Array.isArray(result?.error) ? result.error.length : result?.error;
            if (!result || typeof result !== 'object' || Array.isArray(result) || result.success === false || errors) {
                throw new Error('Server manager request failed.');
            }
            return result;
        } catch (error) {
            console.error('[@cocreate/cluster] Manager request failed; platform notification pending:', {
                phase, httpStatus, method: payload?.method, endpoint: payload?.endpoint,
                server_id: payload?.data?.server_id || payload?.object?._id || server.id,
                cluster_id: payload?.data?.cluster_id || server.cluster_id,
                error_code: error?.cause?.code || error?.code || error?.name
            });
            throw error;
        }
    } });
    const startupMonitor = createStartupMonitor(server);
    const cleanupMonitor = createCleanupMonitor(server);
    Object.defineProperty(manager, 'http', { value: (req, res) => handleManagerHttp(server, startupMonitor, cleanupMonitor, dispatchApi, req, res) });
    const settings = { enabled: false, workflow: 'create-organization-server' };
    if (server.isPrimary) {
        Object.assign(settings, dotNotationToObject(await config.get('organizationConnection') || {}));
        Object.assign(settings, dotNotationToObject(await config.query({
            type: 'organization-connection', organization_id: server.organization_id,
            server: server.host, cluster: server.cluster_id
        }, server.crud) || {}));
    }
    Object.defineProperty(manager, 'config', { value: settings });

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
        await mirror(patch, cluster.manager.for, cluster.host);
        return result;
    }
    async function dispatchApi(request) {
        const data = request.data;
        if (!data?.cluster_id || !data.server_id || !['create-instance', 'delete-instance'].includes(request.endpoint)) {
            if (request.endpoint === 'create-instance' && typeof data?.server_id === 'string') {
                return startupMonitor.track(data.server_id, () => server.api.send(request), result => result?.data?.instanceId);
            }
            return server.api.send(request);
        }
        const [cluster] = await read('clusters', {
            _id: data.cluster_id, type: 'organization', 'manager.organization_id': request.organization_id
        });
        if (!cluster) throw new Error('The managed cluster is unavailable in the platform scope.');
        const deployment = { ...data, api: request.api, method: request.method,
            endpoint: request.endpoint, region: request.region || data.region };
        if (request.endpoint === 'create-instance') {
            const result = await createServer(cluster, deployment);
            return { data: result.data };
        }
        return terminateServer(cluster, deployment);
    }
    async function terminateServer(cluster, deployment) {
        const patch = { _id: deployment.server_id, cluster_id: cluster._id,
            tenant_organization_id: cluster.manager.for, 'master.status': 'terminating' };
        await saveServer(cluster, patch);
        try {
            const result = await server.api.send({
                api: deployment.api, endpoint: deployment.endpoint || 'delete-instance', method: deployment.method,
                organization_id: server.organization_id, host: server.host, region: deployment.region,
                data: { server_id: deployment.server_id, cluster_id: cluster._id, vmId: deployment.vmId,
                    provider: deployment.provider, region: deployment.region }
            });
            if (errors(result)) throw new Error('The cloud API rejected the termination request.');
            cleanupMonitor.schedule(patch, { organization_id: server.organization_id, host: server.host });
            return result;
        } catch (error) {
            cleanupMonitor.cancel(deployment.server_id);
            await saveServer(cluster, { ...patch, 'master.status': 'failed' });
            throw error;
        }
    }
    function startCluster(organizationId) {
        if (!server.isPrimary || !settings.enabled || !organizationId || organizationId === server.organization_id) return;
        read('clusters', {
            type: 'organization', 'manager.for': organizationId,
            'manager.organization_id': server.organization_id
        }).then(([cluster]) => {
            if (cluster) return createServer(cluster);
        }).catch(error => {
            console.error('[@cocreate/cluster] Organization cluster startup failed:', {
                organization_id: organizationId,
                error_code: error?.cause?.code || error?.code || error?.name
            });
        });
    }

    async function createServer(cluster, deployment = cluster.initialDeployment) {
        const organizationId = cluster.manager.for;
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
        const startup = cluster.startupScript || resolved.clouds?.[cloud.provider]?.startupScript || resolved.startupScript;
        if (!startup) throw new Error('Organization deployment requires a configured startup script.');
        const managementKeys = await read('keys', {
            type: 'key', cluster_id: cluster._id, tenant_organization_id: organizationId
        }, { key: 1 });
        if (managementKeys.length !== 1 || typeof managementKeys[0].key !== 'string' || !managementKeys[0].key) {
            throw new Error('The managed cluster requires exactly one management API key.');
        }
        const serverId = deployment.server_id || ObjectId().toString();
        const record = {
            _id: serverId, name: deployment.name || `org-${organizationId}-initial-${serverId.slice(-6)}`,
            cluster_id: cluster._id, tenant_organization_id: organizationId,
            manager: { organization_id: cluster.manager.organization_id, host: cluster.manager.host, apiKey: managementKeys[0].key },
            cloud: cloud._id, storage: cluster.storage, host: cluster.host,
            status: 'provisioning', master: { status: 'provisioning' }, totalWorkers: 0,
            infrastructure: {
                provider: cloud.provider, region: region.region,
                vmType: instance.name, vCPU: Number(instance.vCPU), memoryGb: Number(instance.memoryGb)
            }
        };
        const environment = `server_id=${serverId}\ncluster_id=${cluster._id}`;
        const nodeConfig = { organization_id: organizationId, server_id: serverId, cluster_id: cluster._id, host: cluster.host, storage: bootstrap };
        const sandbox = {
            $payload: { organization_id: organizationId, data: { binaryUrl, host: cluster.host, environment } },
            $storageBase64: Buffer.from(JSON.stringify(bootstrap)).toString('base64'),
            $bootstrapConfigBase64: Buffer.from(JSON.stringify(nodeConfig)).toString('base64')
        };
        // Render the existing script with the tenant's bootstrap before cloud endpoint processing.
        const startupScript = await astAsync({ executeAST: startup, sandbox });
        if (typeof startupScript !== 'string' || !startupScript.trim()) throw new Error('Tenant startup script preparation failed.');
        const created = await crud({ method: 'object.create', array: 'servers', object: copyRecord(record) });
        if (!created.object?.length) throw new Error('Platform server inventory was not persisted.');
        try {
            // Populate tenant inventory before boot so its runtime can resolve the cluster ID immediately.
            await mirror(record, organizationId, cluster.host);
            const name = settings.workflow;
            const result = await startupMonitor.track(record._id, () => server.workflow.run({
                method: `workflow.${name}`, organization_id: server.organization_id, host: server.host,
                [name]: [{ server: { _id: record._id, cluster_id: record.cluster_id },
                    startupScript: JSON.stringify({ $concat: [[startupScript]] }), binaryUrl, environment }]
            }), result => {
                const patch = result?.[name]?.[0]?.patch;
                return patch?._id === record._id ? patch['infrastructure.instanceId'] : undefined;
            });
            if (errors(result)) {
                console.error('[@cocreate/cluster] Initial VM creation failed; platform notification pending:', {
                    organization_id: organizationId, cluster_id: cluster._id, server_id: record._id
                });
                throw new Error('Organization VM creation did not complete; inspect its server inventory.');
            }
            const output = result?.[name]?.[0];
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
    Object.defineProperty(manager, 'startCluster', { value: startCluster });
    Object.defineProperty(manager, 'createServer', { value: createServer });
    Object.defineProperty(manager, 'terminateServer', { value: terminateServer });
    return manager;
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
        observe(serverId) {
            const entry = pending.get(serverId);
            if (!entry) return;
            entry.reported = true;
            if (entry.timer) {
                clearTimeout(entry.timer);
                pending.delete(serverId);
            }
        },
        async track(serverId, create, instanceIdOf) {
            // Register before dispatch so a fast node can report before the provider responds.
            const entry = { reported: false };
            pending.set(serverId, entry);
            let watching = false;
            try {
                const result = await create();
                const instanceId = instanceIdOf(result);
                if (errors(result) || typeof instanceId !== 'string' || !instanceId.trim() || entry.reported || pending.get(serverId) !== entry) return result;
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

/** Handle managed API requests or authorized inventory patches in platform scope. */
function createCleanupMonitor(server) {
    const pending = new Map();
    return {
        cancel(serverId) {
            clearTimeout(pending.get(serverId));
            pending.delete(serverId);
        },
        schedule(record, scope) {
            if (pending.has(record._id)) return;
            const timer = setTimeout(async () => {
                try {
                    if (!record.cluster_id) throw new Error('Managed server cleanup requires a cluster ID.');
                    const response = await server.crud.send({
                        method: 'object.read', array: 'clusters',
                        filter: { query: { _id: record.cluster_id, type: 'organization' }, limit: 1 },
                        ...scope
                    });
                    const cluster = response?.object?.[0];
                    if (errors(response) || !cluster?.manager?.for || !cluster.host) {
                        throw new Error('Managed cluster cleanup scope is unavailable.');
                    }
                    await deleteServer(server, record._id, {
                        scopes: [{ organization_id: cluster.manager.for, host: cluster.host }, scope],
                        notifyManager: false
                    });
                } catch (error) {
                    console.error('[@cocreate/cluster] Managed server inventory cleanup failed:', {
                        server_id: record._id, cluster_id: record.cluster_id, error: error.message
                    });
                } finally {
                    pending.delete(record._id);
                }
            }, 60 * 1000);
            timer.unref?.();
            pending.set(record._id, timer);
        }
    };
}

async function handleManagerHttp(server, startupMonitor, cleanupMonitor, dispatchApi, req, res) {
    const reply = (status, body) => {
        res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end(JSON.stringify(body));
    };
    if (req.method !== 'POST') {
        res.setHeader('Allow', 'POST');
        return reply(405, { error: 'Use POST for this operation.' });
    }
    if (req.headers['content-type']?.split(';')[0].trim().toLowerCase() !== 'application/json') {
        return reply(415, { error: 'An application/json body is required.' });
    }
    let input;
    try {
        const chunks = [];
        let size = 0;
        for await (const chunk of req) {
            const buffer = Buffer.from(chunk);
            size += buffer.length;
            if (size > 1024 * 1024) return reply(413, { error: 'Request body exceeds 1 MiB.' });
            chunks.push(buffer);
        }
        input = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error();
    } catch {
        return reply(400, { error: 'A JSON object is required.' });
    }

    if (input.api) return server.api.http(req, res, input, dispatchApi);
    const bearer = req.headers.authorization?.match(/^Bearer\s+(\S+)$/i)?.[1];
    const apikey = bearer || input.apikey;
    if ((apikey !== undefined && (typeof apikey !== 'string' || !apikey.trim())) ||
        (bearer && input.apikey !== undefined && input.apikey !== bearer)) {
        return reply(401, { error: 'A valid API key is required.' });
    }

    try {
        const host = new URL(`https://${req.headers.host}`).hostname;
        const organization = await server.crud.getOrganization({ host });
        if (!organization?._id || organization.error) return reply(404, { error: 'Organization not found.' });
        const deleting = input.method === 'object.delete' && input.array === 'servers';
        if ((!deleting && (input.method !== 'object.update' || !['servers', 'clusters'].includes(input.array))) || !input.object ||
            typeof input.object !== 'object' || Array.isArray(input.object) || typeof input.object._id !== 'string') {
            return reply(400, { error: 'A server inventory deletion or server/cluster update is required.' });
        }
        const envelope = pickHttpFields(input, ['method', 'array', 'object', 'upsert']);
        // Scope and identity are server-resolved, never supplied by the caller.
        Object.assign(envelope, { organization_id: organization._id, host, apikey });
        const request = await server.authorize?.request(envelope);
        if (!request) return reply(403, { error: 'Operation is not authorized.' });
        if (deleting) {
            const result = await deleteServer(server, request.object._id, {
                scopes: [{ organization_id: organization._id, host }], notifyManager: false
            });
            return reply(200, result);
        }
        const result = await server.crud.send(request);
        const failed = result?.success === false || hasHttpErrors(result?.error);
        if (!failed && request.array === 'servers' && result?.object?.some(record => String(record._id) === request.object._id)) {
            startupMonitor.observe(request.object._id);
            const record = result.object.find(record => String(record._id) === request.object._id);
            const status = request.object['master.status'] || request.object.master?.status || record.master?.status;
            if (status === 'terminating') {
                cleanupMonitor.schedule(record, { organization_id: organization._id, host });
            } else if (status === 'failed') {
                cleanupMonitor.cancel(record._id);
            }
        }
        return reply(failed ? 400 : 200, result);
    } catch (error) {
        if (error.code === 'AUTHORIZATION_DENIED') return reply(403, { error: error.message });
        return reply(500, { error: 'Unable to process the operation.' });
    }
}

function hasHttpErrors(error) {
    return Array.isArray(error) ? error.length > 0 : Boolean(error);
}

function pickHttpFields(input, fields) {
    return Object.fromEntries(fields.filter(field => Object.hasOwn(input, field)).map(field => [field, input[field]]));
}

export default { init };
