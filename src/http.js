import Http from 'node:http';
import Https from 'node:https';
import { uuid } from '@cocreate/utils';

/**
 * Initializes the HTTP and HTTPS listeners on the server context.
 * @param {object} server - CoCreate server instance.
 * @returns {Promise<void>}
 * @public
 * @environment server
 */
async function init(server) {
    const HTTP_PORT = process.env.HTTP_PORT || 80;
    const HTTPS_PORT = process.env.HTTPS_PORT || 443;
    const BIND_IP = process.env.BIND_IP || '0.0.0.0';

    const httpsOptions = server.tls.createHttpsOptions(server);
    server.https = Https.createServer(httpsOptions, (req, res) => handleRequest(server, req, res));

    server.http = Http.createServer((req, res) => {
        const requestUrl = req.url || '/';

        if (requestUrl.startsWith('/.well-known/acme-challenge/')) {
            return handleRequest(server, req, res);
        }

        const rawHost = req.headers.host || 'localhost';
        const host = rawHost.split(':')[0];
        const portSuffix = (HTTPS_PORT && String(HTTPS_PORT) !== '443') ? `:${HTTPS_PORT}` : '';

        res.writeHead(301, { Location: `https://${host}${portSuffix}${requestUrl}` });
        res.end();
    });

    const reportListening = state => {
        if (process.connected) {
            process.send({ type: 'listening_state', state });
        }
    };

    server.http.on('listening', () => reportListening(true));
    server.http.on('close', () => reportListening(false));
    server.http.listen(HTTP_PORT, BIND_IP, () => {
        console.log(`[@cocreate/cluster] [WORKER ${server.workerId}] HTTP listening on ${BIND_IP}:${HTTP_PORT}`);
    });

    server.https.listen(HTTPS_PORT, BIND_IP, () => {
        console.log(`[@cocreate/cluster] [WORKER ${server.workerId}] HTTPS listening on ${BIND_IP}:${HTTPS_PORT}`);
    });
}

/**
 * Handles inbound HTTP requests at the server edge.
 * @param {object} server - CoCreate server instance.
 * @param {import('node:http').IncomingMessage} req - Incoming HTTP request.
 * @param {import('node:http').ServerResponse} res - HTTP response.
 * @returns {Promise<void>}
 * @environment server
 */
async function handleRequest(server, req, res) {
    try {
        const rawUrl = req.url || '/';
        const [reqPath] = rawUrl.split('?');

        if (
            !reqPath.startsWith('/.well-known/acme-challenge/') &&
            server.tls?.checkOrganizationAccess
        ) {
            const access = await server.tls.checkOrganizationAccess(req.headers.host);
            if (access.allowed === false) {
                res.writeHead(402, {
                    'Content-Type': 'application/json; charset=utf-8',
                    'Cache-Control': 'no-store',
                    'Account-Balance': 'false'
                });
                res.end(JSON.stringify({
                    error: 'Organization balance depleted',
                    organizationBalance: false,
                    balance: access.balance,
                    currency: access.currency
                }));
                return;
            }
        }

        // Match registered exact or wildcard routes first.
        let handler = server.routes.get(reqPath);

        if (!handler) {
            for (const [routePath, routeHandler] of server.routes.entries()) {
                if (routePath === '*' || (routePath.endsWith('*') && reqPath.startsWith(routePath.slice(0, -1)))) {
                    handler = routeHandler;
                    break;
                }
            }
        }

        if (typeof handler === 'function') {
            await handler(req, res);
            if (res.writableEnded || res.headersSent) return;
        }

        // Webhooks keep their dedicated request contract.
        if (reqPath.startsWith('/webhooks/')) {
            return server.api.webhook(req, res);
        }

        if (req.method === 'POST') {
            return handleTransport(server, req, res);
        }

        if (['PUT', 'PATCH', 'DELETE'].includes(req.method)) {
            return sendJson(res, 405, {
                error: 'Use POST with a CoCreate payload object containing a method, such as object.read. GET and HEAD requests are handled as files.'
            }, { Allow: 'GET, HEAD, POST' });
        }

        return server.files.send(req, res);
    } catch (error) {
        console.error('[@cocreate/cluster] Edge request routing error:', error);
        if (!res.headersSent) {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
        }
        res.end('Internal Error: Request routing failed');
    }
}

async function handleTransport(server, req, res) {
    let payload;
    let responseUuid;
    try {
        const chunks = [];
        let size = 0;
        for await (const chunk of req) {
            const buffer = Buffer.from(chunk);
            size += buffer.length;
            if (size > 1024 * 1024) {
                return sendJson(res, 413, { error: 'Request body exceeds 1 MiB.' });
            }
            chunks.push(buffer);
        }

        payload = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error();
        if (typeof payload.method !== 'string' || !payload.method.trim()) {
            return sendJson(res, 400, { error: 'A CoCreate payload object with a method is required.' });
        }
        payload.uuid ||= uuid();
    } catch {
        return sendJson(res, 400, { error: 'A valid JSON CoCreate payload is required.' });
    }

    try {
        const host = new URL(`https://${req.headers.host}`).hostname;
        const organization = await server.crud.getOrganization({ host });
        if (!organization?._id || organization.error) {
            return sendJson(res, 404, { error: 'Organization not found for this host.' });
        }

        const bearer = req.headers.authorization?.match(/^Bearer\s+(\S+)$/i)?.[1];
        const token = bearer || getCookieToken(req.headers.cookie);
        let user_id;
        if (token && server.authenticate) {
            try {
                const session = await server.authenticate.decodeToken(token, organization._id, undefined, host);
                if (session?.user_id && session.expires > Date.now()) user_id = session.user_id;
            } catch { /* Invalid sessions remain anonymous. */ }
        }

        const apiKeyHeader = req.headers['x-api-key'];
        if (bearer && apiKeyHeader && bearer !== apiKeyHeader && !user_id) {
            return sendJson(res, 401, { error: 'Conflicting API keys were provided.' });
        }
        const apikey = apiKeyHeader || (user_id ? undefined : bearer);
        if (apikey !== undefined && (typeof apikey !== 'string' || !apikey.trim())) {
            return sendJson(res, 401, { error: 'A valid API key is required.' });
        }

        const authorizationPayload = {
            ...payload,
            organization_id: organization._id,
            host
        };
        delete authorizationPayload.apikey;
        if (apikey) authorizationPayload.apikey = apikey;
        if (user_id) authorizationPayload.user_id = user_id;
        else delete authorizationPayload.user_id;

        const request = await server.authorize.request(authorizationPayload, user_id);
        request.uuid = payload.uuid;

        if (server.responseListeners.has(request.uuid)) {
            return sendJson(res, 409, { error: 'A request with this UUID is already being handled.' });
        }

        server.responseListeners.set(request.uuid, response => {
            const failed = response?.success === false || hasErrors(response?.error);
            sendJson(res, failed ? 400 : 200, response);
        });
        responseUuid = request.uuid;
        res.once('close', () => server.responseListeners.delete(request.uuid));

        try {
            const dispatch = await dispatchPayload(server, request);
            if (!dispatch.emitted && server.responseListeners.has(request.uuid)) {
                const response = dispatch.result && typeof dispatch.result === 'object'
                    ? { ...dispatch.result, uuid: request.uuid }
                    : request;
                await server.send(response);
            }
        } catch (error) {
            if (server.responseListeners.has(request.uuid)) {
                await server.send({ ...request, success: false, error: error.message });
            }
        }
    } catch (error) {
        if (responseUuid) server.responseListeners.delete(responseUuid);
        if (res.writableEnded) return;
        if (error.code === 'AUTHORIZATION_DENIED') {
            return sendJson(res, 403, { error: error.message });
        }
        if (error.code === 'HTTP_HANDLER_NOT_FOUND') {
            return sendJson(res, 404, { error: error.message });
        }
        console.error('[@cocreate/cluster] HTTP transport request failed:', error);
        return sendJson(res, 500, { error: 'Unable to process the transport request.' });
    }
}

function getCookieToken(cookieHeader = '') {
    const cookie = cookieHeader.split(';').map(value => value.trim()).find(value => value.startsWith('token='));
    if (!cookie) return null;
    try { return decodeURIComponent(cookie.slice(6)); }
    catch { return null; }
}

async function dispatchPayload(server, payload) {
    const moduleName = payload.method.split('.')[0];

    if (payload.api) return { result: await server.api.send(payload), emitted: false };
    if (['storage', 'database', 'array', 'index', 'object'].includes(moduleName)) {
        return { result: await server.crud.send(payload), emitted: false };
    }
    if (moduleName === 'workflow') {
        return { result: await server.workflow.run(payload), emitted: false };
    }

    if (!server.emit(payload.method, payload)) {
        throw Object.assign(new Error(`No handler is registered for '${payload.method}'.`), { code: 'HTTP_HANDLER_NOT_FOUND' });
    }
    return { emitted: true };
}

function hasErrors(error) {
    return Array.isArray(error) ? error.length > 0 : Boolean(error);
}

function sendJson(res, status, body, headers = {}) {
    if (res.writableEnded) return;
    res.writeHead(status, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
        ...headers
    });
    res.end(JSON.stringify(body));
}

export default { init };
