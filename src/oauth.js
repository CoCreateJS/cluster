/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 ********************************************************************************/

import { createHash } from 'node:crypto';

const WELL_KNOWN_AUTHORIZATION_SERVER = '/.well-known/oauth-authorization-server';
const WELL_KNOWN_PROTECTED_RESOURCE = '/.well-known/oauth-protected-resource';
const MAX_BODY_SIZE = 32 * 1024;
const TOKEN_ARRAY = 'oauth';
const hash = value => createHash('sha256').update(value).digest('hex');

const workflowRoutes = new Map([
    ['/oauth/register', 'register'],
    ['/oauth/authorize', 'authorize'],
    ['/oauth/request', 'request'],
    ['/oauth/decision', 'decision'],
    ['/oauth/token', 'token'],
    ['/oauth/revoke', 'revoke']
]);

function sendJson(res, status, body, headers = {}) {
    if (res.writableEnded) return;
    res.writeHead(status, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
        Pragma: 'no-cache',
        ...headers
    });
    res.end(JSON.stringify(body));
}

function originFor(req) {
    const host = req.headers.host;
    if (!host) throw new Error('The request Host header is required.');
    return new URL(`https://${host}`).origin;
}

function cookieToken(req) {
    const match = String(req.headers.cookie || '').match(/(?:^|;\s*)token=([^;]+)/);
    if (!match) return '';
    try { return decodeURIComponent(match[1]); }
    catch { return ''; }
}

function resourceFor(req, origin) {
    const prefix = `${WELL_KNOWN_PROTECTED_RESOURCE}/`;
    const pathname = (req.url || '/').split('?')[0];
    const resourcePath = pathname.startsWith(prefix)
        ? pathname.slice(prefix.length)
        : '';
    return new URL(resourcePath ? `/${resourcePath}` : '/', origin).toString();
}

function uniqueParameters(searchParams, label) {
    const result = {};
    for (const [key, value] of searchParams) {
        if (Object.hasOwn(result, key)) {
            throw Object.assign(new Error(`OAuth ${label} parameter '${key}' must not be repeated.`), { status: 400 });
        }
        result[key] = value;
    }
    return result;
}

async function readBody(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
        size += chunk.length;
        if (size > MAX_BODY_SIZE) throw Object.assign(new Error('OAuth request body is too large.'), { status: 413 });
        chunks.push(chunk);
    }

    const text = Buffer.concat(chunks).toString('utf8');
    const contentType = String(req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
    if (!text) return {};
    if (contentType === 'application/json') {
        try {
            const value = JSON.parse(text);
            if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error();
            return value;
        } catch {
            throw Object.assign(new Error('OAuth request body must be a JSON object.'), { status: 400 });
        }
    }
    if (contentType === 'application/x-www-form-urlencoded') {
        return uniqueParameters(new URLSearchParams(text), 'body');
    }
    throw Object.assign(new Error('Unsupported OAuth request content type.'), { status: 415 });
}

async function requestFor(req) {
    const url = new URL(req.url || '/', `https://${req.headers.host}`);
    const body = ['POST', 'PUT', 'PATCH'].includes(req.method) ? await readBody(req) : {};
    return {
        method: req.method,
        url: url.toString(),
        path: url.pathname,
        query: uniqueParameters(url.searchParams, 'query'),
        headers: {
            origin: req.headers.origin,
            contentType: req.headers['content-type'],
            accept: req.headers.accept
        },
        body
    };
}

function sendWorkflowResponse(res, result) {
    const response = result?.httpResponse;
    if (!response || !Number.isInteger(response.status) || response.status < 100 || response.status > 599) {
        return sendJson(res, 500, { error: 'OAuth workflow returned no valid httpResponse.' });
    }

    const headers = response.headers && typeof response.headers === 'object' ? response.headers : {};
    if (response.body !== undefined && response.body !== null &&
        typeof response.body !== 'string' && !Buffer.isBuffer(response.body) &&
        !headers['Content-Type'] && !headers['content-type']) {
        headers['Content-Type'] = 'application/json; charset=utf-8';
    }
    res.writeHead(response.status, { 'Cache-Control': 'no-store', ...headers });
    if (response.body === undefined || response.body === null) return res.end();
    if (typeof response.body === 'string' || Buffer.isBuffer(response.body)) return res.end(response.body);
    return res.end(JSON.stringify(response.body));
}

/**
 * Registers CoCreate's fixed OAuth protocol routes on the server instance.
 * Protocol callbacks normalize HTTP into named workflow envelopes. Workflows
 * set `payload.httpResponse` for the adapter to write to the client.
 * @param {object} server - Initialized CoCreate server context.
 * @returns {{ close: Function }} OAuth route controls.
 * @public
 * @environment server
 */
export function init(server) {
    if (!server?.routes || typeof server.routes.set !== 'function') {
        throw new TypeError('CoCreate OAuth requires the server route registry.');
    }
    if (!server.workflow || typeof server.workflow.run !== 'function') {
        throw new TypeError('CoCreate OAuth requires the server workflow service.');
    }

    const routes = new Map();
    const route = (path, handler) => {
        // OAuth owns these fixed protocol paths.
        routes.set(path, handler);
        server.routes.set(path, handler);
    };

    const metadata = async (req, res) => {
        if (req.method !== 'GET' && req.method !== 'HEAD') {
            return sendJson(res, 405, { error: 'method_not_allowed' }, { Allow: 'GET, HEAD' });
        }
        try {
            const issuer = originFor(req);
            const body = {
                issuer,
                authorization_endpoint: new URL('/oauth/authorize', issuer).toString(),
                token_endpoint: new URL('/oauth/token', issuer).toString(),
                registration_endpoint: new URL('/oauth/register', issuer).toString(),
                revocation_endpoint: new URL('/oauth/revoke', issuer).toString(),
                response_types_supported: ['code'],
                grant_types_supported: ['authorization_code', 'refresh_token'],
                code_challenge_methods_supported: ['S256'],
                token_endpoint_auth_methods_supported: ['none'],
                revocation_endpoint_auth_methods_supported: ['none']
            };
            if (req.method === 'HEAD') {
                res.writeHead(200, {
                    'Content-Type': 'application/json; charset=utf-8',
                    'Cache-Control': 'no-store',
                    Pragma: 'no-cache'
                });
                return res.end();
            }
            return sendJson(res, 200, body);
        } catch (error) {
            return sendJson(res, 400, { error: 'invalid_request', error_description: error.message });
        }
    };

    const protectedResourceMetadata = async (req, res) => {
        if (req.method !== 'GET' && req.method !== 'HEAD') {
            return sendJson(res, 405, { error: 'method_not_allowed' }, { Allow: 'GET, HEAD' });
        }
        const pathname = (req.url || '/').split('?')[0];
        if (pathname !== WELL_KNOWN_PROTECTED_RESOURCE &&
            !pathname.startsWith(`${WELL_KNOWN_PROTECTED_RESOURCE}/`)) {
            return sendJson(res, 404, { error: 'not_found' });
        }
        try {
            const issuer = originFor(req);
            const body = {
                resource: resourceFor(req, issuer),
                authorization_servers: [issuer],
                bearer_methods_supported: ['header']
            };
            if (req.method === 'HEAD') {
                res.writeHead(200, {
                    'Content-Type': 'application/json; charset=utf-8',
                    'Cache-Control': 'no-store',
                    Pragma: 'no-cache'
                });
                return res.end();
            }
            return sendJson(res, 200, body);
        } catch (error) {
            return sendJson(res, 400, { error: 'invalid_request', error_description: error.message });
        }
    };

    const authenticate = async (req, { resource, scope } = {}) => {
        const bearer = req.headers.authorization?.match(/^Bearer\s+(\S+)$/i)?.[1];
        if (!bearer) return null;

        const host = new URL(`https://${req.headers.host || ''}`).hostname;
        const organization = await server.crud.getOrganization({ host });
        if (!organization?._id || organization.error) return null;
        const result = await server.crud.send({
            method: 'object.read',
            array: TOKEN_ARRAY,
            organization_id: organization._id,
            host,
            filter: { query: { type: 'token', token_hash: hash(bearer) }, limit: 1 }
        });
        const errors = Array.isArray(result?.error) ? result.error.length : Boolean(result?.error);
        if (!result || result.success === false || errors) {
            throw Object.assign(new Error('OAuth token lookup failed.'), { status: 503 });
        }

        const token = result.object?.[0];
        const requestedScopes = Array.isArray(scope) ? scope : scope ? [scope] : [];
        if (!token || !token.user_id || token.expires_at <= Date.now() ||
            (resource && token.resource !== resource) ||
            requestedScopes.some(value => !token.scope?.includes(value))) return null;

        return {
            organization_id: organization._id,
            host,
            user_id: token.user_id,
            oauth: true,
            scope: token.scope || []
        };
    };

    const workflowHandler = name => async (req, res) => {
        try {
            const host = new URL(`https://${req.headers.host}`).hostname;
            const organization = await server.crud.getOrganization({ host });
            if (!organization?._id || organization.error) {
                return sendJson(res, 404, { error: 'Organization not found for this host.' });
            }

            const request = await requestFor(req);
            let user_id;
            if (name === 'decision' && request.body.approved === true) {
                const sessionToken = cookieToken(req);
                if (sessionToken && server.authenticate?.decodeToken) {
                    const session = await server.authenticate.decodeToken(
                        sessionToken, organization._id, undefined, host
                    );
                    if (session?.expires > Date.now()) user_id = session.user_id;
                }
            }
            const payload = {
                method: `workflow.oauth.${name}`,
                [`oauth.${name}`]: { request },
                organization_id: organization._id,
                host,
                ...(user_id && { user_id, key_id: user_id })
            };
            const result = await server.workflow.run(payload);
            if (result?.error?.length || result?.success === false) {
                return sendJson(res, 400, { error: 'invalid_request', error_description: result.error });
            }
            return sendWorkflowResponse(res, result);
        } catch (error) {
            return sendJson(res, error.status || 500, {
                error: error.status === 400 ? 'invalid_request' : 'server_error',
                error_description: error.message
            });
        }
    };

    const unknownEndpoint = async (_req, res) => sendJson(res, 404, { error: 'not_found' });

    route(WELL_KNOWN_AUTHORIZATION_SERVER, metadata);
    route(WELL_KNOWN_PROTECTED_RESOURCE, protectedResourceMetadata);
    route(`${WELL_KNOWN_PROTECTED_RESOURCE}*`, protectedResourceMetadata);
    for (const [path, name] of workflowRoutes) route(path, workflowHandler(name));
    route('/oauth/*', unknownEndpoint);

    server.oauth = {
        routes: [...routes.keys()],
        authenticate,
        isReservedPath(path) {
            return path === WELL_KNOWN_AUTHORIZATION_SERVER ||
                path === WELL_KNOWN_PROTECTED_RESOURCE ||
                path.startsWith(`${WELL_KNOWN_PROTECTED_RESOURCE}/`) ||
                path.startsWith('/oauth/');
        },
        handlerFor(path) {
            const exact = routes.get(path);
            if (exact) return exact;
            for (const [routePath, handler] of routes) {
                if (routePath.endsWith('*') && path.startsWith(routePath.slice(0, -1))) return handler;
            }
            return null;
        },
        close() {
            for (const [path, handler] of routes) {
                if (server.routes.get(path) === handler) server.routes.delete(path);
            }
            if (server.oauth === this) delete server.oauth;
        }
    };

    return server.oauth;
}

export default { init };
