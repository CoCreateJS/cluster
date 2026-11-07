import assert from 'node:assert/strict';
import test from 'node:test';
import { Readable } from 'node:stream';
import { createServer } from 'node:http';
import { once } from 'node:events';
import Authorize from '../../authorize/src/index.js';
import { init as initApi } from '../../api/src/server.js';
import { init as initWorkflows } from '../../workflows/src/server.js';
import { init as initManager } from '../src/manager.js';

async function initializeHttp(server) {
    server.api = { ...await initApi(server), ...server.api };
    server.workflow = { ...await initWorkflows(server), ...server.workflow };
    server.manager = await initManager(server);
}

function dispatchHttp(server, kind, req, res) {
    return (kind === 'api' ? server.api : kind === 'manager' ? server.manager : server.workflow).http(req, res);
}

async function request(kind, input = {}, options = {}) {
    const calls = [];
    const req = Readable.from([options.raw ?? JSON.stringify(input)]);
    Object.assign(req, {
        method: options.method || 'POST', url: kind === 'api' ? '/api' : '/workflows/scale',
        headers: { host: 'platform.test', 'content-type': 'application/json',
            ...(options.key !== false && { authorization: 'Bearer cluster-key' }), ...options.headers }
    });
    let status, body;
    const headers = {};
    const res = {
        setHeader(name, value) { headers[name] = value; },
        writeHead(code, values) { status = code; Object.assign(headers, values); },
        end(value) { body = JSON.parse(value); }
    };
    const server = {
        crud: {
            getOrganization: async query => {
                assert.deepEqual(query, { host: 'platform.test' });
                return { _id: 'platform' };
            },
            send: async envelope => {
                calls.push(['crud', envelope]);
                return { object: [envelope.object] };
            }
        },
        authorize: { ...Authorize, check: async envelope => {
            calls.push(['authorize', structuredClone(envelope)]);
            return options.authorize ? options.authorize(envelope) : false;
        } },
        api: { send: async envelope => { calls.push(['api', envelope]); return { data: 'created' }; } },
        workflow: { run: async envelope => { calls.push(['workflow', envelope]); return { scale: [{ accepted: true }] }; } }
    };
    if (options.noAuthorizer) delete server.authorize;
    await initializeHttp(server);
    await dispatchHttp(server, kind, req, res);
    return { status, body, calls, headers };
}

const api = { api: 'cloud', endpoint: 'create-instance', method: 'post', data: { cluster_id: 'cluster' } };

test('component HTTP entrances require affirmative authorization', async () => {
    for (const kind of ['api', 'workflow']) {
        const input = kind === 'api' ? api : { scale: [{}] };
        for (const permission of [false, undefined, {}, { error: 'denied' }, { authorized: false }]) {
            const result = await request(kind, input, { authorize: () => permission });
            assert.equal(result.status, 403);
            assert.equal(result.calls.some(([name]) => ['api', 'workflow'].includes(name)), false);
        }
        assert.equal((await request(kind, input, { noAuthorizer: true })).status, 403);
        assert.equal((await request(kind, input, { key: false })).status, 403);
    }
});

test('manager applies authorized server patches in parent scope without invoking local state save', async () => {
    const patch = { _id: 'server-1', worker2: { status: 'stopped' } };
    const result = await request('manager', {
        method: 'object.update', array: 'servers', object: patch, organization_id: 'tenant', host: 'tenant.test', upsert: true
    }, { authorize: () => true });
    assert.equal(result.status, 200);
    assert.deepEqual(result.calls.find(([name]) => name === 'crud')[1], {
        method: 'object.update', array: 'servers', object: patch, upsert: true, organization_id: 'platform', host: 'platform.test'
    });
    assert.equal((await request('manager', { method: 'object.update', array: 'contacts', object: patch }, { authorize: () => true })).status, 400);
    assert.equal((await request('manager', { method: 'object.update', array: 'servers', object: patch })).status, 403);
});

test('default permissions remain the authorization service responsibility', async () => {
    const result = await request('api', api, { key: false, authorize: () => true });
    assert.equal(result.status, 200);
    assert.equal(result.calls.find(([name]) => name === 'authorize')[1].apikey, undefined);
});

test('authorized API uses host scope, ignores forged runtime identity and omits the caller key', async () => {
    const { status, calls, body } = await request('api', {
        ...api, organization_id: 'victim', host: 'victim.test',
        socket: { organization_id: 'victim' }, user_id: 'admin', url: 'https://unrequested.test'
    }, { authorize: () => true });
    assert.equal(status, 200);
    const envelope = calls.find(([name]) => name === 'api')[1];
    assert.deepEqual(envelope, { ...api, organization_id: 'platform', host: 'platform.test' });
    assert.equal(calls.find(([name]) => name === 'authorize')[1].apikey, 'cluster-key');
    assert.equal(JSON.stringify(body).includes('cluster-key'), false);
});

test('workflow dispatch uses the authorized sanitized envelope', async () => {
    const { status, calls } = await request('workflow', { scale: [{ allowed: true, forbidden: true }], api: 'cloud' }, {
        authorize: envelope => ({ authorized: { ...envelope, scale: [{ allowed: true }] } })
    });
    assert.equal(status, 200);
    assert.deepEqual(calls.find(([name]) => name === 'workflow')[1], {
        method: 'workflow.scale', scale: [{ allowed: true }], organization_id: 'platform', host: 'platform.test'
    });
});

test('invalid requests do not dispatch operations', async () => {
    assert.equal((await request('api', api, { method: 'GET' })).status, 405);
    assert.equal((await request('api', api, { headers: { 'content-type': 'text/plain' } })).status, 415);
    assert.equal((await request('api', {}, { raw: '{' })).status, 400);
    assert.equal((await request('api', [], {})).status, 400);
    assert.equal((await request('api', {}, { raw: 'x'.repeat(1024 * 1024 + 1) })).status, 413);
    assert.equal((await request('api', { ...api, apikey: 'different' })).status, 401);
    assert.equal((await request('api', {})).status, 400);
    assert.equal((await request('workflow', { method: 'workflow.other' })).status, 400);
});

test('an API key can be supplied in the existing JSON envelope', async () => {
    const result = await request('api', { ...api, apikey: 'cluster-key' }, { key: false, authorize: () => true });
    assert.equal(result.status, 200);
    assert.equal(result.calls.find(([name]) => name === 'api')[1].apikey, undefined);
});

test('existing authorization rules grant only the configured workflow method', async t => {
    const { default: crud } = await import('@cocreate/crud-server');
    const { default: authorize } = await import('@cocreate/authorize');
    t.mock.method(crud, 'send', async query => {
        assert.equal(query.array, 'keys');
        return { object: [{ key: 'cluster-key', methods: { 'workflow.scale': true } }] };
    });
    const options = { authorize: envelope => authorize.check(envelope) };
    assert.equal((await request('workflow', { scale: [{}] }, options)).status, 200);
    const denied = await request('api', api, options);
    assert.equal(denied.status, 403);
    assert.equal(denied.calls.some(([name]) => name === 'api'), false);
});

test('real HTTP requests fail without grants and succeed with the configured API key', async t => {
    const { default: crud } = await import('@cocreate/crud-server');
    const { default: authorize } = await import('@cocreate/authorize');
    // In-memory authorization records only; no writes to a running CoCreate database.
    const defaultKey = { default: true, 'methods.signIn': true, 'methods.signUp': true };
    const clusterKey = { key: 'http-test-cluster-key', methods: { post: true, 'workflow.scale': true } };
    t.mock.method(crud, 'send', async query => {
        assert.equal(query.array, 'keys');
        const suppliedKey = query.filter.query.$or.find(item => item.key)?.key;
        return { object: [defaultKey, ...(suppliedKey === clusterKey.key ? [clusterKey] : [])] };
    });
    const executed = [];
    const server = {
        crud: { getOrganization: async () => ({ _id: 'http-test-platform' }), send: () => assert.fail('Execution is stubbed') },
        authorize,
        api: { send: async envelope => { executed.push(envelope); return { data: { success: true } }; } },
        workflow: { run: async envelope => { executed.push(envelope); return { scale: [{ success: true }] }; } }
    };
    await initializeHttp(server);
    const http = createServer((req, res) => dispatchHttp(server, req.url === '/api' ? 'api' : 'workflow', req, res));
    t.after(() => new Promise(resolve => { http.close(resolve); http.closeAllConnections(); }));
    http.listen(0, '127.0.0.1');
    await once(http, 'listening');
    const base = `http://127.0.0.1:${http.address().port}`;
    for (const [path, input] of [['/api', api], ['/workflows/scale', { scale: [{}] }]]) {
        for (const key of [undefined, 'unconfigured-key', clusterKey.key]) {
            const response = await fetch(base + path, {
                method: 'POST', headers: { 'content-type': 'application/json', ...(key && { authorization: `Bearer ${key}` }) },
                body: JSON.stringify(input)
            });
            assert.equal(response.status, key === clusterKey.key ? 200 : 403);
            await response.json();
        }
    }
    assert.equal(executed.length, 2);
    assert.ok(executed.every(envelope => !Object.hasOwn(envelope, 'apikey')));
});

test('manager provider requests authorize once through the API entrance', async () => {
    const { status, calls } = await request('manager', api, { authorize: () => true });
    assert.equal(status, 200);
    assert.equal(calls.filter(([name]) => name === 'authorize').length, 1);
    assert.equal(calls.filter(([name]) => name === 'api').length, 1);
    assert.equal(calls.some(([name]) => name === 'crud'), false);
});

test('each component rejects scope and operation changes made during authorization', async () => {
    for (const [kind, input, field, value] of [
        ['api', api, 'organization_id', 'victim'],
        ['api', api, 'host', 'victim.test'],
        ['api', api, 'api', 'other-provider'],
        ['api', api, 'endpoint', 'delete-instance'],
        ['workflow', { scale: [{}] }, 'method', 'workflow.other'],
        ['manager', { method: 'object.update', array: 'servers', object: { _id: 'server-1' } }, 'array', 'contacts']
    ]) {
        const result = await request(kind, input, { authorize: envelope => {
            envelope[field] = value;
            return { authorized: envelope };
        } });
        assert.equal(result.status, 403, `${kind}:${field}`);
        assert.equal(result.calls.some(([name]) => ['api', 'workflow', 'crud'].includes(name)), false);
    }
});

test('empty authorization errors allow sanitized operations, while malformed grants deny access', async () => {
    const allowed = await request('api', api, { authorize: envelope => ({ authorized: envelope, error: [] }) });
    assert.equal(allowed.status, 200);
    for (const permission of [{ authorized: [] }, { authorized: 'yes' }, { authorized: {}, error: ['denied'] }]) {
        assert.equal((await request('api', api, { authorize: () => permission })).status, 403);
    }
});

test('workflow and manager HTTP entrances retain request validation and body limits', async () => {
    for (const [kind, input] of [
        ['workflow', { scale: [{}] }],
        ['manager', { method: 'object.update', array: 'servers', object: { _id: 'server-1' } }]
    ]) {
        for (const [options, expected] of [
            [{ method: 'GET' }, 405],
            [{ headers: { 'content-type': 'text/plain' } }, 415],
            [{ raw: '{' }, 400],
            [{ raw: 'x'.repeat(1024 * 1024 + 1) }, 413]
        ]) assert.equal((await request(kind, input, options)).status, expected);
        assert.equal((await request(kind, { ...input, apikey: 'other-key' })).status, 401);
    }
});

async function invokeHttp(handler, input, url = '/api') {
    const req = Readable.from([JSON.stringify(input)]);
    Object.assign(req, { method: 'POST', url, headers: {
        host: 'platform.test', 'content-type': 'application/json', authorization: 'Bearer transport-key'
    } });
    const response = {};
    await handler(req, {
        setHeader() {},
        writeHead(status) { response.status = status; },
        end(body) { response.body = JSON.parse(body); }
    });
    return response;
}

test('API HTTP entrance executes the real API pipeline with sanitized input', async () => {
    const order = [];
    const server = {
        organization_id: 'platform',
        send: async envelope => envelope,
        authorize: { ...Authorize, check: async envelope => {
            order.push('authorize');
            assert.equal(envelope.apikey, 'transport-key');
            return { authorized: { ...envelope, data: { allowed: true } } };
        } },
        crud: {
            getOrganization: async () => ({ _id: 'platform' }),
            send: async request => {
                order.push(request.array);
                assert.equal(request.organization_id, 'platform');
                return { object: request.array === 'apis'
                    ? [{ _id: 'cloud-id', name: 'cloud', resolved: true, url: 'https://provider.test' }] : [] };
            }
        }
    };
    server.api = await initApi(server);
    const result = await invokeHttp(server.api.http, { ...api, data: { allowed: true, forbidden: true } });
    assert.equal(result.status, 200);
    assert.deepEqual(result.body.data, { allowed: true });
    assert.equal(result.body.apikey, undefined);
    assert.deepEqual(order, ['authorize', 'apis', 'endpoints']);
});

test('workflow HTTP entrance executes the real workflow AST after authorizing', async () => {
    const order = [];
    const server = {
        send: async envelope => envelope,
        authorize: { ...Authorize, check: async envelope => {
            order.push('authorize');
            assert.equal(envelope.apikey, 'transport-key');
            return { authorized: { ...envelope, scale: [{ allowed: true }] } };
        } },
        crud: {
            getOrganization: async () => ({ _id: 'platform' }),
            send: async request => {
                order.push('workflow');
                assert.equal(request.organization_id, 'platform');
                assert.deepEqual(request.filter.query, { name: 'scale' });
                return { object: [{ name: 'scale', enabled: true, actions: {
                    '$data.accepted': '$payload.scale.0.allowed'
                } }] };
            }
        }
    };
    server.workflow = await initWorkflows(server);
    const result = await invokeHttp(server.workflow.http, { scale: [{ allowed: true, forbidden: true }] }, '/workflows/scale');
    assert.equal(result.status, 200);
    assert.equal(result.body.accepted, true);
    assert.deepEqual(result.body.scale, [{ allowed: true }]);
    assert.equal(result.body.apikey, undefined);
    assert.deepEqual(order, ['authorize', 'workflow']);
});
