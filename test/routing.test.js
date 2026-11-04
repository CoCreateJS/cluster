import assert from 'node:assert/strict';
import test from 'node:test';
import config from '@cocreate/config';
import Routing from '../src/routing.js';

async function setup(t, { system, database, isPrimary = true, run, send, noRemoval } = {}) {
    const requests = [];
    const calls = [];
    const queries = [];
    t.mock.method(config, 'get', async key => {
        assert.equal(key, 'routing');
        return system;
    });
    t.mock.method(config, 'query', async (scope, crud) => {
        queries.push({ scope, crud });
        return database;
    });
    const server = {
        isPrimary, id: 'server-1', cluster_id: 'cluster-1',
        organization_id: 'platform', host: 'platform.test', ip: '203.0.113.1',
        infrastructure: { provider: 'aws', region: 'us-east-1' },
        location: { latitude: 1, longitude: 2 },
        crud: { send() { assert.fail('The node routing handler must not perform direct CRUD'); } },
        workflow: { run: async envelope => {
            requests.push(envelope);
            const name = envelope.method.slice('workflow.'.length);
            if (!noRemoval) envelope[name][0].remove = {
                api: 'selected-provider', endpoint: 'remove-routing',
                data: { server_id: server.id, cluster_id: server.cluster_id, ip: server.ip,
                    registration: { zoneId: 123, recordId: 456 } }
            };
            return run ? run(envelope) : envelope;
        } },
        api: { send: async request => {
            calls.push(request);
            return send ? send(request) : { success: true };
        } }
    };
    return { routing: await Routing.init(server), server, requests, calls, queries };
}

test('the primary initializes routing once and removes it through the prepared API request', async t => {
    const { routing, server, requests, calls, queries } = await setup(t);
    assert.equal(requests.length, 0);
    await routing.add();
    const prepared = structuredClone(requests[0]['configure-cluster-routing'][0].remove);
    server.ip = '192.0.2.99';
    await routing.remove('SIGTERM');
    assert.deepEqual(requests.map(request => request.method), ['workflow.configure-cluster-routing']);
    assert.equal(requests[0].organization_id, 'platform');
    assert.equal(requests[0]['configure-cluster-routing'][0].server_id, 'server-1');
    assert.deepEqual(calls, [{ ...prepared, organization_id: 'platform', host: 'platform.test',
        data: { ...prepared.data, reason: 'SIGTERM' } }]);
    assert.deepEqual(queries[0].scope, {
        type: 'routing', organization_id: 'platform', server: 'platform.test', cluster: 'cluster-1'
    });
    assert.equal(queries[0].crud, server.crud);
});

test('the scoped configuration can override the initialization workflow name', async t => {
    const { routing, requests, calls } = await setup(t, {
        system: { workflow: 'initialName' }, database: { workflow: 'prepareNodeRouting' }
    });
    await routing.add();
    await routing.remove();
    assert.equal(requests[0].method, 'workflow.prepareNodeRouting');
    assert.equal(requests[0].prepareNodeRouting[0].server_id, 'server-1');
    assert.equal(calls.length, 1);
});

test('workers do not load routing configuration or register independently', async t => {
    const { routing, requests, calls, queries } = await setup(t, { isPrimary: false });
    assert.equal(routing, undefined);
    assert.deepEqual(requests, []);
    assert.deepEqual(calls, []);
    assert.deepEqual(queries, []);
    assert.equal(config.get.mock.callCount(), 0);
});

test('routing can be disabled through configuration', async t => {
    const { routing, requests, calls } = await setup(t, { system: { enabled: false } });
    await routing.add();
    await routing.remove();
    assert.deepEqual(requests, []);
    assert.deepEqual(calls, []);
});

test('repeated readiness and shutdown calls invoke initialization and removal once', async t => {
    const { routing, requests, calls } = await setup(t);
    await Promise.all([routing.add(), routing.add()]);
    await Promise.all([routing.remove('SIGINT'), routing.remove('SIGTERM')]);
    await routing.add();
    assert.equal(requests.length, 1);
    assert.equal(calls.length, 1);
});

test('shutdown waits for initialization to return its prepared deletion request', async t => {
    let finish;
    const { routing, calls } = await setup(t, {
        run: envelope => new Promise(resolve => { finish = () => resolve(envelope); })
    });
    const adding = routing.add();
    const removing = routing.remove('SIGTERM');
    assert.equal(calls.length, 0);
    finish();
    await Promise.all([adding, removing]);
    assert.equal(calls[0].endpoint, 'remove-routing');
});

test('a failed registration retains prepared cleanup and does not rerun a workflow at shutdown', async t => {
    const { routing, requests, calls } = await setup(t, {
        run: envelope => ({ ...envelope, error: ['Provider unavailable'] })
    });
    await assert.rejects(routing.add(), /Provider unavailable/);
    await routing.remove('SIGTERM');
    assert.equal(requests.length, 1);
    assert.equal(calls.length, 1);
});

test('shutdown with no prepared cleanup performs no new lookup or workflow', async t => {
    const { routing, requests, calls } = await setup(t);
    await routing.remove('SIGTERM');
    assert.equal(requests.length, 0);
    assert.equal(calls.length, 0);
});

test('a custom initializer without a removal request does not invent one', async t => {
    const { routing, requests, calls } = await setup(t, { noRemoval: true });
    await routing.add();
    await routing.remove();
    assert.equal(requests.length, 1);
    assert.equal(calls.length, 0);
});

test('a rejected deletion API request is surfaced to the coordinated shutdown handler', async t => {
    const { routing } = await setup(t, { send: () => ({ success: false }) });
    await routing.add();
    await assert.rejects(routing.remove(), /routing API rejected/);
});
