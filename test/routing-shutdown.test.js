import assert from 'node:assert/strict';
import test from 'node:test';
import Http from 'node:http';
import config from '@cocreate/config';

let moduleId = 0;

async function setup(t, remove) {
    const order = [];
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const listeners = new Map(['SIGINT', 'SIGTERM', 'SIGBREAK'].map(signal => [signal, process.listeners(signal)]));
    t.after(() => {
        for (const [signal, previous] of listeners) {
            for (const listener of process.listeners(signal)) {
                if (!previous.includes(listener)) process.removeListener(signal, listener);
            }
        }
    });
    t.mock.method(config, 'get', async () => undefined);
    t.mock.method(config, 'query', async () => undefined);
    t.mock.method(Http, 'createServer', () => ({ listen() {}, close() {} }));
    const { default: State, triggerGlobalShutdown } = await import(`../src/state.js?test=${++moduleId}`);
    const server = {
        id: 'server-1', isPrimary: true, totalWorkers: 1, organization_id: 'platform', host: 'platform.test',
        on() {}, workers: new Map(),
        routing: { remove: async reason => {
            order.push(`remove:${reason}`);
            await remove?.();
            order.push('removed');
        } },
        crud: { send: async request => { if (request.method === 'object.read') return { object: [] }; order.push('persist'); return { object: [request.object] }; } },
        send: () => order.push('shutdown-workers')
    };
    server.state = await State.init(server);
    return { order, server, triggerGlobalShutdown };
}

test('coordinated shutdown awaits routing removal before lifecycle persistence and worker shutdown', async t => {
    let finish;
    const { order, triggerGlobalShutdown } = await setup(t, () => new Promise(resolve => { finish = resolve; }));
    const shutdown = triggerGlobalShutdown('SIGTERM', () => order.push('exit'));
    assert.deepEqual(order, ['remove:SIGTERM']);
    finish();
    await shutdown;
    t.mock.timers.tick(1000);
    assert.deepEqual(order.slice(0, 3), ['remove:SIGTERM', 'removed', 'persist']);
    assert.ok(order.indexOf('shutdown-workers') > order.indexOf('removed'));
    assert.equal(order.at(-1), 'exit');
    await triggerGlobalShutdown('SIGINT');
    assert.equal(order.filter(event => event.startsWith('remove:')).length, 1);
});

for (const status of ['failed', 'terminating']) {
    test(`ordinary state saves leave ${status} untouched`, async t => {
        const { server } = await setup(t);
        const methods = [];
        server.crud.send = async request => {
            methods.push(request.method);
            return { object: [{ master: { status } }] };
        };
        await server.state.save();
        await server.state.save('stopped');
        assert.deepEqual(methods, ['object.read', 'object.read']);
    });
    test(`shutdown completes without saving over ${status}`, async t => {
        const { server, order, triggerGlobalShutdown } = await setup(t);
        const requests = [];
        server.crud.send = async request => {
            requests.push(request);
            return { object: [{ _id: server.id, master: { status } }] };
        };
        await triggerGlobalShutdown('OS_SHUTDOWN', () => order.push('poweroff'));
        t.mock.timers.tick(1000);
        assert.equal(order.at(-1), 'poweroff');
        assert.ok(order.includes('shutdown-workers'));
        assert.equal(requests.length, 2);
        assert.ok(requests.every(request => request.method === 'object.read'));
        assert.equal(requests[0].filter.query._id, server.id);
    });
}

test('a failed status read prevents an unchecked lifecycle write', async t => {
    const { server } = await setup(t);
    const methods = [];
    server.crud.send = async request => {
        methods.push(request.method);
        return { error: ['Database unavailable.'] };
    };
    await assert.rejects(server.state.save('stopped'), /Unable to read server status/);
    assert.deepEqual(methods, ['object.read']);
});

test('a failed routing workflow is reported and does not prevent existing shutdown cleanup', async t => {
    const errors = [];
    t.mock.method(console, 'error', (...args) => errors.push(args.join(' ')));
    const { order, triggerGlobalShutdown } = await setup(t, () => { throw new Error('Bunny unavailable'); });
    await triggerGlobalShutdown('SIGINT', () => order.push('exit'));
    t.mock.timers.tick(1000);
    assert.match(errors.join('\n'), /Failed to withdraw node routing.*Bunny unavailable/);
    assert.ok(order.includes('persist'));
    assert.equal(order.at(-1), 'exit');
});

test('master shutdown does not run worker service handlers', async t => {
    const { server, triggerGlobalShutdown } = await setup(t);
    server.totalWorkers = 2;
    let called = false;
    server.shutdownHandlers = new Set([() => { called = true; }]);
    await triggerGlobalShutdown('SIGTERM', () => {});
    assert.equal(called, false);
});

test('master checkpoints once after all workers exit and finalization is shared', async t => {
    const { server, triggerGlobalShutdown, order } = await setup(t);
    server.totalWorkers = 2;
    server.workers.set(1, {});
    let finish;
    server.repository = { stop: async (reason, options) => {
        assert.equal(reason, 'SIGTERM');
        assert.ok(options.timeoutMs > 0);
        order.push('checkpoint');
        await new Promise(resolve => { finish = resolve; });
        order.push('pushed');
    } };
    await triggerGlobalShutdown('SIGTERM', () => {});
    assert.ok(order.includes('shutdown-workers'));
    assert.ok(!order.includes('checkpoint'));
    server.workers.clear();
    const first = server.state.finalizeShutdown();
    assert.equal(first, server.state.finalizeShutdown());
    finish();
    await first;
    assert.equal(order.filter(value => value === 'checkpoint').length, 1);
    assert.ok(order.indexOf('checkpoint') > order.indexOf('shutdown-workers'));
});

test('single-process shutdown stops local consumers before the checkpoint', async t => {
    const { server, triggerGlobalShutdown, order } = await setup(t);
    server.shutdownHandlers = new Set([() => order.push('stop-ai')]);
    server.repository = { stop: async () => { order.push('checkpoint'); } };
    await triggerGlobalShutdown('SIGTERM', () => {});
    assert.ok(order.indexOf('checkpoint') > order.indexOf('stop-ai'));
});

test('failed master checkpoint still completes shutdown', async t => {
    const { server, triggerGlobalShutdown, order } = await setup(t);
    t.mock.method(console, 'error', () => {});
    server.repository = { stop: async () => { throw new Error('remote unavailable'); } };
    await triggerGlobalShutdown('SIGTERM', () => order.push('exit'));
    t.mock.timers.tick(1000);
    assert.equal(order.at(-1), 'exit');
});
