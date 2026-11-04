import assert from 'node:assert/strict';
import test from 'node:test';
import config from '@cocreate/config';

let moduleId = 0;
async function setup(t) {
    const order = [];
    const events = new Map();
    const signals = ['SIGINT', 'SIGTERM', 'SIGBREAK'];
    const previous = new Map(signals.map(signal => [signal, process.listeners(signal)]));
    t.after(() => {
        for (const signal of signals)
            for (const listener of process.listeners(signal))
                if (!previous.get(signal).includes(listener)) process.removeListener(signal, listener);
    });
    t.mock.method(config, 'get', () => undefined);
    t.mock.method(config, 'query', async () => undefined);
    t.mock.method(process, 'exit', code => order.push(`exit:${code}`));
    const stateModule = await import(`../src/state.js?worker-test=${++moduleId}`);
    const server = {
        isPrimary: false, workerId: 1, organization_id: 'platform',
        on: (name, listener) => events.set(name, listener),
        closeEdgeListeners: () => order.push('cordon'),
        wsManager: { destroyAll: () => order.push('drain') },
        send: () => {},
        crud: { send: async request => { order.push('save'); return { object: [request.object] }; } },
        repository: { stop: () => { throw new Error('workers must not checkpoint repositories'); } },
        shutdownHandlers: new Set()
    };
    server.state = await stateModule.init(server);
    return { order, events, server, ...stateModule };
}

test('worker shutdown starts services before drain and waits only for bounded cleanup', async t => {
    const { order, server, shutdownWorker } = await setup(t);
    let finish;
    server.shutdownHandlers.add(() => {
        order.push('checkpoint');
        return new Promise(resolve => { finish = resolve; });
    });
    const pending = shutdownWorker('SIGTERM');
    await new Promise(resolve => setImmediate(resolve));
    assert.ok(order.indexOf('checkpoint') < order.indexOf('cordon'));
    assert.ok(order.includes('drain'));
    assert.ok(!order.includes('exit:0'));
    assert.equal(shutdownWorker('SIGINT'), pending);
    finish();
    await pending;
    assert.equal(order.at(-1), 'exit:0');
});

test('master message, direct worker signals, and worker restart each invoke local services', async t => {
    for (const trigger of ['master', 'SIGTERM', 'SIGINT', 'restart']) {
        await t.test(trigger, async t => {
            const { server, events, order } = await setup(t);
            const reasons = [];
            server.shutdownHandlers.add(reason => reasons.push(reason));
            if (trigger === 'master') await events.get('shutdown')();
            else if (trigger === 'restart') await events.get('restart')({});
            else {
                const listener = process.listeners(trigger).at(-1);
                await listener();
            }
            assert.equal(reasons.length, 1);
            assert.equal(order.at(-1), 'exit:0');
        });
    }
});

test('worker hook failure or timeout does not prevent shutdown', async t => {
    const { server, shutdownWorker, order } = await setup(t);
    t.mock.method(console, 'error', () => {});
    t.mock.timers.enable({ apis: ['setTimeout'] });
    let signal;
    server.shutdownHandlers.add(() => { throw new Error('failed push'); });
    server.shutdownHandlers.add((reason, controllerSignal) => {
        signal = controllerSignal;
        return new Promise(() => {});
    });
    const pending = shutdownWorker('SIGTERM');
    t.mock.timers.tick(10000);
    await pending;
    assert.ok(signal.aborted);
    assert.ok(order.includes('drain'));
    assert.equal(order.at(-1), 'exit:0');
});
