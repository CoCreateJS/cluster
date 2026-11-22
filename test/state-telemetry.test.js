import assert from 'node:assert/strict';
import test from 'node:test';
import { EventEmitter } from 'node:events';
import Config from '@cocreate/config';

let moduleId = 0;

test('worker State applies configured proxy and cordon thresholds from local telemetry events', async t => {
    const processListeners = new Map(['SIGINT', 'SIGTERM', 'SIGBREAK'].map(signal => [signal, process.listeners(signal)]));
    t.after(() => {
        for (const [signal, previous] of processListeners) {
            for (const listener of process.listeners(signal)) {
                if (!previous.includes(listener)) process.removeListener(signal, listener);
            }
        }
    });

    t.mock.method(Config, 'get', async () => undefined);
    let queryOptions;
    const crud = { send: async () => ({}) };
    t.mock.method(Config, 'query', async (options, queryCrud) => {
        queryOptions = options;
        assert.equal(queryCrud, crud);
        return {
            enabled: true,
            proxy: { memory: { on: 75, off: 60 } },
            cordon: { memory: { on: 90, off: 85 } }
        };
    });

    const { default: State } = await import(`../src/state.js?telemetry-test=${++moduleId}`);
    const server = new EventEmitter();
    Object.assign(server, {
        isPrimary: false,
        isWorker: true,
        workerId: 1,
        id: 'server-1',
        organization_id: 'org-1',
        host: 'server.example',
        cluster_id: 'cluster-1',
        crud,
        send: () => {},
        closeEdgeListeners: () => {},
        restoreEdgeListeners: () => {},
        workers: new Map()
    });
    server.state = await State.init(server);

    assert.equal(queryOptions.type, 'state');
    assert.equal(queryOptions.worker, 1);
    assert.equal(server.state.config.proxy.memory.on, 75);

    server.emit('telemetry', { data: {
        memory: { min: 74, max: 76 }
    } });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(server.state.isProxying, true);
    assert.equal(server.state.isCordon, false);

    server.emit('telemetry', { data: {
        memory: { min: 88, max: 91 }
    } });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(server.state.isProxying, true);
    assert.equal(server.state.isCordon, true);

    server.emit('telemetry', { data: {
        memory: { min: 59, max: 84 }
    } });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(server.state.isProxying, false);
    assert.equal(server.state.isCordon, false);
});
