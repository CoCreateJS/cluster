import assert from 'node:assert/strict';
import test from 'node:test';
import { EventEmitter } from 'node:events';
import cluster from 'node:cluster';
import os from 'node:os';
import Workers from '../src/workers.js';

function setup(t, count = 2) {
    const original = new Set(cluster.listeners('exit'));
    t.after(() => {
        for (const listener of cluster.listeners('exit')) if (!original.has(listener)) cluster.removeListener('exit', listener);
    });
    t.mock.method(console, 'log', () => {});
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const forks = [], calls = [];
    t.mock.method(cluster, 'fork', env => {
        const worker = new EventEmitter();
        worker.process = { pid: 1000 + forks.length };
        worker.send = (message, callback) => { calls.push(['send', message]); callback?.(); };
        worker.kill = signal => calls.push(['kill', signal]);
        forks.push({ env, worker });
        return worker;
    });
    const server = {
        id: 'node-1', ip: '192.0.2.1', infrastructure: { region: 'example' }, location: { country: 'US' },
        manager: { organization_id: 'parent', host: 'parent.test', apiKey: 'test-only' },
        state: {
            evaluateMasterFallback: () => calls.push('fallback'),
            save: async status => calls.push(status), getServerObject: object => object
        },
        crud: { send: async request => calls.push(request) },
        routing: { add: async () => calls.push('routing') }, repository: { publish: async () => calls.push('repository') }
    };
    const workers = new Workers(server);
    server.workers = workers;
    workers.configureWorkers(count);
    const exit = async (worker, code = 0) => cluster.listeners('exit').at(-1)(worker, code, null);
    return { workers, server, forks, calls, exit };
}
const flush = () => new Promise(resolve => setImmediate(resolve));

test('add returns the ID-keyed tracked record and Map reads retain their interface', t => {
    const { workers, forks } = setup(t);
    const record = workers.add();
    assert(workers instanceof Map);
    assert.equal(workers.workers, workers);
    assert.equal(record.id, 1);
    assert.equal(record.process, forks[0].worker);
    assert.equal(workers.get(record.id), record);
    assert.deepEqual([...workers], [[record.id, record]]);
    assert.deepEqual([...workers.values()], [record]);
    assert.equal(workers.size, 1);
    assert.equal(Object.hasOwn(workers, 'add'), false);
    assert.equal(workers.fork, undefined);
});

test('startup configuration stays private and does not pretend planned workers already exist', async t => {
    const { workers, server, forks } = setup(t);
    t.mock.method(os, 'cpus', () => Array.from({ length: 4 }, () => ({})));
    workers.configureWorkers(-1);
    assert.equal(workers.size, 0);
    assert.equal('totalWorkers' in workers, false);
    assert.equal('totalWorkers' in server, false);
    assert.equal(Object.keys(workers.initialState()).filter(key => /^worker\d+$/.test(key)).length, 3);
    await workers.setupMasterCluster();
    assert.equal(workers.size, 3);
    assert.equal(forks.length, 3);
});

test('single-process identity stays separate from live child count', t => {
    const { workers, server } = setup(t, 1);
    assert.equal(workers.workerId, 1);
    assert.equal(server.workerId, 1);
    assert.equal(server.isWorker, true);
    assert.equal(workers.size, 0);
    const child = workers.add();
    assert.equal(child.id, 2, 'the primary worker ID is not assigned to a new child');
});

test('add passes custom environment options while preserving runtime bootstrap identity', t => {
    const { workers, server, forks } = setup(t);
    const record = workers.add({ FEATURE: 'enabled', WORKER_ID: 'override', SERVER_ID: 'override' });
    assert.equal(record.id, 1);
    assert.deepEqual(forks[0].env, {
        FEATURE: 'enabled', WORKER_ID: '1', SERVER_ID: server.id, SERVER_IP: server.ip,
        SERVER_INFRASTRUCTURE: JSON.stringify(server.infrastructure), SERVER_LOCATION: JSON.stringify(server.location),
        SERVER_MANAGER: JSON.stringify(server.manager)
    });
});

test('set updates metadata in place without modifying process identity or readiness', t => {
    const { workers } = setup(t);
    const record = workers.add();
    const process = record.process, pid = record.pid;
    assert.equal(workers.set(record.id, { label: 'updated', region: 'example', id: 99, pid: -1, process: {}, isListening: true }), undefined);
    assert.equal(workers.get(record.id), record);
    assert.equal(record.label, 'updated');
    assert.equal(record.region, 'example');
    assert.equal(record.id, 1);
    assert.equal(record.pid, pid);
    assert.equal(record.process, process);
    assert.equal(record.isListening, false);
    assert.throws(() => workers.set(99, { label: 'missing' }), /not tracked/);
    assert.equal(workers.size, 1);
});

test('delete requests graceful shutdown once, keeps tracking until exit, and suppresses respawn', async t => {
    const { workers, forks, calls, exit } = setup(t);
    const record = workers.add();
    assert.equal(workers.delete(record.id), undefined);
    workers.delete(record.id);
    assert.equal(workers.get(record.id), record);
    assert.equal(workers.size, 1);
    assert.deepEqual(calls.filter(Array.isArray), [['send', { method: 'shutdown', master: false, worker: record.id, server: 'node-1' }]]);
    await exit(record.process);
    assert.equal(workers.size, 0);
    t.mock.timers.tick(2000);
    assert.equal(forks.length, 1);
});

test('delete falls back to a graceful signal when IPC is unavailable', async t => {
    const { workers, calls, exit } = setup(t);
    const record = workers.add();
    record.process.send = () => { throw new Error('channel closed'); };
    workers.delete(record.id);
    assert(calls.some(item => Array.isArray(item) && item[0] === 'kill' && item[1] === 'SIGTERM'));
    assert.equal(workers.size, 1);
    await exit(record.process);
    assert.equal(workers.size, 0);
});

test('clear uses lifecycle deletion rather than dropping live process references', async t => {
    const { workers, forks, exit } = setup(t);
    workers.add(); workers.add();
    assert.equal(workers.clear(), undefined);
    assert.equal(workers.size, 2);
    for (const { worker } of forks) await exit(worker);
    assert.equal(workers.size, 0);
    t.mock.timers.tick(2000);
    assert.equal(forks.length, 2);
});

test('unexpected exits respawn with the same ID, environment, and readiness handling', async t => {
    const { workers, forks, calls, exit } = setup(t);
    const first = workers.add({ FEATURE: 'enabled' });
    workers.add();
    forks[1].worker.emit('message', { type: 'listening_state', state: true });
    first.process.emit('message', { type: 'listening_state', state: true });
    await flush();
    assert(calls.includes('repository'));
    await exit(first.process, 1);
    assert.equal(workers.size, 1);
    t.mock.timers.tick(1999);
    assert.equal(forks.length, 2);
    t.mock.timers.tick(1);
    assert.equal(forks.length, 3);
    assert.equal(forks[2].env.WORKER_ID, String(first.id));
    assert.equal(forks[2].env.FEATURE, 'enabled');
    forks[2].worker.emit('message', { type: 'listening_state', state: true });
    await flush();
    assert.equal(calls.filter(item => item === 'repository').length, 2);
});

test('delete cancels a pending replacement after an unexpected exit', async t => {
    const { workers, forks, exit } = setup(t);
    const record = workers.add();
    await exit(record.process, 1);
    workers.delete(record.id);
    t.mock.timers.tick(2000);
    assert.equal(forks.length, 1);
    assert.equal(workers.size, 0);
    assert.equal(workers.add().id, record.id);
});

test('draining removes workers without respawning and completes after the last exit', async t => {
    const { workers, server, forks, exit } = setup(t);
    await workers.setupMasterCluster();
    server.state.isDraining = true;
    let completed = 0;
    server.state.shutdownCompletionCallback = () => completed++;
    await exit(forks[0].worker);
    assert.equal(completed, 0);
    await exit(forks[1].worker);
    assert.equal(completed, 1);
    t.mock.timers.tick(2000);
    assert.equal(workers.size, 0);
    assert.equal(forks.length, 2);
});

test('worker processes cannot create children and unrelated exits do not spawn replacements', async t => {
    const { workers, forks, exit } = setup(t);
    workers.add();
    await exit({ process: { pid: 99999 } }, 1);
    t.mock.timers.tick(2000);
    assert.equal(forks.length, 1);
    workers.isPrimary = false;
    assert.equal(await workers.setupMasterCluster(), false);
    assert.throws(() => workers.add(), /Only the primary/);
});

test('CoCreateServer exposes the managed Map without a runtime totalWorkers property', async () => {
    const { CoCreateServer, ClusterManager } = await import('../src/index.js');
    const server = new CoCreateServer();
    assert(server.workers instanceof Workers);
    assert.notEqual(server.cluster, server.workers);
    assert.equal(typeof server.cluster.send, 'function');
    assert.equal(ClusterManager, Workers);
    assert.equal('totalWorkers' in server, false);
});

test('delete during exit persistence also suppresses the pending crash replacement', async t => {
    const { workers, server, forks, exit } = setup(t);
    const record = workers.add();
    let release;
    server.crud.send = () => new Promise(resolve => { release = resolve; });
    const leaving = exit(record.process, 1);
    assert.equal(workers.size, 0);
    workers.delete(record.id);
    release({});
    await leaving;
    t.mock.timers.tick(2000);
    assert.equal(forks.length, 1);
    assert.equal(server.isWorker, false, 'live count does not change the process role');
});


test('a readiness message arriving after delete cannot republish the stopping worker', async t => {
    const { workers, calls } = setup(t);
    const record = workers.add();
    workers.delete(record.id);
    record.process.emit('message', { type: 'listening_state', state: true });
    await flush();
    assert.equal(record.isListening, false);
    assert.equal(calls.includes('running'), false);
    assert.equal(calls.includes('repository'), false);
});


test('startup profile clears obsolete persisted worker fields using configuration before any child exists', t => {
    const { workers, forks } = setup(t, 3);
    const profile = workers.initialState({
        worker1: { pid: 1 }, worker3: { pid: 3 }, worker4: { pid: 4 }, worker12: { pid: 12 },
        workerId: 99, totalWorkers: 99, manager: { host: 'parent.test' }
    });
    assert.equal(workers.size, 0);
    assert.equal(forks.length, 0);
    assert.equal(profile.totalWorkers, 0);
    assert.equal(profile.worker1.status, 'stopped');
    assert.equal(profile.worker2.status, 'stopped');
    assert.equal(profile.worker3.status, 'stopped');
    assert.equal(profile['$unset.worker4'], true);
    assert.equal(profile['$unset.worker12'], true);
    assert.equal(profile['$unset.worker1'], undefined);
    assert.equal(profile['$unset.worker3'], undefined);
    assert.equal(profile['$unset.workerId'], undefined);
    assert.equal(profile['$unset.manager'], undefined);
});

test('single-process startup retains worker1 profile and clears old child slots', t => {
    const { workers } = setup(t, 1);
    const profile = workers.initialState({ worker1: {}, worker2: {} });
    assert.equal(profile.worker1.status, 'stopped');
    assert.equal(profile['$unset.worker1'], undefined);
    assert.equal(profile['$unset.worker2'], true);
    assert.doesNotThrow(() => workers.initialState(null));
});

test('awaited event dispatch returns the envelope and respects once listeners', async () => {
    const { CoCreateServer } = await import('../src/index.js');
    const server = new CoCreateServer();
    let calls = 0;
    server.once('example.request', async payload => {
        await new Promise(resolve => setImmediate(resolve));
        calls++;
        return { ...payload, result: 'done' };
    });
    const payload = { method: 'example.request', uuid: 'event-uuid' };
    assert.equal((await server.emitAsync(payload.method, payload)).result, 'done');
    assert.equal(await server.emitAsync(payload.method, payload), payload);
    assert.equal(calls, 1);
});
