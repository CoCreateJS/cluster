import { EventEmitter } from 'node:events';
import { performance } from 'node:perf_hooks';
import IPC from '../src/ipc.js';
import { initializeServerDB } from '@cocreate/db';

const server = Object.assign(new EventEmitter(), {
    id: 'ipc-db-node', isPrimary: false, workerId: Number(process.env.COCREATE_TEST_WORKER_ID),
    cluster: { send() {} }
});
server.emitAsync = async (method, payload) => {
    const replies = await Promise.all(server.rawListeners(method).map(listener => listener(payload)));
    return replies[0] ?? payload;
};
server.send = IPC.init(server, new Map());
const db = initializeServerDB(server, { directory: process.env.COCREATE_TEST_DB_DIRECTORY });
server.on('fixture.crud', async payload => {
    const start = performance.now();
    const result = await db.send(payload.data);
    return { ...payload, result, elapsedMs: performance.now() - start };
});
const registrations = [];
server.on('db.distribution.set', payload => { registrations.push(payload.data); return { ...payload, success: true }; });
server.on('fixture.distribution', payload => ({ ...payload, result: registrations }));
process.send({ response: true, fixtureReady: true, driverKeys: Object.keys(db) });
