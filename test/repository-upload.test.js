import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { publishRepository } from '../src/repository/upload.js';

function fixture({ failCompletion = false, failLocal = false } = {}) {
    const url = 'git@github.com:owner/app.git';
    const template = { url, initialUpload: { enabled: true, completed: false }, checkout: { workspace: '/fixture/app' } };
    const cluster = { repositories: [structuredClone(template), { url: 'other', initialUpload: { enabled: false, completed: false } }] };
    const reservations = new Map(), requests = [], mirrored = [];
    const server = {
        id: 'node-1', cluster_id: 'cluster-1', organization_id: 'tenant', host: 'tenant.test',
        manager: { organization_id: 'parent', send: async request => { mirrored.push(structuredClone(request)); } },
        crud: { send: async request => {
            requests.push(structuredClone(request));
            assert.equal(request.organization_id, 'tenant');
            if (request.array === 'clusters') {
                if (request.method === 'object.read') return { object: [structuredClone(cluster)] };
                if (failCompletion) return { error: ['failed'] };
                for (const [field, value] of Object.entries(request.object)) {
                    if (field === '_id') continue;
                    const parts = field.split('.');
                    let target = cluster;
                    for (const part of parts.slice(0, -1)) target = target[part];
                    target[parts.at(-1)] = value;
                }
                return { object: [structuredClone(cluster)] };
            }
            assert.equal(request.array, 'repository_upload_reservations');
            if (request.method === 'object.create') {
                if (failLocal) return { error: ['offline'] };
                if (reservations.has(request.object._id)) return { error: ['duplicate'] };
                reservations.set(request.object._id, structuredClone(request.object));
                return { object: [request.object] };
            }
            if (request.method === 'object.read') return { object: [...reservations.values()] };
            reservations.delete(request.object[0]._id); return { object: [] };
        } }
    };
    return { server, entry: structuredClone(template), cluster, reservations, requests, mirrored };
}

test('publication marks only the URL entry after successful upload and mirrors completion', async () => {
    const f = fixture(); let uploads = 0;
    const result = await publishRepository(f.server, f.entry, { runUpload: async workspace => {
        assert.equal(workspace, '/fixture/app');
        assert.equal(f.cluster.repositories[0].initialUpload.completed, false);
        uploads++;
        f.cluster.repositories.reverse(); // Completion must locate the URL again, not use a stale index.
    } });
    assert.equal(result.uploaded, true);
    assert.equal(uploads, 1);
    assert.equal(f.cluster.repositories.find(item => item.url === f.entry.url).initialUpload.completed, true);
    assert.equal(f.cluster.repositories.find(item => item.url === 'other').initialUpload.completed, false);
    assert.equal(f.reservations.size, 0);
    assert.equal(f.mirrored.length, 1);
});

test('different nodes cannot publish concurrently or repeat a completed upload', async () => {
    const f = fixture(); let finish, started;
    const running = new Promise(resolve => { started = resolve; });
    let uploads = 0;
    const first = publishRepository(f.server, f.entry, { runUpload: async () => {
        uploads++; started(); await new Promise(resolve => { finish = resolve; });
    } });
    await running;
    const anotherEntry = structuredClone(f.entry);
    const second = await publishRepository({ ...f.server, id: 'node-2' }, anotherEntry, { runUpload: () => assert.fail('Concurrent upload') });
    assert.equal(second.reason, 'reserved');
    finish(); await first;
    assert.equal((await publishRepository(f.server, anotherEntry, { runUpload: () => assert.fail('Repeat upload') })).skipped, true);
    assert.equal(uploads, 1);
});

test('upload failure leaves completion pending and releases the claim for a later startup', async () => {
    const f = fixture();
    await assert.rejects(publishRepository(f.server, f.entry, { runUpload: async () => { throw new Error('CLI failed'); } }), /remains pending/);
    assert.equal(f.cluster.repositories[0].initialUpload.completed, false);
    assert.equal(f.reservations.size, 0);
    assert.equal((await publishRepository(f.server, f.entry, { runUpload: async () => {} })).uploaded, true);
});

test('successful upload with failed completion persistence retains the reservation for review', async () => {
    const f = fixture({ failCompletion: true });
    await assert.rejects(publishRepository(f.server, f.entry, { runUpload: async () => {} }), /requires review/);
    assert.equal(f.entry.initialUpload.completed, false);
    assert.equal(f.reservations.size, 1);
});

test('disabled, completed or unprepared repositories never upload', async () => {
    for (const change of [{ initialUpload: { enabled: false } }, { initialUpload: { enabled: true, completed: true } }, { checkout: {} }]) {
        const f = fixture(); Object.assign(f.entry, change);
        assert.equal((await publishRepository(f.server, f.entry, { runUpload: () => assert.fail('Must not upload') })).skipped, true);
        assert.equal(f.requests.length, 0);
    }
});

test('unconfirmed reservation and cancelled startup never upload', async () => {
    const f = fixture({ failLocal: true });
    await assert.rejects(publishRepository(f.server, f.entry, { runUpload: () => assert.fail('No claim') }), /reserve/);
    const controller = new AbortController(); controller.abort();
    await assert.rejects(publishRepository(f.server, f.entry, { signal: controller.signal, runUpload: () => assert.fail('Cancelled') }));
});

test('actual CLI invocation uses coc upload in the checkout and only records success on zero exit', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'coc-initial-upload-'));
    const record = path.join(root, 'invocation.json');
    const previous = { PATH: process.env.PATH, COC_UPLOAD_TEST_RECORD: process.env.COC_UPLOAD_TEST_RECORD, COC_UPLOAD_TEST_EXIT: process.env.COC_UPLOAD_TEST_EXIT };
    t.after(async () => {
        for (const [key, value] of Object.entries(previous)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
        await fs.rm(root, { recursive: true, force: true });
    });
    await fs.writeFile(path.join(root, 'CoCreate.config.js'), 'export default {};');
    await fs.writeFile(path.join(root, 'coc'), '#!/usr/bin/env node\nrequire("fs").writeFileSync(process.env.COC_UPLOAD_TEST_RECORD, JSON.stringify({ cwd: process.cwd(), args: process.argv.slice(2) }));\nprocess.exit(Number(process.env.COC_UPLOAD_TEST_EXIT || 0));\n', { mode: 0o700 });
    process.env.PATH = `${root}:${process.env.PATH}`;
    process.env.COC_UPLOAD_TEST_RECORD = record;
    const f = fixture(); f.entry.checkout.workspace = root;
    process.env.COC_UPLOAD_TEST_EXIT = '1';
    await assert.rejects(publishRepository(f.server, f.entry), /pending/);
    assert.equal(f.entry.initialUpload.completed, false);
    process.env.COC_UPLOAD_TEST_EXIT = '0';
    await publishRepository(f.server, f.entry);
    assert.deepEqual(JSON.parse(await fs.readFile(record, 'utf8')), { cwd: root, args: ['upload'] });
    assert.equal(f.entry.initialUpload.completed, true);
});
