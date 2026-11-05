import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const exec = promisify(execFile);
const failed = response => !response || response.success === false ||
    (Array.isArray(response.error) ? response.error.length > 0 : Boolean(response.error));

async function upload(workspace, { signal, timeoutMs }) {
    await fs.access(path.join(workspace, 'CoCreate.config.js'));
    try {
        // Runtime/global CoCreate Config supplies credentials; no key is written into Git.
        await exec('coc', ['upload'], {
            cwd: workspace, env: process.env, signal, timeout: timeoutMs,
            maxBuffer: 16 * 1024 * 1024, windowsHide: true
        });
    } catch { throw new Error('Initial coc upload failed.'); }
}

/** Publish one cluster repository once; the primary calls this after service readiness. */
export async function publishRepository(server, entry, { signal, runUpload = upload } = {}) {
    if (entry.enabled === false || entry.initialUpload?.enabled !== true || entry.initialUpload.completed === true || !entry.checkout?.workspace) return { skipped: true };
    const scope = { organization_id: server.organization_id, host: server.host };
    const send = request => server.crud.send({ ...request, ...scope });
    const readEntry = async () => {
        const response = await send({ method: 'object.read', array: 'clusters', filter: { query: { _id: server.cluster_id }, key: { repositories: 1 }, limit: 1 } });
        if (failed(response) || !Array.isArray(response.object?.[0]?.repositories)) throw new Error('Unable to read initial repository publication state.');
        const entries = response.object[0].repositories;
        const index = entries.findIndex(item => item.url === entry.url);
        if (index < 0) throw new Error('The repository no longer belongs to this cluster.');
        return { index, entry: entries[index] };
    };
    const current = await readEntry();
    if (current.entry.initialUpload?.enabled !== true || current.entry.initialUpload.completed === true) return { skipped: true };
    signal?.throwIfAborted();
    const id = createHash('sha256').update(`${server.cluster_id}\0${entry.url}`).digest('hex').slice(0, 24);
    let claim;
    try {
        claim = await send({ method: 'object.create', array: 'repository_upload_reservations', object: {
            _id: id, cluster_id: server.cluster_id, repository_url: entry.url,
            server_id: server.id, started_at: new Date().toISOString()
        } });
    } catch { claim = null; }
    if (failed(claim) || !claim.object?.length) {
        const existing = await send({ method: 'object.read', array: 'repository_upload_reservations', filter: { query: { _id: id }, limit: 1 } });
        if (!failed(existing) && existing.object?.length) return { skipped: true, reason: 'reserved' };
        throw new Error('Unable to reserve initial repository publication.');
    }
    const release = async () => {
        const result = await send({ method: 'object.delete', array: 'repository_upload_reservations', object: [{ _id: id }] });
        if (failed(result)) throw new Error('Unable to release initial repository publication reservation.');
    };
    let uploaded = false;
    try {
        const fresh = await readEntry();
        if (fresh.entry.initialUpload?.enabled !== true || fresh.entry.initialUpload.completed === true) {
            await release(); return { skipped: true };
        }
        signal?.throwIfAborted();
        await runUpload(entry.checkout.workspace, {
            signal, timeoutMs: Math.max(1, Number(entry.initialUpload.timeoutMs) || 600000)
        });
        uploaded = true;
        const { index } = await readEntry();
        const patch = {
            _id: server.cluster_id,
            [`repositories.${index}.initialUpload.completed`]: true,
            [`repositories.${index}.initialUpload.completedAt`]: new Date().toISOString()
        };
        const request = { method: 'object.update', array: 'clusters', object: patch };
        const saved = await send(request);
        if (failed(saved) || !saved.object?.length) throw new Error('Initial upload completed but its completion state was not persisted.');
        Object.assign(entry.initialUpload, { completed: true, completedAt: patch[`repositories.${index}.initialUpload.completedAt`] });
        if (server.manager?.organization_id) {
            try { await server.manager.send({ ...request, ...scope }, { timeoutMs: 3000 }); }
            catch { console.error('[@cocreate/cluster] Unable to mirror initial upload completion to the manager.'); }
        }
        await release();
        return { uploaded: true };
    } catch {
        // If upload succeeded but persistence is uncertain, retain the reservation for review.
        if (!uploaded) { try { await release(); } catch {} }
        throw new Error(uploaded ? 'Initial upload completion requires review.' : 'Initial repository upload failed; completion remains pending.');
    }
}
