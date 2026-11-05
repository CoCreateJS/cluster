import { initializeRepository } from './startup.js';
import { saveRepository, authenticatedRepositoryEnvironment } from './git.js';
import { repositoryPaths } from './workspace.js';
import { publishRepository } from './upload.js';

/** Cluster-owned repositories; one primary prepares each checkout before workers fork. */
export async function init(server) {
    if (!server.isPrimary) return;
    const repositories = [];
    if (server.cluster_id) {
        const result = await server.crud.send({
            method: 'object.read', array: 'clusters', organization_id: server.organization_id, host: server.host,
            filter: { query: { _id: server.cluster_id }, key: { repositories: 1 }, limit: 1 }
        });
        const errors = Array.isArray(result?.error) ? result.error.length : result?.error;
        if (!result || result.success === false || errors || !Array.isArray(result.object)) throw new Error('Unable to read cluster repositories.');
        const entries = result.object[0]?.repositories || [];
        if (!Array.isArray(entries)) throw new Error('Cluster repositories must be an array.');
        const urls = new Set(), paths = new Set();
        for (const entry of entries) {
            if (typeof entry?.url !== 'string' || !entry.url.trim()) throw new Error('Every cluster repository requires its URL identity.');
            const settings = {
                enabled: true, branch: 'main', remote: 'origin', localFsRoot: '../',
                onStartup: 'clone', onShutdown: 'commit-and-push', startupTimeoutMs: 60000, shutdownTimeoutMs: 10000,
                ...structuredClone(entry), url: entry.url.trim()
            };
            settings.shutdownTimeoutMs = Math.max(1, Number(settings.shutdownTimeoutMs) || 10000);
            const { workspace, root } = repositoryPaths(settings, server.projectRoot);
            if (urls.has(settings.url) || paths.has(workspace)) throw new Error('Cluster repository URLs and checkout directories must be unique.');
            urls.add(settings.url); paths.add(workspace);
            if (!['clone', false].includes(settings.onStartup) || !['commit-and-push', false].includes(settings.onShutdown)) throw new Error('Unsupported repository lifecycle policy.');
            repositories.push({ ...settings, localFsRoot: root });
        }
    }
    for (const entry of repositories) entry.checkout = await initializeRepository({ server, config: entry });
    const primary = repositories.find(entry => entry.enabled !== false && entry.checkout.workspace);
    if (primary) {
        process.env.SERVER_REPOSITORY_ROOT = primary.checkout.workspace;
        process.env.SERVER_REPOSITORY_ORGANIZATION_ID = server.organization_id;
    } else {
        delete process.env.SERVER_REPOSITORY_ROOT;
        delete process.env.SERVER_REPOSITORY_ORGANIZATION_ID;
    }
    let stopping, publishing;
    const publicationController = new AbortController();
    return {
        repositories,
        publish() {
            if (!publishing) publishing = (async () => {
                const results = [], errors = [];
                for (const entry of repositories) {
                    publicationController.signal.throwIfAborted();
                    try { results.push(await publishRepository(server, entry, { signal: publicationController.signal })); }
                    catch (error) { errors.push(error); }
                }
                if (errors.length) throw new AggregateError(errors, 'Initial repository publication failed.');
                return results;
            })();
            return publishing;
        },
        environment(url = primary?.url, signal) {
            const entry = repositories.find(item => item.url === url);
            if (!entry) throw new Error('The repository is not configured on this cluster.');
            return authenticatedRepositoryEnvironment(server, entry, entry.url, signal);
        },
        stop(reason = 'shutdown', { timeoutMs = repositories.reduce((sum, entry) => sum + entry.shutdownTimeoutMs, 0) } = {}) {
            if (stopping) return stopping;
            stopping = (async () => {
                const deadline = Date.now() + timeoutMs, results = [], errors = [];
                publicationController.abort(new Error('Node is shutting down.'));
                if (publishing) {
                    let waitTimer;
                    try {
                        await Promise.race([
                            publishing,
                            new Promise((_, reject) => { waitTimer = setTimeout(() => reject(new Error('Publication shutdown deadline exceeded.')), Math.max(1, deadline - Date.now())); })
                        ]);
                    } catch {} finally { clearTimeout(waitTimer); }
                }

                for (const entry of repositories) {
                    if (!entry.enabled || entry.onShutdown === false) continue;
                    const budget = Math.min(entry.shutdownTimeoutMs, deadline - Date.now());
                    if (budget <= 0) { errors.push(new Error('Repository shutdown budget exhausted.')); break; }
                    const controller = new AbortController();
                    let timer;
                    try {
                        results.push(await Promise.race([
                            saveRepository({ server, settings: entry, reason, signal: controller.signal }),
                            new Promise((_, reject) => { timer = setTimeout(() => {
                                const error = new Error('Repository shutdown deadline exceeded.');
                                controller.abort(error); reject(error);
                            }, budget); })
                        ]));
                    } catch (error) { errors.push(error); }
                    finally { clearTimeout(timer); }
                }
                if (errors.length) throw new AggregateError(errors, 'One or more repository checkpoints failed.');
                return results;
            })();
            return stopping;
        }
    };
}
export default { init };
