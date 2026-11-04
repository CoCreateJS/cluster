import { join } from 'node:path';
import config from '@cocreate/config';
import { dotNotationToObject } from '@cocreate/utils';
import { initializeRepository } from './repository-startup.js';
import { saveRepository, authenticatedRepositoryEnvironment } from './repository-git.js';

/** One shared checkout per node, prepared by the master before workers fork. */
export async function init(server) {
    if (!server.isPrimary) return;
    const settings = {
        enabled: true,
        url: '',
        api: '',
        branch: 'dev',
        remote: 'origin',
        localFsRoot: join(process.cwd(), 'organizations'),
        onStartup: 'clone',
        onShutdown: 'commit-and-push',
        startupTimeoutMs: 60000,
        shutdownTimeoutMs: 10000
    };
    const systemConfig = await config.get('repository');
    const databaseConfig = await config.query({
        type: 'repository', organization_id: server.organization_id,
        server: server.host, cluster: server.cluster_id
    }, server.crud);
    for (const source of [systemConfig, databaseConfig]) Object.assign(settings, dotNotationToObject(source || {}));
    if (settings.enabled !== false && !['clone', false].includes(settings.onStartup)) throw new Error('Unsupported repository startup policy.');
    if (settings.enabled !== false && !['commit-and-push', false].includes(settings.onShutdown)) throw new Error('Unsupported repository shutdown policy.');
    const checkout = await initializeRepository({ server, config: settings });
    // Forked workers consume this same path; they never initialize the checkout.
    process.env.SERVER_REPOSITORY_ROOT = settings.localFsRoot;
    let stopping;
    return {
        config: settings,
        checkout,
        // Consumers performing network Git operations can obtain fresh credentials.
        environment(url = settings.url, signal) {
            return authenticatedRepositoryEnvironment(server, settings, url, signal);
        },
        stop(reason = 'shutdown', { timeoutMs = settings.shutdownTimeoutMs } = {}) {
            if (stopping) return stopping;
            if (!settings.enabled || settings.onShutdown === false) return Promise.resolve({ skipped: true, reason: 'disabled' });
            const controller = new AbortController();
            const budget = Math.max(1, Math.min(Number(settings.shutdownTimeoutMs) || 10000, timeoutMs));
            let timer;
            const deadline = new Promise((_, reject) => {
                timer = setTimeout(() => {
                    const error = new Error('Repository shutdown deadline exceeded.');
                    controller.abort(error);
                    reject(error);
                }, budget);
            });
            stopping = Promise.race([
                saveRepository({ server, localFsRoot: settings.localFsRoot, settings, reason, signal: controller.signal }),
                deadline
            ]).finally(() => clearTimeout(timer));
            return stopping;
        }
    };
}

export default { init };
