import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

/** Resolve repository paths relative to the running server project. */
export function repositoryPaths(settings, projectRoot = process.cwd()) {
    const root = path.resolve(projectRoot, settings.localFsRoot || '../');
    const name = settings.directory || String(settings.url || '').replace(/\/$/, '').split(/[/:]/).pop().replace(/\.git$/, '');
    if (!name || !/^[A-Za-z0-9_-][A-Za-z0-9_.-]*$/.test(name)) throw new Error('Repository requires a valid checkout directory.');
    const workspace = path.join(root, name);
    if (workspace === path.resolve(projectRoot)) throw new Error('Repository checkout cannot replace the running server project.');
    const metadata = path.join(root, '.cocreate', 'repositories', createHash('sha256').update(settings.url || name).digest('hex'));
    return { root, workspace, metadata };
}

export async function ensureRepositoryPaths(settings, projectRoot) {
    const paths = repositoryPaths(settings, projectRoot);
    await fs.mkdir(paths.root, { recursive: true, mode: 0o700 });
    await fs.mkdir(paths.metadata, { recursive: true, mode: 0o700 });
    await fs.mkdir(paths.workspace, { recursive: true, mode: 0o700 });
    const realRoot = await fs.realpath(paths.root);
    for (const target of [paths.workspace, paths.metadata]) {
        const real = await fs.realpath(target);
        const relative = path.relative(realRoot, real);
        if (relative.startsWith('..' + path.sep) || relative === '..' || path.isAbsolute(relative) || (await fs.lstat(target)).isSymbolicLink()) {
            throw new Error('Repository path escapes the configured root.');
        }
    }
    return paths;
}
