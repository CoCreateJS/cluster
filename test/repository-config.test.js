import assert from 'node:assert/strict';
import test from 'node:test';
import config from '@cocreate/config';
import { init } from '../src/repository.js';

test('workers never load config, query metadata, or initialize a repository', async t => {
    t.mock.method(config, 'get', () => { throw new Error('worker must not load repository config'); });
    assert.equal(await init({ isPrimary: false }), undefined);
});

test('master resolves its own repository config independently of AI and APIs', async t => {
    const previous = process.env.SERVER_REPOSITORY_ROOT;
    t.after(() => {
        if (previous === undefined) delete process.env.SERVER_REPOSITORY_ROOT;
        else process.env.SERVER_REPOSITORY_ROOT = previous;
    });
    t.mock.method(config, 'get', name => {
        assert.equal(name, 'repository');
        return { enabled: true, branch: 'main' };
    });
    t.mock.method(config, 'query', async (query, crud) => {
        assert.equal(query.type, 'repository');
        assert.equal(query.organization_id, 'platform');
        assert.equal(query.cluster, 'cluster-1');
        assert.ok(crud);
        return { enabled: false, branch: 'dev' };
    });
    const repository = await init({ isPrimary: true, organization_id: 'platform', cluster_id: 'cluster-1', crud: {} });
    assert.equal(repository.config.branch, 'dev');
    assert.equal(repository.config.api, '');
    assert.equal(repository.checkout.reason, 'disabled');
    assert.equal((await repository.stop()).reason, 'disabled');
    assert.equal(process.env.SERVER_REPOSITORY_ROOT, repository.config.localFsRoot);
});
