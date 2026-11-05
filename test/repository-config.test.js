import assert from 'node:assert/strict';
import test from 'node:test';
import config from '@cocreate/config';
import { init } from '../src/repository/index.js';

function restoreEnvironment(t) {
    const names = ['SERVER_REPOSITORY_ROOT', 'SERVER_REPOSITORY_ORGANIZATION_ID'];
    const previous = names.map(name => process.env[name]);
    t.after(() => names.forEach((name, i) => { if (previous[i] === undefined) delete process.env[name]; else process.env[name] = previous[i]; }));
}

test('workers never query cluster repositories or prepare checkouts', async () => {
    assert.equal(await init({ isPrimary: false, crud: { send() { assert.fail('Worker must not read repository configuration'); } } }), undefined);
});

test('primary reads cluster-owned entries without repository config overrides', async t => {
    restoreEnvironment(t);
    t.mock.method(config, 'get', () => assert.fail('No repository config lookup'));
    t.mock.method(config, 'query', () => assert.fail('No repository server-config lookup'));
    let reads = 0;
    const repository = await init({ isPrimary: true, projectRoot: '/srv/server', organization_id: 'tenant', cluster_id: 'cluster-1', host: 'tenant.test', crud: { send: async request => {
        reads++;
        assert.equal(request.array, 'clusters');
        assert.equal(request.organization_id, 'tenant');
        assert.deepEqual(request.filter.query, { _id: 'cluster-1' });
        return { object: [{ repositories: [{ url: 'https://example.test/project.git', enabled: false, initialUpload: { enabled: true, completed: false } }] }] };
    } } });
    assert.equal(reads, 1);
    assert.equal(repository.repositories[0].localFsRoot, '/srv');
    assert.equal(repository.repositories[0].branch, 'main');
    assert.equal(repository.repositories[0].checkout.reason, 'disabled');
    assert.equal(repository.repositories[0].initialUpload.completed, false);
    const publication = repository.publish();
    assert.equal(repository.publish(), publication);
    assert.deepEqual(await publication, [{ skipped: true }]);
    assert.deepEqual(await repository.stop(), []);
});

test('duplicate URL identities or checkout destinations fail before cloning', async t => {
    restoreEnvironment(t);
    for (const entries of [
        [{ url: 'https://example.test/one.git' }, { url: 'https://example.test/one.git' }],
        [{ url: 'https://example.test/one.git', directory: 'same' }, { url: 'https://example.test/two.git', directory: 'same' }]
    ]) {
        await assert.rejects(init({ isPrimary: true, cluster_id: 'cluster', crud: { send: async () => ({ object: [{ repositories: entries }] }) } }), /must be unique/);
    }
});
