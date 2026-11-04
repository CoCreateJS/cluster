import assert from 'node:assert/strict';
import test from 'node:test';
import config from '@cocreate/config';
import { registerAdapter } from '../../config/src/core.js';
import ApiKey from '../src/apikey.js';

function setup(options = {}) {
    config.clear();
    const state = {
        global: { organization_id: 'node-org', storage: { name: 'primary', provider: 'custom' }, unrelated: 'preserve' },
        env: {}, writes: [], requests: []
    };
    registerAdapter({
        isBrowser: false,
        loadConfig: async () => ({}),
        getGlobalConfig: async () => structuredClone(state.global),
        setGlobalConfig: async value => {
            state.global = structuredClone(value);
            state.writes.push(structuredClone(value));
        },
        getEnv: key => state.env[key],
        setEnv: (key, value) => { state.env[key] = value; },
        prompt: async () => assert.fail('Boot must never prompt interactively')
    });
    const server = {
        isPrimary: options.isPrimary ?? true,
        organization_id: 'node-org', host: 'node.example.com',
        crud: { send: async request => {
            state.requests.push(request);
            if (options.throwRead) throw new Error('Sensitive provider diagnostics must not be forwarded');
            return options.response ?? { object: [{ key: 'test-only-upload-key' }] };
        } }
    };
    return { server, state };
}

test('primary fetches its own organization key and persists it through real Config', async () => {
    const { server, state } = setup();
    const result = await ApiKey.init(server);
    assert.equal(result, undefined, 'Do not return the credential to callers');
    assert.equal(state.requests.length, 1);
    assert.deepEqual(state.requests[0], {
        method: 'object.read', organization_id: 'node-org', host: 'node.example.com', array: 'keys',
        filter: {
            query: { type: 'key', admin: { $in: [true, 'true', 1, '1'] } },
            key: { key: 1 }, sort: { _id: 1 }, limit: 1
        }
    });
    assert.equal(config.get('apikey'), 'test-only-upload-key');
    assert.equal(state.env.apikey, 'test-only-upload-key');
    assert.deepEqual(state.global, {
        organization_id: 'node-org', storage: { name: 'primary', provider: 'custom' },
        unrelated: 'preserve', apikey: 'test-only-upload-key'
    });
    config.clear();
    await config.prompt({ apikey: {} });
    assert.equal(config.get('apikey'), 'test-only-upload-key', 'A fresh config context can load the saved key');
    assert.equal(state.writes.length, 1);
});

test('workers leave CRUD and global configuration untouched', async () => {
    const { server, state } = setup({ isPrimary: false });
    await ApiKey.init(server);
    assert.equal(state.requests.length, 0);
    assert.equal(state.writes.length, 0);
});

test('no upload key leaves the node running without generating or persisting a credential', async () => {
    const { server, state } = setup({ response: { object: [] } });
    await ApiKey.init(server);
    assert.equal(state.writes.length, 0);
    assert.equal(state.env.apikey, undefined);
});

for (const response of [
    { error: ['Read failed'] }, { success: false },
    { object: [{ key: '' }] }, { object: [{ key: {} }] }
]) {
    test(`failed or invalid key lookup is never persisted: ${JSON.stringify(response)}`, async () => {
        const { server, state } = setup({ response });
        await assert.rejects(ApiKey.init(server), /node organization API key/);
        assert.equal(state.writes.length, 0);
        assert.equal(state.env.apikey, undefined);
    });
}

test('CRUD exceptions do not expose provider diagnostics', async () => {
    const { server, state } = setup({ throwRead: true });
    await assert.rejects(ApiKey.init(server), { message: 'Unable to read the node organization API key.' });
    assert.equal(state.writes.length, 0);
});
