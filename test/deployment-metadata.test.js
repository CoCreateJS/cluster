import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../src/getDeployment.js', import.meta.url), 'utf8')
    .replace(/^import .*;$/gm, '').replace('export default getDeployment;', '')
    .replace('export async function', 'async function');

for (const [probe, document, expected] of [
    ['probeAWS', { instanceId: 'i-provider', instanceType: 't3a.small', region: 'us-east-1', availabilityZone: 'us-east-1a' },
        { provider: 'aws', id: 'i-provider', type: 't3a.small', region: 'us-east-1', zone: 'us-east-1a' }],
    ['probeGCP', { id: '123', machineType: 'projects/example/machineTypes/e2-small', zone: 'projects/example/zones/us-east1-b' },
        { provider: 'gcp', id: '123', type: 'e2-small', region: 'us-east1', zone: 'us-east1-b' }]
]) {
    test(`${probe} normalizes type while preserving the provider deployment ID`, async () => {
        const context = vm.createContext({});
        vm.runInContext(source, context);
        context.request = async options => options.method === 'PUT' ? 'test-token' : JSON.stringify(document);
        const result = JSON.parse(JSON.stringify(await context[probe]()));
        assert.deepEqual(result, expected);
    });
}

test('deployment enrichment preserves saved cloud, dns, capacity and location without mutating defaults', async () => {
    const context = vm.createContext({ process: { env: {} } });
    vm.runInContext(source, context);
    context.request = async options => {
        if (options.path === '/latest/api/token') return 'test-token';
        if (options.path === '/latest/dynamic/instance-identity/document') return JSON.stringify({
            instanceId: 'i-actual', instanceType: 't3a.small', region: 'us-east-1', availabilityZone: 'us-east-1a'
        });
        throw new Error('Unavailable');
    };
    const saved = { cloud: 'cloud-api', provider: 'aws', id: 'i-reserved', type: 't3a.small',
        vCPU: 2, memoryGb: 4, dns: [{ provider: 'bunny', id: 1 }], location: { latitude: 51.5, longitude: -0.1 } };
    const original = structuredClone(saved);
    const actual = JSON.parse(JSON.stringify(await context.getDeployment('server-document-id', saved)));
    assert.deepEqual(actual, { ...saved, id: 'i-actual', region: 'us-east-1', zone: 'us-east-1a' });
    assert.deepEqual(saved, original);
});

test('unavailable metadata does not replace a saved cloud deployment with local fallback identity', async () => {
    const context = vm.createContext({ process: { env: {} } });
    vm.runInContext(source, context);
    context.request = async () => { throw new Error('Unavailable'); };
    const saved = { cloud: 'cloud-api', provider: 'aws', id: 'i-saved', type: 'small', region: 'saved-region',
        dns: [], vCPU: 2, memoryGb: 4 };
    assert.deepEqual(JSON.parse(JSON.stringify(await context.getDeployment('server-document-id', saved))), saved);
});
