import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { initializeServerDB, fileId } from '@cocreate/db'

function createServer({ id, isPrimary, directory, send, clusterSend, peerTimeoutMs }) {
    const server = new EventEmitter()
    server.id = id
    server.isPrimary = isPrimary
    server.cluster = new EventEmitter()
    server.cluster.sent = []
    server.cluster.send = payload => {
        server.cluster.sent.push(payload)
        return clusterSend?.(payload)
    }
    server.emitAsync = async (event, payload) => {
        const replies = await Promise.all(server.rawListeners(event).map(listener => listener(payload)))
        return replies[0] ?? payload
    }
    server.send = send ?? (payload => server.emitAsync(payload.method, payload))
    initializeServerDB(server, { directory, peerTimeoutMs, writeBehind: false })
    const scope = { organization_id: '000000000000000000000001', storage: 'production', storageId: '000000000000000000000099', database: 'app' }
    const sendLocal = server.db.send.bind(server.db)
    let ready = Promise.resolve()
    let initialized = false
    server.db.send = async payload => {
        if (!initialized && /\.(create|update|delete)$/.test(payload.method)) {
            initialized = true
            ready = sendLocal({ ...scope, method: 'storage.create' })
        }
        await ready
        return sendLocal({ ...scope, ...payload })
    }
    for (const method of ['updateDistribution', 'getDistribution', 'listDistribution', 'readLocalEntity', 'applyReplicatedObjectMutation']) {
        if (!server.db[method]) continue
        const local = server.db[method].bind(server.db)
        server.db[method] = async payload => { await ready; return local({ ...scope, ...payload }) }
    }
    if (server.db.index) {
        for (const method of ['read', 'getMetadata', 'create', 'update', 'delete']) {
            const local = server.db.index[method].bind(server.db.index)
            server.db.index[method] = async (...args) => { await ready; return local(scope, ...args) }
        }
        const scan = server.db.index.scan.bind(server.db.index)
        server.db.index.scan = async function* (...args) { await ready; yield* scan(scope, ...args) }
    }
    return server
}

test('server.db broadcasts local availability transitions and applies remote events without rebroadcast', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-'))
    const server = createServer({ id: 'server-local', isPrimary: true, directory })

    try {
        const database = await server.db.send({ method: 'database.create', database: 'events' })
        assert.equal(database.success, true, database.error)
        const entityEvents = server.cluster.sent.filter(message => message.method === 'db.distribution')
        assert.equal(entityEvents.length, 1)
        assert.equal(entityEvents[0].operation, 'add')
        assert.equal(entityEvents[0].serverId, 'server-local')
        assert.equal(entityEvents[0].workers, false)
        assert.ok(server.cluster.sent.some(message => message.method === 'db.partition.availability'))

        const array = await server.db.send({ method: 'array.create', database: 'events', array: 'items' })
        assert.equal(array.success, true, array.error)
        const created = await server.db.send({
            method: 'object.create', database: 'events', array: 'items', object: { value: 1 }
        })
        assert.equal(created.success, true, created.error)
        const localBroadcastCount = server.cluster.sent.length

        const databaseId = database.database[0]._id
        const arrayId = array.array[0]._id
        const objectId = fileId(created.object[0])
        const remoteEvent = {
            method: 'db.distribution',
            serverId: 'server-peer',
            scope: { organization_id: '000000000000000000000001', storage: 'production', storageId: '000000000000000000000099' },
            databaseId,
            arrayId,
            objectId,
            nodeId: 'server-peer',
            operation: 'add'
        }

        const incoming = []
        server.send = payload => {
            const response = server.emitAsync(payload.method, payload)
            incoming.push(response)
            return response
        }
        server.cluster.emit('db.distribution', remoteEvent)
        await Promise.all(incoming)

        assert.deepEqual(await server.db.getDistribution({ databaseId, arrayId, objectId }), [
            'server-local', 'server-peer'
        ])
        assert.equal(server.cluster.sent.length, localBroadcastCount)
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test('worker CRUD executes on the primary and propagates availability once', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-ipc-'))
    const parent = createServer({ id: 'server-local', isPrimary: true, directory })

    try {
        const worker = createServer({
            id: 'server-local',
            isPrimary: false,
            directory,
            send: payload => parent.emitAsync(payload.method, payload)
        })
        const created = await worker.db.send({
            method: 'object.create', database: 'worker-db', array: 'items', object: { value: 2 }
        })
        assert.equal(created.success, true, created.error)
        const database = await parent.db.send({ method: 'database.read' })
        const databaseId = database.database.find(item => item.name === 'worker-db')._id
        const arrays = await parent.db.send({ method: 'array.read', database: 'worker-db' })
        const arrayId = arrays.array[0]._id

        assert.deepEqual(await parent.db.getDistribution({
            databaseId, arrayId, objectId: created.object[0]._id
        }), ['server-local'])
        assert.equal(parent.cluster.sent.filter(message => message.method === 'db.distribution').length, 3)
        assert.equal(worker.cluster.sent.filter(message => message.method === 'db.distribution').length, 0)
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test('peer document requests target one node, correlate replies, and return complete unprojected records', async () => {
    const sourceDirectory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-peer-source-'))
    const targetDirectory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-peer-target-'))
    let source
    let target

    source = createServer({
        id: 'node-source',
        isPrimary: true,
        directory: sourceDirectory,
        clusterSend: payload => {
            if (['db.document.response', 'db.partition.response'].includes(payload.method)) target.cluster.emit(payload.method, payload)
        }
    })
    target = createServer({
        id: 'node-target',
        isPrimary: true,
        directory: targetDirectory,
        clusterSend: payload => {
            if (['db.document.request', 'db.partition.request'].includes(payload.method)) source.cluster.emit(payload.method, payload)
        }
    })

    try {
        const database = (await source.db.send({ method: 'database.create', database: 'peer-db' })).database[0]
        const array = (await source.db.send({ method: 'array.create', database: 'peer-db', array: 'users' })).array[0]
        const object = (await source.db.send({
            method: 'object.create', database: 'peer-db', array: 'users',
            object: { name: 'Ada', secret: { token: 'complete' } }
        })).object[0]

        const inventory = await source.db.distribution.local.snapshot()
        target.db.distribution.peers.replace(source.id, inventory, 'cold-peer-test', 1)
        await target.db.applyRoots(inventory.roots)

        const result = await target.db.send({
            method: 'object.read',
            database: 'peer-db',
            array: 'users',
            object: { _id: object._id },
            filter: { key: { name: 1 } }
        })
        assert.equal(result.success, true, result.error)
        assert.equal(result.object[0].secret, undefined)
        assert.deepEqual(await target.db.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: object._id
        }).then(record => record.secret), { token: 'complete' })

        const requests = target.cluster.sent.filter(message => message.method === 'db.document.request')
        assert.deepEqual(requests.map(message => message.type), ['storage', 'database', 'array', 'object'])
        assert.ok(requests.every(message => message.server === source.id && message.requestId))
    } finally {
        await Promise.all([source.db.close(), target.db.close()])
        await Promise.all([
            rm(sourceDirectory, { recursive: true, force: true }),
            rm(targetDirectory, { recursive: true, force: true })
        ])
    }
})

test('unavailable peer fetches time out and leave the read as a normal miss', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-peer-timeout-'))
    const server = createServer({
        id: 'node-requester',
        isPrimary: true,
        directory,
        peerTimeoutMs: 10
    })

    try {
        const database = (await server.db.send({ method: 'database.create', database: 'timeout-db' })).database[0]
        const array = (await server.db.send({ method: 'array.create', database: 'timeout-db', array: 'items' })).array[0]
        const objectId = '000000000000000000000001'
        await server.db.updateDistribution({
            databaseId: database._id,
            arrayId: array._id,
            objectId,
            nodeId: 'node-unavailable',
            operation: 'add'
        })

        const result = await server.db.send({
            method: 'object.read',
            database: 'timeout-db',
            array: 'items',
            object: { _id: objectId }
        })
        assert.equal(result.success, true, result.error)
        assert.deepEqual(result.object, [])
        assert.equal(server.cluster.sent.at(-1).method, 'db.document.request')
        assert.equal(server.cluster.sent.at(-1).server, 'node-unavailable')
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test('index partitions are acquired from a peer, register holders, and propagate later mutations only to holders', async () => {
    const sourceDirectory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-index-source-'))
    const targetDirectory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-index-target-'))
    const deliveries = new Set()
    const deliver = (from, to, payload) => {
        if (!payload.broadcast && payload.server !== to.id) return
        const task = Promise.resolve().then(() => Promise.all(
            to.cluster.rawListeners(payload.method).map(listener => listener.call(to.cluster, payload))
        ))
        deliveries.add(task)
        task.then(() => deliveries.delete(task), () => {})
    }
    const settleDeliveries = async () => {
        while (deliveries.size) await Promise.all([...deliveries])
    }
    let source
    let target
    source = createServer({
        id: 'index-source',
        isPrimary: true,
        directory: sourceDirectory,
        clusterSend: payload => deliver(source, target, payload)
    })
    target = createServer({
        id: 'index-target',
        isPrimary: true,
        directory: targetDirectory,
        clusterSend: payload => deliver(target, source, payload)
    })

    const collect = async iterable => {
        const values = []
        for await (const value of iterable) values.push(value)
        return values
    }
    const waitFor = async check => {
        let lastError
        for (let attempt = 0; attempt < 100; attempt++) {
            // Partition messages are independent; a polling cursor can observe
            // an old reference after its leaf was removed and before its parent arrives.
            try { if (await check()) return } catch (error) { lastError = error }
            await new Promise(resolve => setTimeout(resolve, 10))
        }
        assert.fail(`timed out waiting for partition synchronization${lastError ? ': ' + lastError.message : ''}`)
    }

    try {
        const databaseName = 'partition-db'
        const arrayName = 'items'
        const database = (await source.db.send({ method: 'database.create', database: databaseName })).database[0]
        const array = (await source.db.send({ method: 'array.create', database: databaseName, array: arrayName })).array[0]
        await source.db.index.create(databaseName, arrayName, 'score', 4, 'object-1')

        for (const entity of [
            { databaseId: database._id },
            { databaseId: database._id, arrayId: array._id }
        ]) await target.db.updateDistribution({ ...entity, storage: 'production', databaseFileId: fileId(database), arrayFileId: fileId(array), nodeId: source.id, operation: 'add' })

        for (const { partitionId, nodeIds } of await source.db.listPartitions())
            for (const nodeId of nodeIds)
                await target.db.updatePartition({ partitionId, nodeId, operation: 'add' })

        await target.db.send({ method: 'database.read' })
        await target.db.send({ method: 'array.read', database: databaseName })

        assert.deepEqual(await collect(target.db.index.scan(databaseName, arrayName, 'score', { type: 'number' })), [
            { value: 4, objectId: 'object-1' }
        ])

        const catalogId = (await source.db.index.read(databaseName, arrayName, 'score')).indexes.number.catalog
        const partitionId = (await source.db.listPartitions())
            .find(({ partitionId }) => partitionId.endsWith(`/indexes/${catalogId}.json`)).partitionId
        await waitFor(async () => (await source.db.getPartitionHolders(partitionId)).includes(target.id))

        const sentBeforeMutation = source.cluster.sent.length
        await source.db.index.delete(databaseName, arrayName, 'score', 4, 'object-1')
        await source.db.index.create(databaseName, arrayName, 'score', 9, 'object-1')
        await waitFor(async () => {
            const entries = await collect(target.db.index.scan(databaseName, arrayName, 'score', { type: 'number' }))
            return entries.length === 1 && entries[0].value === 9
        })
        const updates = source.cluster.sent.slice(sentBeforeMutation)
            .filter(message => message.method === 'db.partition.update')
        assert.ok(updates.length > 0)
        assert.ok(updates.every(message => message.server === target.id))
        await settleDeliveries()
        assert.deepEqual(await collect(target.db.index.scan(databaseName, arrayName, 'score', { type: 'number' })), [{ value: 9, objectId: 'object-1' }])
    } finally {
        await settleDeliveries()
        await Promise.all([source.db.close(), target.db.close()])
        await Promise.all([
            rm(sourceDirectory, { recursive: true, force: true }),
            rm(targetDirectory, { recursive: true, force: true })
        ])
    }
})

test('object create, update, and delete mutations follow index partition holders', async () => {
    const sourceDirectory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-mutation-source-'))
    const targetDirectory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-mutation-target-'))
    const deliver = (from, to, payload) => {
        if (payload.broadcast || payload.server === to.id)
            queueMicrotask(() => to.cluster.emit(payload.method, payload))
    }
    let source
    let target
    source = createServer({ id: 'mutation-source', isPrimary: true, directory: sourceDirectory,
        clusterSend: payload => deliver(source, target, payload) })
    target = createServer({ id: 'mutation-target', isPrimary: true, directory: targetDirectory,
        clusterSend: payload => deliver(target, source, payload) })
    const waitFor = async check => {
        for (let attempt = 0; attempt < 100; attempt++) {
            if (await check()) return
            await new Promise(resolve => setTimeout(resolve, 10))
        }
        assert.fail('timed out waiting for object mutation synchronization')
    }

    try {
        const database = (await source.db.send({ method: 'database.create', database: 'replicated' })).database[0]
        const array = (await source.db.send({ method: 'array.create', database: 'replicated', array: 'items' })).array[0]
        const created = await source.db.send({
            method: 'object.create', database: 'replicated', array: 'items', object: { name: 'Ada', score: 1 }
        })
        assert.equal(created.success, true, created.error)
        const object = created.object[0]
        const catalogId = (await source.db.index.read('replicated', 'items', 'score')).indexes.number.catalog
        const partitions = []
        for (const partition of await source.db.listPartitions()) {
            if (!partition.nodeIds.includes(source.id)) continue
            if ((await source.db.readIndexPartition(partition.partitionId))?.catalog === catalogId) partitions.push(partition)
        }
        assert.ok(partitions.length, JSON.stringify(await source.db.listPartitions()))

        const [databaseRecord, arrayRecord] = await Promise.all([
            source.db.readLocalEntity({ type: 'database', databaseId: database._id }),
            source.db.readLocalEntity({ type: 'array', databaseId: database._id, arrayId: array._id })
        ])
        await target.db.applyReplicatedObjectMutation({ operation: 'set', database: databaseRecord, array: arrayRecord, object })
        for (const { partitionId, nodeIds } of partitions) {
            await target.db.applyIndexPartition(partitionId, await source.db.readIndexPartition(partitionId), { replicated: true })
            for (const nodeId of nodeIds)
                await target.db.updatePartition({ partitionId, nodeId, operation: 'add' })
            await source.db.updatePartition({ partitionId, nodeId: target.id, operation: 'add' })
        }

        const second = await source.db.send({
            method: 'object.create', database: 'replicated', array: 'items', object: { name: 'Ada', score: 1 }
        })
        assert.equal(second.success, true, second.error)
        const replicatedObject = second.object[0]
        await waitFor(async () => (await target.db.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: fileId(replicatedObject)
        }))?.score === 1)

        const updated = await source.db.send({
            method: 'object.update', database: 'replicated', array: 'items', object: { _id: replicatedObject._id, score: 2 }
        })
        assert.equal(updated.success, true, updated.error)
        await waitFor(async () => (await target.db.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: fileId(replicatedObject)
        }))?.score === 2)
        assert.equal((await target.db.readLocalEntity({
            type: 'array', databaseId: database._id, arrayId: array._id
        })).length, 2)

        const deleted = await source.db.send({
            method: 'object.delete', database: 'replicated', array: 'items', object: { _id: replicatedObject._id }
        })
        assert.equal(deleted.success, true, deleted.error)
        await waitFor(async () => !(await target.db.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: fileId(replicatedObject)
        })))
        assert.equal((await target.db.readLocalEntity({
            type: 'array', databaseId: database._id, arrayId: array._id
        })).length, 1)
        assert.ok(source.cluster.sent.some(message =>
            message.method === 'db.object.mutation' && message.server === target.id))
        await new Promise(resolve => setTimeout(resolve, 50))
    } finally {
        await Promise.all([source.db.close(), target.db.close()])
        await Promise.all([
            rm(sourceDirectory, { recursive: true, force: true }),
            rm(targetDirectory, { recursive: true, force: true })
        ])
    }
})
