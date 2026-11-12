import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { initializeServerDB } from '@cocreate/db'

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
    initializeServerDB(server, { directory, peerTimeoutMs })
    return server
}

test('server.DB broadcasts local availability transitions and applies remote events without rebroadcast', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-'))
    const server = createServer({ id: 'server-local', isPrimary: true, directory })

    try {
        const database = await server.DB.send({ method: 'database.create', database: 'events' })
        assert.equal(database.success, true, database.error)
        const entityEvents = server.cluster.sent.filter(message => message.method === 'db.distributed')
        assert.equal(entityEvents.length, 1)
        assert.equal(entityEvents[0].operation, 'add')
        assert.equal(entityEvents[0].serverId, 'server-local')
        assert.equal(entityEvents[0].workers, false)
        assert.ok(server.cluster.sent.some(message => message.method === 'db.partition.availability'))

        const array = await server.DB.send({ method: 'array.create', database: 'events', array: 'items' })
        assert.equal(array.success, true, array.error)
        const created = await server.DB.send({
            method: 'object.create', database: 'events', array: 'items', object: { value: 1 }
        })
        assert.equal(created.success, true, created.error)
        const localBroadcastCount = server.cluster.sent.length

        const databaseId = database.database[0]._id
        const arrayId = array.array[0]._id
        const objectId = created.object[0]._id
        const remoteEvent = {
            method: 'db.distributed',
            serverId: 'server-peer',
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
        server.cluster.emit('db.distributed', remoteEvent)
        await Promise.all(incoming)

        assert.deepEqual(await server.DB.getDistributed({ databaseId, arrayId, objectId }), [
            'server-local', 'server-peer'
        ])
        assert.equal(server.cluster.sent.length, localBroadcastCount)
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test('worker availability updates are serialized through the primary', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'cocreate-server-db-ipc-'))
    const parent = createServer({ id: 'server-local', isPrimary: true, directory })

    try {
        const worker = createServer({
            id: 'server-local',
            isPrimary: false,
            directory,
            send: payload => parent.emitAsync(payload.method, payload)
        })
        const created = await worker.DB.send({
            method: 'object.create', database: 'worker-db', array: 'items', object: { value: 2 }
        })
        assert.equal(created.success, true, created.error)
        const database = await parent.DB.send({ method: 'database.read' })
        const databaseId = database.database.find(item => item.name === 'worker-db')._id
        const arrays = await parent.DB.send({ method: 'array.read', database: 'worker-db' })
        const arrayId = arrays.array[0]._id

        assert.deepEqual(await parent.DB.getDistributed({
            databaseId, arrayId, objectId: created.object[0]._id
        }), ['server-local'])
        assert.equal(parent.cluster.sent.filter(message => message.method === 'db.distributed').length, 3)
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
            if (payload.method === 'db.document.response') target.cluster.emit(payload.method, payload)
        }
    })
    target = createServer({
        id: 'node-target',
        isPrimary: true,
        directory: targetDirectory,
        clusterSend: payload => {
            if (payload.method === 'db.document.request') source.cluster.emit(payload.method, payload)
        }
    })

    try {
        const database = (await source.DB.send({ method: 'database.create', database: 'peer-db' })).database[0]
        const array = (await source.DB.send({ method: 'array.create', database: 'peer-db', array: 'users' })).array[0]
        const object = (await source.DB.send({
            method: 'object.create', database: 'peer-db', array: 'users',
            object: { name: 'Ada', secret: { token: 'complete' } }
        })).object[0]

        for (const entity of [
            { databaseId: database._id },
            { databaseId: database._id, arrayId: array._id },
            { databaseId: database._id, arrayId: array._id, objectId: object._id }
        ]) await target.DB.updateDistributed({ ...entity, nodeId: source.id, operation: 'add' })

        const result = await target.DB.send({
            method: 'object.read',
            database: 'peer-db',
            array: 'users',
            object: { _id: object._id },
            filter: { key: { name: 1 } }
        })
        assert.equal(result.success, true, result.error)
        assert.equal(result.object[0].secret, undefined)
        assert.deepEqual(await target.DB.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: object._id
        }).then(record => record.secret), { token: 'complete' })

        const requests = target.cluster.sent.filter(message => message.method === 'db.document.request')
        assert.deepEqual(requests.map(message => message.type), ['database', 'array', 'object'])
        assert.ok(requests.every(message => message.server === source.id && message.requestId))
    } finally {
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
        const database = (await server.DB.send({ method: 'database.create', database: 'timeout-db' })).database[0]
        const array = (await server.DB.send({ method: 'array.create', database: 'timeout-db', array: 'items' })).array[0]
        const objectId = '000000000000000000000001'
        await server.DB.updateDistributed({
            databaseId: database._id,
            arrayId: array._id,
            objectId,
            nodeId: 'node-unavailable',
            operation: 'add'
        })

        const result = await server.DB.send({
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
    const deliver = (from, to, payload) => {
        if (payload.broadcast || payload.server === to.id)
            queueMicrotask(() => to.cluster.emit(payload.method, payload))
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
        for (let attempt = 0; attempt < 100; attempt++) {
            if (await check()) return
            await new Promise(resolve => setTimeout(resolve, 10))
        }
        assert.fail('timed out waiting for partition synchronization')
    }

    try {
        const databaseName = 'partition-db'
        const arrayName = 'items'
        const database = (await source.DB.send({ method: 'database.create', database: databaseName })).database[0]
        const array = (await source.DB.send({ method: 'array.create', database: databaseName, array: arrayName })).array[0]
        await source.DB.index.set(databaseName, arrayName, 'score', 4, 'object-1')

        for (const entity of [
            { databaseId: database._id },
            { databaseId: database._id, arrayId: array._id }
        ]) await target.DB.updateDistributed({ ...entity, nodeId: source.id, operation: 'add' })

        for (const { partitionId, nodeIds } of await source.DB.listPartitions())
            for (const nodeId of nodeIds)
                await target.DB.updatePartition({ partitionId, nodeId, operation: 'add' })

        await target.DB.send({ method: 'database.read' })
        await target.DB.send({ method: 'array.read', database: databaseName })

        assert.deepEqual(await collect(target.DB.index.scan(databaseName, arrayName, 'score', { type: 'number' })), [
            { value: 4, objectId: 'object-1' }
        ])

        const partitionId = (await source.DB.listPartitions())
            .find(({ partitionId }) => partitionId.endsWith('/index/index.json')).partitionId
        await waitFor(async () => (await source.DB.getPartitionHolders(partitionId)).includes(target.id))

        const sentBeforeMutation = source.cluster.sent.length
        await source.DB.index.remove(databaseName, arrayName, 'score', 4, 'object-1')
        await source.DB.index.set(databaseName, arrayName, 'score', 9, 'object-1')
        await waitFor(async () => {
            const entries = await collect(target.DB.index.scan(databaseName, arrayName, 'score', { type: 'number' }))
            return entries.length === 1 && entries[0].value === 9
        })
        const updates = source.cluster.sent.slice(sentBeforeMutation)
            .filter(message => message.method === 'db.partition.update')
        assert.ok(updates.length > 0)
        assert.ok(updates.every(message => message.server === target.id))
        await new Promise(resolve => setTimeout(resolve, 50))
    } finally {
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
        const database = (await source.DB.send({ method: 'database.create', database: 'replicated' })).database[0]
        const array = (await source.DB.send({ method: 'array.create', database: 'replicated', array: 'items' })).array[0]
        const created = await source.DB.send({
            method: 'object.create', database: 'replicated', array: 'items', object: { name: 'Ada', score: 1 }
        })
        assert.equal(created.success, true, created.error)
        const object = created.object[0]
        const partitions = (await source.DB.listPartitions()).filter(({ partitionId }) =>
            partitionId.includes(`databases/${database._id}/arrays/${array._id}/index/`))
        assert.ok(partitions.length, JSON.stringify(await source.DB.listPartitions()))

        const [databaseRecord, arrayRecord] = await Promise.all([
            source.DB.readLocalEntity({ type: 'database', databaseId: database._id }),
            source.DB.readLocalEntity({ type: 'array', databaseId: database._id, arrayId: array._id })
        ])
        await target.DB.applyReplicatedObjectMutation({ operation: 'set', database: databaseRecord, array: arrayRecord, object })
        for (const { partitionId, nodeIds } of partitions) {
            await target.DB.applyIndexPartition(partitionId, await source.DB.readIndexPartition(partitionId), { replicated: true })
            for (const nodeId of nodeIds)
                await target.DB.updatePartition({ partitionId, nodeId, operation: 'add' })
            await source.DB.updatePartition({ partitionId, nodeId: target.id, operation: 'add' })
        }

        const second = await source.DB.send({
            method: 'object.create', database: 'replicated', array: 'items', object: { name: 'Ada', score: 1 }
        })
        assert.equal(second.success, true, second.error)
        const replicatedObject = second.object[0]
        await waitFor(async () => (await target.DB.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: replicatedObject._id
        }))?.score === 1)

        const updated = await source.DB.send({
            method: 'object.update', database: 'replicated', array: 'items', object: { _id: replicatedObject._id, score: 2 }
        })
        assert.equal(updated.success, true, updated.error)
        await waitFor(async () => (await target.DB.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: replicatedObject._id
        }))?.score === 2)
        assert.equal((await target.DB.readLocalEntity({
            type: 'array', databaseId: database._id, arrayId: array._id
        })).length, 2)

        const deleted = await source.DB.send({
            method: 'object.delete', database: 'replicated', array: 'items', object: { _id: replicatedObject._id }
        })
        assert.equal(deleted.success, true, deleted.error)
        await waitFor(async () => !(await target.DB.readLocalEntity({
            type: 'object', databaseId: database._id, arrayId: array._id, objectId: replicatedObject._id
        })))
        assert.equal((await target.DB.readLocalEntity({
            type: 'array', databaseId: database._id, arrayId: array._id
        })).length, 1)
        assert.ok(source.cluster.sent.some(message =>
            message.method === 'db.object.mutation' && message.server === target.id))
        await new Promise(resolve => setTimeout(resolve, 50))
    } finally {
        await Promise.all([
            rm(sourceDirectory, { recursive: true, force: true }),
            rm(targetDirectory, { recursive: true, force: true })
        ])
    }
})
