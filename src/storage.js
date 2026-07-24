import { EventEmitter } from 'node:events';
import { uuid } from '@cocreate/utils';

export default class Storage extends EventEmitter {
    constructor(server, pendingPromises) {
        super();
        
        this.server = server;
        this.workerId = server.workerId;
        this.pendingPromises = pendingPromises;
        this.store = new Map();
        
        // Listen for top-down state broadcasts from Master
        if (!this.server.isMaster) {
            this.server.on('cluster_state_sync', (payload) => {
                if (payload && payload.key !== undefined) {
                    if (payload.value === undefined) {
                        this.store.delete(payload.key);
                        this.emit(`delete:${payload.key}`);
                        this.emit('delete', { key: payload.key });
                    } else {
                        this.store.set(payload.key, payload.value);
                        this.emit(`change:${payload.key}`, payload.value);
                        this.emit('change', { key: payload.key, value: payload.value });
                    }
                } else if (payload && payload.clear) {
                    this.store.clear();
                    this.emit('clear');
                }
            });
        }
    }

    async set(key, value) {
        if (this.server.isMaster) {
            this.store.set(key, value);
            this.emit(`change:${key}`, value);
            this.emit('change', { key, value });
            
            this.server.send({ 
                method: 'cluster_state_sync', key, value, broadcast: true, master: false 
            });
            return true;
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                this.pendingPromises.set(messageId, { resolve, reject });
                this.server.send({ workerId: this.workerId, messageId, method: 'cluster_state_set', key, value });
            });
        }
    }

    async get(key) {
        if (this.server.isMaster) {
            return this.store.get(key);
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                this.pendingPromises.set(messageId, { resolve, reject });
                this.server.send({ workerId: this.workerId, messageId, method: 'cluster_state_get', key });
            });
        }
    }

    async delete(key) {
        if (this.server.isMaster) {
            const result = this.store.delete(key);
            this.emit(`delete:${key}`);
            this.emit('delete', { key });
            this.server.send({ method: 'cluster_state_sync', key, value: undefined, broadcast: true, master: false });
            return result;
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                this.pendingPromises.set(messageId, { resolve, reject });
                this.server.send({ workerId: this.workerId, messageId, method: 'cluster_state_delete', key });
            });
        }
    }

    async size() {
        if (this.server.isMaster) return this.store.size;
        return new Promise((resolve, reject) => {
            const messageId = uuid();
            this.pendingPromises.set(messageId, { resolve, reject });
            this.server.send({ workerId: this.workerId, messageId, method: 'cluster_state_size' });
        });
    }

    async clear() {
        if (this.server.isMaster) {
            this.store.clear();
            this.emit('clear');
            this.server.send({ method: 'cluster_state_sync', clear: true, broadcast: true, master: false });
            return true;
        } else {
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                this.pendingPromises.set(messageId, { resolve, reject });
                this.server.send({ workerId: this.workerId, messageId, method: 'cluster_state_clear' });
            });
        }
    }
}