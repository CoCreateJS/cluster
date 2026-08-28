/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 ********************************************************************************/

import { EventEmitter } from 'node:events';
import { uuid } from '@cocreate/utils';

/**
 * Initializes distributed storage for cluster workers and returns a clean API object.
 * 
 * @param {Object} server - Core server context instance
 * @returns {Object} Clean storage API instance
 */
export function init(server) {
    const store = new Map();
    const emitter = new EventEmitter();
    const workerId = server?.workerId || 1;
    const pendingPromises = server?.cluster?.pendingPromises;

    if (server && !server.isPrimary) {
        server.on('cluster_state_sync', (payload) => {
            if (payload && payload.key !== undefined) {
                if (payload.value === undefined) {
                    store.delete(payload.key);
                    emitter.emit(`delete:${payload.key}`);
                    emitter.emit('delete', { key: payload.key });
                } else {
                    store.set(payload.key, payload.value);
                    emitter.emit(`change:${payload.key}`, payload.value);
                    emitter.emit('change', { key: payload.key, value: payload.value });
                }
            } else if (payload && payload.clear) {
                store.clear();
                emitter.emit('clear');
            }
        });
    }

    const storageAPI = {
        /**
         * Sets a key-value pair in cluster storage.
         * @param {string} key - Storage key
         * @param {*} value - Storage value
         * @returns {Promise<boolean>} Resolves when state is set or synced
         */
        async set(key, value) {
            if (server.isPrimary) {
                store.set(key, value);
                emitter.emit(`change:${key}`, value);
                emitter.emit('change', { key, value });

                server.send({
                    method: 'cluster_state_sync',
                    key,
                    value,
                    broadcast: true,
                    master: false
                });
                return true;
            } else {
                return new Promise((resolve, reject) => {
                    const messageId = uuid();
                    if (pendingPromises) {
                        pendingPromises.set(messageId, { resolve, reject });
                    }
                    server.send({
                        workerId,
                        messageId,
                        method: 'cluster_state_set',
                        key,
                        value
                    });
                });
            }
        },

        /**
         * Retrieves a value from cluster storage by key.
         * @param {string} key - Storage key
         * @returns {Promise<*>} Stored value
         */
        async get(key) {
            if (server.isPrimary) {
                return store.get(key);
            } else {
                return new Promise((resolve, reject) => {
                    const messageId = uuid();
                    if (pendingPromises) {
                        pendingPromises.set(messageId, { resolve, reject });
                    }
                    server.send({
                        workerId,
                        messageId,
                        method: 'cluster_state_get',
                        key
                    });
                });
            }
        },

        /**
         * Deletes a key from cluster storage.
         * @param {string} key - Storage key
         * @returns {Promise<boolean>} True if key existed and was deleted
         */
        async delete(key) {
            if (server.isPrimary) {
                const result = store.delete(key);
                emitter.emit(`delete:${key}`);
                emitter.emit('delete', { key });
                server.send({
                    method: 'cluster_state_sync',
                    key,
                    value: undefined,
                    broadcast: true,
                    master: false
                });
                return result;
            } else {
                return new Promise((resolve, reject) => {
                    const messageId = uuid();
                    if (pendingPromises) {
                        pendingPromises.set(messageId, { resolve, reject });
                    }
                    server.send({
                        workerId,
                        messageId,
                        method: 'cluster_state_delete',
                        key
                    });
                });
            }
        },

        /**
         * Gets the current size of the storage.
         * @returns {Promise<number>} Number of items in storage
         */
        async size() {
            if (server.isPrimary) return store.size;
            return new Promise((resolve, reject) => {
                const messageId = uuid();
                if (pendingPromises) {
                    pendingPromises.set(messageId, { resolve, reject });
                }
                server.send({
                    workerId,
                    messageId,
                    method: 'cluster_state_size'
                });
            });
        },

        /**
         * Clears all entries in cluster storage.
         * @returns {Promise<boolean>}
         */
        async clear() {
            if (server.isPrimary) {
                store.clear();
                emitter.emit('clear');
                server.send({
                    method: 'cluster_state_sync',
                    clear: true,
                    broadcast: true,
                    master: false
                });
                return true;
            } else {
                return new Promise((resolve, reject) => {
                    const messageId = uuid();
                    if (pendingPromises) {
                        pendingPromises.set(messageId, { resolve, reject });
                    }
                    server.send({
                        workerId,
                        messageId,
                        method: 'cluster_state_clear'
                    });
                });
            }
        },

        /**
         * Subscribes to state change/deletion events.
         */
        on(event, listener) {
            emitter.on(event, listener);
            return storageAPI;
        },

        /**
         * Removes an event subscription.
         */
        off(event, listener) {
            emitter.off(event, listener);
            return storageAPI;
        },

        /**
         * Subscribes once to a state change/deletion event.
         */
        once(event, listener) {
            emitter.once(event, listener);
            return storageAPI;
        }
    };

    return storageAPI;
}

export default {
    init
};