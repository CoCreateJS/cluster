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
/**
 * Initializes primary-owned shared storage and a worker IPC proxy API.
 * 
 * @param {Object} server - Core server context instance
 * @returns {Object} Clean storage API instance
 */
export function init(server) {
    const store = server.isPrimary ? new Map() : null;
    const emitter = new EventEmitter();

    async function request(method, data = {}) {
        const response = await server.send({
            method,
            ...data,
            master: true,
            broadcast: false
        });
        if (response.error || response.success === false) throw new Error(response.error);
        return response.result;
    }

    async function respond(payload, operation) {
        try {
            return { ...payload, success: true, result: await operation() };
        } catch (error) {
            return { ...payload, success: false, error: error.message || String(error) };
        }
    }

    if (server.isPrimary) {
        server.on('storage.set', payload => respond(payload, () => {
            store.set(payload.key, payload.value);
            emitter.emit(`change:${payload.key}`, payload.value);
            emitter.emit('change', { key: payload.key, value: payload.value });
            return true;
        }));
        server.on('storage.get', payload => respond(payload, () => store.get(payload.key)));
        server.on('storage.delete', payload => respond(payload, () => {
            const result = store.delete(payload.key);
            emitter.emit(`delete:${payload.key}`);
            emitter.emit('delete', { key: payload.key });
            return result;
        }));
        server.on('storage.size', payload => respond(payload, () => store.size));
        server.on('storage.clear', payload => respond(payload, () => {
            store.clear();
            emitter.emit('clear');
            return true;
        }));
    }

    const storageAPI = {
        /**
         * Sets a key-value pair in cluster storage.
         * @param {string} key - Storage key
         * @param {*} value - Storage value
         * @returns {Promise<boolean>} Resolves when the primary stores the value
         */
        async set(key, value) {
            if (server.isPrimary) {
                store.set(key, value);
                emitter.emit(`change:${key}`, value);
                emitter.emit('change', { key, value });
                return true;
            }
            return request('storage.set', { key, value });
        },

        /**
         * Retrieves a value from cluster storage by key.
         * @param {string} key - Storage key
         * @returns {Promise<*>} Stored value
         */
        async get(key) {
            if (server.isPrimary) return store.get(key);
            return request('storage.get', { key });
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
                return result;
            }
            return request('storage.delete', { key });
        },

        /**
         * Gets the current size of the storage.
         * @returns {Promise<number>} Number of items in storage
         */
        async size() {
            if (server.isPrimary) return store.size;
            return request('storage.size');
        },

        /**
         * Clears all entries in cluster storage.
         * @returns {Promise<boolean>}
         */
        async clear() {
            if (server.isPrimary) {
                store.clear();
                emitter.emit('clear');
                return true;
            }
            return request('storage.clear');
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
