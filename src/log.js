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

import util from 'node:util';
import config from '@cocreate/config';
import { dotNotationToObject } from '@cocreate/utils';

let server = null;
let logQueue = [];
let recentLogs = [];
let isLoggingInternal = false; // Recursion guard against infinite loops
let flushTimer = null;

let logConfig = {
    maxLogs: 100, // Capped ring buffer size on server document
    flushIntervalMs: 1500, // Time-window flush in milliseconds
    maxBatchSize: 20 // Trigger immediate flush if queue exceeds size
};

// Preserve native console methods for uninhibited terminal stdout/stderr
const originalConsole = {
    log: console.log.bind(console),
    info: console.info.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console)
};

/**
 * Loads system configuration using @cocreate/config and queries database overrides
 * for 'logManager' or 'logger' configurations.
 * 
 * @returns {Promise<Object>} Resolved logging configuration object
 */
export async function loadLogConfig() {
    const sysConfig = await config.get('logger');

    let dbConfig = null;
    if (server && server.organization_id) {
        dbConfig = await config.query({
            type: 'logger',
            organization_id: server.organization_id,
            server: server.host,
            cluster: server.cluster_id
        });
    }

    if (sysConfig) {
        dotNotationToObject(logConfig, sysConfig);
    }

    if (dbConfig) {
        dotNotationToObject(logConfig, dbConfig);
    }

    return logConfig;
}

/**
 * Attaches IPC event listeners on the master process to receive logs from workers.
 */
function setupIPCListeners() {
    if (server?.isMaster) {
        server.on('server.log', (message) => {
            if (message && message.log) {
                handleIncomingWorkerLog(message.log);
            }
        });
    }
}

/**
 * Patches global console log/info/warn/error methods to capture stdout/stderr output.
 */
function patchConsole() {
    const createConsoleProxy = (level, originalFn) => {
        return function (...args) {
            // Always execute original console method to print to standard stdout/stderr
            originalFn.apply(console, args);

            // Skip log capture if internal logging recursion flag is active
            if (isLoggingInternal) return;

            try {
                const message = args.map(arg => {
                    if (typeof arg === 'string') return arg;
                    if (arg instanceof Error) return arg.stack || arg.message;
                    return util.inspect(arg, { depth: 3, colors: false });
                }).join(' ');

                log(level, message);
            } catch (err) {
                // Fail gracefully to ensure native console execution is never disrupted
            }
        };
    };

    console.log = createConsoleProxy('info', originalConsole.log);
    console.info = createConsoleProxy('info', originalConsole.info);
    console.warn = createConsoleProxy('warn', originalConsole.warn);
    console.error = createConsoleProxy('error', originalConsole.error);
}

/**
 * Dispatches a log entry from either master or worker process.
 * 
 * @param {string} level - Log severity level ('info', 'warn', 'error')
 * @param {string} message - Formatted log message text
 * @param {Object|null} metadata - Optional contextual metadata object
 */
export function log(level = 'info', message = '', metadata = null) {
    if (isLoggingInternal) return;

    const sourceWorker = server?.isMaster ? 'master' : (server?.workerId || 1);

    const logEntry = {
        timestamp: Date.now(),
        workerId: sourceWorker,
        level,
        message,
        ...(metadata && typeof metadata === 'object' ? { metadata } : {})
    };

    if (!server?.isMaster) {
        // Worker Process: Relay log entry to Master Orchestrator over IPC
        if (typeof server?.send === 'function') {
            server.send({
                method: 'server.log',
                log: logEntry
            });
        } else if (process.send && process.connected) {
            try {
                process.send({
                    method: 'server.log',
                    log: logEntry
                });
            } catch (err) {
                originalConsole.error('[@cocreate/server] [WORKER LOG IPC ERROR]', err.message);
            }
        }
    } else {
        // Master Process: Append directly to local log queue
        enqueueLog(logEntry);
    }
}

/**
 * Handles incoming IPC log entries dispatched from cluster workers.
 * 
 * @param {Object} logEntry - Received log object
 */
export function handleIncomingWorkerLog(logEntry) {
    if (!server?.isMaster || !logEntry) return;
    enqueueLog(logEntry);
}

/**
 * Enqueues a log entry into the processing queue and checks threshold conditions.
 * 
 * @param {Object} logEntry - Log item to queue
 */
export function enqueueLog(logEntry) {
    logQueue.push(logEntry);

    // Immediately flush if queue hits batch limit or if an error level log is emitted
    if (logQueue.length >= logConfig.maxBatchSize || logEntry.level === 'error') {
        flushLogs();
    }
}

/**
 * Configures interval timer for periodic buffer flushing on the master node.
 */
function setupFlushTimer() {
    if (!server?.isMaster) return;

    if (flushTimer) {
        clearInterval(flushTimer);
    }

    flushTimer = setInterval(() => {
        flushLogs();
    }, logConfig.flushIntervalMs);

    if (flushTimer.unref) {
        flushTimer.unref();
    }
}

/**
 * Flushes pending log items from the queue to the database server object ring buffer.
 */
export async function flushLogs() {
    if (!server?.isMaster || logQueue.length === 0 || isLoggingInternal) return;

    isLoggingInternal = true;

    try {
        const pendingLogs = [...logQueue];
        logQueue = [];

        // Maintain capped ring buffer of recent logs on Master
        recentLogs = [...recentLogs, ...pendingLogs].slice(-logConfig.maxLogs);

        // Persist updated log ring buffer to the server object in DB
        if (server.crud && typeof server.getServerObject === 'function') {
            const payload = server.getServerObject({
                logs: recentLogs
            });

            await server.crud.send(payload);
        }
    } catch (err) {
        originalConsole.error('[@cocreate/server] [LOG FLUSH ERROR]', err.message);
    } finally {
        isLoggingInternal = false;
    }
}

/**
 * Restores original console behavior and clears flush timers.
 */
export function destroy() {
    if (flushTimer) {
        clearInterval(flushTimer);
        flushTimer = null;
    }

    console.log = originalConsole.log;
    console.info = originalConsole.info;
    console.warn = originalConsole.warn;
    console.error = originalConsole.error;
}

/**
 * Initializes the LogManager service.
 * 
 * @param {Object} serverInstance - Core server instance
 * @returns {Promise<Object>} Initialized module functions and data references
 */
export async function init(serverInstance) {
    server = serverInstance;
    server.log = log;

    await loadLogConfig();
    setupIPCListeners();
    patchConsole();
    setupFlushTimer();

    return {
        config: logConfig,
        logQueue,
        recentLogs,
        log,
        flushLogs,
        destroy
    };
}

export default {
    init
};