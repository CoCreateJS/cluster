/********************************************************************************
 * Copyright (C) 2023 CoCreate and Contributors.
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

/**
 * LogManager captures console logs and manual server log dispatches across
 * cluster workers, aggregates them on the master orchestrator, and periodically
 * flushes a capped ring buffer of logs to the server object in the database.
 */
export class LogManager {
    constructor(Server) {
        this.server = Server;
        this.logQueue = [];
        this.recentLogs = [];
        this.maxLogs = 100; // Capped ring buffer size on server document
        this.flushIntervalMs = 1500; // Time-window flush in milliseconds
        this.maxBatchSize = 20; // Trigger immediate flush if queue exceeds size
        this.isLoggingInternal = false; // Recursion guard against infinite loops
        this.flushTimer = null;

        // Preserve native console methods for uninhibited terminal stdout/stderr
        this.originalConsole = {
            log: console.log.bind(console),
            info: console.info.bind(console),
            warn: console.warn.bind(console),
            error: console.error.bind(console)
        };

        this.bindMethods();
        this.setupIPCListeners();
        this.patchConsole();
        this.setupFlushTimer();
    }

    bindMethods() {
        this.server.log = this.log.bind(this);
        this.server.flushLogs = this.flushLogs.bind(this);
        this.server.handleIncomingWorkerLog = this.handleIncomingWorkerLog.bind(this);
    }

    setupIPCListeners() {
        // Listen for 'server.log' events on master process dispatched across IPC
        if (this.server.isMaster) {
            this.server.on('server.log', (message) => {
                if (message && message.log) {
                    this.handleIncomingWorkerLog(message.log);
                }
            });
        }
    }

    patchConsole() {
        const self = this;

        const createConsoleProxy = (level, originalFn) => {
            return function (...args) {
                // Always execute original console method to print to standard stdout/stderr
                originalFn.apply(console, args);

                // Skip log capture if internal logging recursion flag is active
                if (self.isLoggingInternal) return;

                try {
                    const message = args.map(arg => {
                        if (typeof arg === 'string') return arg;
                        if (arg instanceof Error) return arg.stack || arg.message;
                        return util.inspect(arg, { depth: 3, colors: false });
                    }).join(' ');

                    self.log(level, message);
                } catch (err) {
                    // Fail gracefully to ensure native console execution is never disrupted
                }
            };
        };

        console.log = createConsoleProxy('info', this.originalConsole.log);
        console.info = createConsoleProxy('info', this.originalConsole.info);
        console.warn = createConsoleProxy('warn', this.originalConsole.warn);
        console.error = createConsoleProxy('error', this.originalConsole.error);
    }

    log(level = 'info', message = '', metadata = null) {
        if (this.isLoggingInternal) return;

        const sourceWorker = this.server.isMaster ? 'master' : (this.server.workerId || 1);

        const logEntry = {
            timestamp: Date.now(),
            workerId: sourceWorker,
            level,
            message,
            ...(metadata && typeof metadata === 'object' ? { metadata } : {})
        };

        if (!this.server.isMaster) {
            // Worker Process: Relay log entry to Master Orchestrator over IPC
            if (typeof this.server.send === 'function') {
                this.server.send({
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
                    // Fallback to original console error if IPC fails
                    this.originalConsole.error('[@cocreate/server] [WORKER LOG IPC ERROR]', err.message);
                }
            }
        } else {
            // Master Process: Append directly to local log queue
            this.enqueueLog(logEntry);
        }
    }

    handleIncomingWorkerLog(logEntry) {
        if (!this.server.isMaster || !logEntry) return;
        this.enqueueLog(logEntry);
    }

    enqueueLog(logEntry) {
        this.logQueue.push(logEntry);

        // Immediately flush if queue hits batch limit or if an error level log is emitted
        if (this.logQueue.length >= this.maxBatchSize || logEntry.level === 'error') {
            this.flushLogs();
        }
    }

    setupFlushTimer() {
        if (!this.server.isMaster) return;

        this.flushTimer = setInterval(() => {
            this.flushLogs();
        }, this.flushIntervalMs);

        // Ensure flush timer doesn't keep node process alive during shutdown
        if (this.flushTimer.unref) {
            this.flushTimer.unref();
        }
    }

    async flushLogs() {
        if (!this.server.isMaster || this.logQueue.length === 0 || this.isLoggingInternal) return;

        this.isLoggingInternal = true;

        try {
            const pendingLogs = [...this.logQueue];
            this.logQueue = [];

            // Maintain capped ring buffer of recent logs on Master
            this.recentLogs = [...this.recentLogs, ...pendingLogs].slice(-this.maxLogs);

            // Persist updated log ring buffer to the server object in DB
            if (this.server.crud && typeof this.server.getServerObject === 'function') {
                const payload = this.server.getServerObject({
                    logs: this.recentLogs
                });

                await this.server.crud.send(payload);
            }
        } catch (err) {
            this.originalConsole.error('[@cocreate/server] [LOG FLUSH ERROR]', err.message);
        } finally {
            this.isLoggingInternal = false;
        }
    }

    destroy() {
        if (this.flushTimer) {
            clearInterval(this.flushTimer);
            this.flushTimer = null;
        }

        console.log = this.originalConsole.log;
        console.info = this.originalConsole.info;
        console.warn = this.originalConsole.warn;
        console.error = this.originalConsole.error;
    }
}

export default {
    init(Server) {
        return new LogManager(Server);
    }
};