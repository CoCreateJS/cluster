import fs from 'node:fs';
import path from 'node:path';
import tls from 'node:tls';
import { execSync } from 'node:child_process';

// Module-level cache for SSL secure contexts to avoid re-reading disk on every socket request
const certCache = new Map();

/**
 * Checks whether a given host string represents localhost or a local loopback IP address.
 * @param {string} host - The requested hostname.
 * @returns {boolean} True if host is local.
 */
function isLocalHost(host) {
    if (!host) return false;
    const hostname = host.split(':')[0];
    return (
        hostname === 'localhost' ||
        hostname === '127.0.0.1' ||
        hostname === '::1' ||
        hostname.endsWith('.localhost')
    );
}

/**
 * Ensures that a local self-signed certificate and private key exist in `.certificates/<domain>`.
 * If missing, generates them dynamically using OpenSSL.
 * 
 * @param {string} domain - Host domain name (e.g. 'localhost').
 * @returns {{ keyPath: string, certPath: string }} Paths to private key and fullchain cert.
 */
function ensureLocalSelfSignedCert(domain) {
    const certDir = path.join(process.cwd(), '.certificates', domain);
    const keyPath = path.join(certDir, 'private-key.pem');
    const certPath = path.join(certDir, 'fullchain.pem');

    if (!fs.existsSync(keyPath) || !fs.existsSync(certPath)) {
        fs.mkdirSync(certDir, { recursive: true });
        
        try {
            const cmd = `openssl req -x509 -newkey rsa:2048 -keyout "${keyPath}" -out "${certPath}" -days 365 -nodes -subj "/CN=${domain}"`;
            execSync(cmd, { stdio: 'pipe' });
        } catch (err) {
            console.error(`[@cocreate/server] [SSL/TLS] OpenSSL generation failed for "${domain}":`, err.message);
            throw err;
        }
    }

    return { keyPath, certPath };
}

/**
 * TLSManager encapsulates certificate generation, localhost SSL bypassing,
 * and standard public domain ACME certificate resolution via SNICallback.
 */
export class TLSManager {
    constructor() {
        this.server = null;
    }

    /**
     * Initializes the TLS module with the server context.
     * @param {Object} server - CoCreateServer instance.
     * @returns {TLSManager} Self instance.
     */
    init(server) {
        this.server = server;
        return this;
    }

    /**
     * Checks if host is localhost or local loopback.
     * @param {string} host - Requested hostname.
     * @returns {boolean}
     */
    isLocalHost(host) {
        return isLocalHost(host);
    }

    /**
     * Creates standard HTTPS options with an automated SNICallback handler.
     * Completely bypasses ACME for localhost requests, using native Node TLS contexts.
     * @returns {Object} Options object suitable for Https.createServer().
     */
    createHttpsOptions() {
        // Ensure default local self-signed cert exists as initial fallback context for Node TLS
        const { keyPath, certPath } = ensureLocalSelfSignedCert('localhost');
        const defaultKey = fs.readFileSync(keyPath);
        const defaultCert = fs.readFileSync(certPath);

        return {
            key: defaultKey,
            cert: defaultCert,
            SNICallback: async (domain, cb) => {
                const cleanDomain = domain ? domain.split(':')[0] : 'localhost';

                // Localhost route: Completely bypass ACME and use native fs + cached tls context
                if (isLocalHost(cleanDomain)) {
                    try {
                        let sslContext = certCache.get(cleanDomain);
                        if (!sslContext) {
                            const { keyPath: localKey, certPath: localCert } = ensureLocalSelfSignedCert(cleanDomain);

                            const key = fs.readFileSync(localKey);
                            const cert = fs.readFileSync(localCert);

                            sslContext = tls.createSecureContext({ key, cert });
                            certCache.set(cleanDomain, sslContext);
                        }
                        return cb(null, sslContext);
                    } catch (err) {
                        console.error(`[@cocreate/server] [SSL/TLS] Failed to load local self-signed cert for ${cleanDomain}:`, err.message);
                        return cb(err);
                    }
                }

                // Public domain route: Delegate to ACME certificate manager
                if (this.server && this.server.certificate && typeof this.server.certificate.sniCallback === 'function') {
                    return this.server.certificate.sniCallback(domain, cb);
                }

                const err = new Error(`[@cocreate/server] [SSL/TLS] No ACME certificate manager available for public domain "${cleanDomain}"`);
                console.error(err.message);
                return cb(err);
            }
        };
    }
}

export default new TLSManager();