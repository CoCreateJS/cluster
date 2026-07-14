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
 ********************************************************************************/

import https from 'node:https';
import os from 'node:os';

// Prioritized list of reliable public IP lookup APIs
const IP_PROVIDERS = [
    'https://api.ipify.org',
    'https://icanhazip.com',
    'https://ifconfig.me/ip',
    'https://ipinfo.io/ip'
];

/**
 * Initiates an HTTPS GET request to a specific IP provider with a hard timeout.
 * 
 * @param {string} url - The URL of the IP lookup provider 
 * @param {number} timeoutMs - Max execution limit before canceling the promise
 * @returns {Promise<string>} - Resolves with the plain-text IP address
 */
function fetchIpFromProvider(url, timeoutMs = 3000) {
    return new Promise((resolve, reject) => {
        const req = https.get(url, (res) => {
            if (res.statusCode !== 200) {
                reject(new Error(`Non-200 response code: ${res.statusCode}`));
                return;
            }

            let data = '';
            res.on('data', (chunk) => { data += chunk; });

            res.on('end', () => {
                const cleanIp = data.trim();
                const ipv4Regex = /^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$/;
                if (ipv4Regex.test(cleanIp)) {
                    resolve(cleanIp);
                } else {
                    reject(new Error(`Returned invalid IP structure: "${cleanIp}"`));
                }
            });
        });

        req.on('error', (err) => { reject(err); });

        req.setTimeout(timeoutMs, () => {
            req.destroy();
            reject(new Error(`Request timed out`));
        });
    });
}

/**
 * Scans the local machine's physical network adapters to find the primary local IP.
 * Used as a fallback if the machine has no active internet connection or is fully offline.
 * 
 * @returns {string} - Returns the first valid local IPv4 address, or '127.0.0.1'
 */
export function getLocalIPFallback() {
    try {
        const interfaces = os.networkInterfaces();
        for (const devName in interfaces) {
            const face = interfaces[devName];
            for (let i = 0; i < face.length; i++) {
                const alias = face[i];
                // Check for standard IPv4 and exclude typical loopbacks
                if (alias.family === 'IPv4' && alias.address !== '127.0.0.1' && !alias.internal) {
                    return alias.address;
                }
            }
        }
    } catch (err) {
        // Safe, non-blocking fallback context
    }
    return '127.0.0.1';
}

/**
 * Robust universal public IP resolution function.
 * Iterates through highly redundant public IP lookup services.
 * Guaranteed to never throw errors.
 * 
 * @returns {Promise<string>} - The resolved public IP (or local IP if completely offline)
 */
export async function getIp() {
    for (const url of IP_PROVIDERS) {
        try {
            const ip = await fetchIpFromProvider(url);
            return ip;
        } catch (error) {
            // Suppress fallback exceptions to attempt the next endpoint seamlessly
        }
    }

    // Offline / Air-Gapped Fallback
    return getLocalIPFallback();
}

export default getIp;