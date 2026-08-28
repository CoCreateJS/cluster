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

import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import os from 'node:os';

const METADATA_IP = '169.254.169.254';
const PROBE_TIMEOUT_MS = 500; // Fast timeout for parallel background probes

/* STREAMING_CHUNK: Structuring the native network request wrapper... */
/**
 * Native, lightweight HTTP request wrapper supporting standard GET/PUT/POST operations.
 */
function request(options, data = null, useHttps = false) {
    const client = useHttps ? https : http;
    return new Promise((resolve, reject) => {
        const req = client.request(options, (res) => {
            if (res.statusCode < 200 || res.statusCode >= 300) {
                reject(new Error(`Status Code: ${res.statusCode}`));
                return;
            }
            let body = '';
            res.setEncoding('utf8');
            res.on('data', (chunk) => { body += chunk; });
            res.on('end', () => { resolve(body); });
        });

        req.on('error', (err) => { reject(err); });
        req.setTimeout(PROBE_TIMEOUT_MS, () => {
            req.destroy();
            reject(new Error('Request Timeout'));
        });

        if (data) {
            req.write(data);
        }
        req.end();
    });
}

/* STREAMING_CHUNK: Configuring AWS metadata IMDSv2 probe... */
/**
 * AWS IMDSv2 Probe: Acquires session token and retrieves instance metadata.
 */
async function probeAWS() {
    const token = await request({
        host: METADATA_IP,
        path: '/latest/api/token',
        method: 'PUT',
        headers: { 'X-aws-ec2-metadata-token-ttl-seconds': '60' }
    });

    const docStr = await request({
        host: METADATA_IP,
        path: '/latest/dynamic/instance-identity/document',
        method: 'GET',
        headers: { 'X-aws-ec2-metadata-token': token }
    });

    const doc = JSON.parse(docStr);
    return {
        provider: 'aws',
        region: doc.region,
        zone: doc.availabilityZone,
        vmId: doc.instanceId,
        vmType: doc.instanceType
    };
}

/* STREAMING_CHUNK: Configuring Google Cloud (GCP) metadata probe... */
/**
 * GCP Metadata Probe: Requests instance data with the mandatory Flavor header.
 */
async function probeGCP() {
    const docStr = await request({
        host: 'metadata.google.internal',
        path: '/computeMetadata/v1/instance/?recursive=true',
        method: 'GET',
        headers: { 'Metadata-Flavor': 'Google' }
    });

    const doc = JSON.parse(docStr);
    const zone = doc.zone ? doc.zone.split('/').pop() : 'unknown';
    const region = zone.slice(0, zone.lastIndexOf('-'));
    const vmType = doc.machineType ? doc.machineType.split('/').pop() : 'unknown';

    return {
        provider: 'gcp',
        region,
        zone,
        vmId: doc.id ? String(doc.id) : 'unknown',
        vmType
    };
}

/* STREAMING_CHUNK: Configuring Azure metadata probe... */
/**
 * Microsoft Azure Probe: Requests metadata using the validation header.
 */
async function probeAzure() {
    const docStr = await request({
        host: METADATA_IP,
        path: '/metadata/instance?api-version=2021-02-01',
        method: 'GET',
        headers: { 'Metadata': 'true' }
    });

    const doc = JSON.parse(docStr);
    const compute = doc.compute || {};

    return {
        provider: 'azure',
        region: compute.location || 'unknown',
        zone: compute.zone || 'unknown',
        vmId: compute.vmId || 'unknown',
        vmType: compute.vmSize || 'unknown'
    };
}

/* STREAMING_CHUNK: Configuring DigitalOcean metadata probe... */
/**
 * DigitalOcean Probe: Requests droplet metadata configuration.
 */
async function probeDigitalOcean() {
    const docStr = await request({
        host: METADATA_IP,
        path: '/metadata/v1.json',
        method: 'GET'
    });

    const doc = JSON.parse(docStr);
    return {
        provider: 'digitalocean',
        region: doc.region || 'unknown',
        zone: doc.region ? `${doc.region}-a` : 'unknown',
        vmId: doc.droplet_id ? String(doc.droplet_id) : 'unknown',
        vmType: doc.size || 'unknown'
    };
}

/* STREAMING_CHUNK: Configuring Oracle Cloud (OCI) metadata probe... */
/**
 * Oracle Cloud (OCI) Probe: Validates via the Bearer Token header required by IMDSv2.
 */
async function probeOCI() {
    const docStr = await request({
        host: METADATA_IP,
        path: '/opc/v2/instance/',
        method: 'GET',
        headers: { 'Authorization': 'Bearer Oracle' }
    });

    const doc = JSON.parse(docStr);
    return {
        provider: 'oracle',
        region: doc.region || 'unknown',
        zone: doc.faultDomain || 'unknown',
        vmId: doc.id || 'unknown',
        vmType: doc.shape || 'unknown'
    };
}

/* STREAMING_CHUNK: Configuring Hetzner Cloud metadata probe... */
/**
 * Hetzner Cloud Probe: Requests basic instance parameters on Hetzner routes.
 */
async function probeHetzner() {
    const docStr = await request({
        host: METADATA_IP,
        path: '/hetzner/v1/metadata',
        method: 'GET'
    });

    const lines = docStr.split('\n');
    const meta = {};
    for (const line of lines) {
        const splitIdx = line.indexOf(':');
        if (splitIdx !== -1) {
            const key = line.slice(0, splitIdx).trim();
            const val = line.slice(splitIdx + 1).trim();
            meta[key] = val;
        }
    }

    const zone = meta['availability-zone'] || 'unknown';
    const region = meta['region'] || (zone ? zone.slice(0, zone.lastIndexOf('-')) : 'unknown');

    return {
        provider: 'hetzner',
        region,
        zone,
        vmId: meta['instance-id'] || 'unknown',
        vmType: 'cloud-vm'
    };
}

/* STREAMING_CHUNK: Configuring Vultr and Equinix metadata probes... */
/**
 * Vultr Probe: Pulls metadata configurations from standard OpenStack mapping paths.
 */
async function probeVultr() {
    const idStr = await request({
        host: METADATA_IP,
        path: '/v1/instance-v2-id',
        method: 'GET'
    });

    const hostStr = await request({
        host: METADATA_IP,
        path: '/v1/hostname',
        method: 'GET'
    });

    return {
        provider: 'vultr',
        region: 'vultr-global',
        zone: 'vultr-zone',
        vmId: idStr ? idStr.trim() : 'unknown',
        vmType: hostStr ? hostStr.trim() : 'vultr-instance'
    };
}

/**
 * Equinix Metal Probe: Requests local bare metal profile metadata via direct secure gateway.
 */
async function probeEquinix() {
    const docStr = await request({
        host: 'metadata.platformequinix.com',
        path: '/metadata',
        method: 'GET'
    }, null, true);

    const doc = JSON.parse(docStr);
    return {
        provider: 'equinix-metal',
        region: doc.metro || doc.facility || 'unknown',
        zone: doc.facility || 'unknown',
        vmId: doc.id || 'unknown',
        vmType: doc.plan || 'baremetal-metal'
    };
}

/* STREAMING_CHUNK: Resolving local physical hardware profiles... */
/**
 * Safely inspects Linux OS profiles to identify local Bare-Metal / Physical system models.
 */
function resolveBareMetalIdentity() {
    try {
        if (fs.existsSync('/sys/class/dmi/id/product_name')) {
            const productName = fs.readFileSync('/sys/class/dmi/id/product_name', 'utf8').trim();
            const vendorName = fs.existsSync('/sys/class/dmi/id/sys_vendor')
                ? fs.readFileSync('/sys/class/dmi/id/sys_vendor', 'utf8').trim()
                : 'BareMetal';

            let isVirtual = false;
            const virtualClues = ['qemu', 'kvm', 'virtualbox', 'vmware', 'xen'];
            for (const clue of virtualClues) {
                if (productName.toLowerCase().includes(clue) || vendorName.toLowerCase().includes(clue)) {
                    isVirtual = true;
                }
            }

            if (!isVirtual) {
                return {
                    provider: 'baremetal',
                    region: 'on-premise',
                    zone: 'on-premise-datacenter',
                    vmId: productName.replace(/\s+/g, '-').toLowerCase(),
                    vmType: `${vendorName}-${productName}`.replace(/\s+/g, '-').toLowerCase()
                };
            }
        }
    } catch (err) {
        // Non-blocking fallback
    }
    return null;
}

/* STREAMING_CHUNK: Running the infallible infrastructure resolver... */
/**
 * Dynamically resolves the host's physical infrastructure environment.
 * Runs exactly once during the server boot sequence. Guaranteed to never throw.
 * 
 * @param {string} masterVmId - The pre-configured fallback instance ID
 * @returns {Promise<Object>} Resolved server.infrastructure payload
 */
export async function getInfrastructure(masterVmId) {
    const localFallback = {
        provider: 'local',
        region: 'local',
        zone: 'local',
        vmId: masterVmId,
        vmType: 'local'
    };

    try {
        // Phase 1: Concurrently race cloud metadata IMDS probes
        try {
            const cloudMetadata = await Promise.any([
                probeAWS(),
                probeGCP(),
                probeAzure(),
                probeDigitalOcean(),
                probeOCI(),
                probeHetzner(),
                probeVultr(),
                probeEquinix()
            ]);
            if (cloudMetadata) {
                return cloudMetadata;
            }
        } catch (err) {
            // Proceed to local sweeps
        }

        // Phase 2: Detect local On-Prem Bare Metal Hardware
        try {
            const bareMetalInfo = resolveBareMetalIdentity();
            if (bareMetalInfo) {
                bareMetalInfo.instanceId = masterVmId;
                return bareMetalInfo;
            }
        } catch (err) {
            // Proceed to env checks
        }

        // Phase 3: Check Environment Variable Overrides
        if (process.env.PROVIDER) {
            return {
                provider: process.env.PROVIDER,
                region: process.env.REGION || 'unknown',
                zone: process.env.ZONE || 'unknown',
                vmId: process.env.VM_ID || masterVmId,
                vmType: process.env.VM_TYPE || 'unknown'
            };
        }

    } catch (globalErr) {
        // Suppress any unexpected error contexts and fall back cleanly
    }

    // Phase 4: Developer Machine Fallback
    return localFallback;
}

export default getInfrastructure;