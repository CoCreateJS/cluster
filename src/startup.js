import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';

const setupVersion = '1';
const markerPath = '/var/lib/cocreatews/host-setup-version';
const servicePath = '/etc/systemd/system/cocreatews.service';

function run(command, args, { capture = false } = {}) {
    return execFileSync(command, args, {
        encoding: 'utf8',
        stdio: capture ? ['ignore', 'pipe', 'inherit'] : 'inherit'
    });
}

function waitForService() {
    for (let attempt = 0; attempt < 90; attempt++) {
        try {
            run('systemctl', ['is-active', '--quiet', 'cocreatews']);
            const listeners = run('ss', ['-H', '-lnt'], { capture: true });
            if ([80, 443, 8090].every(port => new RegExp(`:${port}\\s`).test(listeners))) return;
        } catch {}
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2000);
    }
    throw new Error('CoCreate Server did not become ready on ports 80, 443 and 8090.');
}

export function setupHost() {
    const osRelease = existsSync('/etc/os-release') ? readFileSync('/etc/os-release', 'utf8') : '';
    if (process.platform !== 'linux' || !/^ID=ubuntu$/m.test(osRelease)) {
        throw new Error('CoCreate host setup currently supports Ubuntu Linux only.');
    }
    if (process.getuid?.() !== 0) throw new Error('CoCreate host setup requires root privileges.');
    if (existsSync(markerPath) && run('cat', [markerPath], { capture: true }).trim() === setupVersion) return;

    run('apt-get', ['update']);
    run('apt-get', ['install', '-y', 'ufw']);
    run('ufw', ['default', 'deny', 'incoming']);
    run('ufw', ['default', 'allow', 'outgoing']);
    for (const port of [80, 443, 8090]) run('ufw', ['allow', `${port}/tcp`]);
    run('ufw', ['deny', '22/tcp']);
    run('ufw', ['--force', 'enable']);

    for (const unit of ['ssh.service', 'sshd.service', 'ssh.socket']) {
        try { run('systemctl', ['disable', '--now', unit]); } catch {}
        try { run('systemctl', ['mask', unit]); } catch {}
    }

    writeFileSync('/usr/local/bin/cocreatews-start', [
        '#!/usr/bin/env bash', 'set -euo pipefail',
        'cd /root',
        'set -a', '. /etc/cocreatews.environment', 'set +a',
        'export storage="$(cat /etc/cocreatews/storage.json)"',
        'exec /opt/cocreatews/node /opt/cocreatews/index.js'
    ].join('\n') + '\n', { mode: 0o755 });
    writeFileSync(servicePath, [
        '[Unit]', 'Description=CoCreate Server', 'After=network-online.target', 'Wants=network-online.target', '',
        '[Service]', 'Type=simple', 'User=root', 'WorkingDirectory=/root', 'Environment=HOME=/root',
        'ExecStart=/usr/local/bin/cocreatews-start', 'Restart=on-failure', 'RestartSec=5',
        'EnvironmentFile=-/etc/cocreatews.environment', '', '[Install]', 'WantedBy=multi-user.target'
    ].join('\n') + '\n');
    run('systemctl', ['daemon-reload']);
    run('systemctl', ['enable', '--now', 'cocreatews.service']);
    waitForService();

    mkdirSync('/var/lib/cocreatews', { recursive: true });
    writeFileSync(markerPath, `${setupVersion}\n`);
    console.log('CoCreate Ubuntu host setup complete.');
}
