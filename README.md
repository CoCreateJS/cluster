# CoCreate Server

A secure, stateless, clustered headless server built for high-performance multi-tenancy, real-time collaboration, dynamic process orchestration, and distributed mesh execution. This module serves as the primary orchestrator of the CoCreate ecosystem.

---

## Documentation

For a complete guide, deployment examples, and API reference, visit the **CoCreate Server Documentation**.

## Table of Contents

- [Architectural Overview & Core Design](#architectural-overview--core-design)
- [Core Server Modules](#core-server-modules)
- [Component Mesh Architecture](#component-mesh-architecture)
- [Programmatic API Reference](#programmatic-api-reference)
- [Ingress & Connection Controls](#ingress--connection-controls)
- [Process & OS Lifecycle Management](#process--os-lifecycle-management)
- [IPC Transport Engine](#ipc-transport-engine)
- [Reactive Cluster Storage Manager](#reactive-cluster-storage-manager)
- [Distributed Cluster Orchestration](#distributed-cluster-orchestration)
- [Targeting Matrix & Routing Directives](#targeting-matrix--routing-directives)
- [Self-Healing & Jittered Reconnection Logic](#self-healing--jittered-reconnection-logic)
- [Dynamic Tenant Eviction & Cascading Teardown Sequence](#dynamic-tenant-eviction--cascading-teardown-sequence)
- [Security-Hardened Design](#security-hardened-design)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Announcements](#announcements)
- [Roadmap](#roadmap)
- [How to Contribute](#how-to-contribute)
- [About](#about)
- [License](#license)

---

## Architectural Overview & Core Design

`@cocreate/server` is engineered to run stateless, highly available workloads across multi-core systems and distributed cloud infrastructure.

### Clustered IPC Architecture

The server operates a single **Primary** process that forks and manages worker processes across CPU cores.

The Primary process is responsible for:

- Cluster orchestration
- Worker lifecycle management
- Fallback HTTP/HTTPS listeners
- Inter-process communication (IPC)

Each Worker runs an identical application stack handling:

- HTTP requests
- WebSocket connections
- API routing
- Database operations
- Static asset delivery

$$
\text{Primary Process}
\xrightarrow{\text{Native IPC}}
\begin{cases}
\text{Worker 1}\\
\text{Worker 2}\\
\vdots\\
\text{Worker N}
\end{cases}
$$

### Zero Dependency Core

The server minimizes startup time, memory usage, and supply-chain risk by maintaining a lightweight dependency footprint while composing functionality through CoCreate modules.

### Automatic Fallback & Cordoning

When every worker enters draining or cordoned mode, the Primary process automatically serves HTTP **307 Temporary Redirect** responses so incoming requests can immediately reconnect through external load balancers without downtime.

---

## Core Server Modules

| Module | Purpose |
| --- | --- |
| `index.js` | Primary server orchestrator |
| `ipc.js` | Inter-process communication engine |
| `storage.js` | Cluster-wide reactive storage |
| `getInfrastructure.js` | Infrastructure detection |
| `getIp.js` | Public and private IP resolution |

---

## Component Mesh Architecture

The server composes multiple standalone CoCreate modules into a unified runtime.

| Module | Responsibility |
| --- | --- |
| `@cocreate/acme` | TLS certificate management |
| `@cocreate/socket-server` | WebSocket server |
| `@cocreate/server-mesh` | Cluster communication |
| `@cocreate/crud-server` | Database routing |
| `@cocreate/server-autoscaler` | Automatic scaling |
| `@cocreate/server-telemetry` | Monitoring and metrics |
| `@cocreate/api` | Dynamic API execution |
| `@cocreate/file-server` | Static file serving |
| `@cocreate/lazy-loader` | Lazy loading and webhooks |

---

## Programmatic API Reference

Every server instance extends Node.js `EventEmitter`.

### Instance Properties

| Property | Description |
| --- | --- |
| `id` | Unique server identifier |
| `isPrimary` | Indicates Primary process |
| `workerId` | Worker identifier |
| `totalWorkers` | Number of workers |
| `state` | Current server state |
| `storage` / `store` | Reactive cluster storage |
| `send()` | IPC message dispatcher |

### Status Events

```javascript
server.on("status_change", ({ oldStatus, newStatus }) => {
	console.log(`State changed from ${oldStatus} to ${newStatus}`);
});
```

---

## Ingress & Connection Controls

### `server.cordon(value)`

Enable or disable maintenance mode.

```javascript
server.cordon(true);

server.emit("cordon", {
	value: true
});

server.cordon(false);
```

---

### `server.shed(organizations)`

Drain selected organizations.

```javascript
server.shed([
	"org_abc123",
	"org_xyz789"
]);

server.shed(false);
```

---

### `server.drain(value)`

Gracefully disconnect active clients.

```javascript
server.drain(true);

server.drain(false);
```

---

## Process & OS Lifecycle Management

### Restart

```javascript
server.restart();

server.restart({
	isPrimary: true
});
```

Worker restart:

- Drains connections
- Exits gracefully
- Primary automatically forks replacement

Primary restart:

- Closes cluster services
- Exits cleanly
- System service restarts application

---

### Reboot

```javascript
server.reboot();
```

Gracefully reboots the operating system after draining connections.

---

### Shutdown

```javascript
server.shutdown();
```

Gracefully powers off the machine after draining connections.

---

## IPC Transport Engine

The IPC subsystem synchronizes Primary and Worker processes.

Broadcast:

```javascript
server.send({
	method: "custom_worker_event",
	broadcast: true,
	data: {
		status: "ok"
	}
});
```

Target worker:

```javascript
server.send({
	method: "restart",
	worker: 2
});
```

Multiple workers:

```javascript
server.send({
	method: "drain",
	workers: [1, 3]
});
```

---

## Reactive Cluster Storage Manager

Cluster-wide synchronized key/value storage.

```javascript
await server.storage.set(
	"node_capacity",
	{
		availableSlots: 45
	}
);

const capacity =
	await server.storage.get(
		"node_capacity"
	);

const totalKeys =
	await server.storage.size();

await server.storage.delete(
	"node_capacity"
);

await server.storage.clear();
```

Reactive listeners:

```javascript
server.storage.on(
	"change:node_capacity",
	(newValue) => {
		console.log(newValue);
	}
);

server.storage.on(
	"delete:node_capacity",
	() => {
		console.log("Deleted");
	}
);
```

---

## Distributed Cluster Orchestration

Cluster communication is provided by `@cocreate/server-mesh`.

```javascript
server.mesh.send({
	method: "mesh.custom_command",
	server: "server_b_id",
	data: {
		key: "value"
	}
});
```

---

## Targeting Matrix & Routing Directives

| Target | Example |
| --- | --- |
| Single server | `{ server: "server_b_id" }` |
| Multiple servers | `{ server: ["server_b_id","server_c_id"] }` |
| Worker | `{ server: "server_b_id", worker: 2 }` |
| Broadcast | `{ broadcast: true }` |

---

## Self-Healing & Jittered Reconnection Logic

Peer reconnections use exponential backoff with randomized jitter.

$$
T_{delay}
=
\min
\left(
30000,
1000\times1.5^{attempt}
\right)
+
random(500,4500)
$$

This prevents reconnect storms after temporary outages.

---

## Dynamic Tenant Eviction & Cascading Teardown Sequence

Tenant lifecycle is driven by active socket connections.

$$
\text{Client Disconnect}
\rightarrow
\text{60 Second Grace Period}
\rightarrow
\text{Organization Removed}
$$

Once the final client disconnects:

1. A 60-second timer begins.
2. Workers release tenant resources.
3. Peer servers receive notification.
4. Database pools, telemetry buffers, and mesh tunnels are released.

---

## Security-Hardened Design

- Mutual TLS encrypted cluster communication
- Dedicated internal mesh network
- Automatic UUID generation for distributed tracing
- Ephemeral TLS certificates managed by `@cocreate/acme`

---

## Installation

### NPM

```bash
npm install @cocreate/server
```

### Yarn

```bash
yarn add @cocreate/server
```

---

## Quick Start

```javascript
import CoCreateServer from "@cocreate/server";

async function startServer() {
	const server =
		await CoCreateServer.init();

	server.on(
		"status_change",
		({ oldStatus, newStatus }) => {
			console.log(
				`${oldStatus} -> ${newStatus}`
			);
		}
	);
}

startServer();
```

---

## Announcements

Release notes include new features, performance improvements, security updates, and compatibility changes. Follow the repository to stay informed about the latest releases.

---

## Roadmap

Upcoming improvements include:

- Enhanced cluster orchestration
- Improved process supervision
- Adaptive worker allocation
- Expanded telemetry integration
- Additional deployment adapters

---

## How to Contribute

Community contributions are welcome.

Please review:

- Documentation
- CONTRIBUTING.md
- GitHub Issues
- GitHub Discussions

Bug reports, feature requests, documentation improvements, and pull requests are appreciated.

---

## About

`@cocreate/server` is designed, built, and maintained by the CoCreate Developer Experience Team.

It serves as the core runtime responsible for process orchestration, clustering, networking, module composition, and lifecycle management across the CoCreate platform.

---

## License

This software is dual-licensed under the GNU Affero General Public License version 3 (AGPLv3) and a commercial license.

### Open Source

For open-source projects and non-commercial use, this software is available under the AGPLv3. See the `LICENSE` file for complete license terms.

### Commercial

Organizations requiring proprietary use may obtain a commercial license from CoCreate.