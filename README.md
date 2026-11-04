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

`@cocreate/cluster` is engineered to run stateless, highly available workloads across multi-core systems and distributed cloud infrastructure.

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
| `@cocreate/certificates` | TLS certificate management |
| `@cocreate/socket-server` | WebSocket server |
| `cluster/src/mesh` | Inter-server communication |
| `@cocreate/crud-server` | Database routing |
| `@cocreate/autoscaler` | Automatic scaling |
| `@cocreate/telemetry` | Monitoring and metrics |
| `@cocreate/api` | Dynamic API execution |
| `@cocreate/file/server` | Static file serving |
| `@cocreate/lazy-loader` | Lazy loading and webhooks |

---

## Programmatic API Reference

### Node API key initialization

After CRUD initializes, the primary runs `src/apikey.js` to read one
upload-capable API key (`type: "key"`, `admin: true`, including the supported
string/number boolean forms) from its configured organization's `keys` array.
Selection is ordered by record ID. It does not infer another organization from
the cluster name or fetch GitHub credentials.

The handler uses `Config.prompt({ apikey: { value } })` to update memory,
the process environment, and `~/CoCreate.config.js` while preserving the other
global settings. Workers inherit the environment rather than repeating the
lookup or writing the file. No key is generated when none exists. Read or
validation failures produce a generic boot diagnostic and do not prevent the
server from starting; credentials are never logged by the handler.

Cloud startup data contains node identity, host, and bootstrap storage, but no
CoCreate API key. The runtime obtains that key after storage is available.

### Node routing workflows

Cluster's `src/routing.js` loads routing configuration after CRUD, workflows,
and the node identity are initialized. The primary invokes `configure-cluster-routing` after
startup readiness (all workers listening in clustered mode; HTTP and HTTPS
listening in single-process mode). Workers do not register independently.

That initialization workflow registers the node and returns a prepared removal
request, retained in memory by `routing.js`. The existing coordinated shutdown
path sends that request directly through `server.api.send` before stopping
workers or closing Mesh. There is no removal workflow. This includes the
existing `SIGINT`, `SIGTERM`, and
`SIGBREAK` handling and application restart/reboot/shutdown paths. Routing does
not install another set of process signal handlers. Workflow failures are
logged without preventing the existing lifecycle from continuing.

The initialization workflow name can be set in CoCreate configuration:

```json
{
  "routing": {
    "enabled": true,
    "workflow": "configure-cluster-routing"
  }
}
```

Matching `servers_config` records with `type: "routing"` override this system
configuration using the existing server/cluster scope rules. Their `config`
object accepts `enabled` and `workflow`; set
`enabled: false` in that object to disable the hooks.

Each invocation uses the node's configured organization and host and the normal
workflow envelope, for example:

```js
{
  method: 'workflow.configure-cluster-routing',
  organization_id: server.organization_id,
  host: server.host,
  'configure-cluster-routing': [{
    server_id: server.id,
    cluster_id: server.cluster_id,
    ip: server.ip,
    host: server.host,
    infrastructure: server.infrastructure,
    location: server.location
  }]
}
```

The workflow returns `configure-cluster-routing[0].remove`, an API request containing
`api`, `endpoint`, and `data`, including provider registration IDs. A custom
initialization workflow returns the same `remove` field under its own input
key. Shutdown adds a `reason`, such as `SIGTERM`, and uses the node's configured
organization and host for API execution. If no removal request was prepared,
shutdown performs no new workflow or inventory lookup.

The hooks do not create workflow definitions or contain Bunny API logic. The
initialization workflow owns provider selection and request preparation. It
must return nonsecret cleanup data, not API credentials. The existing API
dispatcher still resolves the provider configuration and endpoint by name;
this change does not introduce a shared API cache.

Registration is shared across repeated readiness notifications, and withdrawal
waits for an in-flight registration before running. Cleanup requires a graceful
shutdown; abrupt process or host failure remains a routing fallback concern.

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
| `send()` | Server-side IPC and mesh message dispatcher |

### Status Events

```javascript
server.on("status_change", ({ oldStatus, newStatus }) => {
	console.log(`State changed from ${oldStatus} to ${newStatus}`);
});
```

---

## Ingress & Connection Controls

### `server.state.cordon(value)`

Enable or disable maintenance mode.

```javascript
await server.state.cordon(true);

server.emit("cordon", {
	value: true
});

await server.state.cordon(false);
```

---

### `server.state.shed(organizations)`

Drain selected organizations.

```javascript
await server.state.shed([
	"org_abc123",
	"org_xyz789"
]);

await server.state.shed(false);
```

---

### `server.state.drain(value)`

Gracefully disconnect active clients.

```javascript
await server.state.drain(true);

await server.state.drain(false);
```

---

## Process & OS Lifecycle Management

### Restart

```javascript
server.state.restart();

server.state.restart({
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
server.state.reboot();
```

Gracefully reboots the operating system after draining connections.

---

### Shutdown

```javascript
server.state.shutdown();
```

Gracefully powers off the machine after draining connections.

---

## IPC Transport Engine

The IPC subsystem synchronizes Primary and Worker processes and routes messages
to peer servers through the server mesh. `server.send()` is the server-side
transport. A browser does not call this function directly: it calls
`CoCreate.socket.send()` with the same envelope, and the receiving socket server
passes that envelope into `server.send()` when server routing is required.

Browser to server/worker:

```javascript
await CoCreate.socket.send({
	method: "cordon",
	server: "server_b_id",
	worker: 2,
	value: true
});
```

The message follows this path:

```text
CoCreate.socket.send(data)
    -> socket server
    -> server.send(data)
    -> Primary IPC / server mesh
    -> targeted worker event
```

Inside a server process, use `server.send()` directly.

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
	worker: [1, 3]
});
```

Worker control messages use the same envelope:

```javascript
// Allow or prevent new traffic on one worker.
server.send({
	method: "cordon",
	server: "server_b_id",
	worker: 2,
	value: true
});

// Drain a worker's active connections and cordon it.
server.send({
	method: "drain",
	server: "server_b_id",
	worker: 2,
	value: true
});

// Shed selected organizations from a worker.
server.send({
	method: "shed",
	server: "server_b_id",
	worker: 2,
	organizations: ["org_abc123"]
});

// Stop shedding and allow the worker to accept traffic again.
server.send({
	method: "shed",
	server: "server_b_id",
	worker: 2,
	organizations: false
});
```

Primary process and host lifecycle controls target `master: true`:

```javascript
server.send({
	method: "restart",
	server: "server_b_id",
	master: true
});

server.send({
	method: "reboot",
	server: "server_b_id",
	master: true
});

server.send({
	method: "shutdown",
	server: "server_b_id",
	master: true
});
```

`restart` restarts the server application. `reboot` and `shutdown` operate on
the host and therefore should be exposed only through an authorized control
surface with explicit confirmation.

`server`, `cluster`, and `worker` accept either one value or an array of values.
Broadcasting is opt-in: set `broadcast: true` only when a message should reach
all eligible targets. An explicit `worker` target takes precedence over
broadcast worker selection.

Messages carrying `organization_id` are routed only to workers currently serving
that organization and through mesh sockets connected on that organization's URL
path. The field is a routing scope, not a request to broadcast. IPC owns the local
organization-to-worker map. The originating `workerId` and `serverId` are retained
as lifecycle metadata so messages can be routed without duplicate execution.

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

Cluster owns its inter-server mesh implementation in `src/mesh`.

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
- Ephemeral TLS certificates managed by `@cocreate/certificates`

---

## Installation

### NPM

```bash
npm install @cocreate/cluster
```

### Yarn

```bash
yarn add @cocreate/cluster
```

---

## Quick Start

```javascript
import CoCreateServer from "@cocreate/cluster";

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

`@cocreate/cluster` is designed, built, and maintained by the CoCreate Developer Experience Team.

It serves as the core runtime responsible for process orchestration, clustering, networking, module composition, and lifecycle management across the CoCreate platform.

---

## License

This software is dual-licensed under the GNU Affero General Public License version 3 (AGPLv3) and a commercial license.

### Open Source

For open-source projects and non-commercial use, this software is available under the AGPLv3. See the `LICENSE` file for complete license terms.

### Commercial

Organizations requiring proprietary use may obtain a commercial license from CoCreate.

## Node repository

The primary owns one shared repository independently of AI. It reads the global `repository` config and scoped `servers_config` records with `type: "repository"`, applying database overrides. Defaults are:

```js
{
    enabled: true,
    url: "", // falls back to the server organization's git.clone_url
    api: "", // tenant apis record ID/name; defaults to organization.git.api
    branch: "dev",
    remote: "origin",
    localFsRoot: "<working-directory>/organizations",
    onStartup: "clone",
    onShutdown: "commit-and-push",
    startupTimeoutMs: 60000,
    shutdownTimeoutMs: 10000
}
```

Before workers fork, the master clones a missing repository into `<localFsRoot>/<organization_id>/sites` and selects the configured branch. Missing repository metadata is a no-op. An existing checkout is reused without pulling/resetting; switching branches requires a clean working tree and a matching remote. A configured checkout failure prevents worker startup. Set `enabled: false` to disable lifecycle handling, or set either action to `false` independently. The prepared service is available as `server.repository`; forked consumers receive its root through `SERVER_REPOSITORY_ROOT`.

Master shutdown broadcasts the existing worker shutdown instruction and awaits worker exits. Finalization then attempts exactly one commit-and-push of the current branch, within the smaller of the repository timeout and the remaining shutdown budget. A single-process server first quiesces its local services. An independent worker restart never triggers repository checkpointing. If the shutdown deadline expires while workers are still active, finalization skips the checkpoint rather than saving a potentially changing checkout. Errors are reported without preventing shutdown.

Git is executed locally. `repository.api` references an enabled record in this node organization's `apis` array by ID or name; when the URL comes from signup metadata, `git.api` supplies the default reference. Signup deploy-key profiles use `authentication: 'ssh-deploy-key'` and contain the private/public key, matching `repository_url`, deploy-key ID and GitHub `knownHosts`. Credentials are read afresh before managed clone/push operations. The master caches the SSH identity and host keys under the reserved `<localFsRoot>/<organization_id>/.ai` directory with mode 0600 and invokes SSH noninteractively with strict host checking and that specific identity. It never disables host verification or puts the key inside the checkout. Other trusted server consumers may request the same command environment through `server.repository.environment()`.

The deploy key has no scheduled expiration or automatic rotation. Tenant API data is the source of truth; future rotation can update it. Standard Git credentials still work when no API reference is configured. CoCreate API keys do not authenticate Git. Signup repository/template/config/branch/deploy-key registration continues to use GitHub HTTP endpoints separately. Git `user.name` and `user.email` identify commit authors; they do not authenticate clone/push.

Checkpoints never force-push. Default exclusions cover environment files, private key files and node_modules; staged excluded files cause a failure instead of a commit. A failed push retains its local commit. SIGKILL/power loss can interrupt saving. Normal explicit commits remain the primary save mechanism. Initial application publication via `coc upload` is separate and is not performed here.
