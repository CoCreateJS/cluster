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

### Built-in server manager

Cluster initializes `server.manager` after CRUD. Its persisted fields are
`organization_id`, `host` and `apiKey`; a null organization ID means unmanaged.
The existing primary inventory read hydrates those fields, and fork configuration
passes them to workers without another database lookup. The runtime `send`
function is non-enumerable and is not copied into inventory records.

```js
if (server.manager.organization_id) {
    await server.manager.send(existingEnvelope);
}
```

`send` POSTs the envelope to `https://<manager.host>/manager`, with the credential
in the Authorization Bearer header and caller scope removed from JSON. Manager.http delegates provider requests to API.http, which authorizes before
API.send. Manager resolves parent scope and authorizes server/cluster inventory
object.update requests before dispatching to CRUD. The manager handler does
not invoke state.save, preventing mirrored-state loops. Existing API/workflow HTTP
routes remain available independently.

State's existing persistence helper mirrors successful state.save envelopes once
after the local write. Each worker sends its existing worker section; the primary
sends its existing master/node section. Mirror delivery has a three-second timeout;
failure is logged without failing or repeating local persistence. There is no
delivery queue or replay, and direct CRUD writes outside this path are not captured.
Telemetry is not mirrored.

AutoScaler uses manager.send at its existing send points and preserves its drain,
capacity and recovery sequence. Unmanaged nodes retain server.api.send. Initial
server creation persists the manager credential in both inventory copies; future
scale-out reservations copy its connection fields. Cluster-key permissions allow
instance endpoints and server/cluster updates. Cluster/provider AST predicates, metadata
preparation, bootstrap handling and initial provider-result synchronization remain
separate work; this service does not imply those integrations are complete.

### HTTP API and workflow requests

Cluster routes `/api` and its `/api/…` subpaths to `server.api.http(req, res)` and
`POST /workflows/<name>` to `server.workflow.http(req, res)`, following the existing
`server.api.webhook(req, res)` entrance pattern. `/manager` and `/manager/…` subpaths
similarly reach `server.manager.http`. Prefixes respect path segment boundaries,
so `/apis` and `/managerial` retain normal routing. The API HTTP handler still
requires `endpoint` in its JSON envelope; path-based endpoint extraction is a
separate future change. Each module resolves organization
scope from the hostname and authorizes before execution. The former
`src/http-operations.js` adapter is removed. Denial, authorization errors or a
missing authorization service return 403. The shared `server.authorize.request`
helper checks permission, preserves the sanitized envelope, rejects changes to
scope/operation targets, and strips the transport API key. Default/key permission merging is
unchanged, with no extra key-record lookup.

Send `Content-Type: application/json` and an `Authorization: Bearer <key>` header,
or supply the existing envelope's `apikey` field. Without a key, authorization
evaluates its normal default permissions. Conflicting header/body keys are
rejected. Caller-supplied socket/user identity and organization/host scope are
not trusted. The authorized, sanitized envelope is used for execution, with the
transport API key removed. Responses use `Cache-Control: no-store`; bodies are
limited to 1 MiB and only POST is supported.

Example body for `/api`:

```json
{
  "api": "configured-provider",
  "endpoint": "configured-operation",
  "method": "post",
  "data": { "cluster_id": "cluster-document-id" }
}
```

The adapter requires `api`, `endpoint` and `method`; the direct `url` shortcut is
not exposed. Authorization evaluates the existing envelope method, such as
`post`, rather than a new `api.send` permission. Configure operation/data
restrictions instead of granting an unrestricted HTTP verb for managed scaling.

Example body for `/workflows/example`:

```json
{ "example": [{ "input": "value" }] }
```

The adapter sets `method: "workflow.example"`; an explicit method must match the
path. No permissions are granted by these routes. `/webhooks/*` retains its AST
handler, and trusted custom `server.routes` retain their existing precedence.
Managed credentials and parent dispatch remain separate integration work.

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

## Cluster repositories

New signup and repository startup default to `main`, so the first upload targets the primary tenant hostname. Explicit branch choices remain supported; dev/test environment flows can be selected later.

The cluster document owns a `repositories` array. URL is each entry's identity;
no generated repository entry ID is used. The primary queries its own cluster
once during repository initialization, before workers fork. Global repository
config and servers_config type repository records are no longer loaded. The old
App repository server-config seed is removed; existing records of that type are
ignored. Organization git metadata remains descriptive and is not a startup
configuration fallback.

```js
repositories: [{
    url: 'git@github.com:owner/project.git',
    api: '<tenant repository credential record ID>',
    branch: 'main',
    remote: 'origin',
    localFsRoot: '../',
    directory: 'project',
    onStartup: 'clone',
    onShutdown: 'commit-and-push',
    startupTimeoutMs: 60000,
    shutdownTimeoutMs: 10000,
    initialUpload: { enabled: true, completed: false }
}]
```

Relative localFsRoot resolves against server.projectRoot or the running project's
working directory; absolute roots are also supported. The checkout is directly
<resolved-root>/<directory>, normally a sibling of the running server project.
If directory is omitted, it derives from the repository URL name. Duplicate URLs
or checkout destinations fail before cloning, and the running project itself
cannot be a checkout destination. There is no organization/sites nesting.

Each configured checkout is cloned if missing. Existing checkouts are reused
without pulling/resetting; startup branch switches require a clean checkout and
a matching remote. Set enabled:false, onStartup:false or onShutdown:false on an
entry to control its lifecycle. Workers share the primary's prepared checkouts.

The runtime service remains server.repository, with entries available in
server.repository.repositories. The first enabled prepared checkout is the
default AI workspace. Workers receive its path and platform organization through
SERVER_REPOSITORY_ROOT and SERVER_REPOSITORY_ORGANIZATION_ID. AI uses that actual
checkout rather than creating another organization/sites directory. Its platform
access restrictions remain in place. Other consumers can select another entry's
checkout.workspace; authenticated Git environments can be requested by URL with
server.repository.environment(url).

Shutdown attempts each enabled repository's commit-and-push once, preserving its
current branch. Each operation is bounded by its entry timeout and the remaining
node shutdown budget; a failed repository does not silently skip all later ones.
Errors are reported without preventing node shutdown. Credentials still come
from the node organization's enabled apis records, with repository URL matching
and strict SSH host verification. Keys/cache/staging live outside checkouts under
<resolved-root>/.cocreate/repositories/<URL hash>, never in Git.

After checkout and node listener readiness, the primary calls repository.publish.
Entries with initialUpload.enabled:true and completed:false run coc upload from
their checkout; disabled or completed entries are skipped. The CLI must be
available on PATH, with runtime/global CoCreate credentials and a reachable target
from CoCreate.config.js. No credentials are inserted into Git or CLI arguments.

A repository_upload_reservations record keyed by cluster+URL prevents concurrent
nodes from running the initial upload. The primary memoizes publish so repeated
worker-ready events do not launch it twice; workers never call it. Publication
rereads the URL entry and sets initialUpload.completed and completedAt only after
the CLI exits successfully. The cluster patch is mirrored through the manager
when configured; the tenant cluster remains the local publication authority.
Failed uploads leave completion pending and release their reservation for a later
startup. If the process crashes, or upload succeeds but completion persistence is
uncertain, the reservation is retained for explicit reconciliation; no lease
expiry blindly launches a duplicate. Shutdown aborts an in-flight upload and
bounds the wait before attempting Git checkpoints. Later AI/change uploads are
not gated by this initial flag.

Existing clusters require their repositories array and runtime key permissions
to be populated; no automatic migration from old repository configs or
organization git metadata is performed.

## Platform organization activation

Manager exposes non-enumerable send(envelope), createServer(cluster), http and config
properties; only parent connection fields are serialized. Initialization loads the
platform organizationConnection settings after CRUD/API/workflows. Its enabled flag
controls Mesh's optional creation hook; tenant signup does not copy that platform config.

On activation, Mesh reads the organization-type cluster by manager.for (tenant organization ID) and
manager.organization_id (platform ID), then its running servers in platform scope. This query uses cluster_id and master.status
running, without peer discovery's smaller-ID filter. If no running server is returned,
Mesh invokes manager.createServer(cluster). Nonrunning inventory, including failed
records, does not block creation or get deleted by this decision.

Manager executes the command without inventory eligibility queries, reservations or
creation deduplication. It uses the supplied cluster.initialDeployment, provider profile,
storage reference, hostname and runtime settings, resolving the tenant bootstrap
source using cluster.manager.for for script preparation. Cluster.name is an editable label;
cluster.manager stores organization_id, host and for in both database copies. initialDeployment
and repositories remain top-level cluster configuration. Signup prepares those documents
and repository registration before ready; later Mesh activation invokes VM creation. It creates platform and tenant inventory before dispatching
the existing create-organization-server workflow, mirrors the provider patch, and retains
the existing startup watchdog. Concurrent creation commands are allowed; ordinary
AutoScaler policy handles excess running capacity. ensureServer is removed.

## Mesh peer connection and close contract

Outbound discovery already calls `connectPeer` for every returned server with a usable address, preserving the existing smaller-ID query and connection ordering. Cluster grouping and orchestrator-to-tenant links are unchanged.

Outgoing sockets retry from their `close` handler. Normal close code 1000 now suppresses retries; explicit node shutdown and organization closure send that code. Restarts/reboots and uncertain signal exits use 1012, while network loss uses the transport's abnormal closure path and remains retryable. The existing ten-retry limit is unchanged. Failed-peer provider/inventory reconciliation remains a separate TODO; no automatic deletion or new diagnostic protocol is introduced.

### Managed node startup confirmation

Manager observes first-node creation and managed create-instance requests for two minutes after a successful instance-ID response. A matching authorized server update must persist successfully to confirm receipt. An early update arriving before the provider response is retained; an update handled by another managing worker/node is checked through persisted server status at timeout. Missing confirmation logs the server ID and leaves a TODO for platform notification and at most one optional replacement. No restart, deletion, retry or notification service is implemented by this watchdog. Timers are process-local and unreferenced; they do not survive a managing-process restart.

### Accepted termination retains lifecycle records

After the drain/capacity sequence, AutoScaler commits master.status terminating before the provider request. Success retains it; a termination error overrides it with failed. It then dispatches State shutdown to the selected server. State.save skips failed/terminating records while shutdown cleanup proceeds. Server inventory is retained briefly rather than deleted in the termination request.

AutoScaler stores removal outcomes in master.status. Capacity/drain cancellations before provider dispatch restore running; termination failures remain failed. The separate decommission status object is no longer used; decommissionStrategy remains the policy configuration name.

After persisting a managed server terminating update, Manager schedules cleanup once after one minute. It resolves the tenant scope from the stored cluster.manager.for and calls AutoScaler's shared deleteServer helper to delete the tenant and platform copies, without rechecking server status or calling the cloud provider. Duplicate reports do not postpone cleanup. A successfully persisted failed update cancels the pending timer directly. AutoScaler also sweeps its own cluster every ten minutes, starting ten minutes after initialization; managed sweeps request platform deletion through the authorized Manager object.delete servers operation. Already-missing records are successful deletions. Failed records are not selected. The management permission reference includes server-only deletion; existing issued keys require that grant for tenant-initiated platform cleanup. These process-local timers do not survive restarts.


### Organization removal and scale-to-zero

Mesh emits organization.active from activation and organization-socket registration,
and organization.inactive from its existing close/removal callbacks. Organization close
sends the existing mesh.orgDeleted frame before closing its sockets. AutoScaler consumes
these confirmed signals for its own organization: minClusterSize zero permits the existing
scaleIn lifecycle, while CPU/memory alone cannot remove the final node. Socket Server's
existing grace period and IPC worker aggregation remain responsible for local activity
tracking; the own organization can report idle without unloading services needed for shutdown.
