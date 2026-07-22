# CoCreate Server

A secure, stateless, clustered headless server built for high-performance multi-tenancy, real-time collaboration, dynamic process orchestration, and distributed mesh execution. This module serves as the primary orchestrator of the CoCreate ecosystem.

---

## Documentation

For a complete guide and working integration configurations, refer to the **[CoCreate Server Documentation](https://cocreatejs.com/docs/server)**.

---

## Table of Contents

- [Architectural Overview & Core Design](#architectural-overview--core-design)
- [Component Mesh Architecture](#component-mesh-architecture)
- [Programmatic API Reference](#programmatic-api-reference)
- [Ingress & Connection Controls (cordon, shed, drain)](#ingress--connection-controls-cordon-shed-drain)
- [Process & OS Lifecycle Management (restart, reboot, shutdown)](#process--os-lifecycle-management-restart-reboot-shutdown)
- [Reactive Cluster State Manager (server.store)](#reactive-cluster-state-manager-serverstore)
- [Distributed Cluster Orchestration (@cocreate/server-mesh)](#distributed-cluster-orchestration-cocreateserver-mesh)
- [Targeting Matrix & Routing Directives](#targeting-matrix--routing-directives)
- [Self-Healing & Jittered Reconnection Logic](#self-healing--jittered-reconnection-logic)
- [Dynamic Tenant Eviction & Cascading Teardown Sequence](#dynamic-tenant-eviction--cascading-teardown-sequence)
- [Security-Hardened Design](#security-hardened-design)
- [Installation & Startup](#installation--startup)
- [Contributing & Community](#contributing--community)

---

## Architectural Overview & Core Design

`@cocreate/server` is engineered to run stateless, highly available workloads at scale across multi-core systems and distributed cloud nodes.

### Clustered IPC Architecture

The server operates a single Primary process that forks and manages worker processes across CPU cores. The Primary process serves as the central control plane, while Workers manage WebSocket client slices, file serving, and application logic.

$$
\text{Master Process (Primary)}
\xrightarrow{\text{Native OS Child IPC}}
\begin{cases}
\text{Worker 1} & \text{(Client Socket Tunnels)}\\
\text{Worker 2} & \text{(Asset \& File Delivery)}\\
\text{Worker }N & \text{(Execution Engine)}
\end{cases}
$$

### Zero-Dependency Core

To minimize cold-boot latency, prevent supply-chain vulnerabilities, and maintain memory efficiency, the runtime enforces a strict zero-dependency core strategy (excluding audited internal module packages).

### Automatic Fallback & Cordoning

When all worker processes are draining or cordoned, the Primary process automatically binds a temporary HTTP/HTTPS fallback server to dispatch `307 Temporary Redirect` responses with `Retry-After` headers, ensuring smooth traffic handling during maintenance or restarts.

---

## Component Mesh Architecture

`@cocreate/server` dynamically imports and orchestrates standalone core modules into a unified collaboration mesh.

| Component Module | Architectural Role | Functional Responsibility |
| :--- | :--- | :--- |
| **@cocreate/acme** | Certificate Authority Integration | Provisioning and real-time SNI resolution of TLS certificates. |
| **@cocreate/socket-server** | Real-Time Transport Plane | Multiplexing end-user client WebSockets with tenant isolation. |
| **@cocreate/server-mesh** | Inter-Node Transport Fabric | Encrypted TLS P2P mesh sockets for inter-server communication. |
| **@cocreate/crud-server** | Multi-Tenant Database Router | Translating abstract query payloads to database transactions. |
| **@cocreate/server-autoscaler** | Lifecycle & Draining Engine | Monitoring resource utilization and orchestrating node scaling. |
| **@cocreate/server-telemetry** | Observability Engine | Aggregating operational metrics, bandwidth usage, and node health. |

---

## Programmatic API Reference

`CoCreateServer` inherits directly from `EventEmitter`. Control methods can be invoked programmatically via direct class methods or by emitting generic events.

---

## Ingress & Connection Controls (cordon, shed, drain)

### `server.cordon(value)`

Toggles the node's cordon state. When `true`, edge traffic is declined and redirected via HTTP `307` responses.

```javascript
// Programmatic API
server.cordon(true);

// EventEmitter Equivalent
server.emit("cordon", { value: true });
```

### `server.shed(organizations)`

Evicts client connections for specific organization IDs and automatically cordons the node. Passing an empty array (`[]`) or `false` clears shedding.

```javascript
// Evict specific tenants
server.shed(["org_abc123", "org_xyz789"]);

// Restore normal edge traffic listeners
server.shed([]);
```

### `server.drain(value)`

Purges active client WebSocket connections across all worker threads prior to planned maintenance or shutdown.

```javascript
// Initiate connection drain sequence
server.drain(true);
```

---

## Process & OS Lifecycle Management (restart, reboot, shutdown)

### `server.restart(payload)`

Initiates a graceful context restart.

**Worker Context**

- Drains active WebSocket connections.
- Calls `process.exit(0)`.
- Primary automatically forks a replacement worker.

**Primary Context**

- Performs a cluster-wide graceful shutdown.
- Closes mesh connections.
- Exits cleanly so service managers (such as `systemd`) can restart the application.

```javascript
// Restart current context
server.restart();

// Restart primary application
server.restart({ isPrimary: true });
```

### `server.reboot(payload)`

Gracefully reboots the operating system.

```javascript
server.reboot();
```

### `server.shutdown(payload)`

Gracefully powers off the operating system.

```javascript
server.shutdown();
```

---

## Reactive Cluster State Manager (`server.store`)

Every `CoCreateServer` instance exposes a synchronized `ClusterStateManager` through `server.store`. State changes are replicated across all worker processes using Primary IPC.

```javascript
// Set a key
await server.store.set("node_capacity", {
    availableSlots: 45
});

// Read a key
const capacity = await server.store.get("node_capacity");

// Delete a key
await server.store.delete("node_capacity");

// Listen for changes
server.store.on("change:node_capacity", (newValue) => {
    console.log("Capacity updated:", newValue);
});
```

---

## Distributed Cluster Orchestration (`@cocreate/server-mesh`)

Inter-node communication is managed through `@cocreate/server-mesh`. Every transmitted message is automatically assigned a unique UUID for distributed tracing.

```javascript
server.mesh.send({
    method: "mesh.custom_command",
    server: "server_node_b",
    data: {
        key: "value"
    }
});
```

---

## Targeting Matrix & Routing Directives

The `server` key accepts either a single string identifier or an array of identifiers.

| Routing Intent | Mesh Payload | Behavior |
| :--- | :--- | :--- |
| Single Peer | `{ server: "node_b", method: "restart" }` | Routes exclusively to `node_b`. |
| Multicast | `{ server: ["node_b","node_c"], method: "drain" }` | Sends concurrently to specified nodes. |
| Worker Target | `{ server: "node_b", worker: 2, method: "restart" }` | Targets Worker 2 on `node_b`. |
| Cluster Broadcast | `{ broadcast: true, method: "reboot" }` | Sends to every active node and local worker. |

---

## Self-Healing & Jittered Reconnection Logic

When peer nodes disconnect, `@cocreate/server-mesh` uses exponential backoff with randomized jitter to eliminate thundering herd reconnect storms.

$$
T_{\text{delay}}
=
\min\left(30000,\,
1000 \times 1.5^{\text{attempt}}
\right)
+
\text{random}(500,4500)
\quad(ms)
$$

---

## Dynamic Tenant Eviction & Cascading Teardown Sequence

To conserve system memory and database connections, tenant lifecycle is driven entirely by client socket connectivity.

$$
\text{Client Disconnect}
\xrightarrow{\text{60s Grace Window}}
\text{Emit 'orgDeleted'}
\rightarrow
\begin{cases}
\text{Release Database Pool Slots}\\
\text{Flush Billing \& Telemetry Buffers}\\
\text{Close Dedicated P2P Mesh Tunnels}
\end{cases}
$$

When the final socket connection for an organization closes:

1. A 60-second grace timer begins.
2. If no new clients connect, the worker emits `mesh.deregisterOrg`.
3. Once every local worker has evicted the organization, the Primary notifies peer nodes via `mesh.orgDeleted`.
4. Database pools, telemetry buffers, and dedicated peer tunnels are released.

---

## Security-Hardened Design

- **Zero Public Control Ports** — Administrative communication occurs exclusively over mutual TLS (mTLS) or encrypted WebSocket mesh connections on port `8090`. Public ingress remains limited to ports `80` and `443`.
- **Cryptographic UUID Injection** — Every outbound mesh frame is stamped using `crypto.randomUUID()` for distributed tracing and replay protection.
- **Ephemeral TLS Certificates** — Inter-node HTTPS/WSS communication uses dynamically generated TLS certificates with short lifetimes.

---

## Installation & Startup

### Installation

```bash
npm install @cocreate/server
```

### Programmatic Initialization

```javascript
import CoCreateServer from "@cocreate/server";

async function startServer() {
    const server = await CoCreateServer.init();

    server.on("status_change", ({ oldStatus, newStatus }) => {
        console.log(
            `Server transitioned from ${oldStatus} to ${newStatus}`
        );
    });
}

startServer();
```

---

## Contributing & Community

We welcome community contributions to CoCreate.

Please review the following resources before submitting pull requests or feature requests:

- **Documentation**  
  https://cocreatejs.com/docs/server

- **Contributing Guide**  
  https://github.com/CoCreate-app/CoCreate-server/blob/master/CONTRIBUTING.md

- **Issue Tracker**  
  https://github.com/CoCreate-app/CoCreate-server/issues

- **Discussions**  
  https://github.com/CoCreate-app/CoCreate-server/discussions

- **Releases**  
  https://github.com/CoCreate-app/CoCreate-server/releases

Maintained by **CoCreate, LLC**.

Distributed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.