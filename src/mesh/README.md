# @cocreate/server-mesh

A peer-to-peer WebSocket transport for server control, organization-scoped messaging, and inter-node event synchronization across the CoCreate ecosystem.

---

## Documentation

For complete API references, deployment guides, and architecture documentation, visit the CoCreate Server Mesh documentation.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture & Message Pipeline](#architecture--message-pipeline)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Routing Directives & Targeting Matrix](#routing-directives--targeting-matrix)
- [Tenant-Aware Routing Protocols](#tenant-aware-routing-protocols)
- [Connection Lifecycle](#connection-lifecycle)
- [Programmatic API Reference](#programmatic-api-reference)
- [Security & TLS Specification](#security--tls-specification)
- [Environment Variables](#environment-variables)
- [Announcements](#announcements)
- [Roadmap](#roadmap)
- [How to Contribute](#how-to-contribute)
- [About](#about)
- [License](#license)

---

## Overview

`@cocreate/server-mesh` is the inter-server transport plane for `@cocreate/cluster`. It establishes TLS WebSocket connections between server nodes for control messages and organization-scoped data propagation.

IPC handles local worker routing. Server mesh handles only connections and messages between servers.

---

## Key Features

- **Ephemeral TLS:** Automatically generates temporary TLS credentials for encrypted node-to-node communication.
- **Polymorphic Target Directives:** Unicast, multicast, broadcast, worker-specific, or tenant-aware routing using a unified payload format.
- **Message Identity:** Preserves an existing `uuid` and creates one for payloads sent through `ServerMesh.send()` when absent.
- **Tenant Isolation:** Tracks tenant-to-server membership so messages are only delivered to nodes actively serving an organization. Local worker membership is owned by `@cocreate/cluster` IPC.
- **Connection Recovery:** Unexpected network disconnects retry automatically; intentional organization shutdowns do not reconnect.
- **Cluster IPC Integration:** Transparently bridges Node.js Primary and Worker processes while extending communication across distributed servers.
- **Control and Organization Sockets:** Uses pathless sockets for server control and `/<organization_id>` sockets for organization traffic.

---

## Architecture & Message Pipeline

When a payload is dispatched through `@cocreate/server-mesh`, the routing engine evaluates execution context (Primary vs Worker), determines the target destination, and forwards the payload through IPC or encrypted peer connections.

$$
\text{Worker Thread}
\xrightarrow{\text{IPC}}
\text{Primary Process}
\xrightarrow{\text{Route Resolution}}
\begin{cases}
\text{Local Workers} \\
\text{Remote Peer Servers}
\end{cases}
$$

### Worker Context

Worker processes:

- Send outbound messages through native `process.send()`
- Receive inbound mesh traffic through IPC
- Never establish direct peer connections

### Primary Context

The Primary process:

- Hosts the secure mesh WebSocket server
- Loads peer servers from the database and creates pathless control connections
- Tracks active organizations and their organization-specific sockets
- Routes messages between workers and remote servers

---

## Public server interface

The cluster module attaches this transport as `server.cluster`. Use
`server.send(envelope)` for component messages, `server.cluster.send(envelope)`
for direct peer transmission, and `server.socket.send(envelope)` for local client
delivery. Network connections remain primary-owned; direct worker cluster sends
relay through IPC. The source directory and Mesh class names are unchanged.

## Installation

### NPM

```bash
npm install @cocreate/server-mesh
```

### Yarn

```bash
yarn add @cocreate/server-mesh
```

> **Note**
>
> `@cocreate/server-mesh` integrates directly with `@cocreate/cluster`, but it can also be initialized independently.

---

## Quick Start

### Initialize

```javascript
import CoCreateServer from "@cocreate/cluster";
import ServerMesh from "@cocreate/server-mesh";

await ServerMesh.init(CoCreateServer);
```

### Send a Message

Application code normally sends the unified envelope through `server.send()` so
IPC and mesh routing use the same rules:

Unicast to a single server:

```javascript
CoCreateServer.send({
	method: "cache.flush",
	server: "srv_64f8a1bc90d1",
	data: {
		cache: "users"
	}
});
```

Multicast to multiple servers:

```javascript
CoCreateServer.send({
	method: "worker.task",
	server: ["srv_64f8a1bc90d1", "srv_90e3b2ac81f4"],
	worker: 2,
	data: {
		taskId: 1042
	}
});
```

Broadcast to every node:

```javascript
CoCreateServer.send({
	broadcast: true,
	method: "reload.configuration"
});
```

### Listen for Messages

```javascript
ServerMesh.on("cache.flush", (message) => {
	console.log(message.data);
});
```

---

## Routing Directives & Targeting Matrix

| Target | Payload | Description |
| --- | --- | --- |
| Single Server | `{ server: "srv1" }` | Routes over the specified server's control or organization socket. |
| Multiple Servers | `{ server: ["srv1","srv2"] }` | Sends to every listed server. |
| Organization | `{ organization_id: "org1" }` | Uses only sockets in that organization's array. |
| Worker | `{ server: "srv1", worker: 2 }` | IPC targets a worker on the selected server. |
| Local Workers | `{ worker: [1,3] }` | IPC sends only to the selected local workers. |
| Broadcast | `{ broadcast: true }` | Delivers to every connected server and local worker. |

---

## Tenant-Aware Routing Protocols

IPC maintains each server's organization-to-worker map. Mesh creates a separate
peer socket for each active organization, using `/<organization_id>` as the path.

```text
Client Connect
        │
        ▼
orgConnected
        │
        ▼
IPC Maps Organization → Workers
        │
        ▼
Client Disconnect
        │
        ▼
60 Second Grace Timer
        │
        ▼
orgDisconnected
        │
        ▼
Close Organization Mesh Sockets
```

### Organization Events

| Event | Description |
| --- | --- |
| `orgConnected` | A worker reports its first local connection. IPC adds its worker mapping and emits the same event on the primary only when the organization gains its first worker. Mesh and AutoScaler listen independently. |
| `orgDisconnected` | A worker reports its local departure. IPC removes that worker's organization mapping and emits the same event on the primary only when the final worker leaves. Mesh and AutoScaler listen independently. |
| `organizationPeers` | After a successful peer lookup, Mesh emits `{ organization_id, peers }` on the primary server emitter. Interested components decide how to use the result; AutoScaler handles an empty list on the enabled platform primary. |

The first local worker creates an empty entry in `mesh.organizations` and sends
`orgRegistered` over every pathless control socket. A peer responds only when
the same organization exists in its own `mesh.organizations` map. The server
with the higher `serverId` then creates one organization socket to the lower-ID
server, preventing duplicate connections.

Socket Server emits `orgConnected` and `orgDisconnected` on `process`, with
`(organization_id, details)` arguments. It does not send presence reports
through `server.send()` itself. IPC listens to those process events and owns
the upward transport; when the primary also serves clients, IPC updates its
worker map directly. The details include the reporting worker ID.

IPC emits the corresponding event on the primary server emitter only when
that organization's worker set changes between empty and nonempty. These
server events carry `{ organization_id }`; they are separate from the process
emitter and cannot be forwarded back into the worker-reporting path. Worker
exits use the same last-worker transition logic.

Mesh and AutoScaler independently listen to the primary events. On connection,
Mesh opens the organization and discovers peers. On disconnection, Mesh closes
organization sockets with code 1000 and removes its organization entry. Peers
observe socket closure; no organization-removal message is sent. AutoScaler
independently evaluates its configured scale-to-zero policy.

After a successful lookup, Mesh emits `organizationPeers` with the returned
peer records. The current lookup returns running organization peers with IDs
lower than the local server ID, which is the set this node may dial without
creating duplicate peer connections. An empty result therefore means no peers
matched that lookup; it does not prove that no higher-ID organization server
exists. A failed lookup emits no `organizationPeers` event.

All organizations follow the same worker cleanup rules, including the server's
own organization. There are no resource-retention flags. Socket Server deletes
its organization entry after the disconnect grace period, and local cleanup
listeners clear organization-specific resources. CRUD bootstrap configuration
and the bootstrap database worker remain independently available for requests
that reactivate an organization.

Local `orgConnected` fires after socket maps are populated. Additional sockets
and reconnects within the grace period do not repeat it. Modules that initialize
lazily from data continue doing so. Peer socket changes never manufacture a local
worker disconnect: IPC remains the authority for local worker presence.

Organization payloads retain their original envelope. IPC selects local workers;
the mesh selects remote servers from its organization membership map.

---

## Connection Lifecycle

Pathless `wss://ip` sockets form the server control and health channel and are
stored separately. The platform becomes an active organization through the same
`orgConnected` flow as every other organization and receives a distinct
`wss://ip/<platform_organization_id>` socket once both peers report active clients.
An intentional socket close finalizes its references immediately. Unexpected
outbound closure retries up to ten times with 5–7 second delays and a five-second
handshake timeout. A closed socket is replaced on recovery; exhaustion finalizes
its removal. The inbound side waits up to the corresponding two-minute recovery
window for the outbound owner to reconnect, rather than starting another dialer.
A reconnect cancels pending finalization. Local organization closure cancels its
pending recovery and closes only its organization sockets, leaving pathless node
control connections and the listening port available.

Final socket cleanup removes its routing references. An empty organization entry
is removed only when no local workers or pending peer recovery still require it.
A remote departure alone cannot deactivate an organization with local clients.
Node shutdown closes its control sockets separately; transport failures follow
normal retry rules and do not by themselves prove persistent node deletion.

---

## Programmatic API Reference

### `ServerMesh.init(server)`

Initializes the mesh networking layer.

```javascript
await ServerMesh.init(server);
```

---

### `ServerMesh.send(payload)`

Routes an inter-server payload. Without `organization_id`, it uses pathless
control sockets. With `organization_id`, it uses only that organization's
socket array.

```javascript
ServerMesh.send({
	method: "custom.command",
	server: "srv123",
	data: {
		key: "value"
	}
});
```

---

### `ServerMesh.close()`

Gracefully closes all mesh connections and releases resources.

```javascript
await ServerMesh.close();
```

---

### Events

```javascript
ServerMesh.on("mesh.raw_message", handler);
```

---

## Security & TLS Specification

- **Encrypted Transport:** Peer communication uses TLS WebSocket connections.
- **Dedicated Mesh Port:** Mesh traffic is isolated from public HTTP and HTTPS traffic.
- **Ephemeral Certificates:** Temporary TLS certificates are generated during startup and automatically replaced on restart.
- **Peer Metadata:** Control connections include the server ID, IP, host, and configured cluster secret in their WebSocket protocol metadata.
- **UUID Propagation:** Application payloads retain the same UUID across IPC and mesh routing.

---

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `MESH_PORT` | `8090` | Port used for mesh communication. |
| `CLUSTER_SECRET` | — | Shared secret included in peer connection metadata. |
| `SERVER_ID` | Auto Generated | Persistent identifier for the current server node. |

---

## Announcements

Release notes, performance improvements, protocol updates, and networking enhancements are published with every release. Follow the repository to stay informed about new features and compatibility changes.

---

## Roadmap

Upcoming enhancements include:

- Native QUIC / HTTP/3 transport
- Adaptive routing based on latency measurements
- Automatic peer discovery across local networks
- Streaming binary frame optimizations
- Dynamic topology-aware routing

---

## How to Contribute

We welcome pull requests, bug reports, documentation improvements, performance optimizations, and security reviews.

Please review the project's `CONTRIBUTING.md` before submitting changes, and use the GitHub Issues page to report bugs or request new features.

Community feedback helps prioritize development and improve the platform.

---

## About

`@cocreate/server-mesh` is designed, built, and maintained by the CoCreate Developer Experience Team.

It serves as the distributed communication backbone of the CoCreate ecosystem, providing secure, low-latency messaging between clustered servers.

For questions, architecture discussions, or deployment guidance, visit the project's GitHub Discussions or join the CoCreate community.

---

## License

This software is dual-licensed under the GNU Affero General Public License version 3 (AGPLv3) and a commercial license.

### Open Source

For open-source projects and non-commercial use, this software is available under the AGPLv3. See the `LICENSE` file for the complete license text.

### Commercial

Organizations requiring proprietary use may obtain a commercial license from CoCreate.
