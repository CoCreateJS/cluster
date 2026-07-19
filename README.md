# CoCreate Server

A secure, stateless, clustered headless server built for high-performance multi-tenancy, real-time collaboration, and sandboxed AST execution. This module serves as the primary orchestrator of the CoCreate distributed ecosystem.

---

## Documentation

For a complete guide and working integration configurations, refer to the [CoCreate Server Core Documentation](https://cocreatejs.com/docs/server).

## Table of Contents

* [The Power of the Clustered Headless Core](#the-power-of-the-clustered-headless-core)
* [Why CoCreate Server?](#why-cocreate-server)
* [The Architecture & Component Mesh](#the-architecture--component-mesh)
* [Dynamic Tenant Eviction & Self-Healing Chain Reaction](#dynamic-tenant-eviction--self-healing-chain-reaction)
* [Security-Hardened (Zero-Trust) Design](#security-hardened-zero-trust-design)
* [Installation](#installation)
* [Announcements](#announcements)
* [Roadmap](#roadmap)
* [How to Contribute](#how-to-contribute)
* [About](#about)

---

## The Power of the Clustered Headless Core

`@cocreate/server` is a high-performance system designed to scale multi-tenant, real-time collaboration applications with absolute efficiency. Instead of relying on a bloated runtime stack, it utilizes a native, zero-dependency clustered execution topology:

### Multi-Cluster Native IPC Routing

Leveraging the operating system's native multi-core process mapping, the Master orchestrator forks independent worker processes to maximize processing throughput. IPC relays dispatch state packets natively across workers with sub-millisecond overhead:

$$\text{Master Process (Primary)} \xrightarrow{\text{Direct OS IPC}} \begin{cases} \text{Worker 1} & \text{(Client Socket Slices)} \\ \text{Worker 2} & \text{(File / Asset Server)} \\ \text{Worker } N & \text{(AST Execution Engine)} \end{cases}$$

### Zero-Dependency Core Philosophy

To mitigate supply-chain vulnerabilities, prevent package bloat, and guarantee lightning-fast cold boot times, the server enforces a strict zero-dependency policy (the only exception being the highly audited `node-html-parser` for layout rendering).

### AST Flow Logic Isolation

The server does not evaluate raw user scripts. Dynamic client logic is executed exclusively within a locked-down Abstract Syntax Tree (AST) Flow Engine, eliminating Remote Code Execution (RCE) vectors at the structural level.

---

## Why CoCreate Server?

Traditional Node.js backend architectures struggle under high-concurrency multi-tenant loads due to memory-resident connection bloat and shared state conflicts.

`@cocreate/server` solves this by decoupling computing power from database state. By serving as a purely stateless worker runtime, it fetches configurations, connection profiles, and execution routes dynamically from your secured database. If a compute node experiences a resource boundary or high traffic spikes, the `ServerAutoscaler` can cleanly cordon, drain, and destroy the machine while spawning fresh nodes to scale up without disrupting ongoing operations.

---

## The Architecture & Component Mesh

As the central gateway of the platform, the server imports and orchestrates peer modules to form a secure, unified collaboration fabric:

| Component Module | Architectural Role | Functional Responsibility |
| :--- | :--- | :--- |
| **@cocreate/acme** | Certificate Authority Integration | Generates and manages dynamic SSL certificates on-the-fly, dropping SSL-termination penalties. |
| **@cocreate/socket-server** | Real-Time Transport Plane | Multiplexes end-user browser connections using low-overhead WebSocket tunnels. |
| **@cocreate/server-mesh** | Inter-Node Control & Data Plane | Drives on-demand, lazy-loaded server-to-server tunnels isolated by Organization ID. |
| **@cocreate/crud-server** | Multi-Database State Routing | Translates standard MongoDB-style JSON syntax into safe parallel database transactions. |
| **@cocreate/server-autoscaler** | Autonomous Lifecycle Manager | Orchestrates horizontal scaling, P2P unicast cordon, and cooperative node draining. |
| **@cocreate/lazy-loader** | Dynamic Assembly Parser | Lazily streams and constructs custom micro-app endpoints as they are requested. |

---

## Dynamic Tenant Eviction & Self-Healing Chain Reaction

To protect server RAM and database sockets, `@cocreate/server` utilizes an automated, connection-driven lifecycle. The network socket state acts as the single source of truth:

$$\text{Final Client WebSocket Close} \xrightarrow{\text{60s Grace Period}} \text{Emit 'orgDeleted' locally} \xrightarrow{\text{Cooperative Teardown}} \begin{cases} \text{Close MongoDB Connection Pools} \\ \text{Flush Bandwidth \& Billing Telemetry} \\ \text{Close Dedicated P2P Org Tunnels} \end{cases}$$

When the last client browser disconnects from an organization, a 60-second grace timer is initiated. If no clients reconnect, the worker emits a local `orgDeleted` signal. This executes a cascading teardown sequence:

1. **`db.js`** catches the signal, closes the cached connection pools for that specific database, and frees socket slots.
2. **`usage.js`** calculates remaining byte metrics in memory and flushes billing telemetry to the platform database.
3. **`server_mesh.js`** calls `ws.close()` on any dedicated data-plane tunnels to peer servers. Peer servers catch the native TCP close frame and evict the routing records instantly with zero custom control packets.

---

## Security-Hardened (Zero-Trust) Design

The server is engineered for deployment in highly restricted cloud networks (VPC/PrivateLink):

* **No Port Exposure:** All incoming server ports are closed except for standard secure web interfaces (80 / 443). SSH is disabled entirely.
* **Administrative Isolation:** Control plane functions (such as node cordons, traffic shedding, or VM shutdowns) require interactive multi-factor authentication (2FA). Standard platform API keys do not possess the scope or rights to communicate on the 8090 P2P mesh control plane.
* **Cryptographic Blindness:** Integration with client databases utilizes Client-Side Field-Level Encryption (CSFLE). Database connection strings and API keys are stored exclusively as ciphertexts. Plaintext decryption keys are fetched dynamically from isolated Cloud KMS modules and are never exposed to the database server.

---

## Installation

### NPM Installation

```bash
npm i @cocreate/server
```

### Yarn Installation

```bash
yarn add @cocreate/server
```

---

## Announcements

All structural updates, security audits, and API upgrades are documented in our `CHANGELOG.md` and the [CoCreate Server GitHub Releases](https://github.com/CoCreate-app/CoCreate-server/releases) page. You may also subscribe to our release notifications for breaking changes and deployment guidelines.

---

## Roadmap

If you are interested in the future design direction of the core clustered engine, please review our active issues and pull requests. We welcome constructive architectural feedback from the developer community.

---

## How to Contribute

We encourage contribution to our libraries (you might even score some nifty swag), please see our [CONTRIBUTING.md](https://github.com/CoCreate-app/CoCreate-server/blob/master/CONTRIBUTING.md) guide for details. If you encounter any bugs or wish to make feature requests, please submit an issue on our [GitHub Issues](https://github.com/CoCreate-app/CoCreate-server/issues) tracker. We want this library to be community-driven, and CoCreate led. We need your help to realize this goal.

For broader system configurations and API guides, please visit our [CoCreate Server Documentation](https://cocreatejs.com/docs/server).

---

## About

`@cocreate/server` is designed, built, and supported by the CoCreate Developer Experience Team.

> [!NOTE]
> Please contact the Developer Experience Team via [GitHub Discussions](https://github.com/CoCreate-app/CoCreate-server/discussions) or join our Discord channel for any architecture or deployment queries.

`@cocreate/server` is maintained and funded by CoCreate. The names and logos for CoCreate are trademarks of CoCreate, LLC.