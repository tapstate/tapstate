# Tapstate

**Give applications and AI agents a continuously current view of what's
happening across enterprise systems.**

Tapstate captures and unifies database changes, then serves live
operational state to applications and AI agents.

An open-source unified operational data engine for turning fragmented
database changes into a continuously maintained view of business
reality.

```text
Production databases → Capture → Transform → Consolidate state
    → Materialized state → Applications · APIs · AI agents
```

**Capture. Transform. Serve. One deployable.**

> **Product status:** The current open-source release is an Alpha
> preview. It captures changes from MySQL, PostgreSQL, Oracle and
> SQL Server (with Db2 in preview), incrementally transforms and
> assembles data across sources, and materializes the resulting state in
> MongoDB. Stable state-consumption APIs, push/subscription delivery,
> and production-grade high availability are not yet part of the
> preview. See [What Works Today](#what-works-today) for the current
> scope and limitations.

## Why Operational State?

Enterprise AI needs more than knowledge and memory.

There are three complementary types of context:

| Context | The question it answers | What it provides |
|---|---|---|
| **Knowledge (RAG)** | What do our documents, policies, and knowledge sources say? | Retrieved enterprise knowledge |
| **Agent Memory** | What has the agent learned, remembered, or done? | Interaction and task continuity |
| **Operational State** | What is true across business systems right now? | Current business facts and relationships |

RAG helps an agent find what the enterprise knows. Memory helps it
preserve interaction context. Operational state helps it understand what
is happening across live business systems.

All three matter. Tapstate focuses on the third: continuously
maintained, consolidated operational state for applications and AI
agents that need to work with current business reality.

This becomes especially important when AI moves from answering questions
to participating in operational workflows—such as customer service,
order handling, payments, fulfillment, and inventory management.

Giving an agent access to multiple tools does not automatically give it
a coherent view of the business. It still has to reconcile entity
identity, source authority, freshness, partial updates, and conflicting
records. Repeating that work independently in every application and
agent creates unnecessary integration complexity.

An **Operational State Layer** is the shared architectural capability
that addresses this problem: a continuously maintained representation of
current business state, built from the systems of record and made
available to operational consumers.

## Why Tapstate?

Building and maintaining operational state often requires teams to
assemble several infrastructure components: a database, CDC, Kafka or
another message backbone, stream processing or custom jobs, and a serving
database, before applications and AI integrations see anything.

Each component solves an important problem, but teams still have to
integrate and operate the complete source-to-serving path.

Tapstate brings the operational state path into one unified engine:

-   **Capture** — capture database changes through log-based CDC.
-   **Transform** — filter, reshape, join, and enrich data
    incrementally as it changes.
-   **Consolidate** — combine changes across sources and maintain
    current business entities and operational views.
-   **Materialize** — persist the resulting state for downstream use.

One data path. One operational surface.

![Assembled streaming stack versus Tapstate: multiple separate systems
reduced to one deployable](docs/assets/stack-comparison.png)

For a more detailed comparison, see [Tapstate vs. a streaming
stack](https://tapstate.dev/docs/overview/vs-streaming-stack).

### How state is assembled

A **nest** transform builds one document from rows in several tables,
even across different databases, and keeps it current: every insert,
update, and delete on any contributing table lands in that document,
without anyone re-running a query. **Join** (including N:1) and
**unwind** cover flatter shapes.

```yaml
transforms:
  - id: assemble
    type: nest
    from: { order: orders, shipment: shipments }   # MySQL and PostgreSQL
    root:
      from: order
      key: [ id ]
      embed:
        - { from: shipment, on: { order_id: id }, as: array, path: shipments, arrayKey: [ id ] }
```

A nest emits a final-state changelog: a target upserted by key always
converges to the current document, but not every intermediate version
is guaranteed to be visible. Before sizing a large or fan-out-heavy
workload, read [Nest throughput
limits](docs/nest/throughput-limits.md).

### A focused role in the data architecture

Tapstate is designed for workloads whose primary outcome is continuously
maintained operational state.

-   **Tapstate** provides the state path: capture, transform,
    consolidate, and maintain current operational state.
-   **Kafka** remains valuable for event distribution and fan-out.
-   **Flink** remains valuable for complex event processing (CEP) and
    stateful or windowed analytics.

The decision is driven by the workload, not by a blanket
tool-replacement rule:

> Is the result addressed by an entity—what is true now—or by a time
> window and event sequence?

Entity-addressed operational state belongs on the state path. Event
distribution, event sequences, and window-oriented processing may belong
on the event path. The two can coexist.

## Try it in minutes

See how Tapstate turns changes from multiple databases into one
continuously maintained business object.

The local playground starts:

-   **MySQL** — order data
-   **PostgreSQL** — shipment data
-   **MongoDB** — materialized operational state
-   **Tapstate** — CDC, incremental transformation, and state
    maintenance

The example nests each order's shipments into a live order object
assembled from both source databases, and updates it as the underlying
records change.

You need Docker with the Compose plugin.

```sh
curl -fsSL https://install.tapstate.dev/demo | sh
```

The script starts the playground and saves its configuration under
`tapstate-demo/`.

### Use it in the browser

Open `http://127.0.0.1:8080` and sign in as `admin` with the password
saved in `tapstate-demo/.env`.

Inspect the sources and their discovered columns, the pipeline that
assembles them, and its monitoring and logs—or build a pipeline of
your own.

### Use it with an AI agent

The Tapstate MCP integration lets an AI agent create and operate
Tapstate sources and pipelines.

Install the Tapstate authoring skills:

```sh
npx --yes skills add tapstate/tapstate-skills
```

Then connect the MCP configuration generated by the playground. The
configuration is saved at `tapstate-demo/tapstate-mcp.json` and carries
a write token for the local playground.

For example, with Claude Code:

```sh
cd tapstate-demo && claude --mcp-config tapstate-mcp.json
```

Alternatively, copy its `tapstate` entry into your AI agent's MCP
settings.

You can then ask:

> Create a pipeline that keeps a live copy of the shipments in
> fulfillment_db with only their id, carrier and status, start it, and
> tell me its status.

**MCP scope today:** this integration enables AI-assisted creation and
operation of Tapstate sources and pipelines. It is not yet a
general-purpose interface for querying or consuming materialized
operational state.

### Use it from the CLI

For scripts, CI, and machines without a browser or an AI assistant, the
playground installs the CLI at `tapstate-demo/tapstate`.

To install the CLI independently:

```sh
curl -fsSL https://install.tapstate.dev/cli | sh
```

Workspaces are ordinary `.tap.yml` files that you can validate offline,
review in a diff, and apply from a pipeline job.

→ [Follow the quickstart](https://tapstate.dev/docs/overview/quickstart)

## What it's for

Tapstate is intended for applications and AI integrations that need a
current, consolidated view of operational entities across systems.

Common workload patterns include:

-   **AI agents and copilots** — provide fresh business context for
    workflows that act on current operational facts.
-   **Customer and account 360** — maintain a current view of customer
    information across systems.
-   **Order and fulfillment state** — assemble order, payment, and
    shipment information as it changes.
-   **Inventory and entitlement** — maintain current availability and
    access-related state.
-   **Operational data access** — establish a reusable state
    foundation for downstream applications and serving interfaces.
-   **Core-system offloading** — move appropriate read workloads away
    from transactional systems.

## What Works Today

The current open-source Alpha preview supports:

-   Initial load and CDC from **MySQL**, **PostgreSQL**, **Oracle**, and
    **SQL Server** (certified as sources), including common managed
    MySQL and PostgreSQL variants.
-   **Db2** (LUW and IBM i) as a source-only preview.
-   **MongoDB** as a certified source and as the materialization target.
-   Incremental transformation and cross-source assembly: filter, map,
    join (including N:1), unwind, and nest.
-   Creating and operating sources and pipelines through the web
    application, CLI, or an AI agent over MCP.
-   Inspecting pipeline status, metrics, and logs.

See [Connectors](https://tapstate.dev/docs/connectors) for capture modes
and per-database prerequisites.

### The bundled MongoDB instance

The playground starts MongoDB for you and Tapstate manages its
configuration. You do not need to install MongoDB, provide a URI, or
configure its namespaces and indexes.

MongoDB is the current reference backing store. This does not mean
Tapstate is conceptually MongoDB-only, and the bundled instance is not a
managed production database service. For your own installation, point
Tapstate at your MongoDB or Atlas, with separate databases for
configuration, durable cache, and application data. See
[Storage](docs/storage/README.md).

### Runtime and delivery characteristics

The runtime starts as a single member by default, with an [opt-in
cluster preview](docs/cluster/README.md).

MongoDB persists control-plane state and recovery positions. [Restart
recovery](docs/quickstart-online.md#restart-recovery) depends on the
source and read mode. Delivery is at-least-once.

### Current limitations

The Alpha preview is not production-ready. It does not currently
provide:

-   Production-grade high availability.
-   Exactly-once delivery guarantees.
-   A stable Tapstate State Data API.
-   Push or subscription delivery for consuming operational state.

These are important parts of the product's intended evolution. Do not
interpret the current materialization workflow as a generally available
state-serving API.

### Security note for the local playground

**The bundled store is not a security boundary.**

The local MongoDB instance runs without authentication. Tapstate's own
control-plane data—including users, tokens, audit records, connection
configuration, and applied artifacts—shares that instance with data
written by pipelines.

Anyone holding a Tapstate token can read that data back with an ordinary
source declaration. Do not put data in this deployment that its own
users should not see, and do not expose the playground or its bundled
services to untrusted networks.

Use the playground for local evaluation with non-sensitive data.

## Roadmap

The current Alpha preview proves the core path from database changes to
maintained operational state.

Tapstate is evolving toward a complete operational data engine with
broader production capabilities, including:

-   More operational sources, such as Sybase ASE and Kafka, and
    certified targets beyond MongoDB.
-   Bulk initial assembly and group-commit state for nest, so that
    building large documents approaches the speed of a plain copy.
-   Production-grade durability and high availability.
-   Stable interfaces for consuming operational state.
-   Pull and push mechanisms for downstream applications and services.
-   A broader connector ecosystem for operational systems and serving
    targets.

This section describes product direction, not a commitment that these
capabilities are available in the current release.

## Explore Tapstate

-   [Quickstart](https://tapstate.dev/docs/overview/quickstart) — get
    Tapstate running and see live state update.
-   [Concepts](https://tapstate.dev/docs/concepts/dsl) — understand
    sources, pipelines, resources, and the Tapstate DSL.
-   [Architecture](https://tapstate.dev/docs/overview/architecture) —
    understand the Capture → Transform → Serve architecture.
-   [Connectors](https://tapstate.dev/docs/connectors) — supported
    databases, capabilities, and capture modes.
-   [Transforms](https://tapstate.dev/docs/reference/transforms) —
    reshape and assemble operational state.
-   [Nest](docs/nest/README.md) — assemble one live document
    from many tables.
-   [Operations](https://tapstate.dev/docs/guides/observe-a-pipeline)
    — pipelines, status, metrics, logs, and runtime behavior.
-   [MCP & AI](https://tapstate.dev/docs/reference/mcp) — use Tapstate
    with AI agents and developer tools.
-   [Build & contribute](CONTRIBUTING.md) — build from source and
    contribute to Tapstate.
-   [Security & supported versions](SECURITY.md) — report a
    vulnerability and see which release lines still receive fixes.

[tapstate.dev/docs](https://tapstate.dev/docs) is the reviewed,
published documentation and the one to read. Pages under `docs/` in this
repository are engineering drafts kept next to the code, useful when you
are working on Tapstate itself.

**For agents and retrieval tooling:**
[`/llms.txt`](https://tapstate.dev/llms.txt) is a discovery index of the
documentation, and
[`/llms-full.txt`](https://tapstate.dev/llms-full.txt) carries the
combined page context. Prefer them over crawling this repository—they
carry the reviewed wording, including the line between what Alpha does
today and where the product is going.

## About

Tapstate is an Apache-2.0 open-source project from the team behind
TapData, built on years of production CDC and real-time data movement
experience.
