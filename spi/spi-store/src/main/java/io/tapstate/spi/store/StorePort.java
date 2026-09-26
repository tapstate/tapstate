package io.tapstate.spi.store;

/**
 * The persistence port groups the artifact truth, lifecycle state and intent, connector metadata,
 * current observations, bounded history, coordination, editor layout and operator state. A pure
 * interface over the core ring only (rule R2); a database adapter implements the sub-stores behind it.
 */
public interface StorePort {

    /** The canonical, authoritative store of applied resources. */
    ArtifactStore artifacts();

    /** The pipeline state store; transitions land only through its fencing compare-and-swap. */
    StateStore state();

    /** The pipeline desired-state store; plain upsert intent, the split counterpart to {@link #state()}. */
    DesiredStore desired();

    /** The store of registered connection / connector-instance configurations. */
    CatalogStore catalog();

    /** The store of source models discovered off registered connections. */
    SchemaStore schemas();

    /** The connector distribution registry: registered connector artifacts and their bytes. */
    ConnectorRegistry connectors();

    /** The derived connector catalog rows: one normalized capability row per registered connector. */
    ConnectorCatalogStore connectorCatalog();

    /** The spec sources kept beside the derived rows, keyed by content hash. */
    ConnectorSpecStore connectorSpecs();

    /** The store of the latest connection-test result per connection. */
    ConnectionTestResultStore connectionTestResults();

    /** The per-pipeline observation store; plain upsert latest projection, read by the monitor read faces. */
    ObservationStore observations();

    /**
     * The per-pipeline history of movement samples: appended on a cadence, kept for a bounded time, read by
     * one pipeline and one time range.
     */
    RateHistoryStore rateHistory();

    /** The disposable, bounded cache of closed rate-history buckets. */
    default HistoryRollupStore historyRollups() {
        throw new UnsupportedOperationException("history rollups are unavailable");
    }

    /** The bounded, best-effort event history for each pipeline incarnation. */
    default PipelineEventStore events() {
        throw new UnsupportedOperationException("pipeline event history is unavailable");
    }

    /** The SRS meta store: one durable offset / consumer-cursor / schema record per mining chain. */
    SrsMetaStore meta();

    /** Cluster-scoped owner leases for node sessions, pipeline actuation, capture and later operations. */
    default WorkloadClaimStore workloadClaims() {
        throw new UnsupportedOperationException("this store is not cluster-capable");
    }

    /** The majority-committed ACTIVE membership and monotonic topology revision. */
    default ClusterMembershipStore clusterMembership() {
        throw new UnsupportedOperationException("this store is not cluster-capable");
    }

    /**
     * The SRS change log: every change that entered a chain's per-table ring, so the changes outlive the
     * process that read them and a ring can be rebuilt where it left off.
     */
    SrsLogStore srsLog();

    /**
     * The side record of the columns a pipeline step works out for itself, keyed by pipeline and step.
     * Kept beside the artifact rather than inside it: a derived value inside the canonical bytes would
     * make a pipeline nobody edited read as edited the moment the derivation changed.
     */
    DerivedSchemaStore derivedSchemas();

    /** The editor-only canvas layout store; it never changes a Pipeline artifact's content hash. */
    PipelineLayoutStore layouts();

    /**
     * The cold layer under a stateful operator: one opaque state document per key, within a namespace.
     * Read and written on the data path as keys are handled, never enumerated.
     */
    KeyedStateStore keyedState();

    /**
     * Where changes a stateful operator can never place in a document are kept, so that what was discarded
     * can be looked at rather than only counted. Written on the data path, read by whoever is looking.
     */
    NestDeadLetterStore nestDeadLetters();

    /**
     * Operator state routed by database. Store implementations with one physical target inherit a fixed
     * view; adapters that support per-operator placement override it.
     */
    default OperatorStateStores operatorStateStores() {
        return OperatorStateStores.fixed("default", keyedState(), nestDeadLetters());
    }
}
