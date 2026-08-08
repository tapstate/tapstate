package io.tapstate.spi.store;

/**
 * The persistence port: one store surface with ten concerns — the artifact truth layer, the pipeline
 * state store (whose transitions land only through the epoch-fencing compare-and-swap), the pipeline
 * desired-state store (plain upsert intent, the split counterpart to the state store), the connection
 * catalog, the discovered source-schema store, the connector distribution registry, the derived
 * connector catalog rows (one normalized capability row per registered connector), the latest
 * connection-test result per connection, the per-pipeline observation store (plain upsert latest
 * projection, read by the monitor read faces), and the SRS meta store (one durable coordination
 * record per mining chain). A pure interface over the core ring only (rule R2); a store backend
 * (a database adapter) implements the ten sub-stores behind it.
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

    /** The SRS meta store: one durable offset / consumer-cursor / schema record per mining chain. */
    SrsMetaStore meta();

    /**
     * The cold layer under a stateful operator: one opaque state document per key, within a namespace.
     * Read and written on the data path as keys are handled, never enumerated.
     */
    KeyedStateStore keyedState();
}
