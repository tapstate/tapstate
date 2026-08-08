package io.tapstate.app;

import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StorePort;

/**
 * In-memory {@link StorePort} for the assembly-layer tests: it supplies real desired, state and observation
 * sub-stores (what the converger consumes and the publisher writes), plus a real artifact store and SRS meta
 * store (what the data-plane capture and topology assembly read). The remaining sub-stores are not exercised
 * and throw. The no-arg form allocates its own artifact store; the other takes a pre-seeded one.
 */
final class InMemoryStorePort implements StorePort {

    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final InMemoryObservationStore observations = new InMemoryObservationStore();
    private final InMemoryArtifactStore artifacts;
    private final InMemorySrsMetaStore meta = new InMemorySrsMetaStore();
    private final InMemorySchemaStore schemas = new InMemorySchemaStore();
    private final InMemoryKeyedStateStore keyedState = new InMemoryKeyedStateStore();

    InMemoryStorePort() {
        this(new InMemoryArtifactStore());
    }

    InMemoryStorePort(InMemoryArtifactStore artifacts) {
        this.artifacts = artifacts;
    }

    @Override
    public ArtifactStore artifacts() {
        return artifacts;
    }

    @Override
    public StateStore state() {
        return state;
    }

    @Override
    public DesiredStore desired() {
        return desired;
    }

    @Override
    public ObservationStore observations() {
        return observations;
    }

    @Override
    public SrsMetaStore meta() {
        return meta;
    }

    @Override
    public KeyedStateStore keyedState() {
        return keyedState;
    }

    @Override
    public CatalogStore catalog() {
        throw new UnsupportedOperationException("catalog is not exercised by the convergence wiring test");
    }

    @Override
    public SchemaStore schemas() {
        return schemas;
    }

    @Override
    public ConnectorRegistry connectors() {
        throw new UnsupportedOperationException("connectors are not exercised by the convergence wiring test");
    }

    @Override
    public ConnectorCatalogStore connectorCatalog() {
        throw new UnsupportedOperationException("the connector catalog is not exercised by the convergence wiring test");
    }

    @Override
    public ConnectorSpecStore connectorSpecs() {
        throw new UnsupportedOperationException("connector specs are not exercised by the convergence wiring test");
    }

    @Override
    public ConnectionTestResultStore connectionTestResults() {
        throw new UnsupportedOperationException(
                "connection test results are not exercised by the convergence wiring test");
    }
}
