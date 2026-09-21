package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.OperatorStateStore;
import io.tapstate.spi.store.OperatorStateStores;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.PipelineLayoutStore;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StorePort;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The MongoDB implementation of the persistence port: it aggregates fourteen sub-stores — the artifact
 * truth layer, the epoch-fencing pipeline state store, the plain-upsert pipeline desired-state store,
 * the connection catalog, the discovered source-schema store, the connector distribution registry, the
 * derived connector catalog rows, the latest connection-test result per connection, the plain-upsert
 * per-pipeline observation store, the SRS meta store (one durable coordination record per mining chain),
 * and the editor-only Pipeline layout store — each bound to its own collection (or GridFS bucket) on the
 * verified connection's database.
 * Operator state is the one exception and sits in a separately named database on the same client. This
 * is the store bridge the assembly root wires into the platform under {@code --role=all}; the app sees
 * only the driver-free {@link StorePort}, so no driver type escapes this module (rule R3).
 */
public final class MongoStorePort implements StorePort {

    private static final int MAX_DATABASE_NAME_BYTES = 63;
    private static final Set<String> RESERVED_OPERATOR_STATE_DATABASES = Set.of("config", "local");

    /** The collection holding the canonical artifact truth layer. */
    public static final String ARTIFACTS = "artifacts";
    /** The collection holding one epoch-fenced checkpoint per pipeline. */
    public static final String PIPELINE_STATE = "pipeline_state";
    /** The collection holding one plain-upsert desired-intent doc per pipeline. */
    public static final String PIPELINE_DESIRED = "pipeline_desired";
    /** The collection holding one plain-upsert observation doc per pipeline. */
    public static final String PIPELINE_OBSERVATION = "pipeline_observation";
    /** One document per movement sample, left to expire by the server; the one series among these. */
    public static final String PIPELINE_RATE_HISTORY = "pipeline_rate_history";
    /** The collection holding one editor-only canvas layout per pipeline. */
    public static final String PIPELINE_LAYOUTS = "pipeline_layouts";
    /** The collection holding the registered connection configurations. */
    public static final String CONNECTIONS = "connections";
    /** The collection holding one discovered source model per connection. */
    public static final String SOURCE_SCHEMAS = "source_schemas";
    /** The GridFS bucket holding one registered connector artifact per content hash. */
    public static final String CONNECTOR_ARTIFACTS = "connector_artifacts";
    /** The collection holding one derived catalog row per registered connector. */
    public static final String CONNECTOR_CATALOG = "connector_catalog";

    /** Spec sources kept beside the derived rows, keyed by content hash. */
    public static final String CONNECTOR_SPECS = "connector_specs";
    /** The collection holding the latest connection-test result per connection. */
    public static final String CONNECTION_TEST_RESULTS = "connection_test_results";
    /** The collection holding one SRS coordination record per mining chain. */
    public static final String SRS_META = "srs_meta";
    /** The collection holding one durable SRS cursor per consumer pipeline and mining chain. */
    public static final String SRS_CONSUMER_OFFSETS = "srs_consumer_offsets";

    /** The durable change log: one document per change that entered a chain's per-table ring. */
    public static final String SRS_LOG = "srs_log";

    /**
     * The collection holding one document per pipeline, carrying the versioned record of the columns
     * each of its steps works out for itself. Keyed by pipeline id alone so both questions asked of it -
     * one step's latest, and dropping a removed pipeline's whole record - are answered by the {@code _id}
     * index every collection already has.
     */
    public static final String DERIVED_SCHEMAS = "derived_schemas";
    /** The collection holding one stateful-operator state document per key, per namespace. */
    public static final String OPERATOR_STATE = "operator_state";

    /**
     * The collection holding one document per element a stateful operator could never assemble. It sits
     * beside the state rather than with everything else because it is produced by the same run at the same
     * rates and is dropped with the same namespace - and because a dump or a restore aimed at what an
     * operator configured should not carry a pipeline's discarded rows along with it.
     */
    public static final String NEST_DEAD_LETTERS = "nest_dead_letters";

    /**
     * What an operator-state write is acknowledged on. Every other store here can take the client's
     * default, because what it holds can be worked out again from something else that survived. This one
     * cannot: it is where a change let past the source's read offset is being kept, it has no replica of
     * its own to fall back on, and the promise its port makes is that returning from a write means the
     * change is durable. A default that acknowledges on the primary alone breaks exactly that promise -
     * the write is reported done, the frontier advances past the change on the strength of it, and a
     * primary lost before the write replicated takes the change with it, with nothing anywhere reporting
     * a loss. Journaled as well as replicated, because a majority holding it only in memory has the same
     * shape one power cut further out.
     *
     * <p>The dead-letter collection is written with it too: what could not be assembled is the one copy
     * of those rows there is.
     */
    public static final WriteConcern NEST_STATE_WRITE_CONCERN = WriteConcern.MAJORITY.withJournal(true);

    private final ArtifactStore artifacts;
    private final StateStore state;
    private final DesiredStore desired;
    private final CatalogStore catalog;
    private final SchemaStore schemas;
    private final ConnectorRegistry connectors;
    private final ConnectorCatalogStore connectorCatalog;
    private final ConnectorSpecStore connectorSpecs;
    private final ConnectionTestResultStore connectionTestResults;
    private final ObservationStore observations;
    private final RateHistoryStore rateHistory;
    private final PipelineLayoutStore layouts;
    private final SrsMetaStore meta;
    private final SrsLogStore srsLog;
    private final DerivedSchemaStore derivedSchemas;
    private final KeyedStateStore keyedState;
    private final NestDeadLetterStore nestDeadLetters;
    private final OperatorStateStores operatorStateStores;

    /**
     * Binds the sub-stores to their own collections on the verified connection's database, bar operator
     * state, which gets the separately configured {@code operatorStateDatabase} on the same client. The
     * connection must have been verified first (its client opened); the sub-stores share that one client
     * and are closed with it when the connection closes.
     */
    public MongoStorePort(MongoConnection connection, String operatorStateDatabase) {
        this(connection, operatorStateDatabase, MongoRateHistoryStore.DEFAULT_RETENTION);
    }

    /**
     * A port whose sample history keeps samples for {@code rateHistoryRetention}: the one store here with
     * a configured bound, written onto its expiring index at construction.
     */
    public MongoStorePort(
            MongoConnection connection, String operatorStateDatabase, Duration rateHistoryRetention) {
        Objects.requireNonNull(connection, "connection");
        MongoDatabase database = connection.database();
        this.artifacts = new MongoArtifactStore(connection.client(), SystemCollections.ARTIFACTS.on(database));
        this.state = new MongoStateStore(SystemCollections.PIPELINE_STATE.on(database));
        this.desired = new MongoDesiredStore(SystemCollections.PIPELINE_DESIRED.on(database));
        this.catalog = new MongoCatalogStore(SystemCollections.CONNECTIONS.on(database));
        this.schemas = new MongoSchemaStore(SystemCollections.SOURCE_SCHEMAS.on(database));
        this.connectors = new MongoConnectorRegistry(SystemCollections.CONNECTOR_ARTIFACTS.bucketOn(database));
        this.connectorCatalog = new MongoConnectorCatalogStore(SystemCollections.CONNECTOR_CATALOG.on(database));
        this.connectorSpecs = new MongoConnectorSpecStore(SystemCollections.CONNECTOR_SPECS.on(database));
        this.connectionTestResults =
                new MongoConnectionTestResultStore(SystemCollections.CONNECTION_TEST_RESULTS.on(database));
        this.observations = new MongoObservationStore(SystemCollections.PIPELINE_OBSERVATION.on(database));
        this.rateHistory = new MongoRateHistoryStore(
                database, SystemCollections.PIPELINE_RATE_HISTORY.on(database), rateHistoryRetention);
        this.layouts = new MongoPipelineLayoutStore(SystemCollections.PIPELINE_LAYOUTS.on(database));
        this.meta = new MongoSrsMetaStore(connection.client(),
                SystemCollections.SRS_META.on(database), SystemCollections.SRS_CONSUMER_OFFSETS.on(database));
        this.srsLog = new MongoSrsLogStore(SystemCollections.SRS_LOG.on(database));
        this.derivedSchemas = new MongoDerivedSchemaStore(SystemCollections.DERIVED_SCHEMAS.on(database));
        // Operator state alone sits in its configured database on the same client. Same connection, same
        // credentials, same lifecycle - a different database. What that operator could not assemble goes
        // in the same database, being produced by the same run.
        this.operatorStateStores = new MongoOperatorStateStores(
                connection.client(), database.getName(), operatorStateDatabase);
        OperatorStateStore defaultState = operatorStateStores.inDatabase(operatorStateStores.defaultDatabase());
        this.keyedState = defaultState.state();
        this.nestDeadLetters = defaultState.deadLetters();
    }

    static String requireOperatorStateDatabase(String name, String controlDatabase) {
        if (name == null || name.isBlank()) {
            throw invalidOperatorStateDatabase(null);
        }
        try {
            MongoNamespace.checkDatabaseNameValidity(name);
        } catch (IllegalArgumentException e) {
            throw invalidOperatorStateDatabase(e);
        }
        if (name.indexOf('$') >= 0
                || name.getBytes(StandardCharsets.UTF_8).length > MAX_DATABASE_NAME_BYTES
                || RESERVED_OPERATOR_STATE_DATABASES.stream().anyMatch(name::equalsIgnoreCase)
                || name.equalsIgnoreCase(controlDatabase)) {
            throw invalidOperatorStateDatabase(null);
        }
        return name;
    }

    private static TapstateException invalidOperatorStateDatabase(Throwable cause) {
        return new TapstateException(StoreError.INVALID_OPERATOR_STATE_DATABASE, Map.of(), cause);
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
    public CatalogStore catalog() {
        return catalog;
    }

    @Override
    public SchemaStore schemas() {
        return schemas;
    }

    @Override
    public ConnectorRegistry connectors() {
        return connectors;
    }

    @Override
    public ConnectorCatalogStore connectorCatalog() {
        return connectorCatalog;
    }

    @Override
    public ConnectorSpecStore connectorSpecs() {
        return connectorSpecs;
    }

    @Override
    public ConnectionTestResultStore connectionTestResults() {
        return connectionTestResults;
    }

    @Override
    public ObservationStore observations() {
        return observations;
    }

    @Override
    public RateHistoryStore rateHistory() {
        return rateHistory;
    }

    @Override
    public SrsLogStore srsLog() {
        return srsLog;
    }

    @Override
    public PipelineLayoutStore layouts() {
        return layouts;
    }

    @Override
    public SrsMetaStore meta() {
        return meta;
    }

    @Override
    public DerivedSchemaStore derivedSchemas() {
        return derivedSchemas;
    }

    @Override
    public KeyedStateStore keyedState() {
        return keyedState;
    }

    @Override
    public NestDeadLetterStore nestDeadLetters() {
        return nestDeadLetters;
    }

    @Override
    public OperatorStateStores operatorStateStores() {
        return operatorStateStores;
    }
}
