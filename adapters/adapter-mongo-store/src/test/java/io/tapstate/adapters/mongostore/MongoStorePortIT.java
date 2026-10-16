package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Witnesses the aggregated store port against a real Mongo replica-set: one write through each of the
 * sub-stores lands in its own distinct, named storage area and reads back through that same sub-store,
 * so the artifact truth layer, the pipeline state store, the pipeline desired-state store, the
 * per-pipeline observation store, the connection catalog, the discovered source-schema store, the
 * connector distribution registry, the stored connector spec sources, the latest connection-test result
 * per connection and the SRS meta store never share storage. Where Docker is absent this aborts on a developer machine and fails in
 * CI, where a skip would be a green build that ran nothing.
 */
@RequiresDocker
class MongoStorePortIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final DslParser PARSER = new DslParser();
    private static final String OPERATOR_STATE_DATABASE = "tapstate_nest";
    private static final String OPERATOR_STATE_DATABASE_A = "operator_state_route_a";
    private static final String OPERATOR_STATE_DATABASE_B = "operator_state_route_b";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static final String ORDERS = """
            version: tapstate/v1
            kind: source
            id: orders
            connector: mysql
            config:
              host: localhost
            """;

    @Test
    void aggregatesTheElevenSubStoresEachOnItsOwnStorage() {
        // The Testcontainers Mongo speaks plaintext; TLS is opt-in, so a plaintext URL connects with
        // no flag. TLS wiring itself is covered by MongoConnectionTest.
        String uri = REPLICA_SET.getReplicaSetUrl();
        MongoConnectionSettings settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
            MongoStorePort port = new MongoStorePort(connection, OPERATOR_STATE_DATABASE);
            dropAggregateStorage(uri);

            // one write through each of the eleven sub-stores
            port.artifacts().save(PARSER.parse(ORDERS));
            port.state().create("orders_sync", "{\"phase\":\"snapshot\"}", Instant.parse("2026-07-06T00:00:00Z"));
            port.desired().save(new DesiredState("orders_sync", PipelineState.RUNNING, "rev-abc"));
            port.observations().save(new Observation("orders_sync", PipelineState.RUNNING,
                    Map.of(), Map.of(), Map.of("orders", "w7")));
            port.layouts().save(new PipelineLayout("orders_sync",
                    Map.of("source:orders", new PipelineLayout.NodePosition(80, 120)),
                    new PipelineLayout.Viewport(0, 0, 1)));
            port.catalog().save(new ConnectionConfig("mysql-local", "mysql", Map.of("host", "localhost")));
            port.schemas().save(new DiscoveredSourceModel("mysql-local", "mysql", 1783998000000L, new SourceModel(List.of(
                    new SourceTable("orders", List.of(), List.of(), List.of())))));
            port.connectors().register(
                    "mysql", "1.3.5", RegistrationSource.SEED, "mysql-connector-bytes".getBytes(StandardCharsets.UTF_8));
            port.connectorSpecs().put("spec-hash", "{\"properties\":{\"id\":\"mysql\"}}".getBytes(StandardCharsets.UTF_8));
            port.connectionTestResults().save(new ConnectionTestResult(
                    "mysql-local",
                    "mysql",
                    ConnectionTestResult.Outcome.PASSED,
                    List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.PASSED, null, null, null, null)),
                    1783939200000L));
            port.meta().create("orders@mysql-1", "7d");

            // each reads back through its own sub-store
            assertThat(port.artifacts().get("orders")).isPresent();
            assertThat(port.state().read("orders_sync")).isPresent();
            assertThat(port.desired().read("orders_sync")).isPresent();
            assertThat(port.observations().read("orders_sync"))
                    .hasValueSatisfying(observation -> assertThat(observation.positions()).containsEntry("orders", "w7"));
            assertThat(port.layouts().get("orders_sync"))
                    .hasValueSatisfying(layout -> assertThat(layout.nodes()).containsKey("source:orders"));
            assertThat(port.catalog().get("mysql-local")).isPresent();
            assertThat(port.schemas().get("mysql-local")).isPresent();
            assertThat(port.connectors().list()).hasSize(1);
            assertThat(port.connectorSpecs().get("spec-hash")).isPresent();
            assertThat(port.connectionTestResults().find("mysql-local")).isPresent();
            assertThat(port.meta().read("orders@mysql-1")).isPresent();

            // and each landed in its own distinct, named storage — no concern shares storage
            String databaseName = new ConnectionString(uri).getDatabase();
            try (MongoClient raw = MongoClients.create(uri)) {
                MongoDatabase database = raw.getDatabase(databaseName);
                assertThat(database.getCollection(MongoStorePort.ARTIFACTS).countDocuments()).isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.PIPELINE_STATE).countDocuments()).isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.PIPELINE_DESIRED).countDocuments()).isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.PIPELINE_OBSERVATION).countDocuments()).isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.PIPELINE_LAYOUTS).countDocuments()).isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.CONNECTIONS).countDocuments()).isEqualTo(1);
                // Two, not one: a discovery is an envelope naming the generation plus a document per table it
                // found - the single table saved above. It still says the model landed here and nowhere else.
                assertThat(database.getCollection(MongoStorePort.SOURCE_SCHEMAS).countDocuments()).isEqualTo(2);
                assertThat(database.getCollection(MongoStorePort.CONNECTOR_ARTIFACTS + ".files").countDocuments())
                        .isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.CONNECTOR_SPECS).countDocuments()).isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.CONNECTION_TEST_RESULTS).countDocuments())
                        .isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.SRS_META).countDocuments()).isEqualTo(1);
            }
        }
    }

    /**
     * Operator state is the one sub-store that does not live beside the others. It is the working state of
     * a running job rather than anything an operator configured, and it is written at event rates, so it
     * is kept out of the database holding the configuration. This case passes the compatible default
     * explicitly because selecting that name belongs to the assembly root, not to the adapter.
     */
    @Test
    void nestStateLandsInItsOwnDatabaseRatherThanTheOneHoldingPipelineConfiguration() {
        String uri = REPLICA_SET.getReplicaSetUrl();
        MongoConnectionSettings settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
            MongoStorePort port = new MongoStorePort(connection, OPERATOR_STATE_DATABASE);

            port.keyedState().save("nest.orders_sync.assemble.items", "k1",
                    "held-child".getBytes(StandardCharsets.UTF_8));

            assertThat(port.keyedState().load("nest.orders_sync.assemble.items", "k1")).isPresent();
            String configured = new ConnectionString(uri).getDatabase();
            try (MongoClient raw = MongoClients.create(uri)) {
                assertThat(raw.getDatabase(OPERATOR_STATE_DATABASE)
                        .getCollection(MongoStorePort.OPERATOR_STATE).countDocuments()).isEqualTo(1);
                assertThat(raw.getDatabase(configured)
                        .getCollection(MongoStorePort.OPERATOR_STATE).countDocuments()).isZero();
            }
        }
    }

    /**
     * And so does what that operator could never assemble, for the same reason and into the same database:
     * it is produced by the same run at the same rates and dropped with the same namespace, so an operation
     * aimed at what an operator configured should not be able to reach it by accident either.
     */
    @Test
    void nestDeadLettersLandInTheSameDatabaseAsTheStateTheyCameFrom() {
        String uri = REPLICA_SET.getReplicaSetUrl();
        MongoConnectionSettings settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
            MongoStorePort port = new MongoStorePort(connection, OPERATOR_STATE_DATABASE);

            port.nestDeadLetters().record(new NestDeadLetterRecord("nest.orders_sync.assemble.items",
                    "[\"items\"]#[1]~i", "mysql-a", "1:1", 0L, 9_000L, Map.of("id", 1)));

            assertThat(port.nestDeadLetters().read("nest.orders_sync.assemble.items", 10)).hasSize(1);
            String configured = new ConnectionString(uri).getDatabase();
            try (MongoClient raw = MongoClients.create(uri)) {
                assertThat(raw.getDatabase(OPERATOR_STATE_DATABASE)
                        .getCollection(MongoStorePort.NEST_DEAD_LETTERS).countDocuments()).isEqualTo(1);
                assertThat(raw.getDatabase(configured)
                        .getCollection(MongoStorePort.NEST_DEAD_LETTERS).countDocuments()).isZero();
            }
        }
    }

    @Test
    void onePortRoutesAStateAndItsDeadLettersToTheSelectedDatabase() {
        String uri = REPLICA_SET.getReplicaSetUrl();
        MongoConnectionSettings settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
            MongoStorePort port = new MongoStorePort(connection, OPERATOR_STATE_DATABASE);
            try (MongoClient raw = MongoClients.create(uri)) {
                raw.getDatabase(OPERATOR_STATE_DATABASE_A).drop();
                raw.getDatabase(OPERATOR_STATE_DATABASE_B).drop();
            }

            String namespace = "nest.orders.order_doc.$root";
            var first = port.operatorStateStores().inDatabase(OPERATOR_STATE_DATABASE_A);
            var second = port.operatorStateStores().inDatabase(OPERATOR_STATE_DATABASE_B);
            first.state().save(namespace, "same-key", "first".getBytes(StandardCharsets.UTF_8));
            first.deadLetters().record(new NestDeadLetterRecord(
                    namespace, "same-element", "mysql-a", "1:1", 0L, 9_000L, Map.of("id", 1)));

            assertThat(first.state().load(namespace, "same-key")).isPresent();
            assertThat(first.deadLetters().read(namespace, 10)).hasSize(1);
            assertThat(second.state().load(namespace, "same-key")).isEmpty();
            assertThat(second.deadLetters().read(namespace, 10)).isEmpty();

            second.state().save(namespace, "same-key", "second".getBytes(StandardCharsets.UTF_8));
            second.deadLetters().record(new NestDeadLetterRecord(
                    namespace, "same-element", "mysql-b", "1:1", 0L, 9_001L, Map.of("id", 1)));
            assertThat(new String(first.state().load(namespace, "same-key").orElseThrow(),
                    StandardCharsets.UTF_8)).isEqualTo("first");
            assertThat(new String(second.state().load(namespace, "same-key").orElseThrow(),
                    StandardCharsets.UTF_8)).isEqualTo("second");

            first.state().dropNamespace(namespace);
            first.deadLetters().dropNamespace(namespace);
            assertThat(second.state().load(namespace, "same-key")).isPresent();
            assertThat(second.deadLetters().read(namespace, 10)).hasSize(1);
        }
    }

    @Test
    void aPerNestDatabaseUsesTheSameStartupValidationAsTheDeploymentDefault() {
        String uri = REPLICA_SET.getReplicaSetUrl();
        MongoConnectionSettings settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
            MongoStorePort port = new MongoStorePort(connection, OPERATOR_STATE_DATABASE);

            assertThatThrownBy(() -> port.operatorStateStores().inDatabase("local"))
                    .isInstanceOf(TapstateException.class)
                    .extracting(thrown -> ((TapstateException) thrown).code().code())
                    .isEqualTo("store.invalid-operator-state-database");
        }
    }

    @Test
    void reclaimingAPipelineEmptiesItsThreeLifecycleStoresAndLeavesTheSharedChainStanding() {
        String uri = REPLICA_SET.getReplicaSetUrl();
        MongoConnectionSettings settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
            MongoStorePort port = new MongoStorePort(connection, OPERATOR_STATE_DATABASE);
            // The suite shares one replica-set across tests, so the counts below only mean what they say
            // once this test owns these four collections outright.
            dropLifecycleStorage(uri);
            port.state().create("doomed", "{\"state\":\"STOPPED\"}", Instant.parse("2026-07-06T00:00:00Z"));
            port.desired().save(new DesiredState("doomed", PipelineState.STOPPED, "rev-abc"));
            port.observations().save(new Observation("doomed", PipelineState.STOPPED,
                    Map.of(), Map.of(), Map.of("orders", "w7")));
            port.meta().create("orders@mysql-1", "7d");
            port.meta().upsertConsumerOffset("orders@mysql-1", new ConsumerOffset("doomed", Map.of("orders", 5L),
                    new ChainPosition(new SourceOrder(1, 5), "gtid:aaa-1:5")));
            port.meta().upsertConsumerOffset("orders@mysql-1", new ConsumerOffset("survivor", Map.of("orders", 9L),
                    new ChainPosition(new SourceOrder(1, 9), "gtid:aaa-1:9")));

            port.state().delete("doomed");
            port.desired().delete("doomed");
            port.observations().delete("doomed");
            port.meta().miningChainIdsWithConsumer("doomed")
                    .forEach(chain -> port.meta().detachConsumer(chain, "doomed"));

            String databaseName = new ConnectionString(uri).getDatabase();
            try (MongoClient raw = MongoClients.create(uri)) {
                MongoDatabase database = raw.getDatabase(databaseName);
                // The three that belong to this pipeline alone go; each has its own storage, so this is
                // three separate removals rather than one that happens to catch them all.
                assertThat(database.getCollection(MongoStorePort.PIPELINE_STATE).countDocuments()).isZero();
                assertThat(database.getCollection(MongoStorePort.PIPELINE_DESIRED).countDocuments()).isZero();
                assertThat(database.getCollection(MongoStorePort.PIPELINE_OBSERVATION).countDocuments()).isZero();
                // The chain is shared, so its root stays, as does the surviving cursor in its own storage.
                assertThat(database.getCollection(MongoStorePort.SRS_META)
                        .countDocuments(new Document("_id", "orders@mysql-1"))).isEqualTo(1);
                assertThat(database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).countDocuments()).isEqualTo(1);
            }
            assertThat(port.meta().read("orders@mysql-1").orElseThrow().consumerOffsets())
                    .containsExactly(new ConsumerOffset("survivor", Map.of("orders", 9L),
                    new ChainPosition(new SourceOrder(1, 9), "gtid:aaa-1:9")));
        }
    }

    /** Empties the four collections this test counts, so a sibling test's writes cannot answer for it. */
    private static void dropLifecycleStorage(String uri) {
        try (MongoClient raw = MongoClients.create(uri)) {
            MongoDatabase database = raw.getDatabase(new ConnectionString(uri).getDatabase());
            database.getCollection(MongoStorePort.PIPELINE_STATE).drop();
            database.getCollection(MongoStorePort.PIPELINE_DESIRED).drop();
            database.getCollection(MongoStorePort.PIPELINE_OBSERVATION).drop();
            database.getCollection(MongoStorePort.SRS_META).drop();
            database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).drop();
        }
    }

    /** Empties every configuration collection asserted by the aggregate-store witness. */
    private static void dropAggregateStorage(String uri) {
        try (MongoClient raw = MongoClients.create(uri)) {
            MongoDatabase database = raw.getDatabase(new ConnectionString(uri).getDatabase());
            List.of(
                    MongoStorePort.ARTIFACTS,
                    MongoStorePort.PIPELINE_STATE,
                    MongoStorePort.PIPELINE_DESIRED,
                    MongoStorePort.PIPELINE_OBSERVATION,
                    MongoStorePort.PIPELINE_LAYOUTS,
                    MongoStorePort.CONNECTIONS,
                    MongoStorePort.SOURCE_SCHEMAS,
                    MongoStorePort.CONNECTOR_ARTIFACTS + ".files",
                    MongoStorePort.CONNECTOR_ARTIFACTS + ".chunks",
                    MongoStorePort.CONNECTOR_SPECS,
                    MongoStorePort.CONNECTION_TEST_RESULTS,
                    MongoStorePort.SRS_META,
                    MongoStorePort.SRS_CONSUMER_OFFSETS
            ).forEach(collection -> database.getCollection(collection).drop());
        }
    }
}
