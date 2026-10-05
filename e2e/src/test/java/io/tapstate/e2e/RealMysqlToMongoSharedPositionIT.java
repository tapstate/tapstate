package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.adapters.pdk.ConnectorStateNamespace;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Two pipelines share one physical CDC reader and its source position. The second joins the owner's
 * expanded table subscription; after a process replacement both must resume.
 * Deletes made while the server is down distinguish resumed CDC from another snapshot.
 *
 * <p>The declarative vocabulary cannot compare connector-owned identity bytes across a process
 * replacement or require a confirmed tail position before another pipeline starts.
 * The real MySQL connector, its position object and its separate class loader are essential here.
 */
class RealMysqlToMongoSharedPositionIT {
    private static final Duration WAIT = Duration.ofSeconds(60);
    private record SharedReader(String chainId, String namespace) { }
    @BeforeAll
    static void requireConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aSharedPhysicalReaderExpandsItsTablesAndBothPipelinesResumeAfterRestart(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String first = "reader_first_" + suffix;
        String second = "reader_second_" + suffix;
        Map<String, Object> mysql = SharedMySql.settings("reader_position_" + suffix);
        sql(mysql, "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))",
                "CREATE TABLE later_orders (id INT PRIMARY KEY, name VARCHAR(64))",
                "INSERT INTO orders VALUES (1, 'seeded')",
                "INSERT INTO later_orders VALUES (1, 'before-cdc')");
        String store = SharedMongo.replicaSetUrl("reader_position_store_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("reader_position_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        byte[] firstIdentity;
        SharedReader originalReader;

        try (MongoEndpoints mongo = new MongoEndpoints(); MongoClient state = MongoClients.create(store)) {
            try (ServerHandle server = tier.launch(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                apply(control, mysql, targetUri, first, "first_source", "orders", "snapshot_and_cdc");
                awaitIds(control, first, mongo, target, "orders", List.of(1L));
                String safeStart = Await.answered("the first reader's safe start token",
                        () -> Optional.ofNullable(control.resumePoint(first).get("token")));
                sql(mysql, "UPDATE orders SET name='first-tail' WHERE id=1");
                // The connector start boundary is already durable. Wait for a confirmed change that
                // moves beyond that boundary before replacing the process.
                long firstDeadline = System.nanoTime() + WAIT.toNanos();
                Await.until("the first reader's confirmed tail position", remaining(firstDeadline),
                        () -> control.ackedChangeSeq(first).isPresent()
                                && !safeStart.equals(control.resumePoint(first).get("token")),
                        () -> "position=" + control.resumePoint(first) + ", logs=" + control.logs(first));
                originalReader = sharedReader(control, state, store, first, "first_source", "orders", firstDeadline);
                firstIdentity = identity(state, originalReader, firstDeadline);
                assertPhysicalSelection(state, store, originalReader.chainId(), List.of("orders"), firstDeadline);

                // The second pipeline does not open a second connector stream. Its table is added to
                // the first physical reader before its own CDC-only ring reader begins.
                apply(control, mysql, targetUri, second, "second_source", "later_orders", "cdc_only");
                sql(mysql, "INSERT INTO later_orders VALUES (2, 'second-tail')");
                long joinedDeadline = System.nanoTime() + WAIT.toNanos();
                awaitIds(control, second, mongo, target, "later_orders", List.of(2L), joinedDeadline);
                SharedReader firstReader = sharedReader(control, state, store, first, "first_source", "orders", joinedDeadline);
                SharedReader secondReader = sharedReader(control, state, store, second, "second_source", "later_orders", joinedDeadline);
                assertThat(firstReader).as("attaching another source does not replace the original physical reader")
                        .isEqualTo(originalReader);
                assertThat(secondReader).as("both actual scoped consumers share one physical chain and notes namespace")
                        .isEqualTo(originalReader);
                assertPhysicalSelection(state, store, originalReader.chainId(), List.of("orders", "later_orders"), joinedDeadline);
                assertThat(identity(state, firstReader, joinedDeadline)).isEqualTo(firstIdentity);
                assertThat(identity(state, secondReader, joinedDeadline)).as("the attached consumer reads the same physical identity")
                        .isEqualTo(firstIdentity);
                assertThat(control.state(first)).contains(PipelineState.RUNNING);
            }

            sql(mysql, "DELETE FROM orders WHERE id=1", "DELETE FROM later_orders WHERE id=2",
                    "INSERT INTO later_orders VALUES (3, 'during-downtime')");
            try (ServerHandle server = tier.launch(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.login("e2e", "e2e-password");
                sql(mysql, "INSERT INTO orders VALUES (99, 'after-restart')",
                        "INSERT INTO later_orders VALUES (99, 'after-restart')");
                // The old target rows must disappear, and genuinely new rows must arrive on both
                // paths. RUNNING alone could have been an observation left by the previous process.
                awaitIds(control, first, mongo, target, "orders", List.of(99L));
                long restartedDeadline = System.nanoTime() + WAIT.toNanos();
                awaitIds(control, second, mongo, target, "later_orders", List.of(3L, 99L), restartedDeadline);
                SharedReader firstReader = sharedReader(control, state, store, first, "first_source", "orders", restartedDeadline);
                SharedReader secondReader = sharedReader(control, state, store, second, "second_source", "later_orders", restartedDeadline);
                assertThat(firstReader).as("the first live restarted consumer keeps its original physical chain")
                        .isEqualTo(originalReader);
                assertThat(secondReader).as("the second live restarted consumer shares that exact chain and namespace")
                        .isEqualTo(originalReader);
                assertPhysicalSelection(state, store, originalReader.chainId(), List.of("orders", "later_orders"), restartedDeadline);
                assertThat(identity(state, firstReader, restartedDeadline)).isEqualTo(firstIdentity);
                assertThat(identity(state, secondReader, restartedDeadline)).as("restart preserves one shared connector identity, byte for byte")
                        .isEqualTo(firstIdentity);
                assertThat(control.errorCount(first)).contains(0L);
                assertThat(control.errorCount(second)).contains(0L);
            }
        }
    }

    private static void apply(ControlPlane control, Map<String, Object> mysql, String targetUri, String pipeline,
            String source, String table, String mode) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: reader_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        resources.put(source + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(source, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                        mysql.get("username"), mysql.get("password"), table));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: %s }
                serve:
                  from: %s
                  sync:
                    - source: reader_target
                """.formatted(pipeline, source, mode, table));
        control.apply(resources);
        control.discoverSchema(source, "mysql", mysql);
        control.lifecycle(pipeline, LifecycleVerb.START);
    }

    private static void awaitIds(ControlPlane control, String pipeline, MongoEndpoints mongo,
            EndpointAddress target, String table, List<Long> expected) {
        awaitIds(control, pipeline, mongo, target, table, expected, System.nanoTime() + WAIT.toNanos());
    }

    private static void awaitIds(ControlPlane control, String pipeline, MongoEndpoints mongo,
            EndpointAddress target, String table, List<Long> expected, long deadline) {
        Await.until("rows in " + table + " from " + pipeline, remaining(deadline),
                () -> ids(mongo, target, table).equals(expected)
                        && control.state(pipeline).orElse(null) == PipelineState.RUNNING,
                () -> "rows=" + ids(mongo, target, table) + ", state=" + control.state(pipeline)
                        + ", logs=" + control.logs(pipeline));
    }

    private static List<Long> ids(MongoEndpoints mongo, EndpointAddress target, String table) {
        return mongo.documents(target, table).stream()
                .map(row -> ((Number) row.get("id")).longValue()).sorted().toList();
    }

    private static SharedReader sharedReader(ControlPlane control, MongoClient client, String store,
            String pipeline, String source, String table, long deadline) {
        remaining(deadline);
        var position = control.positionRead(pipeline);
        assertThat(position.pipelineId()).isEqualTo(pipeline);
        var matching = position.chains().stream().filter(chain -> source.equals(chain.sourceId())
                && chain.tables().contains(table)).toList();
        assertThat(matching).as("the public position names one actual chain for %s/%s/%s", pipeline, source, table).hasSize(1);
        String chainId = matching.getFirst().chainId();
        assertThat(chainId).isNotBlank();
        String consumerId = SrsConsumerId.of(pipeline, source).value();
        String database = new ConnectionString(store).getDatabase();
        assertThat(database).isNotBlank();
        Document consumer = client.getDatabase(database).getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("_id", new Document("chain", chainId).append("pipeline", consumerId)))
                .projection(new Document("miningChainId", 1).append("pipelineId", 1)
                        .append("ownerPipelineId", 1).append("sourceNodeId", 1))
                .maxTime(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS).first();
        assertThat(consumer).as("the exact scoped consumer links the requested pipeline/source to its physical chain").isNotNull();
        assertThat(consumer.getString("miningChainId")).isEqualTo(chainId);
        assertThat(consumer.getString("pipelineId")).isEqualTo(consumerId);
        assertThat(consumer.getString("ownerPipelineId")).isEqualTo(pipeline);
        assertThat(consumer.getString("sourceNodeId")).isEqualTo(source);
        return new SharedReader(chainId, ConnectorStateNamespace.ofShared(chainId));
    }

    private static byte[] identity(MongoClient client, SharedReader reader, long deadline) {
        Document id = new Document("ns", reader.namespace()).append("k", "SERVER_NAME");
        Document record = client.getDatabase("tapstate_nest").getCollection("operator_state")
                .find(new Document("_id", id)).projection(new Document("state", 1))
                .maxTime(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS).first();
        assertThat(record).as("the actual physical reader identity is durable under %s", reader.namespace()).isNotNull();
        Binary identity = record.get("state", Binary.class);
        assertThat(identity).as("the connector's exact identity bytes are present").isNotNull();
        return identity.getData();
    }

    private static void assertPhysicalSelection(MongoClient client, String store, String chainId,
            List<String> tables, long deadline) {
        String database = new ConnectionString(store).getDatabase();
        assertThat(database).isNotBlank();
        Document root = client.getDatabase(database).getCollection(MongoStorePort.SRS_META)
                .find(new Document("_id", chainId))
                .projection(new Document("epoch", 1).append("captureTables", 1)
                        .append("captureServingEpoch", 1).append("captureServingTables", 1))
                .maxTime(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS).first();
        assertThat(root).as("the actual shared physical chain has one durable root").isNotNull();
        assertThat(root.getString("_id")).isEqualTo(chainId);
        assertThat(root.getList("captureTables", String.class)).as("the physical subscription includes exactly the demanded union")
                .containsExactlyInAnyOrderElementsOf(tables);
        assertThat(root.getList("captureServingTables", String.class)).as("the actual reader has published the complete serving union")
                .containsExactlyInAnyOrderElementsOf(tables);
        assertThat(root.get("epoch")).isInstanceOf(Number.class);
        assertThat(root.get("captureServingEpoch")).isInstanceOf(Number.class);
        long epoch = ((Number) root.get("epoch")).longValue();
        assertThat(epoch).isPositive();
        assertThat(((Number) root.get("captureServingEpoch")).longValue())
                .as("the serving union belongs to the current physical capture epoch").isEqualTo(epoch);
    }

    private static Duration remaining(long deadline) {
        long left = deadline - System.nanoTime();
        assertThat(left).as("the existing shared-reader phase budget remains positive").isPositive();
        return Duration.ofNanos(left);
    }

    private static void sql(Map<String, Object> mysql, String... statements) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
