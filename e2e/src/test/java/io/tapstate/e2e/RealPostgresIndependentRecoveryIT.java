package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Independent native PostgreSQL channels keep their own recovery positions on the same database. */
class RealPostgresIndependentRecoveryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final String TABLE = "support_case";
    private static final String SLOW_SOURCE = "slow_postgres_source";
    private static final String FAST_SOURCE = "fast_postgres_source";
    private static final String SLOW_PIPELINE = "slow_postgres_pipeline";
    private static final String FAST_PIPELINE = "fast_postgres_pipeline";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot_and_cdc", "cdc_only"})
    void aStoppedDirectChannelRecoversHighAndADeleteAfterItsSiblingAlreadyConfirmedThem(
            String readMode) throws Exception {
        try (SharedPostgres.Fixture postgresFixture = SharedPostgres.fixture()) {
            String databaseName = "independent_postgres_" + readMode;
            Map<String, Object> postgres = postgresFixture.settings(databaseName);
            try (Connection connection = SharedPostgres.connect(postgres); Statement sql = connection.createStatement()) {
                assertThat(connection.getAutoCommit()).as("each statement is its own source transaction").isTrue();
                sql.execute("CREATE TABLE support_case (id INT PRIMARY KEY, priority TEXT)");
                sql.execute("ALTER TABLE support_case REPLICA IDENTITY FULL");
                sql.execute("INSERT INTO support_case VALUES (1, 'Seeded')");
            }
            EndpointAddress slowTarget = EndpointAddress.uri(SharedMongo.replicaSetUrl(databaseName + "_slow"));
            EndpointAddress fastTarget = EndpointAddress.uri(SharedMongo.replicaSetUrl(databaseName + "_fast"));
            String storeUri = SharedMongo.replicaSetUrl(databaseName + "_state");

            try (MongoClient reader = MongoClients.create(storeUri);
                    MongoEndpoints targets = new MongoEndpoints();
                    OwnedLaunch launched = launch(postgresFixture, storeUri, databaseName + "_operators");
                    Connection writer = SharedPostgres.connect(postgres);
                    Statement sql = writer.createStatement()) {
                RealProcessServer server = launched.server();
                MongoDatabase store = reader.getDatabase(new ConnectionString(storeUri).getDatabase());
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(workspace(postgres, slowTarget, fastTarget, readMode));
                Map<String, Object> discovery = new LinkedHashMap<>(postgres);
                discovery.put("user", discovery.remove("username"));
                discovery.put("schema", "public");
                control.discoverSchema(SLOW_SOURCE, "postgres", discovery);
                control.discoverSchema(FAST_SOURCE, "postgres", discovery);
                control.lifecycle(SLOW_PIPELINE, LifecycleVerb.START);
                control.lifecycle(FAST_PIPELINE, LifecycleVerb.START);
                awaitState(control, SLOW_PIPELINE, PipelineState.RUNNING);
                awaitState(control, FAST_PIPELINE, PipelineState.RUNNING);
                Await.until("two independent native PostgreSQL slots to be streaming", TIMEOUT,
                        () -> activeSlots(postgres) == 2,
                        () -> "slots=" + activeSlots(postgres) + ", logs=" + control.logs(SLOW_PIPELINE));
                if (readMode.equals("cdc_only")) {
                    assertThat(targets.documents(slowTarget, TABLE))
                            .as("CDC-only does not snapshot rows from before its own start").isEmpty();
                    assertThat(targets.documents(fastTarget, TABLE)).isEmpty();
                    sql.execute("DELETE FROM support_case WHERE id = 1");
                    sql.execute("INSERT INTO support_case VALUES (1, 'Seeded')");
                    awaitRow(control, targets, slowTarget, "1", "Seeded");
                    awaitRow(control, targets, fastTarget, "1", "Seeded");
                } else {
                    awaitRow(control, targets, slowTarget, "1", "Seeded");
                    awaitRow(control, targets, fastTarget, "1", "Seeded");
                }

                sql.execute("UPDATE support_case SET priority = 'Low' WHERE id = 1");
                sql.execute("INSERT INTO support_case VALUES (3, 'delete-while-paused')");
                awaitRow(control, targets, slowTarget, "1", "Low");
                awaitRow(control, targets, fastTarget, "1", "Low");
                awaitRow(control, targets, slowTarget, "3", "delete-while-paused");
                awaitRow(control, targets, fastTarget, "3", "delete-while-paused");
                sql.execute("INSERT INTO support_case VALUES (4, 'baseline-barrier')");
                awaitRow(control, targets, slowTarget, "4", "baseline-barrier");
                awaitRow(control, targets, fastTarget, "4", "baseline-barrier");
                Await.until("both native channels to record confirmed database positions", TIMEOUT,
                        () -> token(consumer(store, SLOW_PIPELINE, SLOW_SOURCE)) != null
                                && token(consumer(store, FAST_PIPELINE, FAST_SOURCE)) != null
                                && consumer(store, SLOW_PIPELINE, SLOW_SOURCE)
                                        .getList("directBatches", Document.class).isEmpty()
                                && consumer(store, FAST_PIPELINE, FAST_SOURCE)
                                        .getList("directBatches", Document.class).isEmpty(),
                        () -> consumers(store));
                control.lifecycle(SLOW_PIPELINE, LifecycleVerb.PAUSE);
                awaitState(control, SLOW_PIPELINE, PipelineState.PAUSED);
                Document paused = consumer(store, SLOW_PIPELINE, SLOW_SOURCE);
                long confirmedSequence = paused.get("sinkAckedSeq", Number.class).longValue();
                String fastBefore = token(consumer(store, FAST_PIPELINE, FAST_SOURCE));
                assertThat(paused.getString("miningChainId"))
                        .isNotEqualTo(consumer(store, FAST_PIPELINE, FAST_SOURCE).getString("miningChainId"));

                long deliveredFrom = System.nanoTime();
                sql.execute("UPDATE support_case SET priority = 'Low' WHERE id = 1");
                sql.execute("UPDATE support_case SET priority = 'High' WHERE id = 1");
                sql.execute("DELETE FROM support_case WHERE id = 3");
                awaitRow(control, targets, fastTarget, "1", "High");
                awaitMissing(control, targets, fastTarget, "3");
                sql.execute("INSERT INTO support_case VALUES (5, 'after-high-barrier')");
                awaitRow(control, targets, fastTarget, "5", "after-high-barrier");
                long deliveryMillis = Duration.ofNanos(System.nanoTime() - deliveredFrom).toMillis();
                Await.until("the healthy channel to advance its own confirmed position", TIMEOUT,
                        () -> !fastBefore.equals(token(consumer(store, FAST_PIPELINE, FAST_SOURCE))),
                        () -> consumers(store));
                Await.until("the paused channel to register pending source changes beyond its confirmed prefix", TIMEOUT,
                        () -> consumer(store, SLOW_PIPELINE, SLOW_SOURCE).getList("directBatches", Document.class)
                                .stream().anyMatch(batch -> batch.get("targets", Document.class)
                                        .get(TABLE, Number.class).longValue() >= confirmedSequence + 3),
                        () -> consumers(store));
                Document held = consumer(store, SLOW_PIPELINE, SLOW_SOURCE);
                long firstPending = held.getList("directBatches", Document.class).getFirst()
                        .get("seq", Number.class).longValue();
                assertThat(held.get("sinkAckedSeq", Number.class).longValue())
                        .as("a confirmed recovery prefix cannot pass the first pending source batch")
                        .isLessThan(firstPending);
                assertThat(row(targets, slowTarget, "1")).isEqualTo("Low");
                assertThat(row(targets, slowTarget, "3")).isEqualTo("delete-while-paused");

                control.stop(SLOW_PIPELINE, false);
                awaitState(control, SLOW_PIPELINE, PipelineState.STOPPED);
                Await.until("only the healthy native channel to remain active after the keep-state stop", TIMEOUT,
                        () -> activeSlots(postgres) == 1,
                        () -> "active slots=" + activeSlots(postgres));
                long resumedFrom = System.nanoTime();
                control.lifecycle(SLOW_PIPELINE, LifecycleVerb.START);
                awaitState(control, SLOW_PIPELINE, PipelineState.RUNNING);
                awaitRow(control, targets, slowTarget, "1", "High");
                awaitMissing(control, targets, slowTarget, "3");
                awaitRow(control, targets, slowTarget, "5", "after-high-barrier");
                long recoveryMillis = Duration.ofNanos(System.nanoTime() - resumedFrom).toMillis();
                Await.until("the resumed independent slot and its sibling to both be active", TIMEOUT,
                        () -> activeSlots(postgres) == 2,
                        () -> "active slots=" + activeSlots(postgres));
                assertThat(control.errorCount(SLOW_PIPELINE)).contains(0L);
                assertThat(row(targets, fastTarget, "1")).isEqualTo("High");
                System.out.println("PostgreSQL direct " + readMode + " carried Low/High and a delete in "
                        + deliveryMillis + " ms; its stopped sibling recovered in " + recoveryMillis + " ms.");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot_and_cdc", "cdc_only"})
    void aSharedNativeCaptureKeepsItsSlotWhenAnotherSourceOwnsItAfterAProcessRestart(
            String readMode) throws Exception {
        try (SharedPostgres.Fixture postgresFixture = SharedPostgres.fixture()) {
            String databaseName = "shared_owner_postgres_" + readMode;
            Map<String, Object> postgres = postgresFixture.settings(databaseName);
            try (Connection connection = SharedPostgres.connect(postgres); Statement sql = connection.createStatement()) {
                assertThat(connection.getAutoCommit()).isTrue();
                sql.execute("CREATE TABLE support_case (id INT PRIMARY KEY, priority TEXT)");
                sql.execute("ALTER TABLE support_case REPLICA IDENTITY FULL");
                sql.execute("INSERT INTO support_case VALUES (1, 'Seeded')");
            }
            EndpointAddress slowTarget = EndpointAddress.uri(SharedMongo.replicaSetUrl(databaseName + "_slow"));
            EndpointAddress fastTarget = EndpointAddress.uri(SharedMongo.replicaSetUrl(databaseName + "_fast"));
            String storeUri = SharedMongo.replicaSetUrl(databaseName + "_state");
            String operatorDatabase = databaseName + "_operators";
            try (MongoClient reader = MongoClients.create(storeUri);
                    MongoEndpoints targets = new MongoEndpoints();
                    Connection writer = SharedPostgres.connect(postgres);
                    Statement sql = writer.createStatement()) {
                MongoDatabase store = reader.getDatabase(new ConnectionString(storeUri).getDatabase());
                List<String> originalSlots;
                String stoppedConfirmation;
                try (OwnedLaunch launched = launch(postgresFixture, storeUri, operatorDatabase)) {
                    RealProcessServer first = launched.server();
                    ControlPlane control = new ControlPlane(first.baseUrl());
                    control.bootstrapAndLogin("e2e", "e2e-password");
                    control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    control.apply(workspace(postgres, slowTarget, fastTarget, readMode, true));
                    Map<String, Object> discovery = new LinkedHashMap<>(postgres);
                    discovery.put("user", discovery.remove("username"));
                    discovery.put("schema", "public");
                    control.discoverSchema(SLOW_SOURCE, "postgres", discovery);
                    control.discoverSchema(FAST_SOURCE, "postgres", discovery);
                    control.lifecycle(SLOW_PIPELINE, LifecycleVerb.START);
                    awaitState(control, SLOW_PIPELINE, PipelineState.RUNNING);
                    control.lifecycle(FAST_PIPELINE, LifecycleVerb.START);
                    awaitState(control, FAST_PIPELINE, PipelineState.RUNNING);
                    Await.until("one native slot to serve both shared sources", TIMEOUT,
                            () -> activeSlots(postgres) == 1,
                            () -> "slots=" + slotNames(postgres) + ", logs=" + control.logs(SLOW_PIPELINE));
                    if (readMode.equals("cdc_only")) {
                        assertThat(targets.documents(slowTarget, TABLE)).isEmpty();
                        assertThat(targets.documents(fastTarget, TABLE)).isEmpty();
                        sql.execute("DELETE FROM support_case WHERE id = 1");
                        sql.execute("INSERT INTO support_case VALUES (1, 'Low')");
                    } else {
                        awaitRow(control, targets, slowTarget, "1", "Seeded");
                        awaitRow(control, targets, fastTarget, "1", "Seeded");
                        sql.execute("UPDATE support_case SET priority = 'Low' WHERE id = 1");
                    }
                    sql.execute("INSERT INTO support_case VALUES (3, 'delete-during-restart')");
                    awaitRow(control, targets, slowTarget, "1", "Low");
                    awaitRow(control, targets, fastTarget, "1", "Low");
                    awaitRow(control, targets, slowTarget, "3", "delete-during-restart");
                    awaitRow(control, targets, fastTarget, "3", "delete-during-restart");
                    sql.execute("INSERT INTO support_case VALUES (4, 'owner-barrier')");
                    awaitRow(control, targets, slowTarget, "4", "owner-barrier");
                    awaitRow(control, targets, fastTarget, "4", "owner-barrier");
                    Await.until("both sources to confirm the shared baseline", TIMEOUT,
                            () -> token(consumer(store, SLOW_PIPELINE, SLOW_SOURCE)) != null
                                    && token(consumer(store, FAST_PIPELINE, FAST_SOURCE)) != null,
                            () -> consumers(store));
                    String chain = consumer(store, SLOW_PIPELINE, SLOW_SOURCE).getString("miningChainId");
                    assertThat(consumer(store, FAST_PIPELINE, FAST_SOURCE).getString("miningChainId"))
                            .isEqualTo(chain);
                    assertThat(store.getCollection(MongoStorePort.SRS_META).find(new Document("_id", chain)).first())
                            .containsEntry("sourceReadDurable", true);
                    originalSlots = slotNames(postgres);
                    assertThat(originalSlots).hasSize(1);
                    control.stop(SLOW_PIPELINE, false);
                    awaitState(control, SLOW_PIPELINE, PipelineState.STOPPED);
                    stoppedConfirmation = token(consumer(store, SLOW_PIPELINE, SLOW_SOURCE));
                    first.kill();
                }
                Await.until("the crashed capture to release its connection while retaining its slot", TIMEOUT,
                        () -> activeSlots(postgres) == 0, () -> "slots=" + slotNames(postgres));
                sql.execute("UPDATE support_case SET priority = 'Low' WHERE id = 1");
                sql.execute("UPDATE support_case SET priority = 'High' WHERE id = 1");
                sql.execute("DELETE FROM support_case WHERE id = 3");
                sql.execute("INSERT INTO support_case VALUES (5, 'written-with-server-down')");
                long recoveredFrom = System.nanoTime();
                try (OwnedLaunch launched = launch(postgresFixture, storeUri, operatorDatabase)) {
                    RealProcessServer second = launched.server();
                    ControlPlane control = new ControlPlane(second.baseUrl());
                    control.login("e2e", "e2e-password");
                    awaitState(control, SLOW_PIPELINE, PipelineState.STOPPED);
                    awaitState(control, FAST_PIPELINE, PipelineState.RUNNING);
                    awaitRow(control, targets, fastTarget, "1", "High");
                    awaitMissing(control, targets, fastTarget, "3");
                    awaitRow(control, targets, fastTarget, "5", "written-with-server-down");
                    assertThat(slotNames(postgres)).as("a different owner reuses the original physical capture slot")
                            .isEqualTo(originalSlots);
                    assertThat(activeSlots(postgres)).isEqualTo(1);
                    assertThat(token(consumer(store, SLOW_PIPELINE, SLOW_SOURCE))).isEqualTo(stoppedConfirmation);
                    assertThat(row(targets, slowTarget, "1")).isEqualTo("Low");
                    assertThat(row(targets, slowTarget, "3")).isEqualTo("delete-during-restart");
                    assertThat(control.errorCount(FAST_PIPELINE)).contains(0L);
                    System.out.println("Shared PostgreSQL " + readMode + " recovered changes written with the server down"
                            + " under another source owner in "
                            + Duration.ofNanos(System.nanoTime() - recoveredFrom).toMillis() + " ms with the same slot.");
                }
            }
        }
    }

    private record OwnedLaunch(RealProcessServer server, SharedPostgres.Closing closing) implements AutoCloseable {
        @Override public void close() throws Exception { closing.close(); }
    }

    private static OwnedLaunch launch(SharedPostgres.Fixture fixture, String store, String operatorDatabase) {
        SharedPostgres.Closing closing = fixture.pending("owned PostgreSQL recovery server");
        RealProcessServer server = RealProcessServer.start(store, operatorDatabase);
        fixture.bind(closing, server, server::terminated);
        return new OwnedLaunch(server, closing);
    }

    private static Map<String, String> workspace(Map<String, Object> postgres, EndpointAddress slowTarget,
            EndpointAddress fastTarget, String readMode) {
        return workspace(postgres, slowTarget, fastTarget, readMode, false);
    }

    private static Map<String, String> workspace(Map<String, Object> postgres, EndpointAddress slowTarget,
            EndpointAddress fastTarget, String readMode, boolean srs) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SLOW_SOURCE + ".tap.yml", source(SLOW_SOURCE, postgres));
        resources.put(FAST_SOURCE + ".tap.yml", source(FAST_SOURCE, postgres));
        resources.put("slow_target.tap.yml", target("slow_target", slowTarget));
        resources.put("fast_target.tap.yml", target("fast_target", fastTarget));
        resources.put(SLOW_PIPELINE + ".tap.yml", pipeline(SLOW_PIPELINE, SLOW_SOURCE, "slow_target", readMode, srs));
        resources.put(FAST_PIPELINE + ".tap.yml", pipeline(FAST_PIPELINE, FAST_SOURCE, "fast_target", readMode, srs));
        return resources;
    }

    private static String source(String id, Map<String, Object> postgres) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: postgres
                config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                mode: cdc
                tables: [ support_case ]
                """.formatted(id, postgres.get("host"), postgres.get("port"), postgres.get("database"),
                postgres.get("username"), postgres.get("password"));
    }

    private static String target(String id, EndpointAddress address) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(id, address.text("uri"));
    }

    private static String pipeline(String id, String source, String target, String readMode, boolean srs) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: %s }
                settings: { read_mode: %s }
                serve:
                  from: support_case
                  sync:
                    - source: %s
                """.formatted(id, source, srs, readMode, target);
    }

    private static List<String> slotNames(Map<String, Object> postgres) {
        try (Connection connection = SharedPostgres.connect(postgres); Statement query = connection.createStatement();
                ResultSet slots = query.executeQuery("SELECT slot_name FROM pg_replication_slots WHERE database = '"
                        + postgres.get("database") + "' ORDER BY slot_name")) {
            List<String> names = new ArrayList<>();
            while (slots.next()) names.add(slots.getString(1));
            return List.copyOf(names);
        } catch (Exception failed) {
            throw new AssertionError("cannot inspect the witness database's replication slot names", failed);
        }
    }

    private static long activeSlots(Map<String, Object> postgres) {
        try (Connection connection = SharedPostgres.connect(postgres); Statement query = connection.createStatement();
                ResultSet slots = query.executeQuery("SELECT count(*) FROM pg_replication_slots WHERE active "
                        + "AND database = '" + postgres.get("database") + "'")) {
            slots.next();
            return slots.getLong(1);
        } catch (Exception failed) {
            throw new AssertionError("cannot inspect the witness database's active replication slots", failed);
        }
    }

    private static Document consumer(MongoDatabase store, String pipeline, String source) {
        return store.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("pipelineId", SrsConsumerId.of(pipeline, source).value())).first();
    }

    private static String token(Document consumer) {
        return consumer == null ? null : consumer.getString("sinkAckedSrcpos");
    }

    private static String consumers(MongoDatabase store) {
        return store.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find().into(new java.util.ArrayList<>()).toString();
    }

    private static String row(MongoEndpoints targets, EndpointAddress target, String id) {
        return targets.documents(target, TABLE).stream()
                .filter(document -> id.equals(String.valueOf(document.get("id"))))
                .map(document -> document.getString("priority")).findFirst().orElse(null);
    }

    private static void awaitRow(ControlPlane control, MongoEndpoints targets, EndpointAddress target,
            String id, String expected) {
        Await.until(TABLE + " row " + id + " to hold " + expected, TIMEOUT,
                () -> expected.equals(row(targets, target, id)),
                () -> "rows=" + targets.documents(target, TABLE) + ", slow=" + control.state(SLOW_PIPELINE)
                        + ", fast=" + control.state(FAST_PIPELINE) + ", slow logs=" + control.logs(SLOW_PIPELINE));
    }

    private static void awaitMissing(ControlPlane control, MongoEndpoints targets, EndpointAddress target, String id) {
        Await.until(TABLE + " row " + id + " to be deleted", TIMEOUT,
                () -> row(targets, target, id) == null,
                () -> "rows=" + targets.documents(target, TABLE) + ", slow logs=" + control.logs(SLOW_PIPELINE));
    }

    private static void awaitState(ControlPlane control, String pipeline, PipelineState state) {
        Await.until(pipeline + " to reach " + state, TIMEOUT,
                () -> control.state(pipeline).filter(state::equals).isPresent(),
                () -> control.state(pipeline) + ", logs=" + control.logs(pipeline));
    }
}
