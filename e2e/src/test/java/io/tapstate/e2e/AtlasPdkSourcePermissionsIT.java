package io.tapstate.e2e;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The checked Atlas JAR must report real source authorization failures in either runtime mode.
 * The source is an owned authenticated local replica set, separate from metadata and targets.
 * Revoking find is independent of revoking changeStream: snapshot-only still crosses without the
 * latter permission, while a CDC pipeline fails and then recovers when that one action is granted.
 * No actual Atlas user or Cloud provisioning is involved in these consumer diagnostics.
 */
@RequiresDocker
class AtlasPdkSourcePermissionsIT {

    private static final String CONNECTOR = "mongodb-atlas";
    private static final String TABLE = "probe";
    private static final String REPLICA_SET = "pdk-permissions-rs";
    private static final String PASSWORD = "controlled-pdk-permissions-password";
    private static final Duration TIMEOUT = Duration.ofSeconds(120);

    @Container
    private static final GenericContainer<?> MONGO = authenticatedMongo();

    private static GenericContainer<?> authenticatedMongo() {
        byte[] key = new byte[64];
        new java.security.SecureRandom().nextBytes(key);
        return new GenericContainer<>(DockerImageName.parse("mongo:7.0"))
                .withLabel("tapstate.test.owner", "atlas-pdk-permissions-" + UUID.randomUUID())
                .withExposedPorts(27017)
                .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
                .withCopyToContainer(Transferable.of(Base64.getEncoder().encode(key), 0400),
                        "/data/configdb/member.key")
                .withCommand("--replSet", REPLICA_SET, "--keyFile", "/data/configdb/member.key", "--bind_ip_all")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*MongoDB init process complete; ready for start up.*", 1))
                        .withStrategy(Wait.forLogMessage("(?i).*waiting for connections.*", 2))
                        .withStrategy(Wait.forListeningPort()))
                .withStartupTimeout(Duration.ofSeconds(90));
    }

    @BeforeAll
    static void initializeCheckedReplicaSet() throws Exception {
        RealConnectorGate.require(CONNECTOR);
        var initiated = MONGO.execInContainer("mongosh", "--quiet", "--username", "root", "--password", PASSWORD,
                "--authenticationDatabase", "admin", "--eval", "rs.initiate({_id:'" + REPLICA_SET
                        + "',members:[{_id:0,host:'localhost:27017'}]})");
        assertThat(initiated.getExitCode()).as("the owned authenticated replica set initializes").isZero();
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            Await.until("the authenticated source primary", Duration.ofSeconds(30),
                    () -> Boolean.TRUE.equals(admin.getDatabase("admin").runCommand(new Document("hello", 1))
                            .getBoolean("isWritablePrimary")), () -> "the source has not elected its primary");
        }
    }

    @ParameterizedTest(name = "{0}/MISSING_FIND")
    @EnumSource(AtlasRuntime.Mode.class)
    void aSourceWithoutReadPermissionFailsThroughThePdkAndRecovers(AtlasRuntime.Mode mode) throws Exception {
        String suffix = suffix(mode);
        String metadata = "ts_plan_prm_" + suffix;
        String targetDatabase = "ts_plan_prt_" + suffix;
        String metadataUri = SharedMongo.replicaSetUrl(metadata);
        String targetUri = SharedMongo.replicaSetUrl(targetDatabase);
        String source = "permission_read_source";
        String target = "permission_read_target";
        String pipeline = "permission_read_pipeline";

        try (SourceUser fixture = new SourceUser("ts_plan_prs_" + suffix);
             MongoClient targetClient = MongoClients.create(targetUri);
             AutoCloseable targetCleanup = () -> targetClient.getDatabase(targetDatabase).drop();
             AtlasRuntime runtime = new AtlasRuntime(mode, metadataUri);
             ServerHandle server = runtime.launch(Tiers.IN_PROCESS)) {
            ControlPlane control = runtime.control(server, true);
            runtime.register(control, CONNECTOR);
            control.apply(workspace(source, target, pipeline, fixture.sourceUri(), targetUri, "snapshot_only"));
            control.discoverSchema(source, CONNECTOR, settings(fixture.sourceUri()));
            fixture.permissions(false, true);
            try (MongoClient reader = MongoClients.create(fixture.sourceUri())) {
                assertDenied(() -> reader.getDatabase(fixture.database).getCollection(TABLE).find().first());
            }
            control.lifecycle(pipeline, LifecycleVerb.START);
            assertSafeCaptureFailure(control, pipeline, fixture.sourceUri(),
                    "Missing privileges when read data on mongodb.");
            MongoCollection<Document> targetRows = targetClient.getDatabase(targetDatabase).getCollection(TABLE);
            assertThat(rows(targetRows)).as("a refused source read delivers no rows").isEmpty();

            fixture.permissions(true, true);
            assertThat(control.testConnection(source, CONNECTOR, settings(fixture.sourceUri())).outcome())
                    .as("the same credentials with find restored pass the real connector test").isEqualTo("PASSED");
            control.stop(pipeline, false);
            awaitState(control, pipeline, PipelineState.STOPPED);
            control.lifecycle(pipeline, LifecycleVerb.START);
            awaitRows(targetRows, Map.of("first", 10L, "second", 20L), "the restored source snapshot");
            Await.until("the restored snapshot's explicit read count", TIMEOUT,
                    () -> Long.valueOf(2).equals(control.snapshotRowsRead(pipeline).get(TABLE)),
                    () -> String.valueOf(control.snapshotRowsRead(pipeline)));
            awaitSnapshotProgress(control, pipeline);
            control.stop(pipeline, false);
            awaitState(control, pipeline, PipelineState.STOPPED);
            runtime.assertAuthenticationBoundary();
        }
    }

    @ParameterizedTest(name = "{0}/MISSING_CHANGE_STREAM")
    @EnumSource(AtlasRuntime.Mode.class)
    void aSourceThatCanSnapshotButCannotWatchFailsThroughThePdkAndRecovers(AtlasRuntime.Mode mode)
            throws Exception {
        String suffix = suffix(mode);
        String metadata = "ts_plan_pcm_" + suffix;
        String snapshotTargetDatabase = "ts_plan_pst_" + suffix;
        String cdcTargetDatabase = "ts_plan_pct_" + suffix;
        String metadataUri = SharedMongo.replicaSetUrl(metadata);
        String snapshotTargetUri = SharedMongo.replicaSetUrl(snapshotTargetDatabase);
        String cdcTargetUri = SharedMongo.replicaSetUrl(cdcTargetDatabase);
        String snapshot = "permission_snapshot_pipeline";
        String cdc = "permission_cdc_pipeline";
        String source = "permission_cdc_source";

        try (SourceUser fixture = new SourceUser("ts_plan_pcs_" + suffix);
             MongoClient targetClient = MongoClients.create(snapshotTargetUri);
             AutoCloseable targetCleanup = () -> {
                 targetClient.getDatabase(snapshotTargetDatabase).drop();
                 targetClient.getDatabase(cdcTargetDatabase).drop();
             };
             AtlasRuntime runtime = new AtlasRuntime(mode, metadataUri);
             ServerHandle server = runtime.launch(Tiers.IN_PROCESS)) {
            fixture.permissions(true, false);
            try (MongoClient reader = MongoClients.create(fixture.sourceUri())) {
                MongoCollection<Document> sourceRows = reader.getDatabase(fixture.database).getCollection(TABLE);
                assertThat(rows(sourceRows)).containsExactlyInAnyOrderEntriesOf(Map.of("first", 10L, "second", 20L));
                assertDenied(() -> {
                    try (var cursor = reader.getDatabase(fixture.database).watch().cursor()) {
                        cursor.tryNext();
                    }
                });
            }
            ControlPlane control = runtime.control(server, true);
            runtime.register(control, CONNECTOR);
            control.apply(workspace("permission_snapshot_source", "permission_snapshot_target", snapshot,
                    fixture.sourceUri(), snapshotTargetUri, "snapshot_only"));
            control.discoverSchema("permission_snapshot_source", CONNECTOR, settings(fixture.sourceUri()));
            control.lifecycle(snapshot, LifecycleVerb.START);
            MongoCollection<Document> snapshotRows = targetClient.getDatabase(snapshotTargetDatabase).getCollection(TABLE);
            awaitRows(snapshotRows, Map.of("first", 10L, "second", 20L), "the actual snapshot without changeStream");
            Await.until("the snapshot-only source's explicit read count", TIMEOUT,
                    () -> Long.valueOf(2).equals(control.snapshotRowsRead(snapshot).get(TABLE)),
                    () -> String.valueOf(control.snapshotRowsRead(snapshot)));
            awaitSnapshotProgress(control, snapshot);
            control.stop(snapshot, false);
            awaitState(control, snapshot, PipelineState.STOPPED);

            control.apply(workspace(source, "permission_cdc_target", cdc, fixture.sourceUri(), cdcTargetUri,
                    "snapshot_and_cdc"));
            control.discoverSchema(source, CONNECTOR, settings(fixture.sourceUri()));
            control.lifecycle(cdc, LifecycleVerb.START);
            // The connector's stream reader uses its shared write-privilege exception collector.
            // The successful snapshot and refused watch above identify this as a CDC permission failure.
            assertSafeCaptureFailure(control, cdc, fixture.sourceUri(),
                    "Missing privileges when write data on mongodb.");

            fixture.permissions(true, true);
            control.stop(cdc, false);
            awaitState(control, cdc, PipelineState.STOPPED);
            control.lifecycle(cdc, LifecycleVerb.START);
            MongoCollection<Document> cdcRows = targetClient.getDatabase(cdcTargetDatabase).getCollection(TABLE);
            awaitRows(cdcRows, Map.of("first", 10L, "second", 20L), "the recovered CDC source snapshot");
            String beforeChange = Await.answered("the recovered snapshot ACK", TIMEOUT,
                    () -> control.durablePosition(cdc, TABLE));
            fixture.admin.getDatabase(fixture.database).getCollection(TABLE).updateOne(new Document("_id", "second"),
                    new Document("$set", new Document("value", 21)));
            awaitRows(cdcRows, Map.of("first", 10L, "second", 21L), "a real change after permission recovery");
            Await.answered("a new target ACK for the permitted change stream", TIMEOUT,
                    () -> control.durablePosition(cdc, TABLE).filter(value -> !value.equals(beforeChange)));
            assertThat(rows(snapshotRows)).as("the stopped snapshot-only pipeline cannot tail the update")
                    .containsExactlyInAnyOrderEntriesOf(Map.of("first", 10L, "second", 20L));
            runtime.assertAuthenticationBoundary();
            control.stop(cdc, false);
            awaitState(control, cdc, PipelineState.STOPPED);
        }
    }

    private static void assertSafeCaptureFailure(ControlPlane control, String pipeline, String sourceUri,
            String expectedDetail) {
        awaitState(control, pipeline, PipelineState.FAILED);
        assertThat(control.failureCode(pipeline)).contains(ConnectorError.CAPTURE_FAILED.code());
        String body = control.sourceRequest("GET", "/api/pipelines/" + pipeline + "/status", null, null, 200).body();
        assertThat(body).doesNotContain(PASSWORD, sourceUri);
        Object parsed = JsonReader.parse(body);
        assertThat(parsed).isInstanceOf(Map.class);
        Map<?, ?> failure = (Map<?, ?>) ((Map<?, ?>) parsed).get("failure");
        assertThat(failure).isNotNull();
        assertThat(failure.get("params")).isInstanceOf(Map.class);
        Map<?, ?> params = (Map<?, ?>) failure.get("params");
        assertThat(params.get("connector")).isEqualTo(CONNECTOR);
        assertThat(params.get("detail")).isInstanceOf(String.class);
        assertThat((String) params.get("detail")).startsWith(expectedDetail);
        assertThat(control.logs(pipeline)).doesNotContain(PASSWORD, sourceUri);
        System.out.println("PERMISSION_DIAGNOSTIC " + JsonWriter.write(Map.of(
                "pipeline", pipeline, "code", failure.get("code"), "detail", params.get("detail"))));
    }

    private static void awaitState(ControlPlane control, String pipeline, PipelineState state) {
        Await.until(pipeline + " to reach " + state, TIMEOUT,
                () -> control.state(pipeline).filter(state::equals).isPresent(),
                () -> "state=" + control.state(pipeline) + ", logs=" + control.logs(pipeline));
    }

    private static void awaitSnapshotProgress(ControlPlane control, String pipeline) {
        Await.until("the explicit confirmed two-row snapshot progress", TIMEOUT,
                () -> control.snapshotTable(pipeline, TABLE)
                        .filter(progress -> progress.rowsDone() == 2 && Integer.valueOf(100).equals(progress.donePct()))
                        .isPresent(),
                () -> "snapshot=" + control.snapshotTable(pipeline, TABLE));
        assertThat(control.state(pipeline)).as("the successful snapshot has a healthy current observation")
                .hasValueSatisfying(state -> assertThat(state).isNotEqualTo(PipelineState.FAILED));
    }

    private static void awaitRows(MongoCollection<Document> collection, Map<String, Long> expected, String phase) {
        Await.until(phase, TIMEOUT, () -> rows(collection).equals(expected), () -> "target=" + rows(collection));
    }

    private static Map<String, Long> rows(MongoCollection<Document> collection) {
        Map<String, Long> result = new TreeMap<>();
        for (Document row : collection.find()) result.put(row.getString("_id"), ((Number) row.get("value")).longValue());
        return result;
    }

    private static String suffix(AtlasRuntime.Mode mode) {
        return mode.name().toLowerCase(java.util.Locale.ROOT) + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static Map<String, Object> settings(String uri) {
        return Map.of("isUri", true, "uri", uri);
    }

    private static Map<String, String> workspace(String source, String target, String pipeline, String sourceUri,
            String targetUri, String readMode) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(source + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb-atlas
                config: %s
                mode: %s
                tables: [ probe ]
                """.formatted(source, JsonWriter.write(settings(sourceUri)),
                "snapshot_only".equals(readMode) ? "snapshot" : "cdc"));
        resources.put(target + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb-atlas
                config: %s
                """.formatted(target, JsonWriter.write(settings(targetUri))));
        resources.put(pipeline + ".tap.yml", Workspaces.pipelineYaml(pipeline, source, target, TABLE)
                .replace("read_mode: snapshot_and_cdc", "read_mode: " + readMode));
        return resources;
    }

    private static void assertDenied(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(MongoCommandException.class,
                failure -> assertThat(failure.getErrorCode()).isEqualTo(13));
    }

    private static String uri(String user, String database) {
        return "mongodb://" + user + ":" + PASSWORD + "@" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017)
                + "/" + database + "?replicaSet=" + REPLICA_SET + "&directConnection=true&authSource=admin";
    }

    /** A role per case prevents one test's permission update from changing another reader. */
    private static final class SourceUser implements AutoCloseable {
        private final MongoClient admin = MongoClients.create(uri("root", "admin"));
        private final String database;
        private final String user;
        private boolean roleCreated;
        private boolean userCreated;

        SourceUser(String database) {
            this.database = database;
            user = database + "_reader";
            try {
                admin.getDatabase(database).getCollection(TABLE).insertMany(List.of(
                        new Document("_id", "first").append("value", 10),
                        new Document("_id", "second").append("value", 20)));
                permissions(true, true);
                admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD)
                        .append("roles", List.of(new Document("role", "source_reader").append("db", database))));
                userCreated = true;
            } catch (RuntimeException | Error failure) {
                try { close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }

        String sourceUri() {
            return uri(user, database);
        }

        void permissions(boolean find, boolean changeStream) {
            List<String> actions = new ArrayList<>(List.of("listCollections", "listIndexes", "collStats",
                    "dbStats", "killCursors"));
            if (find) actions.add("find");
            if (changeStream) actions.add("changeStream");
            admin.getDatabase(database).runCommand(new Document(roleCreated ? "updateRole" : "createRole", "source_reader")
                    .append("roles", List.of()).append("privileges", List.of(new Document("resource",
                            new Document("db", database).append("collection", "")).append("actions", actions))));
            roleCreated = true;
        }

        @Override
        public void close() {
            RuntimeException failed = null;
            try {
                if (userCreated) admin.getDatabase("admin").runCommand(new Document("dropUser", user));
            } catch (RuntimeException failure) {
                failed = failure;
            }
            try {
                if (roleCreated) admin.getDatabase(database).runCommand(new Document("dropRole", "source_reader"));
            } catch (RuntimeException failure) {
                if (failed == null) failed = failure;
                else failed.addSuppressed(failure);
            }
            try {
                admin.getDatabase(database).drop();
            } catch (RuntimeException failure) {
                if (failed == null) failed = failure;
                else failed.addSuppressed(failure);
            } finally {
                admin.close();
            }
            if (failed != null) throw failed;
        }
    }
}
