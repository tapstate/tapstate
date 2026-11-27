package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** A committed target move with a lost acknowledgement must stop before replay can touch a reused key. */
class DefaultKeyMoveRecoveryIT {
    private static final String PIPELINE = "default_key_move_recovery";
    private static final String TABLE = "MOVE_RECOVERY";
    private static final Duration BOUND = Duration.ofMinutes(2);
    private static final String HOLD = "waitAfterCommandFinishesExecution";

    @Test
    void aCrashedMoveCannotReplayOverAnExternalReplacementAtItsOldKey() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        String suffix = UUID.randomUUID().toString().replace("-", "");
        var mysql = SharedMySql.settings("move_recovery_" + suffix);
        try (var connection = SharedMySql.connect(mysql); Statement sql = connection.createStatement();
                var target = NetworkedMongo.startWithTestCommands();
                var storage = PreparedNativeStore.create();
                var targetClient = MongoClients.create(target.uriForThisHost("move_recovery_" + suffix));
                var configClient = MongoClients.create(storage.configurationUri());
                var cacheClient = MongoClients.create(storage.cacheUri())) {
            sql.execute("CREATE TABLE MOVE_RECOVERY (ID INT PRIMARY KEY, VAL INT)");
            sql.executeUpdate("INSERT INTO MOVE_RECOVERY VALUES (1, 20)");
            String database = "move_recovery_" + suffix;
            String application = "move-recovery-" + suffix;
            String targetUri = target.uriForThisHost(database) + "&appName=" + application;
            var targetDatabase = targetClient.getDatabase(database);
            var rows = targetDatabase.getCollection(TABLE);
            var admin = targetClient.getDatabase("admin");
            var windows = configClient.getDatabase(storage.configurationDatabase())
                    .getCollection("pipeline_key_move_windows");
            String originalBoot;
            Document pending;
            var first = (RealProcessServer) Tiers.REAL_PROCESS.launch(storage);
            try {
                var control = new ControlPlane(first.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(workspace(mysql, targetUri));
                control.discoverSchema("recovery-mysql", "mysql", mysql);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the initial row and its load acknowledgement", BOUND,
                        () -> rows.countDocuments(new Document("ID", 1).append("VAL", 20)) == 1
                                && !control.snapshotTables(PIPELINE).isEmpty()
                                && control.snapshotTables(PIPELINE).values().stream().allMatch(table -> table.landed()),
                        () -> control.pipelineStatus(PIPELINE).toString());

                targetDatabase.runCommand(new Document("profile", 2));
                sql.executeUpdate("UPDATE MOVE_RECOVERY SET VAL=21 WHERE ID=1");
                Await.until("the CDC canary to finish its real target command", BOUND,
                        () -> rows.countDocuments(new Document("ID", 1).append("VAL", 21)) == 1
                                && targetWrites(targetDatabase, database, application) > 0,
                        () -> control.pipelineStatus(PIPELINE).toString());
                var hold = admin.runCommand(new Document("configureFailPoint", HOLD).append("mode", "alwaysOn")
                        .append("data", new Document("ns", database + "." + TABLE)
                                .append("commands", List.of("update"))));
                long entered = hold.get("count", Number.class).longValue();
                try {
                    assertThat(sql.executeUpdate("UPDATE MOVE_RECOVERY SET ID=2 WHERE ID=1")).isEqualTo(1);
                    admin.runCommand(new Document("waitForFailPoint", HOLD)
                            .append("timesEntered", entered + 1).append("maxTimeMS", BOUND.toMillis()));
                    Await.until("the move to be committed while its connector command remains held", BOUND,
                            () -> rows.countDocuments(new Document("ID", 2).append("VAL", 21)) == 1
                                    && rows.countDocuments(new Document("ID", 1)) == 0,
                            () -> rows.find().into(new ArrayList<>()).toString());
                    pending = windows.find(new Document("_id", PIPELINE)).first();
                    assertThat(pending).as("the original CONFIG admission must remain unconfirmed").isNotNull();
                    originalBoot = originalBoot(storage, cacheClient.getDatabase(storage.cacheDatabase()));
                    first.kill();
                } finally {
                    admin.runCommand(new Document("configureFailPoint", HOLD).append("mode", "off"));
                }
            } finally {
                first.close();
            }
            assertThat(windows.find(new Document("_id", PIPELINE)).first()).isEqualTo(pending);
            awaitOriginalBootExpired(storage, cacheClient.getDatabase(storage.cacheDatabase()), originalBoot);

            // A separate legitimate writer reuses A while Tapstate is down. Its source row agrees with it.
            assertThat(sql.executeUpdate("INSERT INTO MOVE_RECOVERY VALUES (1, 99)")).isEqualTo(1);
            rows.insertOne(new Document("ID", 1).append("VAL", 99));
            List<Document> preserved = rows.find().sort(new Document("ID", 1)).into(new ArrayList<>());
            assertThat(preserved).hasSize(2);
            long writesBeforeRestart = targetWrites(targetDatabase, database, application);
            try (var restarted = Tiers.REAL_PROCESS.launch(storage)) {
                var control = new ControlPlane(restarted.baseUrl());
                storage.loginAndAwaitBusiness(control);
                Await.until("the replacement execution to refuse the ambiguous move before writing", BOUND,
                        () -> control.failure(PIPELINE).filter(failure -> failure.code().equals("TS0480043")).isPresent(),
                        () -> control.pipelineStatus(PIPELINE).toString());
                assertThat(targetWrites(targetDatabase, database, application)).isEqualTo(writesBeforeRestart);
                assertThat(rows.find().sort(new Document("ID", 1)).into(new ArrayList<>())).isEqualTo(preserved);
                assertThat(windows.find(new Document("_id", PIPELINE)).first()).isEqualTo(pending);
            }
        }
    }

    private static long targetWrites(MongoDatabase database, String name, String application) {
        var writes = new ArrayList<Document>();
        writes.add(new Document("ns", name + "." + TABLE)
                .append("op", new Document("$in", List.of("insert", "update", "remove"))));
        for (String command : List.of("insert", "update", "delete", "findAndModify", "drop", "create", "createIndexes", "collMod")) {
            writes.add(new Document("command." + command, TABLE));
        }
        return database.getCollection("system.profile").countDocuments(new Document("appName", application)
                .append("$or", writes));
    }

    private static String originalBoot(PreparedNativeStore storage, MongoDatabase database) {
        var sessions = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)
                .find(new Document("_id.clusterId", storage.clusterId())
                        .append("_id.resourceType", WorkloadClaimType.NODE_SESSION.name()))
                .into(new ArrayList<>());
        assertThat(sessions).hasSize(1);
        return sessions.getFirst().getString("ownerBootId");
    }

    private static void awaitOriginalBootExpired(PreparedNativeStore storage, MongoDatabase database, String boot) {
        var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        Await.until("the crashed process's claims to expire", BOUND,
                () -> claims.aggregate(List.of(
                        new Document("$match", new Document("_id.clusterId", storage.clusterId()).append("ownerBootId", boot)),
                        new Document("$project", new Document("leased", new Document("$gt", List.of("$leaseUntil", "$$NOW"))))))
                        .into(new ArrayList<>()).stream().noneMatch(claim -> Boolean.TRUE.equals(claim.getBoolean("leased"))),
                () -> "original boot=" + boot);
    }

    private static Map<String, String> workspace(Map<String, Object> mysql, String target) {
        return Map.of("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: recovery-mysql
                connector: mysql
                config: %s
                mode: cdc
                tables: [MOVE_RECOVERY]
                """.formatted(JsonWriter.write(mysql)), "target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: recovery-mongo
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target), "pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: default_key_move_recovery
                source:
                  - { id: recovery-mysql, srs: false }
                serve:
                  from: MOVE_RECOVERY
                  sync:
                    - { source: recovery-mongo, write_mode: upsert }
                """);
    }
}
