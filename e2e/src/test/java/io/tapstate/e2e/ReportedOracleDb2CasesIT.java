package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import io.tapstate.adapters.mongostore.SystemCollections;
import com.mongodb.client.MongoClients;

import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real Oracle/Db2 report regressions across the HTTP control plane and a MongoDB target.
 * Fixtures are supplied as private JSON files; no credentials or database addresses live in this tree.
 * A named but invalid fixture fails rather than becoming a skipped witness.
 */
@org.junit.jupiter.api.extension.ExtendWith(ReportedOracleDb2CasesIT.Progress.class)
class ReportedOracleDb2CasesIT {
    static final class Progress implements org.junit.jupiter.api.extension.TestWatcher {
        private void record(org.junit.jupiter.api.extension.ExtensionContext context, String outcome) {
            try {
                Files.writeString(Path.of(System.getProperty("tapstate.e2e.report.progress", "target/report-case-progress.jsonl")),
                        JsonWriter.write(Map.of("case", context.getRequiredTestMethod().getName(), "invocation", context.getDisplayName(), "outcome", outcome)) + "\n",
                        java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
            } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
        }
        @Override public void testSuccessful(org.junit.jupiter.api.extension.ExtensionContext context) { record(context, "PASS"); }
        @Override public void testFailed(org.junit.jupiter.api.extension.ExtensionContext context, Throwable cause) { record(context, "FAIL"); }
        @Override public void testAborted(org.junit.jupiter.api.extension.ExtensionContext context, Throwable cause) { record(context, "SKIPPED"); }
    }


    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void historicalCalendarDatesSurviveSnapshotAndCdcAtTheMongoTarget(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source)) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V DATE)");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,DATE '1582-10-04')");
            fixture.enableCdc();
            try (ServerHandle server = fixture.startServer();
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = fixture.configure(server);
                EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                Await.until("the historical snapshot date to reach MongoDB", () -> !row(mongo, target, fixture.table, 1).isEmpty(),
                        () -> control.state(fixture.pipeline).toString());
                assertThat(row(mongo, target, fixture.table, 1).get("V")).isInstanceOfSatisfying(java.util.Date.class,
                        date -> assertThat(date.toInstant()).isEqualTo(java.time.Instant.parse("1582-10-04T00:00:00Z")));
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2,DATE '0001-01-01')");
                Await.until("the historical CDC date to reach MongoDB", () -> !row(mongo, target, fixture.table, 2).isEmpty(),
                        () -> control.state(fixture.pipeline).toString());
                assertThat(row(mongo, target, fixture.table, 2).get("V")).isInstanceOfSatisfying(java.util.Date.class,
                        date -> assertThat(date.toInstant()).isEqualTo(java.time.Instant.parse("0001-01-01T00:00:00Z")));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot", "cdc"})
    void thirtyEightDigitOracleNumbersReachMongoWithoutRounding(String phase) throws Exception {
        String expected = "12345678901234567890123456789012345678";
        try (Fixture fixture = Fixture.open("oracle")) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID NUMBER(19) NOT NULL PRIMARY KEY, V NUMBER(38,0))");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1," + (phase.equals("snapshot") ? expected : "1") + ")");
            fixture.enableCdc();
            try (ServerHandle server = fixture.startServer();
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = fixture.configure(server);
                EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                Await.until("the numeric snapshot to reach MongoDB", () -> !row(mongo, target, fixture.table, 1).isEmpty(),
                        () -> control.state(fixture.pipeline).toString());
                int id = 1;
                if (phase.equals("cdc")) {
                    id = 2;
                    fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2," + expected + ")");
                    Await.until("the 38-digit CDC row to reach MongoDB", () -> {
                        assertThat(control.state(fixture.pipeline)).isNotEqualTo(java.util.Optional.of(PipelineState.FAILED));
                        return !row(mongo, target, fixture.table, 2).isEmpty();
                    }, () -> control.state(fixture.pipeline).toString());
                }
                Object actual = row(mongo, target, fixture.table, id).get("V");
                assertThat(actual).isNotNull();
                assertThat(new java.math.BigDecimal(actual.toString())).as("all 38 source digits in " + phase)
                        .isEqualByComparingTo(new java.math.BigDecimal(expected));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void aStoppedPipelineResumesRealChangesWithoutRecreatingItsSource(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source)) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'snapshot')");
            fixture.enableCdc();
            try (ServerHandle server = fixture.startServer();
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = fixture.configure(server);
                EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                awaitValue(mongo, target, fixture.table, 1, "snapshot");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2,'before-stop')");
                awaitValue(mongo, target, fixture.table, 2, "before-stop");
                control.stop(fixture.pipeline, true);
                Await.until("the real source pipeline to stop", () -> control.state(fixture.pipeline)
                        .filter(PipelineState.STOPPED::equals).isPresent(), () -> control.state(fixture.pipeline).toString());
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (3,'while-stopped')");
                control.startAnswering(fixture.pipeline, "target-not-empty", "keep");
                awaitValue(mongo, target, fixture.table, 3, "while-stopped");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (4,'after-restart')");
                awaitValue(mongo, target, fixture.table, 4, "after-restart");
                assertThat(control.state(fixture.pipeline)).contains(PipelineState.RUNNING);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"oracle,apply", "oracle,ignore", "oracle,default", "oracle,ignore-model",
            "db2,apply", "db2,ignore", "db2,default", "db2,ignore-model"})
    void addingAColumnKeepsExistingAndNewValuesCorrectAtTheTarget(String source, String strategy) throws Exception {
        try (Fixture fixture = Fixture.open(source)) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'snapshot')");
            fixture.enableCdc();
            try (ServerHandle server = fixture.startServer();
                    MongoEndpoints mongo = new MongoEndpoints()) {
                fixture.ignoreModelEvolution = strategy.equals("ignore-model");
                ControlPlane control = fixture.configure(server, "snapshot_and_cdc", null,
                        strategy.equals("default") ? null : strategy.equals("ignore-model") ? "ignore" : strategy);
                EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                awaitValue(mongo, target, fixture.table, 1, "snapshot");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2,'before-ddl')");
                awaitValue(mongo, target, fixture.table, 2, "before-ddl");
                fixture.execute("ALTER TABLE " + fixture.qualified + " ADD C VARCHAR(64)");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (3,'after-ddl','new-column')");
                if (strategy.equals("default")) {
                    var failure = Await.answered("the default fail DDL policy's detailed diagnostic", () ->
                            control.failure(fixture.pipeline).filter(value ->
                                    !value.code().equals("monitor.failure-diagnostic-unavailable")));
                    assertThat(failure.code()).isEqualTo("connector.write-failed");
                    assertThat(row(mongo, target, fixture.table, 1).get("V")).isEqualTo("snapshot");
                    return;
                }
                awaitValue(mongo, target, fixture.table, 3, "after-ddl");
                Await.until("the new source column to reach MongoDB", () -> "new-column".equals(
                        row(mongo, target, fixture.table, 3).get("C")),
                        () -> String.valueOf(row(mongo, target, fixture.table, 3)));
                assertThat(row(mongo, target, fixture.table, 1).get("V")).isEqualTo("snapshot");
            }
        }
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> targetColumnDdlCases() {
        return java.util.stream.Stream.of("oracle", "db2").flatMap(source ->
                java.util.stream.Stream.of("rename", "drop", "add-default", "add-null").flatMap(change ->
                        java.util.stream.Stream.of("apply", "ignore").map(policy ->
                                org.junit.jupiter.params.provider.Arguments.of(source, change, policy))));
    }

    @ParameterizedTest(name = "target DDL {0} {1} {2}")
    @org.junit.jupiter.params.provider.MethodSource("targetColumnDdlCases")
    void applyingColumnDdlChangesExistingDocumentsAndIgnoringItPreservesThem(
            String source, String change, String policy) throws Exception {
        try (Fixture fixture = Fixture.open(source); MongoEndpoints mongo = new MongoEndpoints()) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'before')"); fixture.enableCdc();
            EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
            try (ServerHandle server = fixture.startServer()) {
                ControlPlane control = fixture.configure(server, "snapshot_and_cdc", null, policy);
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                awaitValue(mongo, target, fixture.table, 1, "before");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (0,'warm')");
                awaitValue(mongo, target, fixture.table, 0, "warm");
                Object originalId = row(mongo, target, fixture.table, 1).get("_id");
                // A source resnapshot would restore V='before'. Applying target DDL must instead
                // preserve the existing target value and unrelated target-only fields.
                try (var witness = MongoClients.create(fixture.targetUri())) {
                    var collection = witness.getDatabase(new com.mongodb.ConnectionString(fixture.targetUri()).getDatabase())
                            .getCollection(fixture.table);
                    assertThat(collection.updateOne(new Document("ID", 1), new Document("$set",
                            new Document("V", "target-only").append("TARGET_ONLY", "preserved"))).getMatchedCount()).isEqualTo(1);
                }
                String alter = switch (change) {
                    case "rename" -> "RENAME COLUMN V TO W";
                    case "drop" -> "DROP COLUMN V";
                    case "add-default" -> (source.equals("oracle") ? "ADD C NUMBER(10)" : "ADD COLUMN C INT") + " DEFAULT 99";
                    case "add-null" -> source.equals("oracle") ? "ADD C NUMBER(10)" : "ADD COLUMN C INT";
                    default -> throw new IllegalArgumentException(change);
                };
                fixture.execute("ALTER TABLE " + fixture.qualified + " " + alter);
                if (change.equals("drop") && source.equals("db2"))
                    fixture.execute("CALL SYSPROC.ADMIN_CMD('REORG TABLE " + fixture.qualified.replace("'", "''") + "')");
                if (change.equals("rename")) fixture.execute("INSERT INTO " + fixture.qualified + " (ID,W) VALUES (2,'after')");
                else if (change.equals("drop")) fixture.execute("INSERT INTO " + fixture.qualified + " (ID) VALUES (2)");
                else if (change.equals("add-null")) fixture.execute("INSERT INTO " + fixture.qualified + " (ID,V,C) VALUES (2,'after',7)");
                else fixture.execute("INSERT INTO " + fixture.qualified + " (ID,V) VALUES (2,'after')");
                Await.until("the ordered post-DDL row to arrive", () -> {
                    var failure = control.failure(fixture.pipeline);
                    if (failure.filter(value -> value.code().equals("monitor.failure-diagnostic-unavailable")).isPresent()) return false;
                    assertThat(failure).isEmpty();
                    return !row(mongo, target, fixture.table, 2).isEmpty();
                }, () -> "state=" + control.pipelineStatus(fixture.pipeline));
                Map<String,Object> existing = row(mongo, target, fixture.table, 1);
                assertThat(existing.get("_id")).isEqualTo(originalId);
                assertThat(existing.get("TARGET_ONLY")).isEqualTo("preserved");
                if (policy.equals("ignore")) {
                    assertThat(existing.get("V")).isEqualTo("target-only");
                    assertThat(existing).doesNotContainKeys("W", "C");
                } else if (change.equals("rename")) {
                    assertThat(existing.get("W")).isEqualTo("target-only");
                    assertThat(existing).doesNotContainKey("V");
                    assertThat(row(mongo, target, fixture.table, 0).get("W")).isEqualTo("warm");
                } else if (change.equals("drop")) {
                    assertThat(existing).doesNotContainKey("V");
                } else {
                    assertThat(existing.get("V")).isEqualTo("target-only");
                    assertThat(existing).containsKey("C");
                    if (change.equals("add-default")) {
                        assertThat(new java.math.BigDecimal(existing.get("C").toString()).intValueExact()).isEqualTo(99);
                        assertThat(existing.get("C").getClass()).as("backfilled and CDC values must use the same target representation")
                                .isEqualTo(row(mongo, target, fixture.table, 2).get("C").getClass());
                    }
                    else assertThat(existing.get("C")).isNull();
                }
                if (change.equals("rename")) assertThat(row(mongo, target, fixture.table, 2).get("W")).isEqualTo("after");
                if (change.equals("drop")) assertThat(row(mongo, target, fixture.table, 2)).doesNotContainKey("V");
                assertThat(control.failure(fixture.pipeline)).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void aQuietColumnDdlResumesAfterStoppingWithoutClearingStateAndRestartingTheEngine(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source); MongoEndpoints mongo = new MongoEndpoints()) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'snapshot')");
            fixture.enableCdc();
            EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
            try (ServerHandle first = fixture.startServer()) {
                ControlPlane control = fixture.configure(first);
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                awaitValue(mongo, target, fixture.table, 1, "snapshot");
                fixture.execute("ALTER TABLE " + fixture.qualified + " ADD C VARCHAR(64)");
                try (var client = MongoClients.create(fixture.storage.cacheUri())) {
                    var cache = client.getDatabase(fixture.storage.cacheDatabase());
                    Await.until("a quiet column DDL to publish its durable successor and checkpoint", () -> {
                        assertThat(control.failure(fixture.pipeline)).isEmpty();
                        boolean model = SystemCollections.SRS_MODELS.on(cache).countDocuments(
                                new Document("_id.table", fixture.table).append("_id.modelVersion", 1L)) == 1;
                        boolean checkpoint = SystemCollections.SRS_CHAINS.on(cache).countDocuments(
                                new Document("checkpoint.tables", new Document("$elemMatch",
                                        new Document("table", fixture.table).append("sequence", new Document("$gte", 0L))))) == 1;
                        return model && checkpoint;
                    }, () -> "state=" + control.state(fixture.pipeline) + ", failure=" + control.failure(fixture.pipeline));
                }
                try (var witness = MongoClients.create(fixture.storage.cacheUri())) {
                    var models = SystemCollections.SRS_MODELS.on(witness.getDatabase(fixture.storage.cacheDatabase()));
                    Path path = Path.of("target", "report-quiet-models-" + source + ".json");
                    Files.writeString(path, models.find(new Document("_id.table", fixture.table)).into(new java.util.ArrayList<>()).toString());
                }
                control.stop(fixture.pipeline, false);
                Await.until("the quiet DDL pipeline to stop with retained state", () -> control.state(fixture.pipeline)
                        .filter(PipelineState.STOPPED::equals).isPresent(), () -> control.state(fixture.pipeline).toString());
            }
            // A source resume must not re-run the original snapshot. A deliberate
            // target-side deletion makes an unintended fresh full read observable.
            mongo.delete(target, fixture.table, Map.of("ID", 1));
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2,'offline','new-column')");
            try (ServerHandle second = fixture.startServer()) {
                ControlPlane control = new ControlPlane(second.baseUrl());
                fixture.storage.loginAndAwaitBusiness(control);
                control.startAnswering(fixture.pipeline, "target-not-empty", "keep");
                Await.until("the offline row after a quiet DDL restart", () -> {
                    assertThat(control.failure(fixture.pipeline)).isEmpty();
                    return "offline".equals(row(mongo, target, fixture.table, 2).get("V"));
                }, () -> "state=" + control.state(fixture.pipeline) + ", failure=" + control.failure(fixture.pipeline)
                        + ", row=" + row(mongo, target, fixture.table, 2));
                assertThat(row(mongo, target, fixture.table, 2).get("C")).isEqualTo("new-column");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (3,'live','still-correct')");
                awaitValue(mongo, target, fixture.table, 3, "live");
                assertThat(row(mongo, target, fixture.table, 3).get("C")).isEqualTo("still-correct");
                assertThat(row(mongo, target, fixture.table, 1)).as("retained-state resume must not repeat the snapshot").isEmpty();
                assertThat(control.failure(fixture.pipeline)).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void nonDefaultReadWriteAndDdlOptionsAndFalseRemainVisibleThroughThePublicApi(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source)) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
            fixture.enableCdc();
            if (source.equals("db2")) {
                fixture.settings.put("autoLri", false);
                fixture.settings.put("sshPort", "22");
                fixture.settings.put("db2Profile", "/home/db2inst1/sqllib/db2profile");
                fixture.settings.put("archived", false);
            }
            try (ServerHandle server = fixture.startServer()) {
                ControlPlane control = fixture.configure(server, "cdc_only", "append", "apply");
                if (source.equals("db2")) assertThat(((Map<?, ?>) control.source("src_" + fixture.suffix).get("config")).get("autoLri")).isEqualTo(false);
                Map<String, Object> pipeline = control.pipeline(fixture.pipeline);
                assertThat(((Map<?, ?>) pipeline.get("settings")).get("readMode")).isEqualTo("CDC_ONLY");
                Map<?, ?> serve = (Map<?, ?>) pipeline.get("serve");
                Map<?, ?> sync = (Map<?, ?>) ((List<?>) serve.get("sync")).getFirst();
                assertThat(sync.get("writeMode")).isEqualTo("APPEND");
                assertThat(sync.get("ddl")).isEqualTo("APPLY");
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"oracle,true", "oracle,false", "db2,true", "db2,false"})
    void sourceCaptureKeysDefineTheFaultBoundaryAndTheSamePipelineCanRecover(String source, boolean shared) throws Exception {
        try (Fixture good = Fixture.open(source); Fixture bad = Fixture.alongside(good); MongoEndpoints mongo = new MongoEndpoints()) {
            for (Fixture fixture : List.of(good, bad)) {
                fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                        + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'snapshot')");
                fixture.enableCdc();
                fixture.sharingKey = shared ? good.pipeline : fixture.pipeline;
            }
            try (ServerHandle server = good.startServer()) {
                ControlPlane control = good.configure(server);
                bad.configure(server);
                control.lifecycle(good.pipeline, LifecycleVerb.START);
                awaitValue(mongo, EndpointAddress.uri(good.targetUri()), good.table, 1, "snapshot");
                control.lifecycle(bad.pipeline, LifecycleVerb.START);
                awaitValue(mongo, EndpointAddress.uri(bad.targetUri()), bad.table, 1, "snapshot");
                var goodCaptures = control.captureOwnersOf(good.pipeline).keySet();
                var badCaptures = control.captureOwnersOf(bad.pipeline).keySet();
                assertThat(goodCaptures).hasSize(1); assertThat(badCaptures).hasSize(1);
                if (shared) assertThat(goodCaptures).isEqualTo(badCaptures);
                else assertThat(goodCaptures).doesNotContainAnyElementsOf(badCaptures);
                // Table destruction is an unsupported table lifecycle change. It must
                // produce a visible failure rather than silently skipping future data.
                bad.execute("DROP TABLE " + bad.qualified + (source.equals("oracle") ? " PURGE" : ""));
                Await.until("the destructive source DDL failure with its public diagnostic", () -> {
                    Map<String,Object> status = control.pipelineStatus(bad.pipeline);
                    Object observed = status.containsKey("observedState") ? status.get("observedState") : status.get("state");
                    if (!"FAILED".equals(observed)) return false;
                    assertThat(status.get("failure")).as("FAILED must carry its diagnostic in the same public status read").isInstanceOf(Map.class);
                    assertThat(((Map<?,?>) status.get("failure")).get("code")).isInstanceOfSatisfying(String.class, code -> assertThat(code).isNotBlank());
                    return true;
                }, () -> "failure=" + control.failure(bad.pipeline));
                Await.until("the failed run's coded diagnostic", () -> control.failure(bad.pipeline).isPresent(),
                        () -> control.pipelineStatus(bad.pipeline).toString());
                assertThat(control.startExpectingRefusal(bad.pipeline).code()).isEqualTo("lifecycle.illegal-transition");
                assertThat(control.stopWithoutStateChoice(bad.pipeline, false).code()).isEqualTo("lifecycle.purge-state-not-stated");
                assertThat(control.stopWithoutStateChoice(bad.pipeline, true).code()).isEqualTo("lifecycle.purge-state-not-stated");
                if (shared) {
                    Await.until("shared consumers to expose their physical capture's failure", () -> control.state(good.pipeline)
                            .filter(PipelineState.FAILED::equals).isPresent(), () -> "failure=" + control.failure(good.pipeline));
                } else {
                    good.execute("INSERT INTO " + good.qualified + " VALUES (2,'isolated')");
                    awaitValue(mongo, EndpointAddress.uri(good.targetUri()), good.table, 2, "isolated");
                    assertThat(control.failure(good.pipeline)).isEmpty();
                    control.stop(bad.pipeline, true);
                    // No global stop/restart or new Source/Pipeline identity is needed.
                    bad.execute("CREATE TABLE " + bad.qualified + " (ID " + bad.integerType()
                            + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
                    bad.execute("INSERT INTO " + bad.qualified + " VALUES (3,'recovered')");
                    bad.enableCdc();
                    control.discoverSchema("src_" + bad.suffix, source, bad.settings);
                    Await.until("the failed pipeline's stop to settle", () -> control.state(bad.pipeline)
                            .filter(PipelineState.STOPPED::equals).isPresent(), () -> control.state(bad.pipeline).toString());
                    control.startAnswering(bad.pipeline, "target-not-empty", "keep");
                    awaitValue(mongo, EndpointAddress.uri(bad.targetUri()), bad.table, 3, "recovered");
                    good.execute("INSERT INTO " + good.qualified + " VALUES (4,'still-running')");
                    awaitValue(mongo, EndpointAddress.uri(good.targetUri()), good.table, 4, "still-running");
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void timestampPrecisionAtMongoMatchesItsDeclaredStorageTypeInBothReadPhases(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source); MongoEndpoints mongo = new MongoEndpoints()) {
            String extra = source.equals("db2") ? ", EXACT_TS TIMESTAMP(12)" : "";
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V TIMESTAMP(9)" + extra + ")");
            String ordinary = "TIMESTAMP '2026-11-23 12:34:56.123456789'";
            String values = ordinary + (source.equals("db2") ? ",TIMESTAMP '2026-11-23 12:34:56.123456789012'" : "");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1," + values + ")");
            fixture.enableCdc();
            try (ServerHandle server = fixture.startServer()) {
                ControlPlane control = fixture.configure(server);
                EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                Await.until("the timestamp snapshot", () -> !row(mongo, target, fixture.table, 1).isEmpty(),
                        () -> control.failure(fixture.pipeline).toString());
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2," + values + ")");
                Await.until("the timestamp CDC row", () -> !row(mongo, target, fixture.table, 2).isEmpty(),
                        () -> control.failure(fixture.pipeline).toString());
                for (int id : List.of(1, 2)) {
                    assertThat(row(mongo, target, fixture.table, id).get("V")).isInstanceOfSatisfying(java.util.Date.class,
                            date -> assertThat(date.toInstant()).isEqualTo(java.time.Instant.parse("2026-11-23T12:34:56.123Z")));
                    if (source.equals("db2")) assertThat(row(mongo, target, fixture.table, id).get("EXACT_TS").toString())
                            .isEqualTo("2026-11-23 12:34:56.123456789012");
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void aRealSourceWriteBurstWithoutCustomAlertRulesDoesNotOverflowAlertEvaluation(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source); MongoEndpoints mongo = new MongoEndpoints()) {
            fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                    + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
            fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'snapshot')");
            fixture.enableCdc();
            try (ServerHandle server = fixture.startServer()) {
                ControlPlane control = fixture.configure(server);
                EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                awaitValue(mongo, target, fixture.table, 1, "snapshot");
                Await.until("the built-in alert collector to become healthy", () ->
                        Boolean.FALSE.equals(control.alert("health", Map.of()).get("degraded")),
                        () -> control.alert("health", Map.of()).toString());
                long rejectedBefore = ((Number) control.alert("health", Map.of()).get("rejected")).longValue();
                try (var insert = fixture.connection.prepareStatement("INSERT INTO " + fixture.qualified + " VALUES (?,?)")) {
                    for (int id = 2; id <= 2001; id++) {
                        insert.setInt(1, id); insert.setString(2, "burst-" + id); insert.addBatch();
                    }
                    insert.executeBatch();
                }
                Await.until("every committed burst row to reach the actual target", java.time.Duration.ofSeconds(120), () -> {
                    assertThat(control.failure(fixture.pipeline)).isEmpty();
                    return mongo.documents(target, fixture.table).size() == 2001;
                }, () -> "targetCount=" + mongo.documents(target, fixture.table).size() + ", alerts=" + control.alert("health", Map.of()));
                Await.until("the alert evaluation queue to drain", () ->
                        ((Number) control.alert("health", Map.of()).get("queuedInputs")).intValue() == 0,
                        () -> control.alert("health", Map.of()).toString());
                Map<String,Object> health = control.alert("health", Map.of());
                assertThat(((Number) health.get("rejected")).longValue()).isEqualTo(rejectedBefore);
                assertThat(health.get("degraded")).isEqualTo(false);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"oracle,apply", "oracle,ignore", "db2,apply", "db2,ignore"})
    void aPhysicalRenameKeepsTheActiveSelectedStreamAndLaterColumnChangesCorrect(String source, String policy) throws Exception {
        try (Fixture fixture = Fixture.open(source); MongoEndpoints mongo = new MongoEndpoints()) {
            String renamed = fixture.table + "_R";
            String live = fixture.qualified.substring(0, fixture.qualified.lastIndexOf('.')) + ".\"" + renamed + "\"";
            boolean moved = false;
            try {
                fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                        + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'snapshot')");
                fixture.enableCdc();
                try (ServerHandle server = fixture.startServer()) {
                    ControlPlane control = fixture.configure(server, "snapshot_and_cdc", null, policy);
                    EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                    control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                    awaitValue(mongo, target, fixture.table, 1, "snapshot");
                    fixture.execute(fixture.renameSql(fixture.qualified, renamed)); moved = true;
                    fixture.execute("INSERT INTO " + live + " VALUES (2,'renamed')");
                    awaitValue(mongo, target, fixture.table, 2, "renamed");
                    fixture.execute("ALTER TABLE " + live + " ADD C VARCHAR(64)");
                    fixture.execute("INSERT INTO " + live + " VALUES (3,'column-after-rename','exact')");
                    awaitValue(mongo, target, fixture.table, 3, "column-after-rename");
                    assertThat(row(mongo, target, fixture.table, 3).get("C")).isEqualTo("exact");
                    assertThat(control.failure(fixture.pipeline)).isEmpty();
                }
            } finally { if (moved) fixture.execute(fixture.renameSql(live, fixture.table)); }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void aNewMatchingTableIsCapturedWithoutRestartingOrRefreshingDiscovery(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source); MongoEndpoints mongo = new MongoEndpoints()) {
            String added = fixture.table + "_NEW";
            String qualified = fixture.qualified.substring(0, fixture.qualified.lastIndexOf('.')) + ".\"" + added + "\"";
            fixture.tableSelection = List.of("/^" + fixture.table + ".*$/");
            boolean createdAdded = false;
            EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
            Document originalPreparation;
            try {
                fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                        + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'initial')"); fixture.enableCdc();
                try (ServerHandle server = fixture.startServer()) {
                    ControlPlane control = fixture.configure(server);
                    control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                    awaitValue(mongo, target, fixture.table, 1, "initial");
                    fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2,'warm')");
                    awaitValue(mongo, target, fixture.table, 2, "warm");
                    originalPreparation = preparationReceipt(fixture, null);
                    assertThat(originalPreparation).isNotNull();
                    assertThat(originalPreparation.getString("phase")).isEqualTo("READY");
                    // Re-reading an already confirmed table would wrongly recreate this target row.
                    mongo.delete(target, fixture.table, Map.of("ID", 1));
                    fixture.execute("CREATE TABLE " + qualified + " (ID " + fixture.integerType()
                            + " NOT NULL PRIMARY KEY, V VARCHAR(64))"); createdAdded = true;
                    fixture.execute("ALTER TABLE " + qualified + (source.equals("oracle") ? " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                            : " DATA CAPTURE CHANGES INCLUDE LONGVAR COLUMNS"));
                    fixture.execute("INSERT INTO " + qualified + " VALUES (3,'matching-new-table')");
                    fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (4,'past-new-table-commit')");
                    awaitValue(mongo, target, fixture.table, 4, "past-new-table-commit");
                    Await.until("the new matching table to arrive without a lifecycle or discovery operation",
                            java.time.Duration.ofSeconds(120), () -> {
                                assertThat(control.failure(fixture.pipeline)).isEmpty();
                                return "matching-new-table".equals(row(mongo, target, added, 3).get("V"));
                            }, () -> "pipeline=" + control.pipelineStatus(fixture.pipeline)
                                    + ", oldTableSentinel=" + row(mongo, target, fixture.table, 4)
                                    + ", newTable=" + row(mongo, target, added, 3));
                    fixture.execute("INSERT INTO " + qualified + " VALUES (5,'new-table-cdc')");
                    awaitValue(mongo, target, added, 5, "new-table-cdc");
                    fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (6,'original-after-admission')");
                    awaitValue(mongo, target, fixture.table, 6, "original-after-admission");
                    assertThat(row(mongo, target, fixture.table, 1)).as("confirmed old tables must not be snapshotted again").isEmpty();
                    assertThat(preparationReceipt(fixture, originalPreparation)).as("admission must preserve the original target receipt, model and attempt").isEqualTo(originalPreparation);
                    control.stop(fixture.pipeline, false);
                    Await.until("the expanded pipeline to stop retaining its admitted tables", () -> control.state(fixture.pipeline)
                            .filter(PipelineState.STOPPED::equals).isPresent(), () -> control.state(fixture.pipeline).toString());
                }
                fixture.execute("INSERT INTO " + qualified + " VALUES (7,'admitted-table-offline')");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (8,'original-table-offline')");
                try (ServerHandle restarted = fixture.startServer()) {
                    ControlPlane control = new ControlPlane(restarted.baseUrl());
                    fixture.storage.loginAndAwaitBusiness(control);
                    control.startAnswering(fixture.pipeline, "target-not-empty", "keep");
                    awaitValue(mongo, target, added, 7, "admitted-table-offline");
                    awaitValue(mongo, target, fixture.table, 8, "original-table-offline");
                    fixture.execute("INSERT INTO " + qualified + " VALUES (9,'admitted-table-resumed')");
                    awaitValue(mongo, target, added, 9, "admitted-table-resumed");
                    assertThat(row(mongo, target, fixture.table, 1)).as("expanded inventory must survive restart without an old-table snapshot").isEmpty();
                    assertThat(preparationReceipt(fixture, originalPreparation)).as("restart must preserve the original target preparation receipt").isEqualTo(originalPreparation);
                    assertThat(control.failure(fixture.pipeline)).isEmpty();
                }
            } finally { if (createdAdded) fixture.execute("DROP TABLE " + qualified + (source.equals("oracle") ? " PURGE" : "")); }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void refreshingPatternDiscoveryAdmitsANewMatchingTableWithoutRecreatingResources(String source) throws Exception {
        try (Fixture fixture = Fixture.open(source); MongoEndpoints mongo = new MongoEndpoints()) {
            String added = fixture.table + "_NEW";
            String qualified = fixture.qualified.substring(0, fixture.qualified.lastIndexOf('.')) + ".\"" + added + "\"";
            boolean createdAdded = false;
            fixture.tableSelection = List.of("/^" + fixture.table + ".*$/");
            try {
                fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                        + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'initial')"); fixture.enableCdc();
                try (ServerHandle server = fixture.startServer()) {
                    ControlPlane control = fixture.configure(server);
                    EndpointAddress target = EndpointAddress.uri(fixture.targetUri());
                    control.lifecycle(fixture.pipeline, LifecycleVerb.START);
                    awaitValue(mongo, target, fixture.table, 1, "initial");
                    fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (2,'warm')");
                    awaitValue(mongo, target, fixture.table, 2, "warm");
                    fixture.execute("CREATE TABLE " + qualified + " (ID " + fixture.integerType()
                            + " NOT NULL PRIMARY KEY, V VARCHAR(64))"); createdAdded = true;
                    fixture.execute("ALTER TABLE " + qualified + (source.equals("oracle") ? " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                            : " DATA CAPTURE CHANGES INCLUDE LONGVAR COLUMNS"));
                    fixture.execute("INSERT INTO " + qualified + " VALUES (3,'matching-new-table')");
                    fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (4,'past-new-table-commit')");
                    awaitValue(mongo, target, fixture.table, 4, "past-new-table-commit");
                    // Record this finite observation without calling it permanent loss.
                    Files.writeString(Path.of("target", "report-pattern-" + source + ".json"), JsonWriter.write(Map.of(
                            "source", source, "newTableArrivedBeforeExplicitRefresh", !row(mongo, target, added, 3).isEmpty())));
                    // Full-clear preparation freezes its selection: discover before clearing.
                    control.discoverSchema("src_" + fixture.suffix, source, fixture.settings);
                    control.stop(fixture.pipeline, true);
                    Await.until("the pattern pipeline to stop for explicit discovery refresh", () -> control.state(fixture.pipeline)
                            .filter(PipelineState.STOPPED::equals).isPresent(), () -> control.state(fixture.pipeline).toString());
                    control.discoverSchema("src_" + fixture.suffix, source, fixture.settings);
                    control.startAnswering(fixture.pipeline, "target-not-empty", "keep");
                    awaitValue(mongo, target, added, 3, "matching-new-table");
                    fixture.execute("INSERT INTO " + qualified + " VALUES (5,'new-table-cdc')");
                    awaitValue(mongo, target, added, 5, "new-table-cdc");
                    fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (6,'original-table-cdc')");
                    awaitValue(mongo, target, fixture.table, 6, "original-table-cdc");
                    assertThat(control.failure(fixture.pipeline)).isEmpty();
                }
            } finally { if (createdAdded) fixture.execute("DROP TABLE " + qualified + (source.equals("oracle") ? " PURGE" : "")); }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"oracle", "db2"})
    void stoppingARealSourceDoesNotBlockAnotherPipelineStartingInTheSameEngine(String source) throws Exception {
        try (Fixture first = Fixture.open(source); Fixture second = Fixture.alongside(first); MongoEndpoints mongo = new MongoEndpoints()) {
            for (Fixture fixture : List.of(first, second)) {
                fixture.execute("CREATE TABLE " + fixture.qualified + " (ID " + fixture.integerType()
                        + " NOT NULL PRIMARY KEY, V VARCHAR(64))");
                fixture.execute("INSERT INTO " + fixture.qualified + " VALUES (1,'snapshot')"); fixture.enableCdc();
            }
            try (ServerHandle server = first.startServer()) {
                ControlPlane control = first.configure(server); second.configure(server);
                control.lifecycle(first.pipeline, LifecycleVerb.START);
                awaitValue(mongo, EndpointAddress.uri(first.targetUri()), first.table, 1, "snapshot");
                first.execute("INSERT INTO " + first.qualified + " VALUES (2,'live')");
                awaitValue(mongo, EndpointAddress.uri(first.targetUri()), first.table, 2, "live");
                control.stop(first.pipeline, true);
                Map<String,Object> duringStop = control.pipelineStatus(first.pipeline);
                Files.writeString(Path.of("target", "report-stop-start-" + source + ".json"), JsonWriter.write(Map.of(
                        "source", source, "stateWhenTheOtherStartWasSubmitted", String.valueOf(duringStop.get("displayState")),
                        "observedState", String.valueOf(duringStop.get("observedState")))));
                // Submit while the first stop is outstanding; do not await its cleanup first.
                control.lifecycle(second.pipeline, LifecycleVerb.START);
                awaitValue(mongo, EndpointAddress.uri(second.targetUri()), second.table, 1, "snapshot");
                second.execute("INSERT INTO " + second.qualified + " VALUES (2,'independent-start')");
                awaitValue(mongo, EndpointAddress.uri(second.targetUri()), second.table, 2, "independent-start");
                Await.until("the first stop to settle independently of the new run", () -> control.state(first.pipeline)
                        .filter(PipelineState.STOPPED::equals).isPresent(), () -> control.state(first.pipeline).toString());
                assertThat(control.state(second.pipeline)).contains(PipelineState.RUNNING);
                assertThat(control.failure(second.pipeline)).isEmpty();
            }
        }
    }

    private static void awaitValue(MongoEndpoints mongo, EndpointAddress target, String table, int id, String value) {
        Await.until("source row " + id + " to reach MongoDB", () -> value.equals(row(mongo, target, table, id).get("V")),
                () -> String.valueOf(row(mongo, target, table, id)));
    }

    private static Map<String, Object> row(MongoEndpoints mongo, EndpointAddress target, String table, int id) {
        for (Document document : mongo.documents(target, table)) {
            Object key = document.get("ID");
            if (key != null && new java.math.BigDecimal(key.toString()).intValueExact() == id) return document;
        }
        return Map.of();
    }

    private static Document preparationReceipt(Fixture fixture, Document original) {
        try (var client = MongoClients.create(fixture.storage.cacheUri())) {
            var rows = SystemCollections.CACHE_SINK_PREPARATIONS.on(client.getDatabase(fixture.storage.cacheDatabase()));
            return rows.find(original == null ? new Document("_id.scope.pipelineId", fixture.pipeline)
                    : new Document("_id", original.get("_id"))).first();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final String source;
        final Map<String, Object> settings;
        final String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        final String table = "VFY_E2E_" + suffix.toUpperCase(java.util.Locale.ROOT);
        final String pipeline = "report_" + suffix;
        final String qualified;
        final URLClassLoader driverLoader;
        final Connection connection;
        final String mongoTemplate;
        final PreparedNativeStore storage;
        boolean created;
        boolean ownsStorage = true;
        boolean ownsSourceSchema;
        boolean ignoreModelEvolution;
        String sharingKey;
        List<String> tableSelection;

        @SuppressWarnings("unchecked")
        static Fixture open(String source) throws Exception {
            String config = System.getProperty("tapstate.e2e.report." + source + "-config");
            String mongo = System.getProperty("tapstate.e2e.report.mongo-uri-template");
            Assumptions.assumeTrue(config != null && mongo != null,
                    "external Oracle/Db2 and MongoDB fixtures have not been requested");
            RealConnectorGate.require(source, "mongodb");
            assertThat(mongo).as("MongoDB URI template must isolate every test database").contains("{database}");
            Map<String, Object> settings = new LinkedHashMap<>((Map<String, Object>) JsonReader.parse(Files.readString(Path.of(config))));
            return new Fixture(source, settings, mongo);
        }

        static Fixture alongside(Fixture owner) throws Exception {
            var result = new Fixture(owner.source, new LinkedHashMap<>(owner.settings), owner.mongoTemplate, owner.storage);
            result.ownsStorage = false;
            return result;
        }

        Fixture(String source, Map<String, Object> settings, String mongoTemplate) throws Exception {
            this(source, settings, mongoTemplate, null);
        }

        Fixture(String source, Map<String, Object> settings, String mongoTemplate, PreparedNativeStore sharedStorage) throws Exception {
            this.source = source;
            this.settings = settings;
            this.mongoTemplate = mongoTemplate;
            if (source.equals("db2") && sharedStorage == null) {
                settings.put("schema", "VFY_E2E_" + suffix.toUpperCase(java.util.Locale.ROOT));
                ownsSourceSchema = true;
            }
            qualified = "\"" + settings.get("schema").toString().replace("\"", "\"\"") + "\".\"" + table + "\"";
            Path jars = Path.of(System.getProperty("tapstate.e2e.connectors-dir"));
            Path jar;
            try (var files = Files.list(jars)) {
                jar = files.filter(p -> p.getFileName().toString().startsWith(source)
                        && p.toString().endsWith(".jar")).findFirst().orElseThrow();
            }
            driverLoader = new URLClassLoader(new java.net.URL[]{jar.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
            Driver driver = (Driver) driverLoader.loadClass(source.equals("oracle")
                    ? "oracle.jdbc.OracleDriver" : "com.ibm.db2.jcc.DB2Driver").getDeclaredConstructor().newInstance();
            Properties auth = new Properties();
            auth.setProperty("user", settings.get("user").toString());
            auth.setProperty("password", settings.get("password").toString());
            String address = settings.get("host") + ":" + settings.get("port");
            String url = source.equals("oracle") ? "jdbc:oracle:thin:@" + address + ":" + settings.get("database")
                    : "jdbc:db2://" + address + "/" + settings.get("database");
            connection = driver.connect(url, auth);
            if (ownsSourceSchema) execute("CREATE SCHEMA \"" + settings.get("schema") + "\"");
            storage = sharedStorage == null ? PreparedNativeStore.createOn(database -> mongoTemplate.replace("{database}", database)) : sharedStorage;
            if (source.equals("oracle")) {
                settings.put("sid", settings.get("database"));
                settings.put("thinType", "SID");
            }
        }

        String integerType() { return source.equals("oracle") ? "NUMBER(19)" : "INT"; }
        String storeUri() { return mongoTemplate.replace("{database}", "report_store_" + suffix); }
        String targetUri() { return mongoTemplate.replace("{database}", "report_target_" + suffix); }

        void execute(String sql) throws Exception {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
                if (sql.startsWith("CREATE TABLE " + qualified + " ")) created = true;
                String primaryDrop = "DROP TABLE " + qualified;
                if (sql.equals(primaryDrop) || sql.startsWith(primaryDrop + " ")) created = false;
            } catch (java.sql.SQLException failure) {
                throw portableSqlFailure(failure);
            }
        }

        private java.sql.SQLException portableSqlFailure(java.sql.SQLException failure) {
            // Db2 formats some messages lazily through its loader, which cleanup closes.
            // Materialize the diagnostic first so JUnit can report it after the fixture retires.
            String message = failure.getMessage();
            if (message != null) message = message.replace(settings.get("password").toString(), "***");
            var portable = new java.sql.SQLException(message, failure.getSQLState(), failure.getErrorCode());
            portable.setStackTrace(failure.getStackTrace());
            return portable;
        }

        String renameSql(String from, String to) {
            return (source.equals("oracle") ? "ALTER TABLE " : "RENAME TABLE ") + from
                    + (source.equals("oracle") ? " RENAME TO " : " TO ") + "\"" + to.replace("\"", "\"\"") + "\"";
        }

        void enableCdc() throws Exception {
            execute("ALTER TABLE " + qualified + (source.equals("oracle")
                    ? " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS" : " DATA CAPTURE CHANGES INCLUDE LONGVAR COLUMNS"));
        }

        ServerHandle startServer() {
            return InProcessServer.startPrepared(storage, List.of("--tapstate.connectors.seed-dir="
                    + System.getProperty("tapstate.e2e.connectors-dir"),
                    "--logging.level.io.tapstate.adapters.pdk.PdkCapturePort=DEBUG",
                    "--logging.level.io.tapstate.adapters.pdk.PdkDdlModels=DEBUG"));
        }

        ControlPlane configure(ServerHandle server) {
            return configure(server, "snapshot_and_cdc", null, "apply");
        }

        ControlPlane configure(ServerHandle server, String readMode, String writeMode, String ddl) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            assertThat(control.connectorIds()).contains(source, "mongodb");
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("source.tap.yml", JsonWriter.write(Map.of("version", "tapstate/v1", "kind", "source",
                    "id", "src_" + suffix, "connector", source, "config", settings, "mode", "cdc",
                    "tables", tableSelection == null ? List.of(table) : tableSelection, "srs", srsOptions())));
            resources.put("target.tap.yml", JsonWriter.write(Map.of("version", "tapstate/v1", "kind", "source",
                    "id", "tgt_" + suffix, "connector", "mongodb", "config", Map.of("isUri", true, "uri", targetUri()))));
            resources.put("pipeline.tap.yml", JsonWriter.write(Map.of("version", "tapstate/v1", "kind", "pipeline",
                    "id", pipeline, "source", "src_" + suffix, "settings", Map.of("read_mode", readMode),
                    "serve", Map.of("from", "/.*/", "sync", List.of(syncOptions(writeMode, ddl))))));
            control.apply(resources);
            control.discoverSchema("src_" + suffix, source, settings);
            return control;
        }

        private Map<String, Object> srsOptions() {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("key", sharingKey == null ? pipeline : sharingKey);
            if (ignoreModelEvolution) result.put("schema_evolution", "ignore");
            return result;
        }

        private Map<String, Object> syncOptions(String writeMode, String ddl) {
            Map<String, Object> sync = new LinkedHashMap<>(); sync.put("source", "tgt_" + suffix);
            if (ddl != null) sync.put("ddl", ddl);
            if (writeMode != null) sync.put("write_mode", writeMode);
            return sync;
        }

        @Override
        public void close() throws Exception {
            try {
                if (created) execute("DROP TABLE " + qualified + (source.equals("oracle") ? " PURGE" : ""));
                if (ownsSourceSchema) execute("DROP SCHEMA \"" + settings.get("schema") + "\" RESTRICT");
            } finally {
                try { connection.close(); } catch (java.sql.SQLException failure) {
                    throw portableSqlFailure(failure);
                } finally {
                    try { driverLoader.close(); } finally { if (ownsStorage) storage.close(); }
                }
            }
        }
    }
}
