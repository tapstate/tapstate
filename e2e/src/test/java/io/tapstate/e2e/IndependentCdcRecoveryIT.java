package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoSrsLogStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Drives durable CDC recovery through the shipped server, its PDK bridge and actual sink writes. */
class IndependentCdcRecoveryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final String ROOT = "support_case";
    private static final String MAIL = "emailmessage";
    private static final String ROOT_SOURCE = "support_source";
    private static final String MAIL_SOURCE = "mail_source";
    private static final String ROOT_PIPELINE = "support_pipeline";
    private static final String MAIL_PIPELINE = "mail_pipeline";
    private static final String RETAINING_PIPELINE = "retaining_pipeline";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot_and_cdc", "cdc_only"})
    void mailConfirmedAfterAPendingHighCannotMakeAProcessRestartSkipThatHigh(
            String readMode, @TempDir Path temporary) throws Exception {
        Fixture fixture = fixture(temporary, "shared_restart_" + readMode);
        try (MongoClient reader = MongoClients.create(fixture.storeUri())) {
            MongoDatabase database = database(reader, fixture.storeUri());
            Document high;
            List<Path> originalStreams;
            try (RealProcessServer first = RealProcessServer.start(fixture.storeUri())) {
                ControlPlane control = start(first, fixture, readMode, true, false);
                high = pendingWindow(control, fixture, database, MAIL);
                originalStreams = CdcRecoveryFixture.streams(fixture.source());
                assertThat(activeStreams(originalStreams)).as("one physical capture serves both nodes").isEqualTo(1);
                assertThat(consumer(database, ROOT_PIPELINE, ROOT_SOURCE).getString("miningChainId"))
                        .isEqualTo(consumer(database, MAIL_PIPELINE, MAIL_SOURCE).getString("miningChainId"));
                assertThat(priority(fixture.rootTarget(), ROOT, "1")).isEqualTo("Low");
                first.kill();
            }

            Files.writeString(fixture.signals().resolve("release"), "");
            long restartedFrom = System.nanoTime();
            try (RealProcessServer second = RealProcessServer.start(fixture.storeUri())) {
                ControlPlane control = loggedIn(second);
                awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "1", "High");
                long recoveryMillis = Duration.ofNanos(System.nanoTime() - restartedFrom).toMillis();
                Await.until("the replacement capture to resume after the already logged mail batch", TIMEOUT,
                        () -> newStreams(fixture, originalStreams).stream()
                                .anyMatch(path -> firstLine(path).startsWith("START 5 ")),
                        () -> streamText(fixture));
                assertThat(newStreams(fixture, originalStreams).stream()
                        .flatMap(path -> CdcRecoveryFixture.lines(path).stream())
                        .filter(line -> line.startsWith("BATCH ")).toList())
                        .as("High reached the sink from SRS while the source delivered no old changes")
                        .isEmpty();

                long originalEpoch = number(high, "epoch");
                long originalSequence = number(high.get("_id", Document.class), "seq");
                Await.until("the replayed High to retain its original capture generation at confirmation", TIMEOUT,
                        () -> {
                            Document confirmed = tableConfirmation(consumer(database, ROOT_PIPELINE, ROOT_SOURCE), ROOT);
                            return confirmed != null && number(confirmed, "sinkAckedEpoch") == originalEpoch
                                    && number(confirmed, "sinkAckedSeq") >= originalSequence;
                        }, () -> String.valueOf(consumer(database, ROOT_PIPELINE, ROOT_SOURCE)));
                long wroteFrom = System.nanoTime();
                CdcRecoveryFixture.change(fixture.source(), 6, ROOT, "3", "", "after-restart");
                awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "3", "after-restart");
                long writeMillis = Duration.ofNanos(System.nanoTime() - wroteFrom).toMillis();
                awaitCurrentErrorCount(control, database);
                System.out.println("Shared SRS " + readMode + " recovered the held High after a process crash in "
                        + recoveryMillis + " ms; the next source-to-sink write took " + writeMillis + " ms.");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot_and_cdc", "cdc_only"})
    void independentDirectChannelsOnOnePhysicalSourceResumeAtTheirOwnConfirmedPositions(
            String readMode, @TempDir Path temporary) throws Exception {
        Fixture fixture = fixture(temporary, "direct_channels_" + readMode);
        try (MongoClient reader = MongoClients.create(fixture.storeUri());
                RealProcessServer server = RealProcessServer.start(fixture.storeUri())) {
            MongoDatabase database = database(reader, fixture.storeUri());
            ControlPlane control = start(server, fixture, readMode, false, true);
            CdcRecoveryFixture.change(fixture.source(), 1, ROOT, "1", "Low", "Low");
            awaitSourceBatch(fixture, 1, 2);
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "1", "Low");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "1", "Low");
            CdcRecoveryFixture.change(fixture.source(), 2, ROOT, "2", "", "baseline");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "baseline");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "2", "baseline");
            Await.until("both direct channels to confirm their first source batch", TIMEOUT,
                    () -> sourceSequence(consumer(database, ROOT_PIPELINE, ROOT_SOURCE)) >= 1
                            && sourceSequence(consumer(database, MAIL_PIPELINE, MAIL_SOURCE)) >= 1,
                    () -> consumersText(database));
            assertThat(consumer(database, ROOT_PIPELINE, ROOT_SOURCE).getString("miningChainId"))
                    .as("identical physical-source configuration must not mix independent channel recovery records")
                    .isNotEqualTo(consumer(database, MAIL_PIPELINE, MAIL_SOURCE).getString("miningChainId"));
            assertThat(activeStreams(CdcRecoveryFixture.streams(fixture.source()))).isEqualTo(2);

            hold(fixture);
            CdcRecoveryFixture.change(fixture.source(), 3, ROOT, "1", "Low", "High");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "1", "High");
            Await.until("the other channel's High to remain inside its held write", TIMEOUT,
                    () -> Files.exists(fixture.signals().resolve("waiting")), () -> streamText(fixture));
            CdcRecoveryFixture.change(fixture.source(), 4, ROOT, "3", "", "later");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "3", "later");
            Await.until("the fast channel to confirm High without confirming the held channel", TIMEOUT,
                    () -> sourceSequence(consumer(database, MAIL_PIPELINE, MAIL_SOURCE)) >= 3
                            && sourceSequence(consumer(database, ROOT_PIPELINE, ROOT_SOURCE)) < 3,
                    () -> consumersText(database));

            control.stop(ROOT_PIPELINE, false);
            awaitState(control, ROOT_PIPELINE, PipelineState.STOPPED);
            List<Path> beforeRestart = CdcRecoveryFixture.streams(fixture.source());
            Files.writeString(fixture.signals().resolve("release"), "");
            control.lifecycle(ROOT_PIPELINE, LifecycleVerb.START);
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "1", "High");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "3", "later");
            Await.until("the held channel to reopen before its unconfirmed High", TIMEOUT,
                    () -> newStreams(fixture, beforeRestart).stream().anyMatch(path -> {
                        String[] start = firstLine(path).split(" ");
                        return start.length >= 2 && Long.parseLong(start[1]) < 3;
                    }), () -> streamText(fixture));
            awaitCurrentErrorCount(control, database);
        } finally {
            Files.writeString(fixture.signals().resolve("release"), "");
        }
    }

    @Test
    void aPausedConsumerDoesNotPinDurableCaptureAtTheHotRingCapacity(@TempDir Path temporary)
            throws Exception {
        Fixture fixture = fixture(temporary, "paused_consumer_headroom");
        try (MongoClient reader = MongoClients.create(fixture.storeUri());
                RealProcessServer server = RealProcessServer.start(fixture.storeUri())) {
            MongoDatabase database = database(reader, fixture.storeUri());
            ControlPlane control = start(server, fixture, "snapshot_and_cdc", true, false);
            CdcRecoveryFixture.change(fixture.source(), 1, ROOT, "1", "Low", "Low");
            awaitSourceBatch(fixture, 1, 1);
            CdcRecoveryFixture.change(fixture.source(), 2, ROOT, "2", "", "Low");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "Low");
            control.lifecycle(ROOT_PIPELINE, LifecycleVerb.PAUSE);
            awaitState(control, ROOT_PIPELINE, PipelineState.PAUSED);
            Document paused = consumer(database, ROOT_PIPELINE, ROOT_SOURCE);
            assertThat(paused).isNotNull();
            long readBefore = tableSequence(paused, "perTableSeq", ROOT);
            long confirmedBefore = tableSequence(paused, "perTableRingDone", ROOT);
            List<Path> streams = CdcRecoveryFixture.streams(fixture.source());
            long started = System.nanoTime();
            CdcRecoveryFixture.updates(fixture.source(), 3, 2048, ROOT, "2");
            CdcRecoveryFixture.change(fixture.source(), 2051, MAIL, "1", "", "capture-barrier");
            awaitPriority(control, fixture, fixture.mailTarget(), MAIL, "1", "capture-barrier");
            long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();
            Document stillPaused = consumer(database, ROOT_PIPELINE, ROOT_SOURCE);
            assertThat(tableSequence(stillPaused, "perTableSeq", ROOT)).isEqualTo(readBefore);
            assertThat(tableSequence(stillPaused, "perTableRingDone", ROOT)).isEqualTo(confirmedBefore);
            String ring = ring(stillPaused, ROOT);
            assertThat(database.getCollection(MongoStorePort.SRS_LOG).countDocuments(new Document("_id.ring", ring)))
                    .as("unprocessed changes are retained beyond the configured 1024-slot hot ring")
                    .isGreaterThan(1024);
            List<String> batches = streams.stream().flatMap(path -> CdcRecoveryFixture.lines(path).stream())
                    .filter(line -> line.startsWith("BATCH "))
                    .filter(line -> Long.parseLong(line.split(" ")[2]) > 2).toList();
            assertThat(batches).as("the affected path continues to capture whole connector batches").hasSizeLessThanOrEqualTo(4);
            long callbackMillis = batches.stream().mapToLong(line -> Long.parseLong(line.split(" ")[4])).sum();
            System.out.println("Durable shared capture carried 2048 updates for a paused consumer in "
                    + elapsedMillis + " ms through " + batches.size() + " source batches; durable callbacks took "
                    + callbackMillis + " ms in total.");

            long replayedFrom = System.nanoTime();
            control.lifecycle(ROOT_PIPELINE, LifecycleVerb.RESUME);
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "High");
            CdcRecoveryFixture.change(fixture.source(), 2052, ROOT, "3", "", "replay-barrier");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "3", "replay-barrier");
            assertThat(priority(fixture.rootTarget(), ROOT, "2")).isEqualTo("High");
            long replayMillis = Duration.ofNanos(System.nanoTime() - replayedFrom).toMillis();
            awaitCurrentErrorCount(control, database);
            System.out.println("The paused SRS consumer replayed 2048 retained updates to its sink in "
                    + replayMillis + " ms.");
        }
    }

    @Test
    void aFullReloadDoesNotReplayRetainedLowOverItsNewerHighSnapshot(@TempDir Path temporary)
            throws Exception {
        Fixture fixture = fixture(temporary, "reload_snapshot_seam");
        try (MongoClient reader = MongoClients.create(fixture.storeUri());
                RealProcessServer server = RealProcessServer.start(fixture.storeUri())) {
            MongoDatabase database = database(reader, fixture.storeUri());
            Path retainingTarget = Files.createDirectory(temporary.resolve("retaining-target"));
            ControlPlane control = start(server, fixture, "snapshot_and_cdc", true, false, retainingTarget);
            control.lifecycle(RETAINING_PIPELINE, LifecycleVerb.PAUSE);
            awaitState(control, RETAINING_PIPELINE, PipelineState.PAUSED);
            CdcRecoveryFixture.change(fixture.source(), 1, ROOT, "1", "Low", "Low");
            CdcRecoveryFixture.change(fixture.source(), 2, ROOT, "2", "", "barrier");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "barrier");
            Document retainedLow = Await.answered("the captured Low retained on the shared log", TIMEOUT,
                    () -> java.util.Optional.ofNullable(logRecord(database, ROOT, "1", "Low")));
            control.stop(ROOT_PIPELINE, true);
            awaitState(control, ROOT_PIPELINE, PipelineState.STOPPED);
            FileEndpoints.replaceTable(fixture.source().resolve(ROOT + ".csv"), "id,priority\n1,High\n");
            Files.deleteIfExists(fixture.rootTarget().resolve(ROOT + ".csv"));
            control.lifecycle(ROOT_PIPELINE, LifecycleVerb.START);
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "1", "High");
            assertThat(database.getCollection(MongoStorePort.SRS_LOG)
                    .find(new Document("_id", retainedLow.get("_id"))).first())
                    .as("the older value is still available to a wrongly positioned replay reader")
                    .isNotNull();

            CdcRecoveryFixture.change(fixture.source(), 3, MAIL, "1", "", "after-reload");
            awaitPriority(control, fixture, fixture.mailTarget(), MAIL, "1", "after-reload");
            CdcRecoveryFixture.change(fixture.source(), 4, ROOT, "3", "", "reload-barrier");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "3", "reload-barrier");
            assertThat(priority(fixture.rootTarget(), ROOT, "1"))
                    .as("the full load's newer row survives CDC replay even while Low remains retained")
                    .isEqualTo("High");
            awaitCurrentErrorCount(control, database);
        }
    }

    @ParameterizedTest
    @EnumSource(LostHistory.class)
    void aRestartRefusesMissingOrUnprovenRecoveryHistory(LostHistory loss, @TempDir Path temporary)
            throws Exception {
        Fixture fixture = fixture(temporary, "lost_history_" + loss.name().toLowerCase(java.util.Locale.ROOT));
        try (MongoClient reader = MongoClients.create(fixture.storeUri())) {
            MongoDatabase database = database(reader, fixture.storeUri());
            Document high;
            try (RealProcessServer first = RealProcessServer.start(fixture.storeUri())) {
                high = pendingWindow(start(first, fixture, "snapshot_and_cdc", true, false), fixture, database, MAIL);
                first.kill();
            }
            MongoCollection<Document> log = database.getCollection(MongoStorePort.SRS_LOG);
            switch (loss) {
                case HOLE -> log.deleteOne(new Document("_id", high.get("_id")));
                case TRIMMED -> new MongoSrsLogStore(log).trim(
                        high.get("_id", Document.class).getString("ring"),
                        number(high.get("_id", Document.class), "seq"));
                case OLD_GENERATION -> log.updateOne(new Document("_id", high.get("_id")),
                        new Document("$unset", new Document("epoch", "")));
                case OLD_PROGRESS -> {
                    Document old = consumer(database, ROOT_PIPELINE, ROOT_SOURCE);
                    database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                            .deleteOne(new Document("_id", old.get("_id")));
                    old.put("_id", new Document("chain", old.getString("miningChainId"))
                            .append("pipeline", ROOT_PIPELINE));
                    old.put("pipelineId", ROOT_PIPELINE);
                    for (String field : List.of("ownerPipelineId", "sourceNodeId", "progressKind", "sinkAckedByTable")) {
                        old.remove(field);
                    }
                    database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).insertOne(old);
                }
            }
            Files.writeString(fixture.signals().resolve("release"), "");
            try (RealProcessServer second = RealProcessServer.start(fixture.storeUri())) {
                ControlPlane control = loggedIn(second);
                awaitState(control, ROOT_PIPELINE, PipelineState.FAILED);
                String code = loss == LostHistory.OLD_PROGRESS
                        ? CaptureError.RECOVERY_PROGRESS_UNPROVEN.code() : CaptureError.RECOVERY_LOG_GAP.code();
                Await.until("the failed pipeline to expose its recovery diagnostic", TIMEOUT,
                        () -> control.logs(ROOT_PIPELINE).contains(code),
                        () -> control.logs(ROOT_PIPELINE));
                assertThat(priority(fixture.rootTarget(), ROOT, "1")).isEqualTo("Low");
                assertThat(database.getCollection(MongoStorePort.SRS_META).countDocuments()).isPositive();
                if (loss == LostHistory.OLD_PROGRESS) {
                    assertThat(database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                            .find(new Document("pipelineId", ROOT_PIPELINE)).first())
                            .as("refusing ambiguous old progress preserves the user's retained state")
                            .isNotNull();
                }
            }
        }
    }

    private static Document pendingWindow(ControlPlane control, Fixture fixture, MongoDatabase database,
            String secondTable) throws Exception {
        CdcRecoveryFixture.change(fixture.source(), 1, ROOT, "1", "Low", "Low");
        awaitSourceBatch(fixture, 1, 1);
        CdcRecoveryFixture.change(fixture.source(), 2, ROOT, "2", "", "baseline");
        awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "baseline");
        Await.until("the root's first change to be safely confirmed", TIMEOUT,
                () -> tableSequence(consumer(database, ROOT_PIPELINE, ROOT_SOURCE), "perTableRingDone", ROOT) >= 0,
                () -> consumersText(database));
        hold(fixture);
        CdcRecoveryFixture.change(fixture.source(), 3, ROOT, "1", "Low", "High");
        Await.until("High to reach the held target write", TIMEOUT,
                () -> Files.exists(fixture.signals().resolve("waiting")), () -> streamText(fixture));
        CdcRecoveryFixture.change(fixture.source(), 4, secondTable, "1", "", "mail-first");
        awaitPriority(control, fixture, fixture.mailTarget(), secondTable, "1", "mail-first");
        CdcRecoveryFixture.change(fixture.source(), 5, secondTable, "2", "", "mail-barrier");
        awaitPriority(control, fixture, fixture.mailTarget(), secondTable, "2", "mail-barrier");
        Document high = Await.answered("the unconfirmed High durably captured before mail", TIMEOUT,
                () -> java.util.Optional.ofNullable(logRecord(database, ROOT, "1", "High")));
        long highSequence = number(high.get("_id", Document.class), "seq");
        Await.until("mail to be confirmed while High has only been read", TIMEOUT,
                () -> tableSequence(consumer(database, ROOT_PIPELINE, ROOT_SOURCE), "perTableSeq", ROOT) >= highSequence
                        && tableSequence(consumer(database, ROOT_PIPELINE, ROOT_SOURCE), "perTableRingDone", ROOT) < highSequence
                        && tableSequence(consumer(database, MAIL_PIPELINE, MAIL_SOURCE), "perTableRingDone", secondTable) >= 0,
                () -> consumersText(database));
        Document source = database.getCollection(MongoStorePort.SRS_META)
                .find(new Document("_id", consumer(database, ROOT_PIPELINE, ROOT_SOURCE).getString("miningChainId"))).first();
        assertThat(source).containsEntry("sourceReadDurable", true);
        assertThat(sourceSequence(source, "sourceReadOffset"))
                .as("the shared source checkpoint names the complete durable batch, despite root High pending")
                .isEqualTo(5L);
        return high;
    }

    private static Fixture fixture(Path temporary, String database) throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        Path rootTarget = Files.createDirectory(temporary.resolve("root-target"));
        Path mailTarget = Files.createDirectory(temporary.resolve("mail-target"));
        Path signals = Files.createDirectory(temporary.resolve("write-signals"));
        Files.writeString(signals.resolve("release"), "");
        FileEndpoints.replaceTable(source.resolve(ROOT + ".csv"), "id,priority\n1,Low\n");
        FileEndpoints.replaceTable(source.resolve(MAIL + ".csv"), "id,priority\n");
        Path delegate = E2eConnectorJar.buildInto(Files.createDirectory(temporary.resolve("connector")));
        return new Fixture(source, rootTarget, mailTarget, signals, SharedMongo.replicaSetUrl(database),
                CdcRecoveryFixture.connectorJar(delegate));
    }

    private static ControlPlane start(ServerHandle server, Fixture fixture, String readMode,
            boolean srs, boolean secondReadsRoot) {
        return start(server, fixture, readMode, srs, secondReadsRoot, null);
    }

    private static ControlPlane start(ServerHandle server, Fixture fixture, String readMode,
            boolean srs, boolean secondReadsRoot, Path retainingTarget) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector(E2eConnectorJar.CONNECTOR_ID, fixture.connector());
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(ROOT_SOURCE + ".tap.yml", source(ROOT_SOURCE, fixture.source()));
        resources.put(MAIL_SOURCE + ".tap.yml", source(MAIL_SOURCE, fixture.source()));
        resources.put("support_target.tap.yml", target("support_target", fixture.rootTarget(), fixture.signals()));
        resources.put("mail_target.tap.yml", target("mail_target", fixture.mailTarget(), null));
        resources.put(ROOT_PIPELINE + ".tap.yml",
                pipeline(ROOT_PIPELINE, ROOT_SOURCE, "support_target", ROOT, readMode, srs));
        resources.put(MAIL_PIPELINE + ".tap.yml", pipeline(MAIL_PIPELINE, MAIL_SOURCE, "mail_target",
                secondReadsRoot ? ROOT : MAIL, readMode, srs));
        if (retainingTarget != null) {
            resources.put("retaining_target.tap.yml", target("retaining_target", retainingTarget, null));
            resources.put(RETAINING_PIPELINE + ".tap.yml",
                    pipeline(RETAINING_PIPELINE, ROOT_SOURCE, "retaining_target", ROOT, readMode, srs));
        }
        control.apply(resources);
        control.discoverSchema(ROOT_SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", fixture.source().toString()));
        control.discoverSchema(MAIL_SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", fixture.source().toString()));
        control.lifecycle(ROOT_PIPELINE, LifecycleVerb.START);
        control.lifecycle(MAIL_PIPELINE, LifecycleVerb.START);
        if (retainingTarget != null) {
            control.lifecycle(RETAINING_PIPELINE, LifecycleVerb.START);
            awaitState(control, RETAINING_PIPELINE, PipelineState.RUNNING);
        }
        awaitState(control, ROOT_PIPELINE, PipelineState.RUNNING);
        awaitState(control, MAIL_PIPELINE, PipelineState.RUNNING);
        int expectedStreams = srs ? 1 : 2;
        Await.until("the selected source streams to open", TIMEOUT,
                () -> activeStreams(CdcRecoveryFixture.streams(fixture.source())) == expectedStreams
                        && (secondReadsRoot || CdcRecoveryFixture.streams(fixture.source()).stream()
                                .anyMatch(path -> firstLine(path).contains(MAIL))),
                () -> streamText(fixture));
        return control;
    }

    private static String source(String id, Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ support_case, emailmessage ]
                """.formatted(id, directory);
    }

    private static String target(String id, Path directory, Path signals) {
        String config = "uri: \"" + directory + "\"" + (signals == null ? "" : ", hold_writes: \"" + signals + "\"");
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: e2e_file
                config: { %s }
                """.formatted(id, config);
    }

    private static String pipeline(String id, String source, String target, String table,
            String readMode, boolean srs) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: %s }
                settings: { read_mode: %s }
                serve:
                  from: %s
                  sync:
                    - source: %s
                """.formatted(id, source, srs, readMode, table, target);
    }

    private static void hold(Fixture fixture) throws Exception {
        Files.deleteIfExists(fixture.signals().resolve("waiting"));
        Files.deleteIfExists(fixture.signals().resolve("release"));
    }

    private static MongoDatabase database(MongoClient client, String uri) {
        return client.getDatabase(new ConnectionString(uri).getDatabase());
    }

    private static ControlPlane loggedIn(ServerHandle server) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.login("e2e", "e2e-password");
        return control;
    }

    private static Document consumer(MongoDatabase database, String pipeline, String source) {
        return database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("pipelineId", SrsConsumerId.of(pipeline, source).value())).first();
    }

    private static Document tableConfirmation(Document consumer, String table) {
        if (consumer == null || !(consumer.get("sinkAckedByTable") instanceof Document byTable)) return null;
        return byTable.get(table, Document.class);
    }

    private static long tableSequence(Document consumer, String field, String table) {
        if (consumer == null || !(consumer.get(field) instanceof Document positions)) return -1L;
        return number(positions, table);
    }

    private static long sourceSequence(Document consumer) {
        return sourceSequence(consumer, "sinkAckedSrcpos");
    }

    private static long sourceSequence(Document consumer, String field) {
        if (consumer == null || consumer.getString(field) == null) return -1L;
        byte[] bytes = java.util.Base64.getDecoder().decode(consumer.getString(field));
        try (var in = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))) {
            Object position = in.readObject();
            return ((Number) ((Map<?, ?>) position).get("sequence")).longValue();
        } catch (java.io.IOException | ClassNotFoundException malformed) {
            throw new AssertionError("the fixture's recorded source position must be readable", malformed);
        }
    }

    private static long number(Document document, String field) {
        return document != null && document.get(field) instanceof Number value ? value.longValue() : -1L;
    }

    private static String ring(Document consumer, String table) {
        return SrsRingbuffer.ringName(consumer.getString("miningChainId"), table);
    }

    private static Document logRecord(MongoDatabase database, String table, String id, String priority) {
        for (Document document : database.getCollection(MongoStorePort.SRS_LOG).find(new Document("_id", new Document("$type", "object")))) {
            Document key = document.get("_id", Document.class);
            if (!key.getString("ring").endsWith("." + table)) continue;
            Document after = document.get("after", Document.class);
            if (after != null && id.equals(value(after.get("id"))) && priority.equals(value(after.get("priority")))) {
                return document;
            }
        }
        return null;
    }

    private static String value(Object value) {
        return String.valueOf(value instanceof Document carried && carried.containsKey("__tapstate_carried")
                ? carried.get("__tapstate_carried") : value);
    }

    private static String priority(Path target, String table, String id) {
        List<String> lines = CdcRecoveryFixture.lines(target.resolve(table + ".csv"));
        if (lines.isEmpty()) return null;
        List<String> columns = List.of(lines.getFirst().split(",", -1));
        int idColumn = columns.indexOf("id");
        int priorityColumn = columns.indexOf("priority");
        for (String line : lines.subList(1, lines.size())) {
            String[] row = line.split(",", -1);
            if (idColumn >= 0 && priorityColumn >= 0 && row.length > Math.max(idColumn, priorityColumn)
                    && id.equals(row[idColumn])) return row[priorityColumn];
        }
        return null;
    }

    private static void awaitCurrentErrorCount(ControlPlane control, MongoDatabase database) {
        long count;
        try {
            count = Await.answered("the current execution publishes its actual error count", TIMEOUT,
                    () -> control.errorCount(ROOT_PIPELINE));
        } catch (AssertionError unavailable) {
            String identity = "; live identity unavailable";
            try {
                // Capture the live identity before server cleanup; an unavailable count is never zero.
                Document artifact = database.getCollection(MongoStorePort.ARTIFACTS)
                        .find(new Document("_id", ROOT_PIPELINE))
                        .projection(new Document("pipelineIncarnationId", 1)).maxTime(1, java.util.concurrent.TimeUnit.SECONDS).first();
                var authority = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                        .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", ROOT_PIPELINE))
                        .projection(new Document("executionGeneration", 1)).maxTime(1, java.util.concurrent.TimeUnit.SECONDS)
                        .limit(2).into(new java.util.ArrayList<>());
                var key = new org.bson.types.Binary(java.security.MessageDigest.getInstance("SHA-256")
                        .digest(ROOT_PIPELINE.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                Document publication = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION)
                        .find(new Document("_id", key))
                        .projection(new Document("current.pipelineIncarnationId", 1)
                                .append("current.executionGeneration", 1).append("current.observedAt", 1))
                        .maxTime(1, java.util.concurrent.TimeUnit.SECONDS).first();
                identity = "; live artifact=" + artifact + "; live authority=" + authority
                        + "; exact current publication=" + publication;
            } catch (java.security.NoSuchAlgorithmException | RuntimeException diagnosticFailure) {
                unavailable.addSuppressed(diagnosticFailure);
            }
            throw new AssertionError(unavailable.getMessage() + identity, unavailable);
        }
        assertThat(count).as("the current execution's published error count").isZero();
    }

    private static void awaitPriority(ControlPlane control, Fixture fixture, Path target, String table,
            String id, String expected) {
        Await.until(table + " row " + id + " to hold " + expected, TIMEOUT,
                () -> expected.equals(priority(target, table, id)),
                () -> "rows=" + CdcRecoveryFixture.lines(target.resolve(table + ".csv"))
                        + ", root=" + control.state(ROOT_PIPELINE) + ", mail=" + control.state(MAIL_PIPELINE)
                        + ", root logs=" + control.logs(ROOT_PIPELINE) + ", streams=" + streamText(fixture));
    }

    private static void awaitState(ControlPlane control, String pipeline, PipelineState state) {
        Await.until(pipeline + " to reach " + state, TIMEOUT,
                () -> control.state(pipeline).filter(state::equals).isPresent(),
                () -> control.state(pipeline) + ", logs=" + control.logs(pipeline));
    }

    private static void awaitSourceBatch(Fixture fixture, long through, long expectedReaders) {
        Await.until("the source batch ending at " + through + " to be handed over separately", TIMEOUT,
                () -> CdcRecoveryFixture.streams(fixture.source()).stream().filter(path ->
                        CdcRecoveryFixture.lines(path).stream().anyMatch(line -> line.startsWith("BATCH ")
                                && Long.parseLong(line.split(" ")[2]) == through)).count() >= expectedReaders,
                () -> streamText(fixture));
    }

    private static long activeStreams(List<Path> streams) {
        return streams.stream().filter(path -> !CdcRecoveryFixture.lines(path).isEmpty()
                && CdcRecoveryFixture.lines(path).stream().noneMatch(line -> line.startsWith("END "))).count();
    }

    private static String firstLine(Path stream) {
        return CdcRecoveryFixture.lines(stream).stream().findFirst().orElse("");
    }

    private static List<Path> newStreams(Fixture fixture, List<Path> earlier) {
        return CdcRecoveryFixture.streams(fixture.source()).stream().filter(path -> !earlier.contains(path)).toList();
    }

    private static String streamText(Fixture fixture) {
        return CdcRecoveryFixture.streams(fixture.source()).stream()
                .map(path -> path.getFileName() + ":" + CdcRecoveryFixture.lines(path)).toList().toString();
    }

    private static String consumersText(MongoDatabase database) {
        return database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find().into(new java.util.ArrayList<>()).toString();
    }

    private record Fixture(Path source, Path rootTarget, Path mailTarget, Path signals,
            String storeUri, byte[] connector) {}

    private enum LostHistory { HOLE, TRIMMED, OLD_GENERATION, OLD_PROGRESS }
}
