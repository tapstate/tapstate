package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
        try (fixture; MongoClient reader = MongoClients.create(fixture.storage().cacheUri())) {
            MongoDatabase database = database(reader, fixture.storage().cacheUri());
            Document high;
            List<Path> originalStreams;
            try (RealProcessServer first = launch(fixture, List.of("-Xmx768m", "-XX:ActiveProcessorCount=2"))) {
                ControlPlane control = start(first, fixture, readMode, true, false);
                high = pendingWindow(control, fixture, database, MAIL);
                originalStreams = CdcRecoveryFixture.streams(fixture.source());
                assertThat(activeStreams(originalStreams)).as("one physical capture serves both nodes").isEqualTo(1);
                assertThat(consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE).getString("miningChainId"))
                        .isEqualTo(consumer(fixture, database, MAIL_PIPELINE, MAIL_SOURCE).getString("miningChainId"));
                assertThat(priority(fixture.rootTarget(), ROOT, "1")).isEqualTo("Low");
                first.kill();
            }

            Files.writeString(fixture.signals().resolve("release"), "");
            long restartedFrom = System.nanoTime();
            try (RealProcessServer second = launch(fixture, List.of("-Xmx768m", "-XX:ActiveProcessorCount=2"))) {
                ControlPlane control = loggedIn(second, fixture);
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
                            Document confirmed = tableConfirmation(consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE), ROOT);
                            return confirmed != null && number(confirmed, "epoch") == originalEpoch
                                    && number(confirmed, "sequence") >= originalSequence;
                        }, () -> String.valueOf(consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE)));
                long wroteFrom = System.nanoTime();
                CdcRecoveryFixture.change(fixture.source(), 6, ROOT, "3", "", "after-restart");
                awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "3", "after-restart");
                long writeMillis = Duration.ofNanos(System.nanoTime() - wroteFrom).toMillis();
                assertThat(control.errorCount(ROOT_PIPELINE)).contains(0L);
                System.out.println("Shared SRS " + readMode + " recovered the held High after a process crash in "
                        + recoveryMillis + " ms; the next source-to-sink write took " + writeMillis + " ms.");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot_and_cdc", "cdc_only"})
    void independentDirectChannelsOnOnePhysicalSourceResumeAtTheirOwnConfirmedPositions(
            String readMode, @TempDir Path temporary) throws Exception {
        independentDirectChannels(readMode, temporary, false);
    }

    @Test
    void stopObservationConvergesWhileAnIndependentTargetHoldsHigh(@TempDir Path temporary) throws Exception {
        independentDirectChannels("snapshot_and_cdc", temporary, true);
    }

    private static void independentDirectChannels(String readMode, Path temporary, boolean retainStopEvidence)
            throws Exception {
        Fixture fixture = fixture(temporary, "direct_channels_" + readMode);
        try (fixture; MongoClient reader = MongoClients.create(fixture.storage().cacheUri());
                // Match the failing runner's CPU count, which also sizes the JVM's shared async pool.
                RealProcessServer server = retainStopEvidence
                        ? launch(fixture, List.of("-XX:ActiveProcessorCount=4"))
                        : launch(fixture, List.of("-Xmx768m", "-XX:ActiveProcessorCount=2"))) {
            MongoDatabase database = database(reader, fixture.storage().cacheUri());
            ControlPlane control = start(server, fixture, readMode, false, true);
            CdcRecoveryFixture.change(fixture.source(), 1, ROOT, "1", "Low", "Low");
            awaitSourceBatch(fixture, 1, 2);
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "1", "Low");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "1", "Low");
            CdcRecoveryFixture.change(fixture.source(), 2, ROOT, "2", "", "baseline");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "baseline");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "2", "baseline");
            Await.until("both direct channels to confirm their first source batch", TIMEOUT,
                    () -> sourceSequence(directConsumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE)) >= 1
                            && sourceSequence(directConsumer(fixture, database, MAIL_PIPELINE, MAIL_SOURCE)) >= 1,
                    () -> consumersText(fixture, database));
            assertThat(directConsumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE).get("_id", Document.class).getString("channelId"))
                    .as("identical physical-source configuration must not mix independent channel recovery records")
                    .isNotEqualTo(directConsumer(fixture, database, MAIL_PIPELINE, MAIL_SOURCE).get("_id", Document.class).getString("channelId"));
            assertThat(activeStreams(CdcRecoveryFixture.streams(fixture.source()))).isEqualTo(2);

            hold(fixture);
            CdcRecoveryFixture.change(fixture.source(), 3, ROOT, "1", "Low", "High");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "1", "High");
            Await.until("the other channel's High to remain inside its held write", TIMEOUT,
                    () -> Files.exists(fixture.signals().resolve("waiting")), () -> streamText(fixture));
            CdcRecoveryFixture.change(fixture.source(), 4, ROOT, "3", "", "later");
            awaitPriority(control, fixture, fixture.mailTarget(), ROOT, "3", "later");
            Await.until("the fast channel to confirm High without confirming the held channel", TIMEOUT,
                    () -> sourceSequence(directConsumer(fixture, database, MAIL_PIPELINE, MAIL_SOURCE)) >= 3
                            && sourceSequence(directConsumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE)) < 3,
                    () -> consumersText(fixture, database));

            if (retainStopEvidence) {
                // Mail has confirmed the reported later change. Fill the held channel's remaining writers
                // before STOP, so cancellation cannot depend on a lucky gap in its blocking target calls.
                for (int sequence = 5; sequence <= 20; sequence++) {
                    CdcRecoveryFixture.change(fixture.source(), sequence, ROOT,
                            Integer.toString(sequence), "", "held");
                }
                awaitSourceBatch(fixture, 20, 2);
                Await.until("three independent target calls to enter the unchanged hold", TIMEOUT,
                        () -> CdcRecoveryFixture.heldWrites(fixture.signals()) >= 3,
                        () -> "held writes=" + CdcRecoveryFixture.heldWrites(fixture.signals())
                                + ", streams=" + streamText(fixture));
            }
            control.stop(ROOT_PIPELINE, false);
            if (retainStopEvidence) {
                try (StopEvidence evidence = new StopEvidence(database, server, fixture)) {
                    awaitState(control, ROOT_PIPELINE, PipelineState.STOPPED);
                }
            } else {
                awaitState(control, ROOT_PIPELINE, PipelineState.STOPPED);
            }
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
            assertThat(control.errorCount(ROOT_PIPELINE)).contains(0L);
        } finally {
            Files.writeString(fixture.signals().resolve("release"), "");
        }
    }

    /** Samples independently of the STOP wait, before either the held write or the server is released. */
    private static final class StopEvidence implements AutoCloseable {
        private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();
        private final List<? extends ScheduledFuture<?>> samples;
        private final Path directory;
        private final RealProcessServer server;

        private StopEvidence(MongoDatabase database, RealProcessServer server, Fixture fixture) throws Exception {
            long acceptedAt = System.nanoTime();
            this.server = server;
            directory = Files.createDirectories(Path.of(System.getProperty("tapstate.e2e.build-directory"),
                    "stop-observation", java.util.UUID.randomUUID().toString()));
            System.out.println("Held-target STOP evidence: " + directory);
            samples = List.of(5, 35).stream().map(seconds -> sampler.schedule(() -> {
                Path sample = Files.createDirectory(directory.resolve("after-" + seconds + "s"));
                Document snapshot = new Document("elapsedMillis",
                        Duration.ofNanos(System.nanoTime() - acceptedAt).toMillis())
                        .append("serverPid", server.pid())
                        .append("heldWrites", CdcRecoveryFixture.heldWrites(fixture.signals()))
                        .append("writeWaiting", Files.exists(fixture.signals().resolve("waiting")))
                        .append("writeReleased", Files.exists(fixture.signals().resolve("release")));
                for (String pipeline : List.of(ROOT_PIPELINE, MAIL_PIPELINE)) {
                    Document state = new Document();
                    try (var configuration = MongoClients.create(fixture.storage().configurationUri())) {
                        var intent = SystemCollections.PIPELINE_INTENTS.on(configuration.getDatabase(fixture.storage().configurationDatabase()))
                                .find(new Document("_id", pipeline)).first();
                        state.append("pipeline_intents", intent);
                        for (var collection : List.of(SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION)) {
                            var row = collection.on(database).find(new Document("_id", pipeline)).first();
                            if (row != null) {
                                var scope = row.get("scope", Document.class);
                                assertThat(scope.getString("clusterId")).isEqualTo(fixture.srs().scope().clusterId());
                                assertThat(scope.getLong("bindingVersion")).isEqualTo(fixture.srs().scope().bindingVersion());
                                assertThat(scope.getString("cacheIncarnation")).isEqualTo(fixture.srs().scope().cacheIncarnation());
                                assertThat(scope.getLong("stateGeneration")).isEqualTo(intent.getLong("stateGeneration"));
                            }
                            state.append(collection.name(), row);
                        }
                    }
                    snapshot.append(pipeline, state);
                }
                Files.writeString(sample.resolve("state.json"), snapshot.toJson());
                Files.writeString(sample.resolve("streams.txt"), streamText(fixture));
                Files.writeString(sample.resolve("consumers.txt"), consumersText(fixture, database));
                Files.copy(server.output(), sample.resolve("server.log"));
                Process dump = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
                        Long.toString(server.pid()), "Thread.print", "-l")
                        .redirectErrorStream(true).redirectOutput(sample.resolve("threads.txt").toFile()).start();
                if (!dump.waitFor(10, TimeUnit.SECONDS)) {
                    dump.destroyForcibly();
                    throw new AssertionError("the child JVM thread dump exceeded its collection budget: " + sample);
                }
                assertThat(dump.exitValue()).as("the child JVM thread dump at " + sample).isZero();
                return null;
            }, Math.max(0, seconds * 1_000_000_000L - (System.nanoTime() - acceptedAt)), TimeUnit.NANOSECONDS)).toList();
        }

        @Override
        public void close() throws Exception {
            try {
                for (ScheduledFuture<?> sample : samples) {
                    sample.get(50, TimeUnit.SECONDS);
                }
            } finally {
                sampler.shutdownNow();
                Files.copy(server.output(), directory.resolve("server-before-cleanup.log"));
            }
        }
    }

    @Test
    void aPausedConsumerDoesNotPinDurableCaptureAtTheHotRingCapacity(@TempDir Path temporary)
            throws Exception {
        Fixture fixture = fixture(temporary, "paused_consumer_headroom");
        try (fixture; MongoClient reader = MongoClients.create(fixture.storage().cacheUri());
                RealProcessServer server = launch(fixture, List.of("-Xmx768m", "-XX:ActiveProcessorCount=2"))) {
            MongoDatabase database = database(reader, fixture.storage().cacheUri());
            ControlPlane control = start(server, fixture, "snapshot_and_cdc", true, false);
            CdcRecoveryFixture.change(fixture.source(), 1, ROOT, "1", "Low", "Low");
            awaitSourceBatch(fixture, 1, 1);
            CdcRecoveryFixture.change(fixture.source(), 2, ROOT, "2", "", "Low");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "Low");
            control.lifecycle(ROOT_PIPELINE, LifecycleVerb.PAUSE);
            awaitState(control, ROOT_PIPELINE, PipelineState.PAUSED);
            Document paused = consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE);
            assertThat(paused).isNotNull();
            long readBefore = tableSequence(paused, "perTableSeq", ROOT);
            long confirmedBefore = tableSequence(paused, "perTableRingDone", ROOT);
            List<Path> streams = CdcRecoveryFixture.streams(fixture.source());
            long started = System.nanoTime();
            CdcRecoveryFixture.updates(fixture.source(), 3, 2048, ROOT, "2");
            CdcRecoveryFixture.change(fixture.source(), 2051, MAIL, "1", "", "capture-barrier");
            awaitPriority(control, fixture, fixture.mailTarget(), MAIL, "1", "capture-barrier");
            long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();
            Document stillPaused = consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE);
            assertThat(tableSequence(stillPaused, "perTableSeq", ROOT)).isEqualTo(readBefore);
            assertThat(tableSequence(stillPaused, "perTableRingDone", ROOT)).isEqualTo(confirmedBefore);
            String ring = ring(stillPaused, ROOT);
            assertThat(SystemCollections.SRS_EVENTS.on(database).countDocuments(new Document("_id.ring", ring)))
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
            assertThat(control.errorCount(ROOT_PIPELINE)).contains(0L);
            System.out.println("The paused SRS consumer replayed 2048 retained updates to its sink in "
                    + Duration.ofNanos(System.nanoTime() - replayedFrom).toMillis() + " ms.");
        }
    }

    @Test
    void aFullReloadDoesNotReplayRetainedLowOverItsNewerHighSnapshot(@TempDir Path temporary)
            throws Exception {
        Fixture fixture = fixture(temporary, "reload_snapshot_seam");
        try (fixture; MongoClient reader = MongoClients.create(fixture.storage().cacheUri());
                RealProcessServer server = launch(fixture, List.of("-Xmx768m", "-XX:ActiveProcessorCount=2"))) {
            MongoDatabase database = database(reader, fixture.storage().cacheUri());
            Path retainingTarget = Files.createDirectory(temporary.resolve("retaining-target"));
            ControlPlane control = start(server, fixture, "snapshot_and_cdc", true, false, retainingTarget);
            control.lifecycle(RETAINING_PIPELINE, LifecycleVerb.PAUSE);
            awaitState(control, RETAINING_PIPELINE, PipelineState.PAUSED);
            CdcRecoveryFixture.change(fixture.source(), 1, ROOT, "1", "Low", "Low");
            CdcRecoveryFixture.change(fixture.source(), 2, ROOT, "2", "", "barrier");
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "2", "barrier");
            Document retainedLow = Await.answered("the captured Low retained on the shared log", TIMEOUT,
                    () -> java.util.Optional.ofNullable(logRecord(fixture, database, ROOT, "1", "Low")));
            control.stop(ROOT_PIPELINE, true);
            awaitState(control, ROOT_PIPELINE, PipelineState.STOPPED);
            FileEndpoints.replaceTable(fixture.source().resolve(ROOT + ".csv"), "id,priority\n1,High\n");
            Files.deleteIfExists(fixture.rootTarget().resolve(ROOT + ".csv"));
            control.lifecycle(ROOT_PIPELINE, LifecycleVerb.START);
            awaitPriority(control, fixture, fixture.rootTarget(), ROOT, "1", "High");
            assertThat(SystemCollections.SRS_EVENTS.on(database)
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
            assertThat(control.errorCount(ROOT_PIPELINE)).contains(0L);
        }
    }

    @ParameterizedTest
    @EnumSource(LostHistory.class)
    void aRestartRefusesMissingOrUnprovenRecoveryHistory(LostHistory loss, @TempDir Path temporary)
            throws Exception {
        Fixture fixture = fixture(temporary, "lost_history_" + loss.name().toLowerCase(java.util.Locale.ROOT));
        try (fixture; MongoClient reader = MongoClients.create(fixture.storage().cacheUri())) {
            MongoDatabase database = database(reader, fixture.storage().cacheUri());
            Document high;
            try (RealProcessServer first = launch(fixture, List.of("-Xmx768m", "-XX:ActiveProcessorCount=2"))) {
                high = pendingWindow(start(first, fixture, "snapshot_and_cdc", true, false), fixture, database, MAIL);
                first.kill();
            }
            MongoCollection<Document> log = SystemCollections.SRS_EVENTS.on(database);
            Document damagedProgress = null;
            switch (loss) {
                case HOLE -> assertThat(log.deleteOne(new Document("_id", high.get("_id"))).getDeletedCount()).isEqualTo(1);
                case TRIMMED -> {
                    var key = high.get("_id", Document.class);
                    String ring = key.getString("ring"); long through = number(key, "seq");
                    var bounds = SystemCollections.SRS_LOG_BOUNDS.on(database);
                    var original = bounds.find(new Document("_id", "srs-log-bounds:" + ring)).first();
                    assertThat(original).isNotNull();
                    assertThat(number(original, "largestSequence")).isGreaterThanOrEqualTo(through);
                    assertThat(number(original, "trimmedThrough")).isLessThan(through);
                    try (var session = reader.startSession()) {
                        session.withTransaction(() -> {
                            assertThat(bounds.updateOne(session, new Document("_id", original.get("_id")),
                                    new Document("$set", new Document("trimmedThrough", through))).getModifiedCount()).isEqualTo(1);
                            assertThat(log.deleteMany(session, new Document("_id.ring", ring)
                                    .append("_id.seq", new Document("$lte", through))).getDeletedCount()).isPositive();
                            return null;
                        });
                    }
                    assertThat(log.find(new Document("_id", high.get("_id"))).first()).isNull();
                }
                case OLD_GENERATION -> assertThat(log.updateOne(new Document("_id", high.get("_id")),
                        new Document("$unset", new Document("epoch", ""))).getModifiedCount()).isEqualTo(1);
                case OLD_PROGRESS -> {
                    Document original = consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE);
                    assertThat(original).isNotNull();
                    damagedProgress = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database)
                            .find(new Document("_id", original.get("_id"))).first();
                    assertThat(damagedProgress).isNotNull();
                    damagedProgress.remove("progress");
                    // Current progress is mandatory; no unscoped legacy row can replace its original cursor.
                    assertThat(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).updateOne(
                            new Document("_id", original.get("_id")), new Document("$unset", new Document("progress", "")))
                            .getModifiedCount()).isEqualTo(1);
                }
            }
            Files.writeString(fixture.signals().resolve("release"), "");
            try (RealProcessServer second = launch(fixture, List.of("-Xmx768m", "-XX:ActiveProcessorCount=2"))) {
                ControlPlane control = loggedIn(second, fixture);
                awaitState(control, ROOT_PIPELINE, PipelineState.FAILED);
                String code = switch (loss) {
                    case OLD_PROGRESS -> IoError.DOCUMENT_UNREADABLE.code();
                    case OLD_GENERATION -> IoError.SRS_EVENT_MODEL_UNAVAILABLE.code();
                    case TRIMMED -> CaptureError.RECOVERY_LOG_GAP.code();
                    case HOLE -> IoError.SRS_CHECKPOINT_UNPROVEN.code();
                };
                Await.until("the failed pipeline to expose its recovery diagnostic", TIMEOUT,
                        () -> control.logs(ROOT_PIPELINE).contains(code),
                        () -> control.logs(ROOT_PIPELINE));
                assertThat(priority(fixture.rootTarget(), ROOT, "1")).isEqualTo("Low");
                assertThat(SystemCollections.SRS_CHAINS.on(database).countDocuments()).isPositive();
                if (loss == LostHistory.OLD_PROGRESS) {
                    assertThat(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database)
                            .find(new Document("_id", damagedProgress.get("_id"))).first())
                            .as("refusing incomplete native progress preserves the original cursor identity")
                            .isEqualTo(damagedProgress);
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
                () -> tableSequence(consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE), "perTableRingDone", ROOT) >= 0,
                () -> consumersText(fixture, database));
        hold(fixture);
        CdcRecoveryFixture.change(fixture.source(), 3, ROOT, "1", "Low", "High");
        Await.until("High to reach the held target write", TIMEOUT,
                () -> Files.exists(fixture.signals().resolve("waiting")), () -> streamText(fixture));
        CdcRecoveryFixture.change(fixture.source(), 4, secondTable, "1", "", "mail-first");
        awaitPriority(control, fixture, fixture.mailTarget(), secondTable, "1", "mail-first");
        CdcRecoveryFixture.change(fixture.source(), 5, secondTable, "2", "", "mail-barrier");
        awaitPriority(control, fixture, fixture.mailTarget(), secondTable, "2", "mail-barrier");
        Document high = Await.answered("the unconfirmed High durably captured before mail", TIMEOUT,
                () -> java.util.Optional.ofNullable(logRecord(fixture, database, ROOT, "1", "High")));
        long highSequence = number(high.get("_id", Document.class), "seq");
        Await.until("mail to be confirmed while High has only been read", TIMEOUT,
                () -> tableSequence(consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE), "perTableSeq", ROOT) >= highSequence
                        && tableSequence(consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE), "perTableRingDone", ROOT) < highSequence
                        && tableSequence(consumer(fixture, database, MAIL_PIPELINE, MAIL_SOURCE), "perTableRingDone", secondTable) >= 0,
                () -> consumersText(fixture, database));
        Document source = SystemCollections.SRS_CHAINS.on(database)
                .find(new Document("_id", consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE).getString("miningChainId"))).first();
        assertThat(source.get("checkpoint", Document.class)).isNotNull();
        assertThat(sourceSequence(source.get("checkpoint", Document.class)))
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
        var connector = CdcRecoveryFixture.connectorJar(delegate);
        var storage = PreparedNativeStore.create();
        try {
            return new Fixture(source, rootTarget, mailTarget, signals, storage, new NativeSrsDocuments(storage), connector);
        } catch (RuntimeException | Error failed) {
            try { storage.close(); } catch (RuntimeException cleanup) { failed.addSuppressed(cleanup); }
            throw failed;
        }
    }

    private static ControlPlane start(ServerHandle server, Fixture fixture, String readMode,
            boolean srs, boolean secondReadsRoot) {
        return start(server, fixture, readMode, srs, secondReadsRoot, null);
    }

    private static ControlPlane start(ServerHandle server, Fixture fixture, String readMode,
            boolean srs, boolean secondReadsRoot, Path retainingTarget) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        fixture.storage().loginAndAwaitBusiness(control);
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

    private static ControlPlane loggedIn(ServerHandle server, Fixture fixture) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        fixture.storage().loginAndAwaitBusiness(control);
        return control;
    }

    private static Document consumer(Fixture fixture, MongoDatabase database, String pipeline, String source) {
        var member = fixture.srs().pipelineMembership(pipeline, source);
        if (member == null) return null;
        var key = member.get("_id", Document.class);
        var cursor = fixture.srs().consumerOffset(key.getString("chain"), key.getString("consumer"));
        // This is a read-only joined observation, never a replacement persisted cursor.
        return cursor == null ? null : new Document(cursor).append("miningChainId", key.getString("chain"));
    }

    private static Document directConsumer(Fixture fixture, MongoDatabase database, String pipeline, String source) {
        return fixture.srs().directChannel(pipeline, source);
    }

    private static Document tableConfirmation(Document consumer, String table) {
        if (consumer == null || !(consumer.get("progress") instanceof Document progress)) return null;
        for (Document entry : progress.getList("sinkAckedByTable", Document.class))
            if (table.equals(entry.getString("table"))) return entry.get("position", Document.class);
        return null;
    }

    private static long tableSequence(Document consumer, String field, String table) {
        if (consumer == null) return -1;
        var positions = "perTableRingDone".equals(field) ? consumer.getList("ringDoneThrough", Document.class)
                : consumer.get("progress", Document.class).getList(field, Document.class);
        for (var position : positions) if (table.equals(position.getString("table"))) return number(position, "sequence");
        return -1;
    }

    private static long sourceSequence(Document row) {
        if (row == null) return -1;
        Document position = row;
        if (row.get("cursor") instanceof Document cursor) position = cursor.get("sinkAcked", Document.class);
        else if (row.get("progress") instanceof Document progress) position = progress.get("sinkAcked", Document.class);
        if (position == null || position.getString("token") == null) return -1;
        byte[] bytes = java.util.Base64.getDecoder().decode(position.getString("token"));
        try (var in = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))) {
            Object sourceOffset = in.readObject();
            return ((Number) ((Map<?, ?>) sourceOffset).get("sequence")).longValue();
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

    private static Document logRecord(Fixture fixture, MongoDatabase database, String table, String id, String priority) {
        var original = consumer(fixture, database, ROOT_PIPELINE, ROOT_SOURCE);
        if (original == null) return null;
        String originalRing = ring(original, table);
        for (Document document : SystemCollections.SRS_EVENTS.on(database).find(new Document("_id.ring", originalRing))) {
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

    private static String consumersText(Fixture fixture, MongoDatabase database) {
        return "shared=" + SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(
                new Document("_id.cacheIncarnation", fixture.srs().scope().cacheIncarnation())).into(new java.util.ArrayList<>())
                + ", direct=" + SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).find(
                        new Document("_id.scope.cacheIncarnation", fixture.srs().scope().cacheIncarnation())).into(new java.util.ArrayList<>());
    }

    private static RealProcessServer launch(Fixture fixture, List<String> jvmOptions) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (true) {
            var server = RealProcessServer.launchingPreparedInJvm(fixture.storage().configurationUri(), jvmOptions,
                    port -> List.of("--tapstate.cluster.id=" + fixture.storage().clusterId(), "--tapstate.hz.member-port=0"));
            var refused = new java.util.concurrent.atomic.AtomicBoolean();
            try {
                Await.until("the prepared boot to complete or refuse a leased identity or occupied port",
                        Duration.ofNanos(Math.max(1, deadline - System.nanoTime())), () -> {
                            String output;
                            try { output = Files.readString(server.output()); }
                            catch (java.io.IOException unreadable) { throw new java.io.UncheckedIOException(unreadable); }
                            boolean ready = output.contains("Tapstate application is ready");
                            boolean occupiedPort = output.contains("Web server failed to start. Port "
                                    + server.baseUrl().getPort() + " was already in use.");
                            if (!ready && (output.contains("boot.node-id-in-use") || occupiedPort)) {
                                refused.set(true); return true;
                            }
                            if (!server.isAlive()) throw new AssertionError("the prepared boot exited: " + output);
                            return output.contains("Tapstate application is ready");
                        }, server::tail);
                if (!refused.get()) {
                    server.awaitReady(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
                    if (System.nanoTime() >= deadline) throw new AssertionError("the prepared startup exceeded " + TIMEOUT);
                    return server;
                }
            } catch (RuntimeException | Error failed) {
                try { server.close(); } catch (RuntimeException | Error cleanup) { failed.addSuppressed(cleanup); }
                throw failed;
            }
            server.close();
            if (System.nanoTime() >= deadline)
                throw new AssertionError("the prepared boot remained refused within " + TIMEOUT);
        }
    }

    private record Fixture(Path source, Path rootTarget, Path mailTarget, Path signals,
            PreparedNativeStore storage, NativeSrsDocuments srs, byte[] connector) implements AutoCloseable {
        @Override public void close() {
            try { srs.close(); } finally { storage.close(); }
        }
    }

    private enum LostHistory { HOLE, TRIMMED, OLD_GENERATION, OLD_PROGRESS }
}
