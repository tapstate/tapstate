package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoHistoryRollupStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.nio.file.Path;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Arm;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Namespace;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Unit;
import static io.tapstate.e2e.BenchmarkJdiTelemetrySession.Feature;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Proves supported cost units and health boundaries against actual immutable boot processes. */
class BenchmarkJdiTelemetryWitnessIT {
    @BeforeAll
    static void immutableInputsAndDocker() {
        String reference = System.getProperty("tapstate.e2e.jdi-cost.reference-jar");
        String observability = System.getProperty("tapstate.e2e.jdi-cost.observability-jar");
        assumeTrue(reference != null || observability != null,
                "The boot capture witness requires both named immutable artifacts.");
        assertThat(reference).isNotBlank(); assertThat(observability).isNotBlank();
        DockerGate.require();
    }

    @Test
    void listenerCleanupFailureCannotDiscardOwnershipOfALiveBootProcess() throws Exception {
        String database = "jdi_telemetry_failed_launch";
        String uri = SharedMongo.replicaSetUrl(database);
        AtomicReference<RealProcessServer> launched = new AtomicReference<>();
        try (var artifact = BenchmarkJdiCostObserver.Artifact.open(Path.of(System.getProperty(
                "tapstate.e2e.jdi-cost.observability-jar")), Arm.OBSERVABILITY)) {
            try {
                assertThatThrownBy(() -> BenchmarkJdiTelemetrySession.launch(artifact, uri, database,
                        launched::set, () -> { throw new AssertionError("deliberate listener cleanup failure"); }))
                        .isInstanceOf(AssertionError.class)
                        .hasMessage("deliberate listener cleanup failure");
                assertThat(launched.get()).as("the real owned child process was launched").isNotNull();
                assertThat(launched.get().isAlive()).as("failed launch retains no live owned Boot process").isFalse();
            } finally {
                if (launched.get() != null) { launched.get().close(); }
            }
        }
    }

    @Test
    void aFailedDebugDetachStopsTheActualOwnedBootProcess() throws Exception {
        String database = "jdi_telemetry_failed_detach";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var artifact = BenchmarkJdiCostObserver.Artifact.open(Path.of(System.getProperty(
                    "tapstate.e2e.jdi-cost.observability-jar")), Arm.OBSERVABILITY);
                var session = BenchmarkJdiTelemetrySession.launch(artifact, uri, database)) {
            ControlPlane readiness = new ControlPlane(session.server().baseUrl());
            Await.until("failed-detach witness boot readiness", readiness::healthy,
                    () -> "owned process alive=" + session.server().isAlive());
            assertThat(session.server().isAlive()).as("the actual accepted Boot process is alive").isTrue();
            session.stopEventPumpForWitness();
            var cleanup = Executors.newSingleThreadExecutor();
            try {
                var finished = cleanup.submit(() -> session.detachOrStopOwnedProcess(
                        () -> { throw new IllegalStateException("deliberate detach failure"); }));
                assertThatCode(() -> finished.get(5, TimeUnit.SECONDS))
                        .as("fatal cleanup cannot await graceful shutdown with no event handler")
                        .doesNotThrowAnyException();
                assertThat(session.server().isAlive()).as("failed detach leaves no orphaned owned Boot process").isFalse();
            } finally {
                session.server().kill(); cleanup.shutdownNow();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Arm.class)
    void passiveBootKeepsHealthAvailableWithoutInventingScopedCounts(Arm arm,
            @TempDir Path directory) throws Exception {
        String property = arm == Arm.REFERENCE ? "tapstate.e2e.jdi-cost.reference-jar"
                : "tapstate.e2e.jdi-cost.observability-jar";
        String database = "jdi_passive_" + arm.name().toLowerCase(Locale.ROOT);
        String uri = SharedMongo.replicaSetUrl(database);
        try (var artifact = BenchmarkJdiCostObserver.Artifact.open(Path.of(System.getProperty(property)), arm);
                var session = BenchmarkJdiTelemetrySession.launch(artifact, uri, database, database + "_operator",
                        BenchmarkJdiTelemetrySession.Mode.PASSIVE_JDWP)) {
            ControlPlane readiness = new ControlPlane(session.server().baseUrl());
            Await.until("passive immutable boot readiness", () -> { session.checkCapture(); return readiness.healthy(); },
                    () -> "owned process alive=" + session.server().isAlive());
            session.begin();
            RunningPipeline pipeline = RunningPipeline.started(session.server(), directory);
            Await.until("passive fixture rows reached the target",
                    () -> pipeline.rowsAtTarget() == RunningPipeline.SEEDED_ROWS,
                    () -> "visible rows=" + pipeline.rowsAtTarget());
            pipeline.stopAndSettle(); session.cutoff();
            var evidence = session.shutdownAndFinish();
            assertThat(evidence.mode()).isEqualTo(BenchmarkJdiTelemetrySession.Mode.PASSIVE_JDWP);
            assertThat(evidence.fullyDrained()).as("passive mode cannot prove scoped invocation drain").isFalse();
            assertThat(evidence.unavailable()).contains(BenchmarkJdiCostObserver.Unavailable.SCOPED_COST_CAPTURE);
            assertThat(evidence.costs()).isEmpty(); assertThat(evidence.commands()).isEmpty();
            assertThat(evidence.callbacks()).isEmpty();
            assertThat(evidence.handlingNanos()).as("passive measured group has no runtime breakpoint handler").isZero();
            assertThat(evidence.windowBreakpointEvents()).isZero();
            assertThatThrownBy(() -> evidence.entries(Unit.SYNC_COMMAND_SEND))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("unavailable in passive mode");
            Map<?, ?> scoped = (Map<?, ?>) PipelineBenchmarkLiveRunIT.telemetryEvidence(java.util.Optional.of(evidence))
                    .get("scopedCosts");
            assertThat(scoped.get("state")).isEqualTo("UNAVAILABLE");
            if (arm == Arm.OBSERVABILITY) {
                assertThat(evidence.drainBreakpointEvents()).isEqualTo(1);
                assertThat(evidence.drainHandlingNanos()).isPositive();
                assertThat(evidence.shutdown().health().quiescent()).isTrue();
                assertThat(evidence.shutdown().health().closed()).isTrue();
            } else {
                assertThat(evidence.begin().health()).isNull(); assertThat(evidence.deltas()).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Arm.class)
    void actualBootTelemetryHasAttributedUnitsAndAnOwnedDrainedBoundary(Arm arm,
            @TempDir Path directory) throws Exception {
        String property = arm == Arm.REFERENCE ? "tapstate.e2e.jdi-cost.reference-jar"
                : "tapstate.e2e.jdi-cost.observability-jar";
        String database = "jdi_telemetry_" + arm.name().toLowerCase(Locale.ROOT);
        String uri = SharedMongo.replicaSetUrl(database);
        try (var artifact = BenchmarkJdiCostObserver.Artifact.open(Path.of(System.getProperty(property)), arm);
                var session = BenchmarkJdiTelemetrySession.launch(artifact, uri, database);
                StoreDocuments documents = StoreDocuments.at(uri)) {
            ControlPlane readiness = new ControlPlane(session.server().baseUrl());
            Await.until("immutable telemetry boot readiness", readiness::healthy,
                    () -> "owned process alive=" + session.server().isAlive());
            session.begin();
            RunningPipeline pipeline;
            try {
                pipeline = RunningPipeline.started(session.server(), directory);
            } catch (Exception | AssertionError failedStart) {
                session.checkCapture();
                throw failedStart;
            }
            Await.until("four fixture rows reached the real target",
                    () -> pipeline.rowsAtTarget() == RunningPipeline.SEEDED_ROWS,
                    () -> "visible rows=" + pipeline.rowsAtTarget());
            Await.until("the real raw sampler persisted a sample",
                    () -> documents.rateSamplesOf(pipeline.pipelineId()) > 0,
                    () -> "samples=" + documents.rateSamplesOf(pipeline.pipelineId()));
            pipeline.stopAndSettle();
            session.cutoff();
            var evidence = session.shutdownAndFinish();
            assertThat(evidence.artifactSha256()).isEqualTo(arm.sha256);
            assertThat(evidence.fullyDrained()).isTrue();
            assertThat(evidence.entries(Unit.RATE_DOCUMENT_BUILD)).isPositive();
            assertThat(evidence.entries(Unit.WIRE_COMMAND_BINARY_ENCODER_INVOCATION)).isPositive();
            assertThat(evidence.entries(Unit.SYNC_COMMAND_SEND)).isPositive();
            assertThat(evidence.commands().keySet()).anyMatch(key -> key.wire().namespace() == Namespace.RAW_HISTORY);
            assertThat(evidence.callbacks().keySet()).anyMatch(key -> key.origin()
                    == BenchmarkJdiTelemetrySession.Origin.READ && key.namespace() == Namespace.OBSERVATION);
            assertThat(evidence.commands().keySet()).anyMatch(key -> key.origin()
                    == BenchmarkJdiTelemetrySession.Origin.WRITE && key.wire().namespace() == Namespace.RAW_HISTORY);
            assertBoundary(evidence.begin()); assertBoundary(evidence.cutoff());
            if (evidence.shutdown() != null) { assertBoundary(evidence.shutdown()); }
            assertThat(evidence.unavailable()).contains(
                    BenchmarkJdiCostObserver.Unavailable.ASYNC_WRITE_COMPLETION,
                    BenchmarkJdiCostObserver.Unavailable.WIRE_DOCUMENT_COUNT,
                    BenchmarkJdiCostObserver.Unavailable.WIRE_BYTE_COUNT);
            if (arm == Arm.REFERENCE) {
                assertThat(evidence.available()).doesNotContain(Feature.LATEST_PAYLOAD, Feature.CHUNKS,
                        Feature.ROLLUPS, Feature.EVENTS, Feature.DISPATCHER, Feature.JANITOR);
                assertThat(evidence.entries(Unit.OBSERVATION_DOCUMENT_BUILD)).isPositive();
                assertThat(evidence.begin().health()).isNull();
                assertThat(evidence.deltas()).isEmpty();
            } else {
                assertThat(evidence.available()).contains(Feature.LATEST_PAYLOAD, Feature.CHUNKS,
                        Feature.ROLLUPS, Feature.EVENTS, Feature.DISPATCHER, Feature.JANITOR);
                assertThat(evidence.entries(Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION)).isPositive();
                assertThat(evidence.begin().health()).isNotNull();
                assertThat(evidence.shutdown().health().closed()).isTrue();
                assertThat(evidence.shutdown().health().quiescent()).isTrue();
                assertThat(evidence.deltas()).containsKeys(BenchmarkJdiTelemetrySession.Sink.LATEST,
                        BenchmarkJdiTelemetrySession.Sink.HISTORY, BenchmarkJdiTelemetrySession.Sink.EVENT);
                evidence.deltas().values().forEach(delta -> {
                    assertThat(delta.coalesced()).isNotNegative();
                    assertThat(delta.dropped()).isZero();
                    assertThat(delta.failures()).isZero();
                    assertThat(delta.timeouts()).isZero();
                });
            }
        }
    }

    @Test
    void aRealClosedRollupCapturesNestedEncodersBesideItsCommandCount(@TempDir Path directory) throws Exception {
        String database = "jdi_nonempty_closed_rollup";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var artifact = BenchmarkJdiCostObserver.Artifact.open(Path.of(System.getProperty(
                    "tapstate.e2e.jdi-cost.observability-jar")), Arm.OBSERVABILITY);
                var session = BenchmarkJdiTelemetrySession.launch(artifact, uri, database);
                var client = MongoClients.create(uri)) {
            ControlPlane control = new ControlPlane(session.server().baseUrl());
            Await.until("nonempty rollup immutable boot readiness", control::healthy,
                    () -> "owned process alive=" + session.server().isAlive());
            session.begin();
            RunningPipeline pipeline = RunningPipeline.started(session.server(), directory);
            var mongo = client.getDatabase(new ConnectionString(uri).getDatabase());
            var observations = new MongoObservationStore(client,
                    mongo.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    mongo.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            Await.until("the actual allocated execution published its scope",
                    () -> { session.checkCapture(); return observations.readStored(pipeline.pipelineId())
                            .filter(stored -> stored.scope().isPresent()).isPresent(); },
                    () -> "scoped latest is pending");
            var owner = observations.readStored(pipeline.pipelineId()).orElseThrow().scope().orElseThrow();
            Instant start = Instant.ofEpochSecond(Math.floorDiv(Instant.now().getEpochSecond(), 300) * 300 - 600);
            var samples = new ArrayList<org.bson.Document>();
            for (int sample = 0; sample < 3; sample++) {
                samples.add(MongoRateHistoryStore.toDocument(new RateSample(pipeline.pipelineId(),
                        start.plusSeconds(sample * 60L), Map.of("records.out", sample * 10L), Map.of(), start.minusSeconds(60)))
                        .append("pipelineIncarnationId", owner.pipelineIncarnationId())
                        .append("executionGeneration", owner.executionGeneration()));
            }
            // Publish the fixture atomically: the live worker must never cache a partial seed.
            try (var transaction = client.startSession()) {
                transaction.withTransaction(() -> {
                    mongo.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY).insertMany(transaction, samples);
                    return null;
                }, TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
            }
            var key = new HistoryRollupStore.Key(pipeline.pipelineId(),
                    HistoryRollupStore.Scope.incarnation(owner.pipelineIncarnationId()),
                    HistoryRollupStore.Resolution.PT5M, start);
            var rollups = new MongoHistoryRollupStore(mongo,
                    mongo.getCollection(MongoStorePort.PIPELINE_HISTORY_ROLLUPS), Duration.ofDays(15));
            // A normal miss offers a refresh hint even if the worker already passed this bucket.
            pipeline.control().history(pipeline.pipelineId(), start, key.bucketEnd(), "PT5M");
            var completeBucket = new AtomicReference<HistoryRollupStore.Bucket>();
            Await.until("the protected worker persisted all three closed-bucket inputs",
                    () -> {
                        session.checkCapture();
                        rollups.read(key).filter(found -> found.inWindowSamples() == 3
                                && recordsOutDelta(found).compareTo(BigDecimal.valueOf(20)) == 0)
                                .ifPresent(completeBucket::set);
                        return completeBucket.get() != null;
                    }, () -> "the requested bucket is absent or does not contain all three inputs");
            var bucket = completeBucket.get();
            assertThat(bucket.inWindowSamples()).isEqualTo(3);
            assertThat(bucket.fragments()).isNotEmpty();
            assertThat(bucket.requiresFinerResolution()).isFalse();
            assertThat(recordsOutDelta(bucket)).isEqualByComparingTo("20");
            pipeline.stopAndSettle(); session.cutoff();
            var evidence = session.shutdownAndFinish();
            assertThat(evidence.artifactSha256()).isEqualTo(Arm.OBSERVABILITY.sha256);
            assertThat(evidence.fullyDrained()).isTrue();
            for (Unit unit : new Unit[]{Unit.ROLLUP_DOCUMENT_BUILD, Unit.BSON_BINARY_ENCODER_INVOCATION,
                    Unit.BSON_DOCUMENT_BINARY_ENCODER_INVOCATION, Unit.WIRE_COMMAND_BINARY_ENCODER_INVOCATION,
                    Unit.SYNC_COMMAND_SEND}) {
                var rows = evidence.costs().entrySet().stream().filter(entry -> entry.getKey().segment()
                        == BenchmarkJdiTelemetrySession.Segment.WINDOW
                        && entry.getKey().origin() == BenchmarkJdiTelemetrySession.Origin.ROLLUP_BATCH
                        && entry.getKey().namespace() == Namespace.HISTORY_ROLLUPS
                        && entry.getKey().unit() == unit).toList();
                assertThat(rows).as("actual nonempty rollup cost unit %s", unit).isNotEmpty();
                rows.forEach(entry -> {
                    assertThat(entry.getValue().entries()).isPositive();
                    assertThat(entry.getValue().normalReturns()).isEqualTo(entry.getValue().entries());
                });
            }
            assertBoundary(evidence.begin()); assertBoundary(evidence.cutoff()); assertBoundary(evidence.shutdown());
            System.out.println("benchmark-nonempty-rollup samples=3 delta=20 evidence="
                    + JsonWriter.write(PipelineBenchmarkLiveRunIT.telemetryEvidence(java.util.Optional.of(evidence))));
        }
    }

    private static BigDecimal recordsOutDelta(HistoryRollupStore.Bucket bucket) {
        return bucket.fragments().stream().map(fragment -> fragment.recordsOut() == null
                ? BigDecimal.ZERO : fragment.recordsOut().delta()).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private static void assertBoundary(BenchmarkJdiTelemetrySession.Boundary boundary) {
        assertThat(boundary.pending().callbacks().values().stream().mapToLong(Long::longValue).sum())
                .isEqualTo(boundary.activeScopes());
        assertThat(boundary.pending().costs().values().stream().mapToLong(Long::longValue).sum())
                .isEqualTo(boundary.openCalls());
        assertThat(boundary.pending().commands().values().stream().mapToLong(Long::longValue).sum())
                .isLessThanOrEqualTo(boundary.openCalls());
    }
}
