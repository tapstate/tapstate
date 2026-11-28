package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.ResumeMetrics;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RequiresDocker
class MongoResumeMetricsIT {
    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 1);
    private static final CacheBusinessScope CACHE = new CacheBusinessScope("cluster-a", 1, "incarnation-a");
    private static final Instant START = Instant.parse("2026-01-02T00:00:00Z");

    @Test void admittedBaselineReopensInAnotherProcessAndOnlyItsOriginalIntentCanAdvanceIt() {
        verifyRecovery(MONGO.getReplicaSetUrl());
    }

    static void verifyRecovery(String uri) {
        MongoCachePipelineStoresIT.withFixture(uri, f -> {
            var claims = f.domain().workloadClaims(CACHE);
            var owner = claims.read(f.fence().key()).orElseThrow().claim();
            var old = claims.advanceExecution(owner, 1, Set.of("node-a")).orElseThrow();
            var fence = WorkloadClaimFence.from(old);
            var state = f.domain().pipelineStates(SCOPE, fence, () -> {});
            var attrs = Map.of(io.tapstate.core.lifecycle.MetricAttributes.PIPELINE_ID, "orders", "direction", "out");
            var count = MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                    MetricPoint.accumulated(attrs, START, START, 7));
            var facts = List.of(count);
            assertThat(state.compareAndSwap("orders", 0, "RUNNING", START)).isInstanceOf(CasOutcome.Applied.class);
            f.domain().pipelineObservations(SCOPE, fence, () -> {}).save(new Observation("orders",
                    PipelineState.RUNNING, Map.of("records.out", 7L), Map.of(), Map.of(), null, START, facts), 1);
            var pause = new ResumeMetrics.Transition(ResumeMetrics.Action.PAUSE, 2, 101, facts);
            var resume = new ResumeMetrics.Transition(ResumeMetrics.Action.REBUILD, 3, 101, List.of());
            var pauseWork = measure("freeze", () -> state.transitionMetrics("orders", 1, "PAUSED", START, pause));
            assertThat(pauseWork.result()).isInstanceOf(CasOutcome.Applied.class);
            assertThat(pauseWork.commands().get("find/pipeline_observation")).isOne();
            var lateConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri, f.database().getName(), null);
            try (var other = MongoCacheDomain.connect(lateConnection, Duration.ofSeconds(5), "late-observation")) {
                var late = MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                        MetricPoint.accumulated(attrs, START, START.plusMillis(1), 70));
                other.pipelineObservations(SCOPE, fence, () -> {}).save(new Observation("orders", PipelineState.RUNNING,
                        Map.of("records.out", 70L), Map.of(), Map.of(), null, START.plusMillis(1), List.of(late)), 1);
            }
            var resumeWork = measure("bind", () -> state.transitionMetrics("orders", 2, "RUNNING", START, resume));
            assertThat(resumeWork.result()).isInstanceOf(CasOutcome.Applied.class);
            var expected = new java.util.TreeMap<>(pauseWork.commands());
            expected.remove("find/pipeline_observation");
            assertThat(resumeWork.commands()).isEqualTo(expected);
            var admitted = measure("advance", () -> claims.advanceResumeExecution(old, 1, Set.of("node-a"), 3)).result().orElseThrow();

            verifyProducerWindows(f, state, WorkloadClaimFence.from(admitted));

            String db = f.database().getName();
            var child = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
                    System.getProperty("java.class.path"), MongoResumeMetricsIT.class.getName(), uri, db)
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.INHERIT).start();
            try {
                assertThat(child.waitFor(30, TimeUnit.SECONDS)).isTrue();
                assertThat(child.exitValue()).isZero();
            } finally {
                if (child.isAlive()) child.destroyForcibly();
            }
            var bound = ((StateStore.MetricReader) state).readForMetrics("orders").orElseThrow().applied();
            assertThat(bound.fence().executionGeneration()).isEqualTo(admitted.executionGeneration() + 1);
            assertThat(bound.baseline().facts()).containsExactly(count);
            var current = claims.read(f.fence().key()).orElseThrow().claim();
            claims.advanceExecution(current, 1, Set.of("node-a")).orElseThrow();
            assertThat(((StateStore.MetricReader) state).readForMetrics("orders").orElseThrow().applied()).isNull();
        });
    }

    private static void verifyProducerWindows(MongoCachePipelineStoresIT.Fixture f, StateStore state, WorkloadClaimFence successor) {
        var sink = f.domain().pipelineObservations(SCOPE, successor, () -> {});
        var reader = (StateStore.MetricReader) state;
        var absent = reader.readForMetrics("orders").orElseThrow().prior();
        assertThat(absent).isEqualTo(ResumeMetrics.Prior.ABSENT);
        var legacy = output(9, START, START.plusSeconds(1));
        // An older producer can publish a flat total before it has any structured points.
        sink.save(new Observation("orders", PipelineState.RUNNING, legacy.metrics(), Map.of(), Map.of(),
                null, legacy.observedAt(), List.of()), 3);
        var ordinaryRead = measure("ordinary-checkpoint", () -> state.read("orders"));
        var recovered = measure("resume-checkpoint", () -> reader.readForMetrics("orders")).result().orElseThrow().prior();
        assertThat(recovered.window()).isNull();
        assertThat(recovered.cumulative()).containsExactly(ResumeMetrics.Producer.OUT);
        var first = new ResumeMetrics.Window(3, 202, Map.of(ResumeMetrics.Producer.OUT,
                new ResumeMetrics.Slot(START.plusSeconds(1).toEpochMilli(), false)));
        var raw = output(2, START.plusSeconds(1), START.plusSeconds(2));
        var key = new ResumeMetrics.ObservationKey(SCOPE, 3, successor);
        var publication = new ResumeMetrics.Publication(key, recovered, first);
        var accepted = measure("save-window", () -> sink.saveResumed(raw, 3, publication));
        assertThat(accepted.result()).isTrue();
        assertThat(accepted.commands()).containsEntry("update/pipeline_observation", 1);
        var restarted = measure("reopen-window", () -> reader.readForMetrics("orders")).result().orElseThrow().prior();
        assertThat(restarted.window()).isEqualTo(first);
        assertThat(restarted.observedAt()).isEqualTo(raw.observedAt());
        var newStart = START.plusSeconds(3);
        var next = new ResumeMetrics.Window(3, 202, Map.of(ResumeMetrics.Producer.OUT,
                new ResumeMetrics.Slot(newStart.toEpochMilli(), false)));
        var latest = output(1, newStart, newStart);
        var batch = List.of(
                new MongoCacheDomain.ObservationWrite(SCOPE, successor, () -> {}, output(8, START, newStart.plusMillis(1)), 3, publication),
                new MongoCacheDomain.ObservationWrite(SCOPE, successor, () -> {}, latest, 3,
                        new ResumeMetrics.Publication(key, restarted, next)));
        var result = measure("batch-window-conflict", () -> f.domain().writeObservations(batch));
        assertThat(result.result().get(0)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThat(result.result().get(1)).isNull();
        assertThat(result.commands()).containsEntry("update/pipeline_observation", 1);
        assertThat(sink.read("orders")).contains(latest);
        assertThat(sink.saveResumed(output(8, START, newStart.plusMillis(2)), 3, publication)).isFalse();
        var wrongKey = new MongoCacheDomain.ObservationWrite(SCOPE, successor, () -> {}, latest, 4, publication);
        assertThat(f.domain().writeObservations(List.of(wrongKey)).getFirst()).isInstanceOfSatisfying(
                io.tapstate.core.common.TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED));
        var removedKey = new MongoCacheDomain.ObservationWrite(SCOPE, successor, () -> {},
                output(8, START, newStart.plusMillis(3)), 4,
                new ResumeMetrics.Publication(new ResumeMetrics.ObservationKey(SCOPE, 4, successor), recovered, first));
        assertThat(f.domain().writeObservations(List.of(removedKey)).getFirst()).isNotNull();
        assertThat(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database())
                .countDocuments(new org.bson.Document("_id.epoch", 4L))).isZero();
        var readBack = measure("read-after-window", () -> reader.readForMetrics("orders"));
        assertThat(readBack.commands()).isEqualTo(ordinaryRead.commands());
        var fallbackClient = mock(MongoClient.class, delegatesTo(f.admin()));
        doReturn(mock(ClusterDescription.class)).when(fallbackClient).getClusterDescription();
        var access = new MongoCachePipelineAccess(fallbackClient, f.database(), SCOPE, null, () -> {});
        var fallback = new MongoCachePipelineStateStore(access, null);
        assertThat(fallback.readForMetrics("orders")).isEqualTo(readBack.result());
    }

    private static Observation output(long count, Instant start, Instant at) {
        var attributes = Map.of(io.tapstate.core.lifecycle.MetricAttributes.PIPELINE_ID, "orders", "direction", "out");
        var fact = MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                MetricPoint.accumulated(attributes, start, at, count));
        return new Observation("orders", PipelineState.RUNNING, Map.of("records.out", count),
                Map.of(), Map.of(), null, at, List.of(fact));
    }

    @Test void oversizedMetricsLeaveTheJobAdmissibleAndTheCheckpointBounded() {
        verifyOverflow(MONGO.getReplicaSetUrl());
    }

    static void verifyOverflow(String uri) {
        MongoCachePipelineStoresIT.withFixture(uri, f -> {
            var claims = f.domain().workloadClaims(CACHE);
            var old = claims.advanceExecution(claims.read(f.fence().key()).orElseThrow().claim(), 1,
                    Set.of("node-a")).orElseThrow();
            var fence = WorkloadClaimFence.from(old);
            var state = f.domain().pipelineStates(SCOPE, fence, () -> {});
            var attrs = Map.of(io.tapstate.core.lifecycle.MetricAttributes.PIPELINE_ID, "orders",
                    io.tapstate.core.lifecycle.MetricAttributes.TABLE_ID, "t".repeat(1024 * 1024), "direction", "out");
            var facts = List.of(MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                    MetricPoint.accumulated(attrs, START, START, 7)));
            state.compareAndSwap("orders", 0, "RUNNING", START);
            f.domain().pipelineObservations(SCOPE, fence, () -> {}).save(new Observation("orders", PipelineState.RUNNING,
                    Map.of("records.out", 7L), Map.of(), Map.of(), null, START, facts), 1);
            assertThat(state.transitionMetrics("orders", 1, "PAUSED", START,
                    new ResumeMetrics.Transition(ResumeMetrics.Action.PAUSE, 2, 101, facts))).isInstanceOf(CasOutcome.Applied.class);
            assertThat(state.transitionMetrics("orders", 2, "RUNNING", START,
                    new ResumeMetrics.Transition(ResumeMetrics.Action.REBUILD, 3, 101, List.of()))).isInstanceOf(CasOutcome.Applied.class);
            assertThat(claims.advanceResumeExecution(old, 1, Set.of("node-a"), 3)).isPresent();
            var active = ((StateStore.MetricReader) state).readForMetrics("orders").orElseThrow().applied();
            assertThat(active.baseline().unavailable()).isTrue();
            assertThat(active.baseline().facts()).isEmpty();
            var handoff = SystemCollections.CACHE_PIPELINE_STATE.on(f.database())
                    .find(new org.bson.Document("_id", "orders")).projection(new org.bson.Document("_id", 0).append("resumeMetrics", 1)).first();
            assertThat(new org.bson.RawBsonDocument(handoff, new org.bson.codecs.DocumentCodec()).getByteBuffer().remaining())
                    .isLessThanOrEqualTo(1024 * 1024);
        });
    }

    @Test void transientObservationReadRetriesTheExactPauseAndSupersedingStopFencesResume() {
        verifyRetry(MONGO.getReplicaSetUrl());
    }

    static void verifyRetry(String uri) {
        MongoCachePipelineStoresIT.withFixture(uri, f -> {
            var claims = f.domain().workloadClaims(CACHE);
            var old = claims.advanceExecution(claims.read(f.fence().key()).orElseThrow().claim(), 1, Set.of("node-a")).orElseThrow();
            var state = f.domain().pipelineStates(SCOPE, WorkloadClaimFence.from(old), () -> {});
            var point = MetricPoint.accumulated(Map.of("direction", "out"), START, START, 7);
            var facts = List.of(MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}", point));
            state.compareAndSwap("orders", 0, "RUNNING", START);
            f.domain().pipelineObservations(SCOPE, WorkloadClaimFence.from(old), () -> {}).save(
                    new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, START, facts), 1);
            var pause = new ResumeMetrics.Transition(ResumeMetrics.Action.PAUSE, 2, 101, facts);
            var admin = f.admin().getDatabase("admin");
            admin.runCommand(new org.bson.Document("configureFailPoint", "failCommand")
                    .append("mode", new org.bson.Document("times", 1))
                    .append("data", new org.bson.Document("failCommands", List.of("find")).append("errorCode", 112)
                            .append("errorLabels", List.of("TransientTransactionError"))));
            try {
                var work = measure("pause-write-conflict", () -> state.transitionMetrics("orders", 1, "PAUSED", START, pause));
                assertThat(work.result()).isInstanceOf(CasOutcome.Applied.class);
                assertThat(work.commands().get("find/pipeline_observation")).isEqualTo(2);
                assertThat(work.commands().get("update/pipeline_state")).isOne();
            } finally {
                admin.runCommand(new org.bson.Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
            var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri, f.database().getName(), null);
            try (var other = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "control-race")) {
                var stopped = other.pipelineStates(SCOPE, WorkloadClaimFence.from(old), () -> {});
                assertThat(stopped.compareAndSwap("orders", 2, "STOPPED", START)).isInstanceOf(CasOutcome.Applied.class);
            }
            var resume = new ResumeMetrics.Transition(ResumeMetrics.Action.REBUILD, 3, 101, List.of());
            assertThat(state.transitionMetrics("orders", 2, "RUNNING", START, resume)).isInstanceOf(CasOutcome.Fenced.class);
            assertThat(claims.read(f.fence().key()).orElseThrow().claim().executionGeneration()).isEqualTo(old.executionGeneration());
            assertThat(((StateStore.MetricReader) state).readForMetrics("orders").orElseThrow().applied()).isNull();
        });
    }

    private record Work<T>(T result, Map<String, Integer> commands) {}

    private static <T> Work<T> measure(String phase, java.util.function.Supplier<T> operation) {
        var commands = new java.util.TreeMap<String, Integer>();
        var bytes = new java.util.concurrent.atomic.AtomicLong();
        var previous = MongoCommandObservation.COLLECTIONS.get();
        var previousBytes = MongoCommandObservation.BYTES.get();
        try {
            MongoCommandObservation.COLLECTIONS.set((command, collection) -> commands.merge(command + "/" + collection, 1, Integer::sum));
            MongoCommandObservation.BYTES.set(bytes::addAndGet);
            long start = System.nanoTime();
            T result = operation.get();
            System.out.printf("RESUME_CACHE %s commands=%s bytes=%d elapsedUs=%d%n",
                    phase, commands, bytes.get(), TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start));
            return new Work<>(result, commands);
        } finally {
            if (previous == null) MongoCommandObservation.COLLECTIONS.remove(); else MongoCommandObservation.COLLECTIONS.set(previous);
            if (previousBytes == null) MongoCommandObservation.BYTES.remove(); else MongoCommandObservation.BYTES.set(previousBytes);
        }
    }

    public static void main(String[] args) {
        var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, args[0], args[1], null);
        try (var domain = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "resume-recovery")) {
            var states = (StateStore.MetricReader) domain.pipelineStates(SCOPE);
            var reading = states.readForMetrics("orders").orElseThrow();
            var window = reading.prior().window();
            assertThat(window.jobId()).isEqualTo(202);
            assertThat(window.slots().get(ResumeMetrics.Producer.OUT))
                    .isEqualTo(new ResumeMetrics.Slot(START.plusSeconds(3).toEpochMilli(), false));
            var applied = reading.applied();
            assertThat(reading.state().epoch()).isEqualTo(3);
            assertThat(applied.revision()).isEqualTo(3);
            assertThat(applied.baseline().jobId()).isEqualTo(101);
            assertThat(applied.baseline().facts().getFirst().points().getFirst().value()).isEqualTo(7L);
            assertThat(applied.baseline().facts().getFirst().points().getFirst().startTime()).isEqualTo(START);
            var claims = domain.workloadClaims(CACHE);
            var claim = claims.read(applied.fence().key()).orElseThrow().claim();
            assertThat(WorkloadClaimFence.from(claim)).isEqualTo(applied.fence());
            assertThat(claims.advanceResumeExecution(claim, 1, Set.of("node-a"), applied.revision())).isPresent();
        }
    }
}
