package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** A cold cleanup snapshot does not authorize removal of a live continuation's cumulative source. */
@RequiresDocker
class MongoObservationContinuationJanitorIT {
    private static final String PIPE = "orders", CLUSTER = "cluster-a", INC = "inc-a";
    private static final Instant START = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant AT = START.plusSeconds(10);
    private static final ObservationStore.Scope SOURCE = new ObservationStore.Scope(INC, 1);
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void aLiveContinuationKeepsItsQualifiedLegacySourceAfterSuccessorAdmission() {
        try (Fixture f = new Fixture()) {
            assertThat(f.legacy.saveScoped(observation(7), SOURCE)).isTrue();
            ObservationStore.LatestSnapshot scanned = f.observations.scanLatestAfter(Optional.empty(), 1).getFirst();
            StopReservation pending = f.state.markReplacementPending(f.stop(), AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            assertThat(admitted.successor().scope().executionGeneration()).isEqualTo(2);

            assertThat(f.observations.deleteIfUnchanged(scanned)).isFalse();
            assertThat(f.observations.readStored(PIPE).orElseThrow().scope()).contains(SOURCE);
            assertThat(f.observations.read(PIPE).orElseThrow().facts()).containsExactly(fact(7));
        }
    }

    @Test
    void anAdmittedContinuationKeepsItsPendingPrivateFloorWhenEveryScannedScopeIsOld() {
        try (Fixture f = new Fixture()) {
            assertThat(f.observations.saveScoped(observation(7), SOURCE)).isTrue();
            StopReservation stop = f.stop();
            var floor = new ObservationContinuation(stop.token(), SOURCE, Optional.empty(), Optional.empty(),
                    List.of(fact(7)), List.of());
            f.observations.saveContinuation(PIPE, stop, Optional.empty(), floor).orElseThrow();
            StopReservation pending = f.state.markReplacementPending(stop, AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            var scanned = f.observations.scanManifestsAfter(Optional.empty(), 1).getFirst();
            assertThat(scanned.scopes()).allSatisfy(scope ->
                    assertThat(scope.executionGeneration()).isLessThan(admitted.successor().scope().executionGeneration()));

            assertThat(f.observations.deleteManifestIfUnchanged(scanned, PIPE)).isFalse();
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(floor);
            assertThat(f.observations.read(PIPE).orElseThrow().facts()).containsExactly(fact(7));
        }
    }

    @Test
    void aNewContinuationTokenStillProtectsThePreviousPrivateTargetsLogicalTotal() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(), AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            var job = new StopReservation.JobIdentity(CLUSTER, 77, "submit-b");
            StopReservation bound = f.state.bindSuccessor(admitted, admitted.successor().scope(), job, AT).orElseThrow();
            var target = new ObservationContinuation.Target(bound.successor().scope(), Optional.of(job));
            var nativeState = new ObservationContinuation.ProducerState("tapstate.pipeline.records", MetricType.COUNTER,
                    "{record}", "out", "", AT, List.of(), fact(9).points());
            var oldCarrier = new ObservationContinuation(bound.token(), SOURCE, Optional.of(target), Optional.empty(),
                    List.of(fact(7)), List.of(nativeState));
            var receipt = f.observations.saveContinuation(PIPE, bound, Optional.empty(), oldCarrier).orElseThrow();
            var publication = f.observations.saveScoped(observation(9), target.scope(),
                    ObservationStore.ContinuationWrite.store(oldCarrier, Optional.of(receipt)));
            assertThat(publication.committed()).isTrue();
            assertThat(f.state.completeHandoff(bound, bound.handoffIdentity(), AT)).isPresent();
            CheckpointDoc running = f.state.read(PIPE).orElseThrow();
            assertThat(f.state.compareAndSwap(PIPE, running.epoch(), StateJson.of(PipelineState.PAUSED), AT))
                    .isInstanceOf(CasOutcome.Applied.class);
            StopReservation next = f.stop(target.scope(), job, 2);
            assertThat(next.token()).isNotEqualTo(oldCarrier.token());
            var scanned = f.observations.scanManifestsAfter(Optional.empty(), 1).getFirst();

            assertThat(f.observations.deleteManifestIfUnchanged(scanned, PIPE)).isFalse();
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(oldCarrier);
            assertThat(f.observations.read(PIPE).orElseThrow().facts()).containsExactly(fact(9));
        }
    }

    @Test
    void aReservationRacingTheDeleteConflictsWithItsCheckpointGuard() throws Exception {
        DeleteGate gate = new DeleteGate();
        try (Fixture f = new Fixture(gate)) {
            assertThat(f.legacy.saveScoped(observation(7), SOURCE)).isTrue();
            var scanned = f.observations.scanLatestAfter(Optional.empty(), 1).getFirst();
            var executor = Executors.newSingleThreadExecutor();
            try {
                gate.armed.set(true);
                var result = executor.submit(() -> f.observations.deleteIfUnchanged(scanned));
                assertThat(gate.entered.await(5, TimeUnit.SECONDS)).isTrue();
                StopReservation stop = f.stop();
                gate.release.countDown();
                assertThat(result.get(10, TimeUnit.SECONDS)).isFalse();
                assertThat(f.state.readStopReservation(PIPE)).contains(stop);
                assertThat(f.observations.read(PIPE).orElseThrow().facts()).containsExactly(fact(7));
            } finally {
                gate.release.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void missingCheckpointRequiresExplicitArtifactQualifiedOrphanCleanup() {
        try (Fixture f = new Fixture()) {
            assertThat(f.legacy.saveScoped(observation(7), SOURCE)).isTrue();
            var scanned = f.observations.scanLatestAfter(Optional.empty(), 1).getFirst();
            f.database.getCollection("states").deleteOne(new Document("_id", PIPE));
            f.database.getCollection("artifacts").deleteOne(new Document("_id", PIPE));

            assertThat(f.observations.deleteIfUnchanged(scanned)).isFalse();
            assertThat(f.observations.read(PIPE).orElseThrow().facts()).containsExactly(fact(7));
            assertThat(f.observations.deleteOrphanIfUnchanged(scanned)).isTrue();
            assertThat(f.observations.read(PIPE)).isEmpty();
        }
    }

    private static Observation observation(long total) {
        return new Observation(PIPE, PipelineState.PAUSED, Map.of("records.out", total), Map.of(), Map.of(),
                null, AT, List.of(fact(total)));
    }

    private static MetricFact fact(long total) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPE,
                        MetricAttributes.TABLE_ID, "table-a", MetricAttributes.DIRECTION, "out"), START, AT, total));
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient client;
        final MongoDatabase database;
        final MongoWorkloadClaimStore claims;
        final MongoDesiredStore desired;
        final MongoStateStore state;
        final MongoObservationStore legacy;
        final MongoObservationStore observations;

        Fixture() { this(new CommandListener() { }); }
        Fixture(CommandListener listener) {
            client = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build());
            database = client.getDatabase("continuation_gc_" + UUID.randomUUID().toString().replace("-", ""));
            claims = new MongoWorkloadClaimStore(database.getCollection("workload_claims"));
            desired = new MongoDesiredStore(database.getCollection("desired"));
            state = new MongoStateStore(client, database.getCollection("states"), database.getCollection("desired"),
                    database.getCollection("workload_claims"), database.getCollection("artifacts"));
            legacy = new MongoObservationStore(database.getCollection("observations"));
            observations = new MongoObservationStore(client, database.getCollection("observations"),
                    database.getCollection("chunks"), state.handoffWrites());
            database.getCollection("artifacts").insertOne(new Document("_id", PIPE).append("kind", "pipeline")
                    .append("pipelineIncarnationId", INC));
            state.create(PIPE, StateJson.of(PipelineState.PAUSED), AT);
            claims.advanceStandalone(CLUSTER, PIPE);
        }

        StopReservation stop() {
            return stop(SOURCE, new StopReservation.JobIdentity(CLUSTER, 11, "boot-a"), 1);
        }

        StopReservation stop(ObservationStore.Scope source, StopReservation.JobIdentity job, long generation) {
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-a", false, "assembly-a", false, null);
            desired.save(intent);
            CheckpointDoc actual = state.read(PIPE).orElseThrow();
            StopReservation marker = StopReservation.stopping(PIPE, UUID.randomUUID().toString(), actual.epoch(), intent,
                    new StopReservation.Source(CLUSTER, source, job),
                    StopReservation.CounterPolicy.CONTINUE, StopAuthority.standalone(CLUSTER, generation));
            return state.reserveStop(actual, marker, AT).orElseThrow();
        }

        @Override public void close() { database.drop(); client.close(); }
    }

    private static final class DeleteGate implements CommandListener {
        final AtomicBoolean armed = new AtomicBoolean();
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);

        @Override public void commandStarted(CommandStartedEvent event) {
            if (!event.getCommandName().equals("update")) { return; }
            boolean guard = event.getCommand().getArray("updates").stream().map(value -> value.asDocument().get("u"))
                    .filter(value -> value != null && value.isDocument()).map(value -> value.asDocument())
                    .anyMatch(update -> update.containsKey("$inc")
                            && update.getDocument("$inc").containsKey("stopGuardVersion"));
            if (!guard || !armed.compareAndSet(true, false)) { return; }
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("cold delete gate timed out"); }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("cold delete gate was interrupted", interrupted);
            }
        }
    }
}
