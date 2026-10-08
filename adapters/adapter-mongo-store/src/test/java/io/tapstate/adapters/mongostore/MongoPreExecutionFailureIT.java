package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PreExecutionFailure;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Instant;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real transactions prove cold ownership and current-role replacement without a synthetic execution. */
@RequiresDocker
class MongoPreExecutionFailureIT {
    private static final String PIPE = "orders", SOURCE = "src_x", CLUSTER = "cluster-a";
    private static final Instant AT = Instant.parse("2026-10-08T03:00:00Z");
    private static final Resource PIPELINE = new DslParser().parse("""
            version: tapstate/v1
            kind: pipeline
            id: orders
            source: src_x
            view:
              from: src_x
              primary_key: id
            """);
    private static final Resource SOURCE_RESOURCE = source(3306);
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void originalRefusalUsesOneLatestAndNeverAdvancesOrCreatesAClaim(boolean historical) {
        try (Fixture f = new Fixture(historical)) {
            long claimsBefore = f.db.getCollection("claims").countDocuments();
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt)).isTrue();
            assertThat(f.observations.readStored(PIPE).orElseThrow().refusal()).contains(f.receipt.owner());
            assertThat(f.observations.readStored(PIPE).orElseThrow().scope()).isEmpty();
            assertThat(f.observations.isCurrentPreExecutionFailure(f.receipt.owner())).isTrue();
            assertThat(f.db.getCollection("observations").countDocuments()).isEqualTo(1);
            assertThat(f.db.getCollection("claims").countDocuments()).isEqualTo(claimsBefore);
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).isEqualTo(f.receipt.owner().generationFrontier());
            assertThat(f.observations.refreshPreExecutionFailure(PIPE, AT.plusSeconds(1))).isTrue();
            assertThat(f.observations.read(PIPE).orElseThrow().observedAt()).isEqualTo(AT.plusSeconds(1));
            assertThat(f.observations.read(PIPE).orElseThrow().failure()).isEqualTo(f.failure(AT, "src_x").failure());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aRealAdmissionInvalidatesBeforeFirstCurrentAndOnlyANewerExecutionCanReplace(boolean historical) {
        try (Fixture f = new Fixture(historical)) {
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt)).isTrue();
            if (historical) {
                assertThat(f.observations.saveScoped(f.running(AT.plusSeconds(20)), new ObservationStore.Scope(f.incarnation, 1))).isFalse();
            }
            long execution = f.claims.advanceStandalone(CLUSTER, PIPE).orElseThrow();
            assertThat(f.observations.isCurrentPreExecutionFailure(f.receipt.owner())).isFalse();
            assertThat(f.observations.refreshPreExecutionFailure(PIPE, AT.plusSeconds(21))).isFalse();
            var next = new ObservationStore.Scope(f.incarnation, execution);
            assertThat(f.observations.saveScoped(f.running(AT.plusSeconds(22)), next)).isTrue();
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(23), "src_x"), f.receipt)).isFalse();
            assertThat(f.observations.readStored(PIPE).orElseThrow().scope()).contains(next);
        }
    }

    @Test
    void aLargeCodedCauseUsesTheExistingCompleteChunkProtocol() {
        try (Fixture f = new Fixture(false)) {
            Observation large = f.failure(AT, "x".repeat(13 * 1024 * 1024));
            assertThat(f.observations.savePreExecutionFailure(large, f.receipt)).isTrue();
            assertThat(f.observations.read(PIPE)).contains(large);
            assertThat(f.db.getCollection("chunks").countDocuments()).isGreaterThan(1);
            for (String collection : List.of("observations", "chunks")) {
                f.db.getCollection(collection).find().forEach(document ->
                        assertThat(ObservationBsonBounds.bsonBytes(document)).isLessThanOrEqualTo(ObservationBsonBounds.ENVELOPE_BYTES));
            }
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).isEmpty();
        }
    }

    @Test
    void sourceRevisionCheckpointAndIncarnationChangesMakeTheOriginalOwnerUnreadable() {
        try (Fixture f = new Fixture(false)) {
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt)).isTrue();
            f.artifacts.replace(SOURCE, CanonicalHash.of(SOURCE_RESOURCE), source(3307));
            assertThat(f.observations.isCurrentPreExecutionFailure(f.receipt.owner())).isFalse();
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(1), "src_x"), f.receipt)).isFalse();
        }
        try (Fixture f = new Fixture(false)) {
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt)).isTrue();
            f.state.compareAndSwap(PIPE, f.receipt.checkpoint().epoch(), StateJson.of(PipelineState.FAILED), AT.plusSeconds(1));
            assertThat(f.observations.isCurrentPreExecutionFailure(f.receipt.owner())).isFalse();
        }
        try (Fixture f = new Fixture(false)) {
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt)).isTrue();
            f.artifacts.delete(PIPE, CanonicalHash.of(PIPELINE)); f.artifacts.create(PIPELINE);
            assertThat(f.observations.isCurrentPreExecutionFailure(f.receipt.owner())).isFalse();
        }
    }

    @Test
    void malformedOwnerCannotFallBackToALegacyValue() {
        try (Fixture f = new Fixture(false)) {
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt)).isTrue();
            f.db.getCollection("observations").insertOne(MongoObservationStore.toDocument(f.running(AT)));
            f.db.getCollection("observations").updateOne(new Document("_id", MongoLatestObservationStorage.manifestKey(PIPE)),
                    new Document("$set", new Document("current.ownerKind", "UNKNOWN")));
            assertThatThrownBy(() -> f.observations.readStored(PIPE)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @ParameterizedTest
    @MethodSource("malformedOwnerFields")
    void malformedPrivateOwnerFieldsAreCodedAndNeverReadTheLegacyFallback(String field, Object value) {
        try (Fixture f = new Fixture(false)) {
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, SOURCE), f.receipt)).isTrue();
            f.db.getCollection("observations").insertOne(MongoObservationStore.toDocument(f.running(AT)));
            f.db.getCollection("observations").updateOne(new Document("_id", MongoLatestObservationStorage.manifestKey(PIPE)),
                    new Document("$set", new Document("current." + field, value)));
            assertThatThrownBy(() -> f.observations.readStored(PIPE)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    private static java.util.stream.Stream<Arguments> malformedOwnerFields() {
        return java.util.stream.Stream.of(
                Arguments.of("diagnosticOwner.version", 2),
                Arguments.of("diagnosticOwner.checkpointEpoch", 1.5),
                Arguments.of("diagnosticOwner.checkpointDigest", "not-a-digest"),
                Arguments.of("diagnosticOwner.pipelineId", "another-pipeline"),
                Arguments.of("diagnosticOwner.pipelineIncarnationId", "another-incarnation"),
                Arguments.of("diagnosticOwner.generationFrontier", new Document("presence", "ABSENT").append("value", 0L)),
                Arguments.of("diagnosticOwner.generationFrontier", new Document("presence", "PRESENT").append("value", 0L)),
                Arguments.of("executionGeneration", 1L));
    }

    @Test
    void fractionalOrWrongTypedDesiredCannotMatchAnOriginalDigest() {
        try (Fixture f = new Fixture(false)) {
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt)).isTrue();
            f.db.getCollection("desired").updateOne(new Document("_id", PIPE),
                    new Document("$set", new Document("rebuiltAtStateEpoch", 1.5)));
            assertThatThrownBy(() -> f.observations.isCurrentPreExecutionFailure(f.receipt.owner()))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            f.db.getCollection("desired").updateOne(new Document("_id", PIPE),
                    new Document("$unset", new Document("rebuiltAtStateEpoch", true)).append("$set", new Document("purgeState", "false")));
            assertThatThrownBy(() -> f.observations.isCurrentPreExecutionFailure(f.receipt.owner()))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @Test
    void aRetiredArtifactCannotFailARecreatedCheckpointAtTheSameEpoch() {
        try (Fixture f = new Fixture(false)) {
            var original = new PreExecutionFailure.Attempt(PIPE, CLUSTER, f.incarnation,
                    new CheckpointDoc(PIPE, StateJson.of(PipelineState.NEW), 0, AT), f.receipt.desired(),
                    f.receipt.artifactHashes(), OptionalLong.empty(), null);
            f.artifacts.delete(PIPE, CanonicalHash.of(PIPELINE)); f.state.delete(PIPE);
            f.artifacts.create(PIPELINE); f.state.create(PIPE, StateJson.of(PipelineState.NEW), AT);
            assertThat(f.state.failPreExecution(original, AT.plusSeconds(1))).isEmpty();
            assertThat(f.state.read(PIPE).orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.NEW));
            assertThat(f.state.read(PIPE).orElseThrow().epoch()).isZero();
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).isEmpty();
        }
    }

    @Test
    void aQualifiedRefusalTransitionNeedsNoClaimRowOrExecution() {
        try (Fixture f = new Fixture(false)) {
            f.state.delete(PIPE); f.state.create(PIPE, StateJson.of(PipelineState.NEW), AT);
            var original = new PreExecutionFailure.Attempt(PIPE, CLUSTER, f.incarnation, f.state.read(PIPE).orElseThrow(),
                    f.receipt.desired(), f.receipt.artifactHashes(), OptionalLong.empty(), null);
            var failed = f.state.failPreExecution(original, AT.plusSeconds(1)).orElseThrow();
            assertThat(StateJson.parse(failed.stateJson())).isEqualTo(PipelineState.FAILED);
            assertThat(failed.epoch()).isEqualTo(1);
            assertThat(f.db.getCollection("claims").countDocuments()).isZero();
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(2), "src_x"), original.failed(failed))).isTrue();
        }
    }

    @Test
    void aRealUnadmittedClaimIsQualifiedAndOwnerLossCannotRefreshItsFailure() {
        try (Fixture f = new Fixture(false)) {
            f.state.delete(PIPE); f.state.create(PIPE, StateJson.of(PipelineState.NEW), AT);
            var key = new io.tapstate.spi.store.WorkloadClaimKey(CLUSTER, io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION, PIPE);
            var acquired = f.claims.acquire(key, new io.tapstate.spi.store.WorkloadOwner("node-a", "boot-a"), 3L,
                    java.time.Duration.ofMinutes(1));
            assertThat(acquired.acquired()).isTrue();
            assertThat(acquired.claim().executionGeneration()).isZero();
            var attempt = new PreExecutionFailure.Attempt(PIPE, CLUSTER, f.incarnation, f.state.read(PIPE).orElseThrow(),
                    f.receipt.desired(), f.receipt.artifactHashes(), OptionalLong.empty(),
                    io.tapstate.spi.store.WorkloadClaimFence.from(acquired.claim()));
            var failed = f.state.failPreExecution(attempt, AT.plusSeconds(1)).orElseThrow();
            var receipt = attempt.failed(failed);
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(2), "src_x"), receipt)).isTrue();
            assertThat(f.observations.isCurrentPreExecutionFailure(receipt.owner())).isTrue();
            f.claims.release(acquired.claim());
            var successor = f.claims.acquire(key, new io.tapstate.spi.store.WorkloadOwner("node-b", "boot-b"), 3L,
                    java.time.Duration.ofMinutes(1));
            assertThat(successor.acquired()).isTrue();
            assertThat(f.observations.isCurrentPreExecutionFailure(receipt.owner())).isFalse();
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(3), "src_x"), receipt)).isFalse();
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"desired", "states", "artifacts"})
    void exactGuardsConflictWithRealConcurrentMutation(String collection) throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean();
        CommandListener listener = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (!event.getCommandName().equals("update") || !event.getCommand().getString("update").getValue().equals(collection)) { return; }
                var write = event.getCommand().getArray("updates").get(0).asDocument();
                var query = write.getDocument("q");
                if (collection.equals("artifacts") && !query.getString("_id").getValue().equals(SOURCE)) { return; }
                if (!armed.compareAndSet(true, false)) { return; }
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) { throw new AssertionError("guard was not released"); }
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
        };
        try (Fixture f = new Fixture(false, listener); var worker = Executors.newSingleThreadExecutor()) {
            armed.set(true);
            var result = worker.submit(() -> f.observations.savePreExecutionFailure(f.failure(AT, "src_x"), f.receipt));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                switch (collection) {
                    case "desired" -> f.desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "new-revision", true, "new-assembly", true, 1L));
                    case "states" -> f.state.compareAndSwap(PIPE, f.receipt.checkpoint().epoch(), StateJson.of(PipelineState.STOPPED), AT.plusSeconds(1));
                    case "artifacts" -> f.artifacts.replace(SOURCE, CanonicalHash.of(SOURCE_RESOURCE), source(3307));
                    default -> throw new AssertionError(collection);
                }
            } finally { release.countDown(); }
            assertThat(result.get(15, TimeUnit.SECONDS)).isFalse();
            assertThat(f.observations.read(PIPE)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aLiveContinueMustCoverTheLatestPublicTotalBeforeAColdDiagnosticCanReplaceIt(boolean legacy) {
        try (Fixture f = new Fixture(true)) {
            var scope = new ObservationStore.Scope(f.incarnation, 1);
            f.saveSource(sourceObservation(7, AT), scope, legacy);
            StopReservation marker = f.seedLiveContinue(scope);
            var first = f.observations.saveContinuation(PIPE, marker, Optional.empty(), floor(marker, 7)).orElseThrow();
            f.saveSource(sourceObservation(8, AT.plusSeconds(1)), scope, legacy);

            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(2), SOURCE), f.receipt)).isFalse();
            assertThat(f.observations.preExecutionFailureInputsCurrent(f.receipt.owner())).isTrue();
            assertThat(f.observations.read(PIPE)).contains(sourceObservation(8, AT.plusSeconds(1)));
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(first);
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().continuation().baselineFacts())
                    .containsExactly(records(7, AT));

            var updated = f.observations.saveContinuation(PIPE, marker, Optional.of(first), floor(marker, 8)).orElseThrow();
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(3), SOURCE), f.receipt)).isTrue();
            MongoObservationStore cold = f.newStore(f.db.getCollection("chunks"));
            assertThat(cold.read(PIPE)).contains(f.failure(AT.plusSeconds(3), SOURCE));
            assertThat(cold.isCurrentPreExecutionFailure(f.receipt.owner())).isTrue();
            assertThat(cold.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(updated);
            assertThat(cold.readContinuation(PIPE).orElseThrow().continuation().baselineFacts())
                    .containsExactly(records(8, AT));
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).isEqualTo(OptionalLong.of(1));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aConcurrentSourceChangeConflictsWithTheActualDiagnosticCommit(boolean legacy) throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean();
        CommandListener listener = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (!event.getCommandName().equals("update")
                        || !event.getCommand().getString("update").getValue().equals("observations")) { return; }
                var query = event.getCommand().getArray("updates").getFirst().asDocument().getDocument("q");
                var id = query.get("_id");
                if (id == null || legacy != id.isString() || id.isString() && !id.asString().getValue().equals(PIPE)
                        || !armed.compareAndSet(true, false)) { return; }
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) { throw new AssertionError("source guard was not released"); }
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
        };
        try (Fixture f = new Fixture(true, listener); var worker = Executors.newSingleThreadExecutor()) {
            var scope = new ObservationStore.Scope(f.incarnation, 1);
            f.saveSource(sourceObservation(7, AT), scope, legacy);
            StopReservation marker = f.seedLiveContinue(scope);
            var first = f.observations.saveContinuation(PIPE, marker, Optional.empty(), floor(marker, 7)).orElseThrow();
            armed.set(true);
            var result = worker.submit(() -> f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(2), SOURCE), f.receipt));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                f.saveSource(sourceObservation(8, AT.plusSeconds(1)), scope, legacy);
            } finally { release.countDown(); }
            assertThat(result.get(15, TimeUnit.SECONDS)).isFalse();
            assertThat(f.observations.read(PIPE)).contains(sourceObservation(8, AT.plusSeconds(1)));
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(first);
            assertThat(f.observations.readStored(PIPE).orElseThrow().refusal()).isEmpty();
        }
    }

    @Test
    void aSourcePayloadOutageKeepsItsKnownFloorAndTheOriginalCauseRetryable() {
        try (Fixture f = new Fixture(true)) {
            var scope = new ObservationStore.Scope(f.incarnation, 1);
            Observation source = new Observation(PIPE, PipelineState.PAUSED, Map.of("records.out", 8L), Map.of(),
                    Map.of("orders", "x".repeat(600 * 1024)), null, AT, List.of(records(8, AT)));
            assertThat(f.observations.saveScoped(source, scope)).isTrue();
            StopReservation marker = f.seedLiveContinue(scope);
            var first = f.observations.saveContinuation(PIPE, marker, Optional.empty(), floor(marker, 7)).orElseThrow();
            AtomicBoolean unavailable = new AtomicBoolean(true);
            MongoObservationStore outage = f.newStore(failOneFind(f.db.getCollection("chunks"), unavailable));
            assertThatThrownBy(() -> outage.savePreExecutionFailure(f.failure(AT.plusSeconds(1), SOURCE), f.receipt))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(unavailable).isFalse();
            assertThat(f.observations.read(PIPE)).contains(source);
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(first);
            assertThat(outage.preExecutionFailureInputsCurrent(f.receipt.owner())).isTrue();
            assertThat(outage.savePreExecutionFailure(f.failure(AT.plusSeconds(2), SOURCE), f.receipt)).isFalse();
            f.observations.saveContinuation(PIPE, marker, Optional.of(first), floor(marker, 8)).orElseThrow();
            assertThat(outage.savePreExecutionFailure(f.failure(AT.plusSeconds(3), SOURCE), f.receipt)).isTrue();
            assertThat(f.newStore(f.db.getCollection("chunks")).read(PIPE)).contains(f.failure(AT.plusSeconds(3), SOURCE));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aReadableUnknownSourceStaysUnknownAcrossTheDiagnosticAndColdRefresh(boolean legacy) {
        try (Fixture f = new Fixture(true)) {
            var scope = new ObservationStore.Scope(f.incarnation, 1);
            f.saveSource(f.running(AT), scope, legacy);
            StopReservation marker = f.seedLiveContinue(scope);
            var unknown = new ObservationContinuation(marker.token(), scope, Optional.empty(), Optional.empty(), List.of(), List.of());
            var retained = f.observations.saveContinuation(PIPE, marker, Optional.empty(), unknown).orElseThrow();
            assertThat(retained.knownBaseline()).isFalse();
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(1), SOURCE), f.receipt)).isTrue();
            MongoObservationStore cold = f.newStore(f.db.getCollection("chunks"));
            assertThat(cold.refreshPreExecutionFailure(PIPE, AT.plusSeconds(2))).isTrue();
            assertThat(cold.read(PIPE)).contains(f.failure(AT.plusSeconds(2), SOURCE));
            assertThat(cold.readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(unknown);
            assertThat(cold.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(retained);
            assertThat(cold.read(PIPE).orElseThrow().metrics()).isEmpty();
            assertThat(cold.read(PIPE).orElseThrow().facts()).isEmpty();
        }
    }

    @Test
    void onlyTheOriginalQualifiedResetCanClearTheCarrierAfterDiagnosticPublication() {
        try (Fixture f = new Fixture(true)) {
            var scope = new ObservationStore.Scope(f.incarnation, 1);
            f.saveSource(sourceObservation(7, AT), scope, false);
            StopReservation marker = f.seedLiveContinue(scope);
            var first = f.observations.saveContinuation(PIPE, marker, Optional.empty(), floor(marker, 7)).orElseThrow();
            StopReservation reset = withPolicy(marker, StopReservation.CounterPolicy.RESET, marker.writerAuthority());
            f.db.getCollection("states").updateOne(new Document("_id", PIPE),
                    new Document("$set", new Document(StopReservationDocument.FIELD, StopReservationDocument.write(reset))));
            assertThat(f.observations.savePreExecutionFailure(f.failure(AT.plusSeconds(1), SOURCE), f.receipt)).isTrue();
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(first);
            assertThat(f.observations.clearContinuation(PIPE, withPolicy(reset, reset.counterPolicy(),
                    StopAuthority.standalone(CLUSTER, 2)), first)).isFalse();
            assertThat(f.observations.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(first);
            assertThat(f.observations.clearContinuation(PIPE, reset, first)).isTrue();
            MongoObservationStore cold = f.newStore(f.db.getCollection("chunks"));
            assertThat(cold.refreshPreExecutionFailure(PIPE, AT.plusSeconds(2))).isTrue();
            assertThat(cold.readContinuation(PIPE)).isEmpty();
            assertThat(cold.read(PIPE)).contains(f.failure(AT.plusSeconds(2), SOURCE));
        }
    }

    private static StopReservation withPolicy(StopReservation marker, StopReservation.CounterPolicy policy, StopAuthority authority) {
        return new StopReservation(marker.pipelineId(), marker.token(), marker.sourceEpoch(), marker.reservedEpoch(),
                marker.originalDesired(), marker.source(), marker.phase(), policy, authority, marker.successor(), marker.formatVersion());
    }

    private static ObservationContinuation floor(StopReservation marker, long value) {
        return new ObservationContinuation(marker.token(), marker.source().scope(), Optional.empty(), Optional.empty(),
                List.of(records(value, AT)), List.of());
    }

    private static Observation sourceObservation(long value, Instant at) {
        return new Observation(PIPE, PipelineState.PAUSED, Map.of("records.out", value), Map.of(), Map.of(), null, at, List.of(records(value, at)));
    }

    private static MetricFact records(long value, Instant at) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}", MetricPoint.accumulated(
                Map.of(MetricAttributes.PIPELINE_ID, PIPE, MetricAttributes.TABLE_ID, "orders", MetricAttributes.DIRECTION, "out"),
                AT.minusSeconds(60), at, value));
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> failOneFind(MongoCollection<Document> target, AtomicBoolean armed) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("find") && armed.compareAndSet(true, false)) {
                        throw new MongoException("source payload is temporarily unavailable");
                    }
                    try {
                        Object result = method.invoke(target, arguments);
                        return result instanceof MongoCollection<?> collection
                                ? failOneFind((MongoCollection<Document>) collection, armed) : result;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }

    private static Resource source(int port) {
        return new DslParser().parse("version: tapstate/v1\nkind: source\nid: src_x\nconnector: mysql\nconfig:\n  host: localhost\n  port: "
                + port + "\ntables: [orders]\n");
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient client;
        final MongoDatabase db;
        final MongoArtifactStore artifacts;
        final MongoDesiredStore desired;
        final MongoStateStore state;
        final MongoWorkloadClaimStore claims;
        final MongoObservationStore observations;
        final String incarnation;
        final PreExecutionFailure.Receipt receipt;
        Fixture(boolean historical) { this(historical, new CommandListener() { }); }
        Fixture(boolean historical, CommandListener listener) {
            client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build());
            db = client.getDatabase("refusal_" + UUID.randomUUID().toString().replace("-", ""));
            for (String name : List.of("artifacts", "desired", "states", "claims", "observations", "chunks")) { db.createCollection(name); }
            artifacts = new MongoArtifactStore(client, db.getCollection("artifacts"));
            desired = new MongoDesiredStore(db.getCollection("desired"));
            claims = new MongoWorkloadClaimStore(db.getCollection("claims"));
            state = new MongoStateStore(client, db.getCollection("states"), db.getCollection("desired"), db.getCollection("claims"), db.getCollection("artifacts"));
            observations = new MongoObservationStore(client, db.getCollection("observations"), db.getCollection("chunks"), state.handoffWrites());
            artifacts.create(PIPELINE); artifacts.create(SOURCE_RESOURCE);
            incarnation = artifacts.pipelineIncarnationId(PIPE).orElseThrow();
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, CanonicalHash.of(PIPELINE)); desired.save(intent);
            state.create(PIPE, StateJson.of(historical ? PipelineState.STOPPED : PipelineState.NEW), AT.minusSeconds(2));
            if (historical) { claims.advanceStandalone(CLUSTER, PIPE); }
            CheckpointDoc before = state.read(PIPE).orElseThrow();
            var attempt = new PreExecutionFailure.Attempt(PIPE, CLUSTER, incarnation, before, intent,
                    Map.of(PIPE, CanonicalHash.of(PIPELINE), SOURCE, CanonicalHash.of(SOURCE_RESOURCE)),
                    historical ? OptionalLong.of(1) : OptionalLong.empty(), null);
            state.compareAndSwap(PIPE, before.epoch(), StateJson.of(PipelineState.FAILED), AT.minusSeconds(1));
            receipt = attempt.failed(state.read(PIPE).orElseThrow());
        }
        Observation failure(Instant at, String source) {
            return new Observation(PIPE, PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new ObservationFailure("actuation.source-schema-not-discovered", Map.of("source", source)), at, List.of());
        }
        Observation running(Instant at) { return new Observation(PIPE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, at, List.of()); }
        MongoObservationStore newStore(MongoCollection<Document> chunks) {
            return new MongoObservationStore(client, db.getCollection("observations"), chunks, state.handoffWrites());
        }
        void saveSource(Observation observation, ObservationStore.Scope scope, boolean legacy) {
            if (!legacy) { assertThat(observations.saveScoped(observation, scope)).isTrue(); return; }
            Document document = MongoObservationStore.toDocument(observation)
                    .append("pipelineIncarnationId", scope.pipelineIncarnationId()).append("executionGeneration", scope.executionGeneration());
            db.getCollection("observations").replaceOne(new Document("_id", PIPE), document,
                    new com.mongodb.client.model.ReplaceOptions().upsert(true));
        }
        StopReservation seedLiveContinue(ObservationStore.Scope scope) {
            // Seed the conditional durable shape; this is not a native replacement or a revival of a retired marker.
            StopReservation marker = new StopReservation(PIPE, UUID.randomUUID().toString(), 0, receipt.checkpoint().epoch(),
                    receipt.desired(), new StopReservation.Source(CLUSTER, scope, null), StopReservation.Phase.REPLACEMENT_PENDING,
                    StopReservation.CounterPolicy.CONTINUE, StopAuthority.standalone(CLUSTER, scope.executionGeneration()), null,
                    StopReservation.CURRENT_FORMAT);
            db.getCollection("states").updateOne(new Document("_id", PIPE),
                    new Document("$set", new Document(StopReservationDocument.FIELD, StopReservationDocument.write(marker))));
            return marker;
        }
        @Override public void close() { db.drop(); client.close(); }
    }
}
