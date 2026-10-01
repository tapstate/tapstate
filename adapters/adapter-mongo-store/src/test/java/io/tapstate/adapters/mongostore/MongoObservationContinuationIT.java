package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorEnd;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Public state and the private cumulative checkpoint share one conditional latest version. */
@RequiresDocker
class MongoObservationContinuationIT {
    private static final String PIPE = "orders", CLUSTER = "cluster-a", INC = "inc-a";
    private static final Instant START = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant NATIVE_START = START.plusSeconds(20).plusNanos(123_456_789);
    private static final Instant AT = START.plusSeconds(30);
    private static final ObservationStore.Scope SOURCE = new ObservationStore.Scope(INC, 1);
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void aPendingFloorBindsOnceAndSurvivesCompletionColdReadAndNormalKeep() {
        try (Fixture f = new Fixture()) {
            assertThat(f.store.saveScoped(observation(7, AT, "table-a"), SOURCE)).isTrue();
            StopReservation stop = f.stop();
            var floor = continuation(stop, Optional.empty(), Optional.empty(), 7, "table-a", List.of());
            var first = f.store.saveContinuation(PIPE, stop, Optional.empty(), floor).orElseThrow();
            assertThat(first.target()).isEmpty();
            assertThat(f.store.read(PIPE).orElseThrow().facts()).containsExactly(fact(7, AT, "table-a"));

            StopReservation bound = f.bound(f.state.markReplacementPending(stop, AT).orElseThrow());
            var target = target(bound);
            var admitted = continuation(bound, Optional.of(target), Optional.empty(), 7, "table-a", List.of());
            var boundReceipt = f.store.saveContinuation(PIPE, bound, Optional.of(first), admitted).orElseThrow();
            var packet = continuation(bound, Optional.of(target), Optional.empty(), 7, "table-a",
                    List.of(producer(9, NATIVE_START, List.of(), "table-a")));
            var published = f.store.saveScoped(observation(9, AT.plusSeconds(1), "table-a"), target.scope(),
                    ObservationStore.ContinuationWrite.store(packet, Optional.of(boundReceipt)));
            assertThat(published.committed()).isTrue();
            assertThat(published.continuationReceipt().orElseThrow().matches(bound.handoffIdentity())).isTrue();
            assertThat(f.state.completeHandoff(bound, bound.handoffIdentity(), AT)).isPresent();

            MongoObservationStore cold = f.newStore(f.chunks);
            assertThat(cold.readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(packet);
            assertThat(cold.saveScoped(observation(9, AT.plusSeconds(2), "table-a"), target.scope(),
                    ObservationStore.ContinuationWrite.keep()).committed()).isTrue();
            assertThat(cold.readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(packet);

            var nextEpoch = continuation(bound, Optional.of(target), Optional.empty(), 7, "table-a",
                    List.of(producer(12, NATIVE_START.plusNanos(1), List.of(point(9, AT, "table-a")), "table-a")));
            var prior = cold.readContinuation(PIPE).orElseThrow().receipt();
            f.commands.clear();
            assertThat(cold.saveScoped(observation(12, AT.plusSeconds(3), "table-a"), target.scope(),
                    ObservationStore.ContinuationWrite.store(nextEpoch, Optional.of(prior))).committed()).isTrue();
            assertThat(f.commands).containsExactly("findAndModify");
            assertThat(cold.readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(nextEpoch);
            assertThat(cold.read(PIPE).orElseThrow().facts()).containsExactly(fact(12, AT.plusSeconds(3), "table-a"));
        }
    }

    @Test
    void anUnknownPacketCannotEraseKnownPublicFactsOrItsActualCarrier() {
        try (Fixture f = new Fixture()) {
            StopReservation bound = f.bound(f.state.markReplacementPending(f.stop(), AT).orElseThrow());
            var target = target(bound);
            var known = continuation(bound, Optional.of(target), Optional.empty(), 7, "table-a",
                    List.of(producer(9, NATIVE_START, List.of(), "table-a")));
            var receipt = f.store.saveContinuation(PIPE, bound, Optional.empty(), known).orElseThrow();
            assertThat(f.store.saveScoped(observation(9, AT, "table-a"), target.scope(),
                    ObservationStore.ContinuationWrite.store(known, Optional.of(receipt))).committed()).isTrue();
            var before = f.store.readContinuation(PIPE).orElseThrow();
            var unknown = new ObservationContinuation(bound.token(), SOURCE, Optional.of(target), Optional.empty(),
                    List.of(), List.of());

            Observation absent = new Observation(PIPE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null,
                    AT.plusSeconds(1));
            for (var expected : List.of(Optional.<ObservationStore.ContinuationReceipt>empty(), Optional.of(before.receipt()))) {
                var publication = f.store.saveScoped(absent, target.scope(),
                        ObservationStore.ContinuationWrite.store(unknown, expected));
                assertThat(publication.committed()).isFalse();
                assertThat(publication.continuationReceipt()).isEmpty();
                assertThat(f.store.read(PIPE).orElseThrow().facts()).containsExactly(fact(9, AT, "table-a"));
                assertThat(f.store.read(PIPE).orElseThrow().metrics()).containsEntry("records.out", 9L);
                assertThat(f.store.readContinuation(PIPE)).contains(before);
            }
            assertThat(f.store.saveContinuation(PIPE, bound, Optional.of(before.receipt()), unknown)).contains(before.receipt());
            assertThat(f.store.readContinuation(PIPE)).contains(before);
            MetricFact gauge = MetricFact.single("tapstate.pipeline.work.active", MetricType.GAUGE, "1",
                    MetricPoint.reading(Map.of(MetricAttributes.PIPELINE_ID, PIPE), AT.plusSeconds(2), 3));
            Observation callerPrepared = new Observation(PIPE, PipelineState.RUNNING,
                    Map.of("records.out", 9L, "work.active", 3L), Map.of(), Map.of(), null, AT.plusSeconds(2),
                    List.of(fact(9, AT.plusSeconds(2), "table-a"), gauge));
            var publication = f.store.saveScoped(callerPrepared, target.scope(),
                    ObservationStore.ContinuationWrite.store(known, Optional.of(before.receipt())));
            assertThat(publication.committed()).isTrue();
            assertThat(publication.continuationReceipt().orElseThrow().knownBaseline()).isTrue();
            assertThat(f.store.read(PIPE)).contains(callerPrepared);
        }
    }

    @Test
    void sourceReadFailureCannotPromoteUnknownOrEraseAQualifiedPublicFloor() {
        try (Fixture f = new Fixture()) {
            Observation source = new Observation(PIPE, PipelineState.PAUSED, Map.of("records.out", 7L), Map.of(),
                    Map.of("table-a", "x".repeat(600 * 1024)), null, AT, List.of(fact(7, AT, "table-a")));
            assertThat(f.store.saveScoped(source, SOURCE)).isTrue();
            StopReservation bound = f.bound(f.state.markReplacementPending(f.stop(), AT).orElseThrow());
            var target = target(bound);
            var unknown = new ObservationContinuation(bound.token(), SOURCE, Optional.of(target), Optional.empty(),
                    List.of(), List.of());
            AtomicBoolean unavailable = new AtomicBoolean(true);
            MongoObservationStore failing = f.newStore(failOneFind(f.chunks, unavailable));
            Observation current = new Observation(PIPE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null,
                    AT.plusSeconds(1));
            assertThatThrownBy(() -> failing.saveScoped(current, target.scope(),
                    ObservationStore.ContinuationWrite.store(unknown, Optional.empty())))
                    .isInstanceOfSatisfying(TapstateException.class, failure ->
                            assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(unavailable).isFalse();
            assertThat(f.store.readStored(PIPE)).contains(new ObservationStore.Stored(source, Optional.of(SOURCE)));
            assertThat(f.store.readContinuation(PIPE)).isEmpty();
            assertThat(f.store.saveScoped(current, target.scope(),
                    ObservationStore.ContinuationWrite.store(unknown, Optional.empty())).committed()).isFalse();
            assertThat(f.store.readStored(PIPE)).contains(new ObservationStore.Stored(source, Optional.of(SOURCE)));
        }
    }

    @Test
    void aReadableEmptySourceCanPersistAnExplicitUnknownBoundCarrier() {
        try (Fixture f = new Fixture()) {
            assertThat(f.store.saveScoped(new Observation(PIPE, PipelineState.PAUSED, Map.of(), Map.of(), Map.of(),
                    null, AT), SOURCE)).isTrue();
            StopReservation bound = f.bound(f.state.markReplacementPending(f.stop(), AT).orElseThrow());
            var target = target(bound);
            var unknown = new ObservationContinuation(bound.token(), SOURCE, Optional.of(target), Optional.empty(),
                    List.of(), List.of());
            var receipt = f.store.saveContinuation(PIPE, bound, Optional.empty(), unknown).orElseThrow();
            assertThat(receipt.knownBaseline()).isFalse();
            assertThat(receipt.matches(bound.handoffIdentity())).isTrue();
            assertThat(f.newStore(f.chunks).readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(unknown);
        }
    }

    @Test
    void previousBoundTargetBecomesOneNewFloorWhileTheOriginalAnchorRemains() {
        try (Fixture f = new Fixture()) {
            StopReservation bound = f.bound(f.state.markReplacementPending(f.stop(), AT).orElseThrow());
            var previousTarget = target(bound);
            var first = continuation(bound, Optional.of(previousTarget), Optional.empty(), 7, "table-a",
                    List.of(producer(9, NATIVE_START, List.of(), "table-a")));
            var firstReceipt = f.store.saveContinuation(PIPE, bound, Optional.empty(), first).orElseThrow();
            var pendingFloor = continuation(bound, Optional.empty(), Optional.of(previousTarget), 9, "table-a", List.of());
            var pendingReceipt = f.store.saveContinuation(PIPE, bound, Optional.of(firstReceipt), pendingFloor).orElseThrow();
            StopReservation pending = f.state.retireSuccessor(bound,
                    new SuccessorEnd.Terminal(bound.successor().scope(), bound.successor().job()), AT).orElseThrow();
            StopReservation next = f.bound(pending);
            var floor = continuation(next, Optional.of(target(next)), Optional.of(previousTarget), 9, "table-a", List.of());
            assertThat(f.store.saveContinuation(PIPE, next, Optional.of(pendingReceipt), floor)).isPresent();
            var saved = f.newStore(f.chunks).readContinuation(PIPE).orElseThrow().continuation();
            assertThat(saved.sourceScope()).isEqualTo(SOURCE);
            assertThat(saved.baselineOrigin()).contains(previousTarget);
            assertThat(saved.target().orElseThrow().scope().executionGeneration()).isEqualTo(3);
            assertThat(saved.baselineFacts()).containsExactly(fact(9, AT, "table-a"));
            assertThat(saved.producerStates()).isEmpty();
        }
    }

    @Test
    void aRetargetCannotReplaceLogicalNineWithTheOriginalSevenFloor() {
        try (Fixture f = new Fixture()) {
            StopReservation bound = f.bound(f.state.markReplacementPending(f.stop(), AT).orElseThrow());
            var first = continuation(bound, Optional.of(target(bound)), Optional.empty(), 7, "table-a",
                    List.of(producer(9, NATIVE_START, List.of(), "table-a")));
            var receipt = f.store.saveContinuation(PIPE, bound, Optional.empty(), first).orElseThrow();
            // The pending floor write did not commit before a caller retired the physical successor.
            StopReservation pending = f.state.retireSuccessor(bound,
                    new SuccessorEnd.Terminal(bound.successor().scope(), bound.successor().job()), AT).orElseThrow();
            StopReservation next = f.bound(pending);
            var wrongFloor = continuation(next, Optional.of(target(next)), Optional.empty(), 7, "table-a", List.of());

            assertThat(f.store.saveContinuation(PIPE, next, Optional.of(receipt), wrongFloor)).isEmpty();
            assertThat(f.store.readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(first);
            assertThat(f.store.readContinuation(PIPE).orElseThrow().receipt()).isEqualTo(receipt);
        }
    }

    @Test
    void largePrivateFactsStreamThroughSharedChunksAndCorruptionIsCoded() {
        try (Fixture f = new Fixture()) {
            StopReservation stop = f.stop();
            String table = "large".repeat(130_000);
            var floor = continuation(stop, Optional.empty(), Optional.empty(), 7, table, List.of());
            f.store.saveContinuation(PIPE, stop, Optional.empty(), floor).orElseThrow();
            Document manifest = f.database.getCollection("observations")
                    .find(new Document("_id", MongoLatestObservationStorage.manifestKey(PIPE))).first();
            Document descriptor = manifest.get("continuation", Document.class);
            assertThat(descriptor.getString("mode")).isEqualTo("chunked");
            assertThat(descriptor).doesNotContainKey("inlinePayload");
            List<Document> chunks = f.chunks.find().into(new ArrayList<>());
            assertThat(chunks).isNotEmpty().allSatisfy(chunk -> {
                assertThat(chunk.getInteger("encodingVersion")).isEqualTo(3);
                assertThat(chunk.get("payload", Binary.class).getData().length)
                        .isBetween(1, LatestObservationPayloadCodec.CHUNK_PAYLOAD_LIMIT);
            });
            assertThat(f.newStore(f.chunks).readContinuation(PIPE).orElseThrow().continuation()).isEqualTo(floor);
            Document first = chunks.getFirst();
            byte[] payload = first.get("payload", Binary.class).getData();
            payload[payload.length / 2] ^= 1;
            f.chunks.updateOne(new Document("_id", first.get("_id")),
                    new Document("$set", new Document("payload", new Binary(payload))));
            assertThatThrownBy(() -> f.store.readContinuation(PIPE))
                    .isInstanceOfSatisfying(TapstateException.class, failure ->
                            assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    private static ObservationContinuation continuation(StopReservation marker,
            Optional<ObservationContinuation.Target> target, Optional<ObservationContinuation.Target> origin,
            long baseline, String table, List<ObservationContinuation.ProducerState> producers) {
        return new ObservationContinuation(marker.token(), SOURCE, target, origin, List.of(fact(baseline, AT, table)), producers);
    }

    private static ObservationContinuation.Target target(StopReservation marker) {
        return new ObservationContinuation.Target(marker.successor().scope(), Optional.of(marker.successor().job()));
    }

    private static ObservationContinuation.ProducerState producer(long published, Instant nativeStart,
            List<MetricPoint> offsets, String table) {
        return new ObservationContinuation.ProducerState("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                "out", "", nativeStart, offsets, List.of(point(published, AT, table)));
    }

    private static Observation observation(long total, Instant at, String table) {
        return new Observation(PIPE, PipelineState.RUNNING, Map.of("records.out", total), Map.of(), Map.of(),
                null, at, List.of(fact(total, at, table)));
    }

    private static MetricFact fact(long total, Instant at, String table) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}", point(total, at, table));
    }

    private static MetricPoint point(long total, Instant at, String table) {
        return MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPE,
                MetricAttributes.TABLE_ID, table, MetricAttributes.DIRECTION, "out"), START, at, total);
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> failOneFind(MongoCollection<Document> target, AtomicBoolean armed) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("find") && armed.compareAndSet(true, false)) {
                        throw new MongoException("the private source payload is temporarily unreadable");
                    }
                    try {
                        Object result = method.invoke(target, arguments);
                        return result instanceof MongoCollection<?> collection
                                ? failOneFind((MongoCollection<Document>) collection, armed) : result;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
    }

    private static final class Fixture implements AutoCloseable {
        final String name = "continuation_" + UUID.randomUUID().toString().replace("-", "");
        final List<String> commands = new CopyOnWriteArrayList<>();
        final MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (name.equals(event.getDatabaseName()) && List.of("find", "aggregate", "count", "update", "findAndModify")
                                .contains(event.getCommandName())) { commands.add(event.getCommandName()); }
                    }
                }).build());
        final MongoDatabase database = client.getDatabase(name);
        final MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(database.getCollection("workload_claims"));
        final MongoDesiredStore desired = new MongoDesiredStore(database.getCollection("desired"));
        final MongoStateStore state = new MongoStateStore(client, database.getCollection("states"),
                database.getCollection("desired"), database.getCollection("workload_claims"), database.getCollection("artifacts"));
        final MongoCollection<Document> chunks = database.getCollection("chunks");
        final MongoObservationStore store = newStore(chunks);

        Fixture() {
            database.getCollection("artifacts").insertOne(new Document("_id", PIPE).append("kind", "pipeline")
                    .append("pipelineIncarnationId", INC));
            state.create(PIPE, StateJson.of(PipelineState.PAUSED), AT);
            claims.advanceStandalone(CLUSTER, PIPE);
        }

        MongoObservationStore newStore(MongoCollection<Document> chunkCollection) {
            return new MongoObservationStore(client, database.getCollection("observations"), chunkCollection, state.handoffWrites());
        }

        StopReservation stop() {
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-a", false, "assembly-a", false, null);
            desired.save(intent);
            CheckpointDoc actual = state.read(PIPE).orElseThrow();
            StopReservation stop = StopReservation.stopping(PIPE, UUID.randomUUID().toString(), actual.epoch(), intent,
                    new StopReservation.Source(CLUSTER, SOURCE, new StopReservation.JobIdentity(CLUSTER, 11, "boot-a")),
                    StopReservation.CounterPolicy.CONTINUE, StopAuthority.standalone(CLUSTER, 1));
            return state.reserveStop(actual, stop, AT).orElseThrow();
        }

        StopReservation bound(StopReservation pending) {
            String boot = "submit-" + UUID.randomUUID();
            StopReservation admitted = state.admitSuccessor(pending, INC, boot, AT).orElseThrow().reservation();
            return state.bindSuccessor(admitted, admitted.successor().scope(),
                    new StopReservation.JobIdentity(CLUSTER, 77 + admitted.successor().scope().executionGeneration(), boot), AT).orElseThrow();
        }

        @Override public void close() { database.drop(); client.close(); }
    }
}
