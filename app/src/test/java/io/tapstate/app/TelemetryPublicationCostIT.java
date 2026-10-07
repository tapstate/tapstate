package io.tapstate.app;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoPipelineEventStore;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** One logical minute through the real dispatcher and Mongo stores, without a wall-clock verdict. */
@RequiresDocker
class TelemetryPublicationCostIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private enum Extra { NONE, LATEST, RAW }

    @Test
    void sixtyTicksKeepOneRawSampleAndOnlyTheInFlightAndLatestPendingWrites() throws Exception {
        run(Extra.valueOf(System.getProperty("tapstate.cost-gate.telemetry-mutation", "none")
                .toUpperCase(Locale.ROOT)));
    }

    @Test
    void anIdempotentExtraLatestWriteCannotHideBehindTheSameStoredObservation() {
        assertThatThrownBy(() -> run(Extra.LATEST)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("one minute of telemetry commands");
    }

    @Test
    void anExtraRawAppendCannotHideBehindTheSamplerCadence() {
        assertThatThrownBy(() -> run(Extra.RAW)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("one minute of telemetry commands");
    }

    private static void run(Extra extra) throws Exception {
        String name = "telemetry_publication_cost_" + extra.name().toLowerCase(Locale.ROOT);
        Commands commands = new Commands(name);
        var settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl(name)))
                .addCommandListener(commands).build();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Instant start = Instant.now().truncatedTo(ChronoUnit.MILLIS).minusSeconds(60);
        try (var client = MongoClients.create(settings)) {
            var database = client.getDatabase(name);
            database.drop();
            var latest = new MongoObservationStore(client, database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            var raw = new MongoRateHistoryStore(database, database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY),
                    Duration.ofDays(15));
            var events = new MongoPipelineEventStore(database, database.getCollection(MongoStorePort.PIPELINE_EVENTS),
                    Duration.ofDays(15));
            var scopes = new ObservationScopeRegistry();
            var scope = scopes.begin("orders", "cost-inc", 7);
            var sampler = new RateSampler(raw, Duration.ofMinutes(1));
            // Initialize the manifest and cadence before the measured (start, start + 60s] interval.
            assertThat(latest.saveScoped(frame(start, 0).observation(), scope)).isTrue();
            assertThat(sampler.appendIfDue(frame(start, 0).observation(), scope)).isTrue();
            var first = new AtomicBoolean(true);
            ObservationStore controlled = new ObservationStore() {
                @Override public void save(Observation observation) { throw new AssertionError("unscoped publication"); }
                @Override public boolean saveScoped(Observation observation, Scope owner) {
                    if (first.compareAndSet(true, false)) {
                        entered.countDown();
                        try {
                            if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("first write was not released"); }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError("first write was interrupted", interrupted);
                        }
                    }
                    return latest.saveScoped(observation, owner);
                }
                @Override public Optional<Observation> read(String id) { return latest.read(id); }
                @Override public Optional<Stored> readStored(String id) { return latest.readStored(id); }
                @Override public void delete(String id) { throw new AssertionError("unexpected cleanup in publication window"); }
            };
            var dispatcher = new TelemetryDispatcher(new ObservationPublisher(new InMemoryStateStore(), controlled),
                    sampler, MetricsExport.none(), scopes, events,
                    TelemetryDispatcher.DEFAULT_LATEST_WORKERS, TelemetryDispatcher.DEFAULT_QUEUE_CAPACITY);
            try (dispatcher) {
                commands.enabled = true;
                try {
                    dispatcher.offer(frame(start, 1), scope);
                    assertThat(entered.await(5, TimeUnit.SECONDS)).as("the first actual latest write is in flight").isTrue();
                    for (int second = 2; second <= 60; second++) { dispatcher.offer(frame(start, second), scope); }
                    dispatcher.offerEvent(new PipelineEvent("cost-event", "orders", "cost-inc", 7,
                            PipelineEvent.Kind.STATE_CHANGED, start.plusSeconds(1), PipelineState.NEW,
                            PipelineState.RUNNING, null, null, null));
                    var blocked = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
                    assertThat(blocked.inFlight()).isEqualTo(1);
                    assertThat(blocked.queueDepth()).isEqualTo(1);
                    assertThat(blocked.coalesced()).isEqualTo(58);
                    assertThat(blocked.dropped()).isZero();
                } finally { release.countDown(); }
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 2
                        && dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).successes() == 1
                        && dispatcher.health().get(TelemetryDispatcher.Sink.EVENT).successes() == 1);
            } finally { release.countDown(); }
            dispatcher.health().values().forEach(health -> {
                assertThat(health.queueDepth()).isZero();
                assertThat(health.inFlight()).isZero();
                assertThat(health.dropped()).isZero();
                assertThat(health.failures()).isZero();
                assertThat(health.timeouts()).isZero();
                assertThat(health.openGaps()).isZero();
                assertThat(health.pendingRestorations()).isZero();
            });
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).coalesced()).isEqualTo(58);
            if (extra == Extra.LATEST) { assertThat(latest.saveScoped(frame(start, 60).observation(), scope)).isTrue(); }
            if (extra == Extra.RAW) {
                raw.appendScoped(new RateSample("orders", start.plusSeconds(60), Map.of("records.out", 60L), Map.of(), null), scope);
            }
            commands.enabled = false;
            assertThat(commands.overflow.get()).as("the bounded command capture retained every operation").isFalse();
            assertThat(List.copyOf(commands.values)).as("one minute of telemetry commands")
                    .containsExactlyInAnyOrder(new Command("update", MongoStorePort.PIPELINE_OBSERVATION, 1),
                            new Command("update", MongoStorePort.PIPELINE_OBSERVATION, 1),
                            new Command("insert", MongoStorePort.PIPELINE_RATE_HISTORY, 1),
                            new Command("insert", MongoStorePort.PIPELINE_EVENTS, 1));
            var stored = latest.readStored("orders").orElseThrow();
            assertThat(stored.scope()).contains(scope);
            assertThat(stored.observation()).isEqualTo(frame(start, 60).observation());
            var samples = database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY).find()
                    .sort(new org.bson.Document("observedAt", 1)).into(new java.util.ArrayList<org.bson.Document>());
            assertThat(samples).extracting(sample -> sample.getDate("observedAt").toInstant())
                    .containsExactly(start, start.plusSeconds(60));
            assertThat(samples).extracting(sample -> sample.get("counters", org.bson.Document.class).getLong("records.out"))
                    .containsExactly(0L, 60L);
            assertThat(samples).allSatisfy(sample -> {
                assertThat(sample.getString("pipelineIncarnationId")).isEqualTo(scope.pipelineIncarnationId());
                assertThat(sample.getLong("executionGeneration")).isEqualTo(scope.executionGeneration());
                assertThat(sample.containsKey("gapFrom")).isFalse();
            });
            assertThat(database.getCollection(MongoStorePort.PIPELINE_EVENTS).countDocuments()).isEqualTo(1);
            assertThat(database.getCollection(MongoStorePort.PIPELINE_HISTORY_ROLLUPS).countDocuments()).isZero();
        } finally { release.countDown(); }
    }

    private static ObservationPublisher.Prepared frame(Instant start, int second) {
        var observation = new Observation("orders", PipelineState.RUNNING, Map.of("records.out", (long) second),
                Map.of(), Map.of(), null, start.plusSeconds(second));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) { TimeUnit.MILLISECONDS.sleep(5); }
        assertThat(condition.getAsBoolean()).as("all admitted publication work completed").isTrue();
    }

    private record Command(String name, String collection, int operations) { }

    private static final class Commands implements CommandListener {
        private final String database;
        private final List<Command> values = new CopyOnWriteArrayList<>();
        private final AtomicBoolean overflow = new AtomicBoolean();
        private volatile boolean enabled;
        private Commands(String database) { this.database = database; }
        @Override public synchronized void commandStarted(CommandStartedEvent event) {
            if (!enabled || !database.equals(event.getDatabaseName())) { return; }
            if (values.size() >= 128) { overflow.set(true); return; }
            String name = event.getCommandName();
            var collection = event.getCommand().get(name.equals("getMore") ? "collection" : name);
            String key = name.equals("update") ? "updates" : name.equals("insert") ? "documents" : "UNMAPPED";
            var operations = event.getCommand().get(key);
            values.add(new Command(name, collection != null && collection.isString()
                    ? collection.asString().getValue() : "UNMAPPED", operations != null && operations.isArray()
                    ? operations.asArray().size() : -1));
        }
    }
}
