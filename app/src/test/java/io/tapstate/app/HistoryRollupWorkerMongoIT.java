package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Key;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-store witness for the cold worker and its persisted incarnation-scoped bucket. */
@RequiresDocker
class HistoryRollupWorkerMongoIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void coldWorkerPersistsOnlyCurrentIncarnationAndRestartSkipsCompletedBucket() {
        Instant bucketStart = Instant.parse("2026-09-27T10:00:00Z");
        HistoryRollupStore.Scope current = HistoryRollupStore.Scope.incarnation("inc-current");
        HistoryRollupStore.Key key = new HistoryRollupStore.Key("orders", current,
                HistoryRollupStore.Resolution.PT5M, bucketStart);
        Clock clock = Clock.fixed(bucketStart.plus(Duration.ofMinutes(6)), ZoneOffset.UTC);
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl("history_rollup_worker_it"), null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_rollup_worker_it");
            store.rateHistory().appendScoped(sample(bucketStart, 0),
                    new ObservationStore.Scope("inc-current", 1));
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(60), 10),
                    new ObservationStore.Scope("inc-current", 1));
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(120), 20),
                    new ObservationStore.Scope("inc-current", 1));
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(180), 900),
                    new ObservationStore.Scope("inc-old", 1));
            List<HistoryRollupWorker.Work> work = List.of(new HistoryRollupWorker.Work("orders", current));

            try (HistoryRollupWorker worker = worker(store, clock, work)) {
                worker.runOneBatch();
            }
            HistoryRollupStore.Bucket persisted = store.historyRollups().read(key).orElseThrow();
            assertThat(persisted.requiresFinerResolution()).isFalse();
            assertThat(persisted.fragments()).isNotEmpty();
            assertThat(persisted.fragments().stream()
                    .map(fragment -> fragment.recordsOut() == null ? BigDecimal.ZERO
                            : fragment.recordsOut().delta())
                    .reduce(BigDecimal.ZERO, BigDecimal::add)).isEqualByComparingTo("20");
            assertThat(store.historyRollups().read(new HistoryRollupStore.Key("orders",
                    HistoryRollupStore.Scope.incarnation("inc-old"),
                    HistoryRollupStore.Resolution.PT5M, bucketStart))).isEmpty();

            try (HistoryRollupWorker restarted = worker(store, clock, work)) {
                restarted.runOneBatch();
            }
            assertThat(store.historyRollups().read(key)).contains(persisted);
        }
    }

    @Test
    void aClosedCoarseBucketRetainsItsRawAnchorAndInputDeadlineInMongo() {
        Instant bucketStart = Instant.parse("2026-09-27T10:00:00Z");
        HistoryRollupStore.Scope scope = HistoryRollupStore.Scope.incarnation("inc-coarse");
        HistoryRollupStore.Key coarse = new HistoryRollupStore.Key("orders", scope,
                HistoryRollupStore.Resolution.PT30M, bucketStart);
        Clock clock = Clock.fixed(bucketStart.plus(Duration.ofMinutes(31)), ZoneOffset.UTC);
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl("history_rollup_coarse_worker_it"), null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_rollup_coarse_it");
            ObservationStore.Scope owner = new ObservationStore.Scope("inc-coarse", 1);
            store.rateHistory().appendScoped(sample(bucketStart, 0), owner);
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(60), 10), owner);
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(120), 20), owner);

            try (HistoryRollupWorker worker = worker(store, clock,
                    List.of(new HistoryRollupWorker.Work("orders", scope)), 16)) {
                worker.runOneBatch();
            }

            HistoryRollupStore.Bucket persisted = store.historyRollups().read(coarse).orElseThrow();
            assertThat(persisted.requiresFinerResolution()).isFalse();
            assertThat(persisted.inWindowSamples()).isEqualTo(3);
            assertThat(persisted.validUntil())
                    .isEqualTo(persisted.inputReadStartedAt().plus(Duration.ofMinutes(5)));
            assertThat(persisted.fragments()).singleElement().satisfies(fragment -> {
                assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("20");
                assertThat(fragment.recordsOutStats()).isNotNull();
                assertThat(fragment.resumeAfter()).isNotNull();
                assertThat(fragment.resumeAt()).isNotNull();
            });
        }
    }

    @Test
    void freshPersistedFiveMinuteChildrenCanRebuildTheCoarseBucketWithoutRawFallback() {
        Instant bucketStart = Instant.parse("2026-09-27T10:00:00Z");
        HistoryRollupStore.Scope scope = HistoryRollupStore.Scope.incarnation("inc-cascade");
        HistoryRollupStore.Key coarse = new HistoryRollupStore.Key("orders", scope,
                HistoryRollupStore.Resolution.PT30M, bucketStart);
        Clock clock = Clock.fixed(bucketStart.plus(Duration.ofMinutes(31)), ZoneOffset.UTC);
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl("history_rollup_cascade_it"), null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_rollup_cascade_it");
            ObservationStore.Scope owner = new ObservationStore.Scope("inc-cascade", 1);
            for (int minute = 0; minute <= 30; minute++) {
                store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(minute * 60L),
                        minute * 10L), owner);
            }
            try (HistoryRollupWorker worker = worker(store, clock,
                    List.of(new HistoryRollupWorker.Work("orders", scope)), 16)) {
                for (int pass = 0; pass < 6; pass++) {
                    worker.runOneBatch();
                }
                for (int slot = 0; slot < 6; slot++) {
                    HistoryRollupStore.Key child = new HistoryRollupStore.Key("orders", scope,
                            HistoryRollupStore.Resolution.PT5M,
                            bucketStart.plus(Duration.ofMinutes(slot * 5L)));
                    assertThat(store.historyRollups().read(child)).isPresent();
                }
                long rawFallbackBefore = worker.health().levels().get(
                        HistoryRollupStore.Resolution.PT30M).rawFallback();
                long builtBefore = worker.health().levels().get(
                        HistoryRollupStore.Resolution.PT30M).computed();

                assertThat(worker.requestRefresh(coarse)).isTrue();
                worker.runOneBatch();

                HistoryRollupStore.Bucket rebuilt = store.historyRollups().read(coarse).orElseThrow();
                assertThat(rebuilt.fragments()).singleElement().satisfies(fragment -> {
                    assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("300");
                    assertThat(fragment.recordsOutStats()).isNotNull();
                    assertThat(fragment.resumeAfter()).isNotNull();
                });
                assertThat(worker.health().levels().get(HistoryRollupStore.Resolution.PT30M).rawFallback())
                        .isEqualTo(rawFallbackBefore);
                assertThat(worker.health().levels().get(HistoryRollupStore.Resolution.PT30M).computed())
                        .isEqualTo(builtBefore + 1);
            }
        }
    }

    @Test
    void newOwnerResumesPersistedProgressAcrossAllFiveLevelsWithoutAnOldOwnerWrite() {
        Instant start = Instant.parse("2026-09-27T00:00:00Z");
        Clock clock = Clock.fixed(start.plus(Duration.ofHours(6)).plus(Duration.ofMinutes(1)), ZoneOffset.UTC);
        HistoryRollupStore.Scope scope = HistoryRollupStore.Scope.incarnation("inc-takeover");
        List<HistoryRollupWorker.Work> work = List.of(new HistoryRollupWorker.Work("orders", scope));
        AtomicBoolean owner = new AtomicBoolean(true);
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl("history_rollup_owner_takeover_it"), null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_rollup_owner_takeover_it");
            ObservationStore.Scope execution = new ObservationStore.Scope("inc-takeover", 17);
            for (int minute = 0; minute <= 360; minute++) {
                store.rateHistory().appendScoped(sample(start.plus(Duration.ofMinutes(minute)), minute * 10L),
                        execution);
            }
            HistoryRollupStore.Key first = new HistoryRollupStore.Key("orders", scope,
                    HistoryRollupStore.Resolution.PT5M, start);
            HistoryRollupStore.Key second = new HistoryRollupStore.Key("orders", scope,
                    HistoryRollupStore.Resolution.PT5M, start.plus(Duration.ofMinutes(5)));

            try (HistoryRollupWorker old = worker(store, clock, work, 1)) {
                old.runOneBatch();
            }
            HistoryRollupStore.Bucket firstByOldOwner = store.historyRollups().read(first).orElseThrow();
            assertThat(store.historyRollups().read(second)).isEmpty();

            owner.set(false);
            try (HistoryRollupWorker denied = new HistoryRollupWorker(store.rateHistory(),
                    store.historyRollups(), clock, Duration.ofMinutes(1), 64,
                    Duration.ofSeconds(5), () -> work, ignored -> owner.get(), false)) {
                denied.runOneBatch();
            }
            assertThat(store.historyRollups().read(second)).isEmpty();

            owner.set(true);
            HistoryRollupStore durable = store.historyRollups();
            AtomicBoolean failFirstCoarseWrite = new AtomicBoolean(true);
            HistoryRollupStore onceUnavailable = new HistoryRollupStore() {
                @Override
                public void upsert(Bucket bucket) {
                    if (bucket.key().resolution() == Resolution.PT30M
                            && bucket.key().bucketStart().equals(start)
                            && failFirstCoarseWrite.compareAndSet(true, false)) {
                        throw new IllegalStateException("one coarse write is unavailable");
                    }
                    durable.upsert(bucket);
                }

                @Override public java.util.Optional<Bucket> read(Key key) { return durable.read(key); }
                @Override public List<Bucket> readRange(String id, Scope owner, Resolution resolution,
                        Instant from, Instant to, int limit) {
                    return durable.readRange(id, owner, resolution, from, to, limit);
                }
                @Override public void deleteIncarnation(String id, String incarnation) {
                    durable.deleteIncarnation(id, incarnation);
                }
                @Override public void deleteLegacy(String id) { durable.deleteLegacy(id); }
                @Override public Duration retention() { return durable.retention(); }
            };
            try (HistoryRollupWorker successor = new HistoryRollupWorker(store.rateHistory(),
                    onceUnavailable, clock, Duration.ofMinutes(1), 64,
                    Duration.ofSeconds(5), () -> work, ignored -> owner.get(), false)) {
                for (int pass = 0; pass < 12; pass++) {
                    successor.runOneBatch();
                }
                assertThat(failFirstCoarseWrite).isFalse();
                assertThat(successor.health().levels().get(Resolution.PT30M).failed()).isEqualTo(1);
                assertThat(successor.health().levels().get(Resolution.PT30M).retried()).isPositive();
                assertThat(successor.health().degraded()).isFalse();
            }
            assertThat(store.historyRollups().read(first)).contains(firstByOldOwner);
            for (HistoryRollupStore.Resolution resolution : HistoryRollupStore.Resolution.values()) {
                int expected = (int) (Duration.ofHours(6).toMinutes() / resolution.duration().toMinutes());
                assertThat(store.historyRollups().readRange("orders", scope, resolution,
                        start, start.plus(Duration.ofHours(6)), HistoryRollupStore.MAX_PAGE_SIZE))
                        .as("the successor closes every %s bucket without losing the old owner's first", resolution)
                        .hasSize(expected);
            }
        }
    }

    private static HistoryRollupWorker worker(MongoStorePort store, Clock clock,
            List<HistoryRollupWorker.Work> work) {
        return worker(store, clock, work, 1);
    }

    private static HistoryRollupWorker worker(MongoStorePort store, Clock clock,
            List<HistoryRollupWorker.Work> work, int batchSize) {
        return new HistoryRollupWorker(store.rateHistory(), store.historyRollups(), clock,
                Duration.ofMinutes(1), batchSize, Duration.ofSeconds(5), () -> work, ignored -> true, false);
    }

    private static RateSample sample(Instant at, long records) {
        return new RateSample("orders", at, Map.of("records.out", records), Map.of(),
                Instant.parse("2026-09-27T09:00:00Z"));
    }
}
