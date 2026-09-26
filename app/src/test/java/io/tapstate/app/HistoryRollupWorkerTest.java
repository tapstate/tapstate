package io.tapstate.app;

import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Key;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.RateHistoryStore.Visibility;
import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class HistoryRollupWorkerTest {

    private static final String ID = "orders";
    private static final Instant TEN = Instant.parse("2026-09-27T10:00:00Z");
    private static final Instant START = TEN.minus(Duration.ofHours(1));
    private static final Scope SCOPED = Scope.incarnation("inc-a");
    private static final Key FIRST = new Key(ID, SCOPED, Resolution.PT5M, TEN);

    @Test
    void closedFiveMinuteBucketUsesRawAggregatorAndKeepsIncarnationsSeparate() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.append(sample(TEN, 900, 90), null);
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 2), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(120), 40, 7), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(180), 50, 3), owner(2));
        raw.appendScoped(sample(TEN.plusSeconds(240), 60, 1), owner(2));
        raw.appendScoped(sample(TEN.plusSeconds(300), 70, 1), owner(2));
        MemoryRollups rollups = new MemoryRollups();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.key().scope()).isEqualTo(SCOPED);
        assertThat(bucket.inputReadStartedAt()).isEqualTo(clock.instant());
        assertThat(bucket.validUntil()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        assertThat(bucket.requiresFinerResolution()).isFalse();
        assertThat(bucket.fragments()).hasSize(2);
        assertThat(bucket.fragments().getFirst().recordsOut().delta())
                .isEqualByComparingTo(new BigDecimal("30"));
        assertThat(bucket.fragments().getFirst().lag()).singleElement()
                .satisfies(lag -> assertThat(lag.max()).isEqualTo(7));
        assertThat(bucket.fragments().get(1).startReason())
                .isEqualTo(HistoryRollupStore.StartReason.CONTINUATION);
        assertThat(bucket.fragments().get(1).recordsOut().delta())
                .isEqualByComparingTo(new BigDecimal("20"));
        assertThat(rollups.read(new Key(ID, Scope.legacy(), Resolution.PT5M, TEN))).isEmpty();
    }

    @Test
    void failedUpsertRetainsTheContiguousPositionAndRestartSkipsCompletedBuckets() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(11)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 1, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(360), 2, 2), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        rollups.failNextUpsert = true;
        List<HistoryRollupWorker.Work> work = List.of(new HistoryRollupWorker.Work(ID, SCOPED));

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1, work, ignored -> true)) {
            worker.runOneBatch();
            assertThat(rollups.read(FIRST)).isEmpty();
            worker.runOneBatch();
            assertThat(rollups.read(FIRST)).isPresent();
        }
        try (HistoryRollupWorker restarted = worker(raw, rollups, clock, 2, work, ignored -> true)) {
            restarted.runOneBatch();
        }

        assertThat(rollups.attempts).containsExactly(FIRST, FIRST,
                new Key(ID, SCOPED, Resolution.PT5M, TEN.plus(Duration.ofMinutes(5))));
        assertThat(rollups.rows).hasSize(2);
    }

    @Test
    void oneFailedBucketIsAttemptedOnlyOncePerColdPass() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 1, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        rollups.alwaysFail = true;

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        assertThat(rollups.attempts).containsExactly(FIRST);
    }


    @Test
    void ownerDenialDoesNotReadRawOrWriteAndLateRefreshIsBoundedAndExplicit() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(120), 20, 2), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        AtomicBoolean owner = new AtomicBoolean(false);
        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> owner.get())) {
            worker.runOneBatch();
            assertThat(rollups.rows).isEmpty();
            owner.set(true);
            worker.runOneBatch();
            Bucket first = rollups.read(FIRST).orElseThrow();

            clock.at = TEN.plus(Duration.ofMinutes(12));
            raw.appendScoped(sample(TEN.plusSeconds(180), 30, 8), owner(1));
            worker.runOneBatch();
            assertThat(rollups.read(FIRST).orElseThrow()).isEqualTo(first);
            assertThat(worker.requestRefresh(FIRST)).isTrue();
            assertThat(worker.requestRefresh(FIRST)).isTrue();
            worker.runOneBatch();
            Bucket refreshed = rollups.read(FIRST).orElseThrow();
            assertThat(refreshed.validUntil()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
            assertThat(refreshed.fragments().getFirst().lag().getFirst().max()).isEqualTo(8);
        }
        assertThat(rollups.attempts.stream().filter(FIRST::equals).count()).isEqualTo(2);
    }

    @Test
    void excessiveResetFragmentsUseFallbackMarkerInsteadOfUnboundedDocument() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        for (int second = 0; second < 75; second++) {
            Instant at = TEN.plusSeconds(second);
            raw.appendScoped(new RateSample(ID, at, Map.of("records.out", (long) second),
                    Map.of("orders", (long) second), at), owner(1));
        }
        MemoryRollups rollups = new MemoryRollups();
        try (HistoryRollupWorker worker = new HistoryRollupWorker(raw, rollups, clock,
                Duration.ofSeconds(1), 1, Duration.ofSeconds(5),
                () -> List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true, false)) {
            worker.runOneBatch();
        }
        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.requiresFinerResolution()).isTrue();
        assertThat(bucket.fragments()).isEmpty();
        assertThat(bucket.gaps()).isEmpty();
    }

    @Test
    void aStoppedPipelineDoesNotAccumulateEmptyFutureBuckets() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofHours(2)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
            worker.runOneBatch();
        }

        assertThat(rollups.rows).containsOnlyKeys(FIRST);
    }

    @Test
    void resetAndGapBoundariesMatchTheRawAggregationRules() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN, 0, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 4), owner(1));
        Instant restarted = START.plusSeconds(1);
        raw.appendScoped(new RateSample(ID, TEN.plusSeconds(120), Map.of("records.out", 0L),
                Map.of("orders", 2L), restarted), owner(2));
        raw.appendScoped(new RateSample(ID, TEN.plusSeconds(180), Map.of("records.out", 5L),
                Map.of("orders", 7L), restarted), owner(2));
        raw.appendScoped(new RateSample(ID, TEN.plusSeconds(300), Map.of("records.out", 6L),
                Map.of("orders", 1L), restarted), owner(2));
        MemoryRollups rollups = new MemoryRollups();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.requiresFinerResolution()).isFalse();
        assertThat(bucket.fragments()).hasSize(2);
        assertThat(bucket.fragments().getFirst().recordsOut().delta()).isEqualByComparingTo("10");
        assertThat(bucket.fragments().get(1).startReason())
                .isEqualTo(HistoryRollupStore.StartReason.COUNTER_RESET);
        assertThat(bucket.fragments().get(1).recordsOut().delta()).isEqualByComparingTo("5");
        assertThat(bucket.fragments().get(1).lag().getFirst().max()).isEqualTo(7);
        assertThat(bucket.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.intervalStart()).isEqualTo(TEN.plusSeconds(180));
            assertThat(gap.intervalEnd()).isEqualTo(TEN.plusSeconds(300));
        });
    }

    @Test
    void expiryIsAnchoredToInputReadRatherThanComputationCompletion() {
        TickingClock clock = new TickingClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.computedAt()).isAfter(bucket.inputReadStartedAt());
        assertThat(bucket.validUntil()).isEqualTo(bucket.inputReadStartedAt().plus(Duration.ofMinutes(5)));
        assertThat(bucket.validUntil()).isBefore(bucket.computedAt().plus(Duration.ofMinutes(5)));
    }

    @Test
    void wiringKeepsLegacyAndCurrentIncarnationAsDistinctWorkAndRejectsStaleOwner() {
        Map<String, ArtifactStore.HistoryOwner> owners = Map.of(
                "legacy", new ArtifactStore.HistoryOwner(new Visibility(null, true)),
                "upgraded", new ArtifactStore.HistoryOwner(new Visibility("inc-a", true)),
                "new", new ArtifactStore.HistoryOwner(new Visibility("inc-b", false)));
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<Resource> resources) { throw new UnsupportedOperationException(); }
            @Override public Optional<Resource> get(String id) { return Optional.empty(); }
            @Override public List<Resource> list() { return List.of(); }
            @Override public Optional<HistoryOwner> pipelineHistoryOwner(String id) {
                return Optional.ofNullable(owners.get(id));
            }
        };

        assertThat(HistoryRollupConfiguration.currentWork(
                List.of("legacy", "upgraded", "new", "deleted"), artifacts)).containsExactly(
                new HistoryRollupWorker.Work("legacy", Scope.legacy()),
                new HistoryRollupWorker.Work("upgraded", Scope.legacy()),
                new HistoryRollupWorker.Work("upgraded", Scope.incarnation("inc-a")),
                new HistoryRollupWorker.Work("new", Scope.incarnation("inc-b")));
        assertThat(HistoryRollupConfiguration.currentOwner(artifacts,
                new HistoryRollupWorker.Work("upgraded", Scope.incarnation("inc-old")))).isFalse();
        assertThat(HistoryRollupConfiguration.currentOwner(artifacts,
                new HistoryRollupWorker.Work("new", Scope.legacy()))).isFalse();
    }

    @Test
    void onePassSharesItsFixedBatchAcrossPipelines() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(16)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 1, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(360), 2, 2), owner(1));
        raw.appendScoped(new RateSample("other", TEN.plusSeconds(60), Map.of("records.out", 1L),
                Map.of(), START), new ObservationStore.Scope("inc-b", 1));
        raw.appendScoped(new RateSample("other", TEN.plusSeconds(360), Map.of("records.out", 2L),
                Map.of(), START), new ObservationStore.Scope("inc-b", 1));
        MemoryRollups rollups = new MemoryRollups();
        List<HistoryRollupWorker.Work> work = List.of(new HistoryRollupWorker.Work(ID, SCOPED),
                new HistoryRollupWorker.Work("other", Scope.incarnation("inc-b")));

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 2, work, ignored -> true)) {
            worker.runOneBatch();
        }

        assertThat(rollups.attempts).containsExactly(
                FIRST, new Key("other", Scope.incarnation("inc-b"), Resolution.PT5M, TEN));
    }

    private static HistoryRollupWorker worker(InMemoryRateHistoryStore raw,
            MemoryRollups rollups, Clock clock, int batchSize, List<HistoryRollupWorker.Work> work,
            java.util.function.Predicate<HistoryRollupWorker.Work> allowed) {
        return new HistoryRollupWorker(raw, rollups, clock, Duration.ofMinutes(1), batchSize,
                Duration.ofSeconds(5), () -> work, allowed, false);
    }

    private static ObservationStore.Scope owner(long generation) {
        return new ObservationStore.Scope("inc-a", generation);
    }

    private static RateSample sample(Instant at, long records, long lag) {
        return new RateSample(ID, at, Map.of("records.out", records), Map.of("orders", lag), START);
    }

    private static final class MutableClock extends Clock {
        private Instant at;

        private MutableClock(Instant at) { this.at = at; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return at; }
    }

    private static final class TickingClock extends Clock {
        private Instant next;

        private TickingClock(Instant at) { next = at; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() {
            Instant current = next;
            next = next.plusSeconds(1);
            return current;
        }
    }

    private static final class MemoryRollups implements HistoryRollupStore {
        private final Map<Key, Bucket> rows = new HashMap<>();
        private final List<Key> attempts = new ArrayList<>();
        private boolean failNextUpsert;
        private boolean alwaysFail;

        @Override public void upsert(Bucket bucket) {
            attempts.add(bucket.key());
            if (failNextUpsert || alwaysFail) {
                failNextUpsert = false;
                throw new IllegalStateException("injected rollup write failure");
            }
            rows.put(bucket.key(), bucket);
        }
        @Override public Optional<Bucket> read(Key key) { return Optional.ofNullable(rows.get(key)); }
        @Override public List<Bucket> readRange(String pipelineId, Scope scope, Resolution resolution,
                Instant from, Instant to, int limit) {
            return rows.values().stream().filter(bucket -> bucket.key().pipelineId().equals(pipelineId))
                    .filter(bucket -> bucket.key().scope().equals(scope))
                    .filter(bucket -> bucket.key().resolution() == resolution)
                    .filter(bucket -> !bucket.key().bucketStart().isBefore(from)
                            && bucket.key().bucketStart().isBefore(to))
                    .sorted(java.util.Comparator.comparing(bucket -> bucket.key().bucketStart()))
                    .limit(limit).toList();
        }
        @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
        @Override public void deleteLegacy(String pipelineId) { }
        @Override public Duration retention() { return Duration.ofDays(15); }
    }
}
