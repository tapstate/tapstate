package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.WorkloadClaim;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservationJanitorTest {

    private static final Instant AT = Instant.parse("2026-09-27T10:00:00Z");

    @Test
    void processHealthIsLabelFreeAndOmitsAgeUntilAFullBatchSucceeds() {
        ObservationJanitor.Health neverSucceeded = new ObservationJanitor.Health(
                2, 1, 1, 15, OptionalLong.empty(), true);
        List<MetricFact> facts = ObservationJanitorFacts.snapshot(
                neverSucceeded, AT.minusSeconds(30), AT);

        assertThat(facts).extracting(MetricFact::name).doesNotContain(
                "tapstate.process.observation_janitor.last_success.age");
        assertThat(facts).allSatisfy(fact -> assertThat(fact.points())
                .allSatisfy(point -> assertThat(point.attributes()).isEmpty()));
        assertThat(facts.stream().filter(fact -> fact.name().equals(
                "tapstate.process.observation_janitor.degraded"))
                .flatMap(fact -> fact.points().stream()).mapToLong(point -> point.value()).findFirst())
                .hasValue(1L);
    }


    @Test
    void invalidOperatorBudgetsFailAtStartupWithACodedDiagnostic() {
        assertThatThrownBy(() -> new ObservationJanitor(new MemoryLatest(),
                new MemoryArtifacts(Map.of(), Set.of()), new MemoryGenerations(Map.of()),
                "cluster", ObservationStore.MAX_LATEST_SCAN_BATCH + 1,
                Duration.ofMinutes(1), false))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(BootError.OBSERVABILITY_JANITOR_CONFIG_INVALID));
    }

    @Test
    void eachColdPassIsBoundedAndOnlyProvableOrphansAreRemoved() {
        MemoryLatest latest = new MemoryLatest();
        latest.put("a", "inc-old", 1);
        latest.put("b", "inc-b", 2);
        latest.put("c", null, 0);
        latest.put("d", null, 0);
        latest.put("e", "inc-e", 1);
        latest.put("f", "inc-f", 2);
        latest.put("g", "inc-g", 3);
        MemoryArtifacts artifacts = new MemoryArtifacts(
                Map.of("a", "inc-new", "b", "inc-b", "e", "inc-e", "f", "inc-f", "g", "inc-g"),
                Set.of("c"));
        MemoryGenerations generations = new MemoryGenerations(Map.of("b", 2L, "e", 2L, "f", 2L));
        try (ObservationJanitor janitor = new ObservationJanitor(
                latest, artifacts, generations, "cluster", 2, Duration.ofMinutes(1), false)) {
            for (int pass = 0; pass < 4; pass++) {
                janitor.runOneBatch();
            }

            assertThat(latest.rows.keySet()).containsExactly("b", "c", "f", "g");
            assertThat(latest.requestedLimits).containsExactly(2, 2, 2, 2);
            assertThat(latest.afterCursors).containsExactly(Optional.empty(), Optional.of("b"),
                    Optional.of("d"), Optional.of("f"));
            ObservationJanitor.Health health = janitor.health();
            assertThat(health.scanned()).isEqualTo(7);
            assertThat(health.deleted()).isEqualTo(3);
            assertThat(health.failures()).isZero();
            assertThat(health.lastSuccessAgeMillis()).isPresent();
            assertThat(health.degraded()).isFalse();
        }
    }

    @Test
    void oneFailedOwnerLookupKeepsTheCursorAndTheNextColdPassRetries() {
        MemoryLatest latest = new MemoryLatest();
        latest.put("a", "inc-old", 1);
        latest.put("b", "inc-b", 2);
        AtomicBoolean failOnce = new AtomicBoolean(true);
        MemoryArtifacts artifacts = new MemoryArtifacts(
                Map.of("a", "inc-new", "b", "inc-b"), Set.of()) {
            @Override public Optional<String> pipelineIncarnationId(String id) {
                if (id.equals("a") && failOnce.compareAndSet(true, false)) {
                    throw new IllegalStateException("artifact store unavailable");
                }
                return super.pipelineIncarnationId(id);
            }
        };
        try (ObservationJanitor janitor = new ObservationJanitor(
                latest, artifacts, new MemoryGenerations(Map.of("b", 2L)),
                "cluster", 2, Duration.ofMinutes(1), false)) {
            janitor.runOneBatch();
            assertThat(janitor.health().failures()).isEqualTo(1);
            assertThat(janitor.health().degraded()).isTrue();
            janitor.runOneBatch();
            assertThat(latest.afterCursors).containsExactly(Optional.empty(), Optional.empty());
            assertThat(latest.rows.keySet()).containsExactly("b");
            assertThat(janitor.health().degraded()).isFalse();
        }
    }

    private static final class MemoryLatest implements ObservationStore {
        private final TreeMap<String, LatestSnapshot> rows = new TreeMap<>();
        private final List<Integer> requestedLimits = new ArrayList<>();
        private final List<Optional<String>> afterCursors = new ArrayList<>();

        void put(String id, String incarnation, long generation) {
            Optional<Scope> owner = incarnation == null ? Optional.empty()
                    : Optional.of(new Scope(incarnation, generation));
            rows.put(id, new LatestSnapshot(id, owner, Optional.of(AT)));
        }

        @Override public void save(Observation observation) { throw new UnsupportedOperationException(); }
        @Override public Optional<Observation> read(String id) { return Optional.empty(); }
        @Override public void delete(String id) { throw new UnsupportedOperationException(); }

        @Override public List<LatestSnapshot> scanLatestAfter(Optional<String> after, int limit) {
            requestedLimits.add(limit);
            afterCursors.add(after);
            return rows.values().stream().filter(row -> after.map(id ->
                    row.pipelineId().compareTo(id) > 0).orElse(true)).limit(limit).toList();
        }

        @Override public boolean deleteIfUnchanged(LatestSnapshot snapshot) {
            return rows.remove(snapshot.pipelineId(), snapshot);
        }
    }

    private static class MemoryArtifacts implements ArtifactStore {
        private final Map<String, String> incarnations;
        private final Set<String> legacy;

        private MemoryArtifacts(Map<String, String> incarnations, Set<String> legacy) {
            this.incarnations = incarnations;
            this.legacy = legacy;
        }

        @Override public void saveAll(List<Resource> resources) { throw new UnsupportedOperationException(); }
        @Override public Optional<Resource> get(String id) {
            return incarnations.containsKey(id) || legacy.contains(id)
                    ? Optional.of(new PipelineResource(id, null, List.of(), null,
                            null, null, null, null)) : Optional.empty();
        }
        @Override public List<Resource> list() { return List.of(); }
        @Override public Optional<String> pipelineIncarnationId(String id) {
            return Optional.ofNullable(incarnations.get(id));
        }
    }

    private record MemoryGenerations(Map<String, Long> values) implements ExecutionGenerationStore {
        @Override public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected,
                long topologyRevision) { return Optional.empty(); }
        @Override public OptionalLong advanceStandalone(String clusterId, String id) {
            return OptionalLong.empty();
        }
        @Override public OptionalLong currentGeneration(String clusterId, String id) {
            Long value = values.get(id);
            return value == null ? OptionalLong.empty() : OptionalLong.of(value);
        }
    }
}
