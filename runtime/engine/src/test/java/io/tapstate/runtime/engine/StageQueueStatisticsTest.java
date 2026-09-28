package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.StageQueueReading;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Zero active work does not hide occupancy; missing or torn native queue fields never become partial sums. */
class StageQueueStatisticsTest {
    private static final Map<String, String> EXPECTED = Map.of("transform-a", "transform", "sink-a", "sink");
    private static final Set<String> MEMBERS = Set.of("member-a", "member-b");
    private static final long NOW = 10_000;

    @Test
    void idleBusinessCallbacksCanHaveACompleteNonemptyInputQueue() {
        JobMetrics metrics = JobMetrics.of(fixture());
        Engine.WorkSnapshot snapshot = snapshot(metrics);
        assertThat(snapshot.work().activeByStage()).isEmpty();
        var sampled = Engine.stageQueuesIn(metrics, snapshot, "job-a", MEMBERS, NOW, 2_000).orElseThrow();
        assertThat(sampled.byStage().get("transform").depth()).isEqualTo(3);
        assertThat(sampled.byStage().get("transform").capacity()).isEqualTo(128);
        assertThat(sampled.byStage().get("transform").sampledAt()).isEqualTo(NOW);
        StageQueueReading reading = new Engine.StageQueueAccounts(2).accept("flow", 1, sampled).orElseThrow();
        assertThat(reading.byStage()).containsOnlyKeys("transform");
        assertThat(reading.byStage().get("transform").queue().highWater()).isEqualTo(3);
    }

    @Test
    void missingDuplicateMixedAndOlderQueueMeasurementsAreAbsent() {
        Map<String, List<Measurement>> missing = fixture();
        missing.get(MetricNames.QUEUES_SIZE).removeLast();
        assertThat(sample(missing)).isEmpty();
        Map<String, List<Measurement>> duplicate = fixture();
        duplicate.get(MetricNames.QUEUES_SIZE).add(duplicate.get(MetricNames.QUEUES_SIZE).getFirst());
        assertThat(sample(duplicate)).isEmpty();
        for (String tag : List.of(MetricTags.JOB, MetricTags.EXECUTION)) {
            Map<String, List<Measurement>> mixed = fixture();
            Measurement first = mixed.get(MetricNames.QUEUES_SIZE).getFirst();
            Map<String, String> tags = StageWorkCompletenessTest.tagsOf(first);
            tags.put(tag, "old");
            mixed.get(MetricNames.QUEUES_SIZE).set(0, Measurement.of(MetricNames.QUEUES_SIZE, 3, NOW, tags));
            assertThat(sample(mixed)).isEmpty();
        }
        Map<String, List<Measurement>> older = fixture();
        Measurement first = older.get(MetricNames.QUEUES_SIZE).getFirst();
        older.get(MetricNames.QUEUES_SIZE).set(0, Measurement.of(MetricNames.QUEUES_SIZE,
                first.value(), NOW - 1, StageWorkCompletenessTest.tagsOf(first)));
        assertThat(sample(older)).isEmpty();
    }

    @Test
    void anExtraProcessorSizeCannotBorrowACompleteBusinessRoster() {
        Map<String, List<Measurement>> extra = fixture();
        Measurement row = extra.get(MetricNames.QUEUES_SIZE).stream()
                .filter(reading -> "transform-a".equals(reading.tag(MetricTags.VERTEX))).findFirst().orElseThrow();
        Map<String, String> tags = StageWorkCompletenessTest.tagsOf(row);
        tags.put(MetricTags.PROCESSOR, "99");
        extra.get(MetricNames.QUEUES_SIZE).add(Measurement.of(MetricNames.QUEUES_SIZE, 0, NOW, tags));
        assertThat(sample(extra)).isEmpty();
    }

    @Test
    void compilerKnownInertNativeSlotsMayBeIgnoredOnlyWhenTheirCapacityIsRostered() {
        Map<String, List<Measurement>> pinned = fixture();
        pinned.replaceAll((name, readings) -> name.startsWith("stage.") ? readings.stream()
                .filter(reading -> "member-b".equals(reading.tag(MetricTags.MEMBER)))
                .map(reading -> name.endsWith(".expected") ? Measurement.of(name, 1, NOW,
                        StageWorkCompletenessTest.tagsOf(reading)) : reading).toList() : readings);
        for (String name : List.of(MetricNames.QUEUES_CAPACITY, MetricNames.QUEUES_SIZE)) {
            pinned.computeIfPresent(name, (key, readings) -> new ArrayList<>(readings.stream().map(reading -> {
                boolean inert = "member-a".equals(reading.tag(MetricTags.MEMBER));
                long value = inert ? 0 : name.equals(MetricNames.QUEUES_CAPACITY) ? 64
                        : "transform-a".equals(reading.tag(MetricTags.VERTEX)) ? 3 : 0;
                return Measurement.of(name, value, NOW, StageWorkCompletenessTest.tagsOf(reading));
            }).toList()));
        }
        JobMetrics metrics = JobMetrics.of(pinned);
        var snapshot = Engine.validatedWorkIn(metrics, "job-a", EXPECTED, EXPECTED.keySet(),
                MEMBERS, NOW, 2_000).orElseThrow();
        var sample = Engine.stageQueuesIn(metrics, snapshot, "job-a", MEMBERS, NOW, 2_000);
        assertThat(sample).isPresent();
        assertThat(sample.orElseThrow().byStage().get("transform").depth()).isEqualTo(3);
        assertThat(sample.orElseThrow().byStage().get("transform").capacity()).isEqualTo(64);
    }

    @Test
    void anOccupancyBeyondCapacityIsNotAnObservedQueue() {
        Map<String, List<Measurement>> invalid = fixture();
        Measurement first = invalid.get(MetricNames.QUEUES_SIZE).getFirst();
        invalid.get(MetricNames.QUEUES_SIZE).set(0, Measurement.of(MetricNames.QUEUES_SIZE,
                65, NOW, StageWorkCompletenessTest.tagsOf(first)));
        assertThat(sample(invalid)).isEmpty();
    }

    private static java.util.Optional<Engine.StageQueuesSample> sample(Map<String, List<Measurement>> fixture) {
        JobMetrics metrics = JobMetrics.of(fixture);
        return Engine.stageQueuesIn(metrics, snapshot(metrics), "job-a", MEMBERS, NOW, 2_000);
    }

    private static Engine.WorkSnapshot snapshot(JobMetrics metrics) {
        return Engine.validatedWorkIn(metrics, "job-a", EXPECTED, Set.of(), MEMBERS, NOW, 2_000).orElseThrow();
    }

    private static Map<String, List<Measurement>> fixture() {
        Map<String, List<Measurement>> fixture = StageWorkCompletenessTest.fixture();
        fixture.replaceAll((name, readings) -> name.endsWith(".active") ? readings.stream()
                .map(reading -> Measurement.of(name, 0, NOW, StageWorkCompletenessTest.tagsOf(reading))).toList() : readings);
        List<Measurement> sizes = new ArrayList<>();
        for (Measurement capacity : fixture.get(MetricNames.QUEUES_CAPACITY)) {
            long depth = "transform-a".equals(capacity.tag(MetricTags.VERTEX))
                    && "0".equals(capacity.tag(MetricTags.PROCESSOR)) ? 3 : 0;
            sizes.add(Measurement.of(MetricNames.QUEUES_SIZE, depth, NOW, StageWorkCompletenessTest.tagsOf(capacity)));
        }
        fixture.put(MetricNames.QUEUES_SIZE, sizes);
        return fixture;
    }
}
