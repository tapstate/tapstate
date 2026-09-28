package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.StageWorkReading;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Missing processors, members, vertices or collection components never become a whole-job active sum. */
class StageWorkCompletenessTest {
    private static final long NOW = 10_000;
    private static final String JOB = "job-a";
    private static final Map<String, String> EXPECTED = Map.of("transform-a", "transform", "sink-a", "sink");

    @Test
    void completeBusinessVerticesProduceStageCountsAndACollectedTimestamp() {
        StageWorkReading reading = read(fixture());
        assertThat(reading.activeByStage()).containsExactlyInAnyOrderEntriesOf(Map.of("transform", 1L, "sink", 1L));
        assertThat(reading.totalActive()).isEqualTo(2);
        assertThat(reading.observedAt()).isEqualTo(Instant.ofEpochMilli(NOW));
    }

    @Test
    void aPinnedBusinessOwnerRetainsItsNativeNonzeroGlobalIndexWithoutInventingAnotherMembersZero() {
        Map<String, List<Measurement>> ownerOnly = fixture();
        ownerOnly.replaceAll((name, readings) -> readings.stream()
                .filter(reading -> "member-b".equals(reading.tag(MetricTags.MEMBER)))
                .map(reading -> name.endsWith(".expected") || name.endsWith(".active")
                        ? Measurement.of(name, 1, NOW, tagsOf(reading)) : reading).toList());
        StageWorkReading complete = Engine.activeWorkIn(JobMetrics.of(ownerOnly), JOB, EXPECTED,
                EXPECTED.keySet(), Set.of("member-a", "member-b"), NOW, 2_000);
        assertThat(complete.activeByStage()).containsExactlyInAnyOrderEntriesOf(Map.of("transform", 1L, "sink", 1L));
        assertThat(complete.totalActive()).isEqualTo(2);
        Map<String, List<Measurement>> invalidIndex = new HashMap<>();
        ownerOnly.forEach((name, readings) -> invalidIndex.put(name, readings.stream().map(reading -> {
            Map<String, String> tags = tagsOf(reading);
            if (tags.containsKey(MetricTags.PROCESSOR)) {
                tags.put(MetricTags.PROCESSOR, "2");
            }
            return Measurement.of(name, reading.value(), NOW, tags);
        }).toList()));
        assertThat(Engine.activeWorkIn(JobMetrics.of(invalidIndex), JOB, EXPECTED,
                EXPECTED.keySet(), Set.of("member-a", "member-b"), NOW, 2_000)).isEqualTo(StageWorkReading.NONE);
        assertThat(read(ownerOnly)).as("regular distributed vertices still require the other real processors")
                .isEqualTo(StageWorkReading.NONE);
    }

    @Test
    void aMissingIdentityTagStaysAbsentWithoutFailingTheObservation() {
        Map<String, List<Measurement>> missing = fixture();
        Measurement reading = missing.get(MetricNames.EXECUTION_START_TIME).getFirst();
        Map<String, String> tags = tagsOf(reading);
        tags.remove(MetricTags.MEMBER);
        missing.get(MetricNames.EXECUTION_START_TIME).set(0,
                Measurement.of(MetricNames.EXECUTION_START_TIME, 1, NOW, tags));
        assertThat(read(missing)).isEqualTo(StageWorkReading.NONE);
    }

    @Test
    void aMissingMemberProcessorVertexOrPartIsAbsent() {
        for (String component : List.of(MetricNames.EXECUTION_START_TIME, MetricNames.QUEUES_CAPACITY,
                "stage.transform.ready", "stage.transform.active", "stage.sink.expected")) {
            Map<String, List<Measurement>> fixture = fixture();
            fixture.get(component).removeLast();
            assertThat(read(fixture)).as(component).isEqualTo(StageWorkReading.NONE);
        }
        Map<String, List<Measurement>> missingVertex = fixture();
        missingVertex.keySet().removeIf(name -> name.startsWith("stage.sink."));
        assertThat(read(missingVertex)).isEqualTo(StageWorkReading.NONE);
    }

    @Test
    void mixedJobsExecutionsCollectionsAndExpiredSamplesAreAbsent() {
        for (String tag : List.of(MetricTags.JOB, MetricTags.EXECUTION)) {
            Map<String, List<Measurement>> fixture = fixture();
            replace(fixture, "stage.transform.active", tag, "old", NOW);
            assertThat(read(fixture)).isEqualTo(StageWorkReading.NONE);
        }
        Map<String, List<Measurement>> mixed = fixture();
        replace(mixed, "stage.transform.active", MetricTags.JOB, JOB, NOW - 1);
        assertThat(read(mixed)).isEqualTo(StageWorkReading.NONE);
        assertThat(Engine.activeWorkIn(JobMetrics.of(fixture()), JOB, EXPECTED,
                Set.of("member-a", "member-b"), NOW + 2_001, 2_000)).isEqualTo(StageWorkReading.NONE);
        assertThat(Engine.activeWorkIn(JobMetrics.of(fixture()), "job-new", EXPECTED,
                Set.of("member-a", "member-b"), NOW, 2_000)).isEqualTo(StageWorkReading.NONE);
    }

    @Test
    void anInternallyConsistentOlderProcessorTupleCannotBorrowTheMembersNewCollection() {
        Map<String, List<Measurement>> older = fixture();
        older.replaceAll((name, readings) -> readings.stream().map(reading ->
                "member-a".equals(reading.tag(MetricTags.MEMBER))
                        && "transform-a".equals(reading.tag(MetricTags.VERTEX))
                        ? Measurement.of(name, reading.value(), NOW - 1, tagsOf(reading)) : reading).toList());
        assertThat(read(older)).isEqualTo(StageWorkReading.NONE);
    }

    @Test
    void unwiredAndCompletelyIdleJobsAreAbsentRatherThanZero() {
        Map<String, List<Measurement>> quiet = fixture();
        for (String stage : List.of("transform", "sink")) {
            quiet.computeIfPresent("stage." + stage + ".active", (name, readings) -> readings.stream()
                    .map(reading -> Measurement.of(name, 0, reading.timestamp(), tagsOf(reading))).toList());
        }
        assertThat(read(quiet)).isEqualTo(StageWorkReading.NONE);
        assertThat(Engine.activeWorkIn(JobMetrics.of(fixture()), JOB, Map.of(),
                Set.of("member-a", "member-b"), NOW, 2_000)).isEqualTo(StageWorkReading.NONE);
    }

    private static void replace(Map<String, List<Measurement>> fixture, String name, String tag,
            String value, long timestamp) {
        Measurement previous = fixture.get(name).getFirst();
        Map<String, String> tags = tagsOf(previous);
        tags.put(tag, value);
        fixture.get(name).set(0, Measurement.of(name, previous.value(), timestamp, tags));
    }

    private static StageWorkReading read(Map<String, List<Measurement>> fixture) {
        return Engine.activeWorkIn(JobMetrics.of(fixture), JOB, EXPECTED,
                Set.of("member-a", "member-b"), NOW, 2_000);
    }

    private static Map<String, String> tagsOf(Measurement reading) {
        Map<String, String> tags = new HashMap<>();
        for (String tag : List.of(MetricTags.JOB, MetricTags.EXECUTION, MetricTags.MEMBER,
                MetricTags.VERTEX, MetricTags.PROCESSOR, MetricTags.PROCESSOR_TYPE)) {
            if (reading.tag(tag) != null) {
                tags.put(tag, reading.tag(tag));
            }
        }
        if (reading.tag(MetricTags.USER) != null) {
            tags.put(MetricTags.USER, reading.tag(MetricTags.USER));
        }
        return tags;
    }

    private static Map<String, List<Measurement>> fixture() {
        Map<String, List<Measurement>> fixture = new HashMap<>();
        for (int index = 0; index < 2; index++) {
            String member = index == 0 ? "member-a" : "member-b";
            add(fixture, MetricNames.EXECUTION_START_TIME, 1,
                    Map.of(MetricTags.JOB, JOB, MetricTags.EXECUTION, "execution-a", MetricTags.MEMBER, member));
            for (Map.Entry<String, String> vertex : EXPECTED.entrySet()) {
                Map<String, String> tags = Map.of(MetricTags.JOB, JOB, MetricTags.EXECUTION, "execution-a",
                        MetricTags.MEMBER, member, MetricTags.VERTEX, vertex.getKey(),
                        MetricTags.PROCESSOR, Integer.toString(index), MetricTags.PROCESSOR_TYPE, "BusinessProcessor",
                        MetricTags.USER, "true");
                add(fixture, MetricNames.QUEUES_CAPACITY, 64, tags);
                add(fixture, "stage." + vertex.getValue() + ".active", index == 0 ? 1 : 0, tags);
                add(fixture, "stage." + vertex.getValue() + ".ready", 1, tags);
                add(fixture, "stage." + vertex.getValue() + ".expected", 2, tags);
                add(fixture, "stage." + vertex.getValue() + ".members", 2, tags);
            }
        }
        return fixture;
    }

    private static void add(Map<String, List<Measurement>> fixture, String name, long value, Map<String, String> tags) {
        fixture.computeIfAbsent(name, ignored -> new ArrayList<>()).add(Measurement.of(name, value, NOW, tags));
    }
}
