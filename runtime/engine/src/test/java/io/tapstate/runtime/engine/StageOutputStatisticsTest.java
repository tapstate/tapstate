package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.StageOutputReading;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static io.tapstate.runtime.engine.OutputPressureMetricNames.Kind;
import static org.assertj.core.api.Assertions.assertThat;

/** Output tuples must be complete even for quiet processors, and must share the native collection. */
class StageOutputStatisticsTest {
    private static final long NOW = 10_000;
    private static final Set<String> MEMBERS = Set.of("member-a", "member-b");
    private static final Map<String, String> EXPECTED = Map.of("transform-a", "transform", "sink-a", "sink");

    @Test
    void completeQuietOutputIsAbsentAndSinkDoesNotRequireAnOutputWrapper() {
        assertThat(read(fixture(0, 0))).isEqualTo(StageOutputReading.NONE);
        Map<String, List<Measurement>> unwired = StageWorkCompletenessTest.fixture();
        assertThat(read(unwired)).isEqualTo(StageOutputReading.NONE);
    }

    @Test
    void completedRetriesSumOnlyTheCompleteWrappedCohortWithActualStartAndCollectionTimes() {
        Map<String, List<Measurement>> metrics = fixture(2, 1);
        metrics.replaceAll((name, rows) -> rows.stream().map(row ->
                "member-b".equals(row.tag(MetricTags.MEMBER))
                        ? Measurement.of(name, row.value(), NOW - 100, StageWorkCompletenessTest.tagsOf(row)) : row).toList());
        var reading = read(metrics).byStage().get("transform");
        assertThat(reading.refused()).isEqualTo(4);
        assertThat(reading.retryDuration().count()).isEqualTo(2);
        assertThat(reading.retryDuration().sum()).isEqualTo(0.0002);
        assertThat(reading.retryDuration().bucketCounts().getFirst()).isEqualTo(2);
        assertThat(reading.countingSince()).isEqualTo(Instant.ofEpochMilli(1_000));
        assertThat(reading.observedAt()).isEqualTo(Instant.ofEpochMilli(NOW - 100));
        assertThat(read(metrics).byStage()).containsOnlyKeys("transform");
    }

    @Test
    void anUnfinishedRefusalPublishesItsCounterWithoutInventingACompletedInterval() {
        var reading = read(fixture(1, 0)).byStage().get("transform");
        assertThat(reading.refused()).isEqualTo(2);
        assertThat(reading.retryDuration()).isNull();
    }

    @Test
    void missingMemberOrComponentAndDuplicateComponentsAreAbsent() {
        Map<String, List<Measurement>> missingMember = fixture(2, 1);
        missingMember.replaceAll((name, rows) -> name.contains(".output.") ? new ArrayList<>(rows.stream()
                .filter(row -> "member-a".equals(row.tag(MetricTags.MEMBER))).toList()) : rows);
        assertThat(read(missingMember)).isEqualTo(StageOutputReading.NONE);
        for (String name : List.of(OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.READY),
                OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.SINCE),
                OutputPressureMetricNames.bucketName(Stage.TRANSFORM, 16))) {
            Map<String, List<Measurement>> missing = fixture(2, 1);
            missing.get(name).removeLast();
            assertThat(read(missing)).as(name).isEqualTo(StageOutputReading.NONE);
        }
        Map<String, List<Measurement>> duplicate = fixture(2, 1);
        String name = OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.COUNT);
        duplicate.get(name).add(duplicate.get(name).getFirst());
        assertThat(read(duplicate)).isEqualTo(StageOutputReading.NONE);
    }

    @Test
    void oldMixedOrUnrecognizedTuplePartsCannotBorrowNativeReadiness() {
        String name = OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.COUNT);
        for (String tag : List.of(MetricTags.JOB, MetricTags.EXECUTION, MetricTags.MEMBER,
                MetricTags.PROCESSOR, MetricTags.USER)) {
            Map<String, List<Measurement>> mixed = fixture(2, 1);
            Measurement row = mixed.get(name).getFirst();
            Map<String, String> tags = StageWorkCompletenessTest.tagsOf(row);
            tags.put(tag, "old");
            mixed.get(name).set(0, Measurement.of(name, row.value(), NOW, tags));
            assertThat(read(mixed)).as(tag).isEqualTo(StageOutputReading.NONE);
        }
        Map<String, List<Measurement>> older = fixture(2, 1);
        Measurement row = older.get(name).getFirst();
        older.get(name).set(0, Measurement.of(name, row.value(), NOW - 1, StageWorkCompletenessTest.tagsOf(row)));
        assertThat(read(older)).isEqualTo(StageOutputReading.NONE);
        for (String unsupported : List.of("stage.transform.output.retry.bucket.17", "stage.transform.output.future",
                "stage.unknown.output.ready", "stage.join.output.ready")) {
            Map<String, List<Measurement>> bad = fixture(2, 1);
            bad.put(unsupported, List.of(Measurement.of(unsupported, 1, NOW, StageWorkCompletenessTest.tagsOf(row))));
            assertThat(read(bad)).as(unsupported).isEqualTo(StageOutputReading.NONE);
        }
    }

    @Test
    void malformedCountsBucketsAndStartsStayAbsent() {
        for (Kind kind : List.of(Kind.READY, Kind.SINCE, Kind.REFUSED, Kind.COUNT, Kind.SUM_NANOS)) {
            Map<String, List<Measurement>> bad = fixture(2, 1);
            replaceValue(bad, OutputPressureMetricNames.name(Stage.TRANSFORM, kind), -1);
            assertThat(read(bad)).as(kind.toString()).isEqualTo(StageOutputReading.NONE);
        }
        Map<String, List<Measurement>> unready = fixture(2, 1);
        replaceValue(unready, OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.READY), 0);
        assertThat(read(unready)).isEqualTo(StageOutputReading.NONE);
        Map<String, List<Measurement>> future = fixture(2, 1);
        replaceValue(future, OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.SINCE), NOW + 1);
        assertThat(read(future)).isEqualTo(StageOutputReading.NONE);
        Map<String, List<Measurement>> previousExecution = fixture(2, 1);
        replaceValue(previousExecution, MetricNames.EXECUTION_START_TIME, 5_000);
        assertThat(read(previousExecution)).isEqualTo(StageOutputReading.NONE);
        Map<String, List<Measurement>> countExceedsRefused = fixture(0, 1);
        assertThat(read(countExceedsRefused)).isEqualTo(StageOutputReading.NONE);
        Map<String, List<Measurement>> bucketsDisagree = fixture(2, 1);
        replaceValue(bucketsDisagree, OutputPressureMetricNames.bucketName(Stage.TRANSFORM, 0), 2);
        assertThat(read(bucketsDisagree)).isEqualTo(StageOutputReading.NONE);
        Map<String, List<Measurement>> quietWithSum = fixture(1, 0);
        replaceValue(quietWithSum, OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.SUM_NANOS), 1);
        assertThat(read(quietWithSum)).isEqualTo(StageOutputReading.NONE);
    }

    @Test
    void overflowsAndExtraUnrosteredProcessorPartsStayAbsent() {
        Map<String, List<Measurement>> overflow = fixture(Long.MAX_VALUE, 0);
        assertThat(read(overflow)).isEqualTo(StageOutputReading.NONE);
        Map<String, List<Measurement>> extra = fixture(2, 1);
        String name = OutputPressureMetricNames.name(Stage.TRANSFORM, Kind.READY);
        Map<String, String> tags = StageWorkCompletenessTest.tagsOf(extra.get(name).getFirst());
        tags.put(MetricTags.PROCESSOR, "99");
        extra.get(name).add(Measurement.of(name, 1, NOW, tags));
        assertThat(read(extra)).isEqualTo(StageOutputReading.NONE);
    }

    @Test
    void forcedOneOwnerMayHaveANonzeroNativeIndexWithoutRequiringTheInertMemberOutput() {
        Map<String, List<Measurement>> pinned = fixture(2, 1);
        pinned.replaceAll((name, rows) -> name.startsWith("stage.") ? new ArrayList<>(rows.stream()
                .filter(row -> "member-b".equals(row.tag(MetricTags.MEMBER)))
                .map(row -> name.endsWith(".expected") ? Measurement.of(name, 1, NOW,
                        StageWorkCompletenessTest.tagsOf(row)) : row).toList()) : rows);
        JobMetrics metrics = JobMetrics.of(pinned);
        var snapshot = Engine.validatedWorkIn(metrics, "job-a", EXPECTED, EXPECTED.keySet(),
                MEMBERS, NOW, 2_000).orElseThrow();
        var reading = Engine.outputPressureIn(metrics, snapshot, "job-a", MEMBERS, NOW, 2_000);
        assertThat(reading.byStage()).containsOnlyKeys("transform");
        assertThat(reading.byStage().get("transform").refused()).isEqualTo(2);
        assertThat(reading.byStage().get("transform").retryDuration().count()).isEqualTo(1);
        assertThat(reading.byStage().get("transform").countingSince()).isEqualTo(Instant.ofEpochMilli(2_000));
    }

    @Test
    void replacementExecutionStartsCountsAgainInsteadOfCarryingOldTotals() {
        assertThat(read(fixture(5, 2)).byStage().get("transform").refused()).isEqualTo(10);
        Map<String, List<Measurement>> replacement = fixture(1, 1);
        replacement.replaceAll((name, rows) -> rows.stream().map(row -> {
            Map<String, String> tags = StageWorkCompletenessTest.tagsOf(row);
            tags.put(MetricTags.EXECUTION, "execution-b");
            long value = name.endsWith(".output.since") ? 9_000
                    : name.equals(MetricNames.EXECUTION_START_TIME) ? 8_000 : row.value();
            return Measurement.of(name, value, NOW, tags);
        }).toList());
        var reading = read(replacement).byStage().get("transform");
        assertThat(reading.refused()).isEqualTo(2);
        assertThat(reading.retryDuration().count()).isEqualTo(2);
        assertThat(reading.countingSince()).isEqualTo(Instant.ofEpochMilli(9_000));
    }

    @Test
    void missingOutputDoesNotInvalidateCompleteActivityOrNonemptyNativeQueues() {
        Map<String, List<Measurement>> fixture = fixture(2, 1);
        fixture.remove(OutputPressureMetricNames.bucketName(Stage.TRANSFORM, 16));
        List<Measurement> sizes = new ArrayList<>();
        for (Measurement capacity : fixture.get(MetricNames.QUEUES_CAPACITY)) {
            sizes.add(Measurement.of(MetricNames.QUEUES_SIZE, 1, NOW, StageWorkCompletenessTest.tagsOf(capacity)));
        }
        fixture.put(MetricNames.QUEUES_SIZE, sizes);
        JobMetrics metrics = JobMetrics.of(fixture);
        var snapshot = snapshot(metrics);
        assertThat(Engine.outputPressureIn(metrics, snapshot, "job-a", MEMBERS, NOW, 2_000)).isEqualTo(StageOutputReading.NONE);
        assertThat(snapshot.work().totalActive()).isEqualTo(2);
        assertThat(Engine.stageQueuesIn(metrics, snapshot, "job-a", MEMBERS, NOW, 2_000).orElseThrow()
                .byStage().get("transform").depth()).isEqualTo(2);
    }

    private static void replaceValue(Map<String, List<Measurement>> fixture, String name, long value) {
        Measurement row = fixture.get(name).getFirst();
        fixture.get(name).set(0, Measurement.of(name, value, row.timestamp(), StageWorkCompletenessTest.tagsOf(row)));
    }

    private static StageOutputReading read(Map<String, List<Measurement>> fixture) {
        JobMetrics metrics = JobMetrics.of(fixture);
        return Engine.outputPressureIn(metrics, snapshot(metrics), "job-a", MEMBERS, NOW, 2_000);
    }

    private static Engine.WorkSnapshot snapshot(JobMetrics metrics) {
        return Engine.validatedWorkIn(metrics, "job-a", EXPECTED, Set.of(), MEMBERS, NOW, 2_000).orElseThrow();
    }

    private static Map<String, List<Measurement>> fixture(long refused, long count) {
        Map<String, List<Measurement>> fixture = StageWorkCompletenessTest.fixture();
        for (Measurement capacity : fixture.get(MetricNames.QUEUES_CAPACITY)) {
            if (!"transform-a".equals(capacity.tag(MetricTags.VERTEX))) {
                continue;
            }
            Map<String, String> tags = new HashMap<>(StageWorkCompletenessTest.tagsOf(capacity));
            for (Kind kind : Kind.values()) {
                if (kind == Kind.BUCKET) {
                    continue;
                }
                long value = switch (kind) {
                    case READY -> 1;
                    case SINCE -> "member-a".equals(capacity.tag(MetricTags.MEMBER)) ? 1_000 : 2_000;
                    case REFUSED -> refused;
                    case COUNT -> count;
                    case SUM_NANOS -> count * 100_000;
                    default -> throw new AssertionError();
                };
                add(fixture, OutputPressureMetricNames.name(Stage.TRANSFORM, kind), value, tags);
            }
            for (int index = 0; index < HistogramBounds.STAGE_OUTPUT_RETRY_DURATION.buckets(); index++) {
                add(fixture, OutputPressureMetricNames.bucketName(Stage.TRANSFORM, index), index == 0 ? count : 0, tags);
            }
        }
        return fixture;
    }

    private static void add(Map<String, List<Measurement>> fixture, String name, long value, Map<String, String> tags) {
        fixture.computeIfAbsent(name, ignored -> new ArrayList<>()).add(Measurement.of(name, value, NOW, tags));
    }
}
