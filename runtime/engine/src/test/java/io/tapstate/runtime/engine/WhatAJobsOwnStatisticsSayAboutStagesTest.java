package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.StageReading;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stage's distribution travels as bare numbers under names in the job's own statistics, and the engine
 * reassembles it. What is checked is how the parts combine across the processors of one stage, and what
 * a collection that caught a processor half-way through reporting reads as.
 */
class WhatAJobsOwnStatisticsSayAboutStagesTest {

    private static JobMetrics statistics(Map<String, List<Long>> valuesByMetric) {
        Map<String, List<Measurement>> measurements = new LinkedHashMap<>();
        valuesByMetric.forEach((metric, values) -> measurements.put(metric,
                values.stream().map(value -> Measurement.of(metric, value, 0L, Map.of())).toList()));
        return JobMetrics.of(measurements);
    }

    /** One stage's distribution as its processors report it, each processor's figures scaled by its share. */
    private static Map<String, List<Long>> aStage(String stage, long count, long sumMicros, int inBucket,
            List<Long> perProcessor, List<Long> since) {
        Map<String, List<Long>> statistics = new LinkedHashMap<>();
        statistics.put("stage." + stage + ".count", perProcessor.stream().map(share -> count * share).toList());
        statistics.put("stage." + stage + ".sumMicros", perProcessor.stream().map(share -> sumMicros * share).toList());
        for (int index = 0; index < HistogramBounds.PROCESS_DURATION.buckets(); index++) {
            int bucket = index;
            statistics.put("stage." + stage + ".bucket." + index,
                    perProcessor.stream().map(share -> bucket == inBucket ? count * share : 0L).toList());
        }
        statistics.put("stage." + stage + ".since", since);
        return statistics;
    }

    @Test
    @DisplayName("a statistic named for a stage this build does not know is skipped, not carried to the reading")
    void aStageWordThisBuildDoesNotKnowIsSkipped() {
        // What a rolling upgrade looks like from here: the job's statistics are aggregated across members,
        // so a member running a build with one more stage in its graph puts that stage's numbers in front
        // of this reader. Every part of it is well formed -- count, sum, all the buckets, a start -- so
        // nothing else filters it out.
        Map<String, List<Long>> statistics = new LinkedHashMap<>(
                aStage("transform", 40L, 12_000L, 2, List.of(1L), List.of(1_700_000_000_000L)));
        statistics.putAll(aStage("inference", 7L, 900L, 1, List.of(1L), List.of(1_700_000_000_001L)));

        StageReading read = Engine.stageDurationsIn(statistics(statistics));

        assertThat(read.durationByStage()).containsOnlyKeys("transform");
        assertThat(read.durationByStage().get("transform").count()).isEqualTo(40L);
        assertThat(read.countingSince()).isNotNull();
    }

    @Test
    @DisplayName("a stage's distribution is read back whole, with its start")
    void aStageIsReadBackWhole() {
        JobMetrics collected = statistics(aStage("transform", 40L, 12_000L, 2, List.of(1L), List.of(1_700_000_000_000L)));

        StageReading read = Engine.stageDurationsIn(collected);

        assertThat(read.durationByStage()).containsOnlyKeys("transform");
        HistogramValue transform = read.durationByStage().get("transform");
        assertThat(transform.count()).isEqualTo(40L);
        assertThat(transform.sum()).isEqualTo(0.012);
        assertThat(transform.bounds()).isEqualTo(HistogramBounds.PROCESS_DURATION.bounds());
        assertThat(transform.bucketCounts().get(2)).isEqualTo(40L);
        assertThat(read.countingSince()).isEqualTo(Instant.ofEpochMilli(1_700_000_000_000L));
    }

    @Test
    @DisplayName("several processors of one stage have their distributions added, and the latest start is kept")
    void theProcessorsOfAStageAreAddedAndTheLatestStartKept() {
        JobMetrics collected = statistics(aStage("nest", 10L, 5_000L, 4, List.of(1L, 3L),
                List.of(1_700_000_000_000L, 1_700_000_005_000L)));

        StageReading read = Engine.stageDurationsIn(collected);

        assertThat(read.durationByStage().get("nest").count()).isEqualTo(40L);
        assertThat(read.durationByStage().get("nest").sum()).isEqualTo(0.02);
        assertThat(read.durationByStage().get("nest").bucketCounts().get(4)).isEqualTo(40L);
        assertThat(read.countingSince()).isEqualTo(Instant.ofEpochMilli(1_700_000_005_000L));
    }

    @Test
    @DisplayName("two stages stay apart")
    void twoStagesStayApart() {
        Map<String, List<Long>> parts = aStage("source", 5L, 100L, 0, List.of(1L), List.of(1L));
        parts.putAll(aStage("sink", 2L, 3_000_000L, 12, List.of(1L), List.of(1L)));
        JobMetrics collected = statistics(parts);

        StageReading read = Engine.stageDurationsIn(collected);

        assertThat(read.durationByStage()).containsOnlyKeys("source", "sink");
        assertThat(read.durationByStage().get("sink").bucketCounts().get(12)).isEqualTo(2L);
        assertThat(read.durationByStage().get("source").count()).isEqualTo(5L);
    }

    @Test
    @DisplayName("a stage caught with a bucket missing is left out rather than read as a shape")
    void aHalfReportedStageIsLeftOut() {
        Map<String, List<Long>> parts = aStage("join", 7L, 700L, 1, List.of(1L), List.of(1L));
        parts.remove("stage.join.bucket.9");
        JobMetrics collected = statistics(parts);

        assertThat(Engine.stageDurationsIn(collected)).isEqualTo(StageReading.NONE);
    }

    @Test
    @DisplayName("the delivery names beside these are not read as stages")
    void theStageReadbackTakesItsOwnFamilyOfNames() {
        JobMetrics collected = statistics(new LinkedHashMap<>(Map.of(
                "recordsOut.i.orders", List.of(7L),
                "outDeliveryCount.orders", List.of(7L),
                "outCountingSince", List.of(1_700_000_000_000L))));

        assertThat(Engine.stageDurationsIn(collected)).isEqualTo(StageReading.NONE);
    }
}
