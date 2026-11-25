package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.HistogramBounds;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NestStateSamplesTest {
    @Test void sumsMembersButDoesNotMultiplySharedCountersByProcessorCount() {
        var f = new Samples();
        f.add("a", "0", 1, 100, 200, 150);
        f.add("a", "1", 1, 90, 180, 140);
        f.add("b", "0", 1, 10, 20, 15);
        var reading = NestStateSamples.keyAccess(f.metrics()).get("ns");
        assertThat(reading.reads()).isEqualTo(110);
        assertThat(reading.writes()).isEqualTo(220);
        assertThat(reading.coldLoads()).isEqualTo(165);
    }

    @Test void neverCombinesNumeratorAndDenominatorFromDifferentProcessors() {
        var f = new Samples();
        f.add("a", "0", 1, 100, 100, 190);
        f.add("a", "1", 1, 200, 0, 100);
        var reading = NestStateSamples.keyAccess(f.metrics()).get("ns");
        assertThat(reading.attempts()).isEqualTo(200);
        assertThat(reading.coldLoads()).isEqualTo(190);
    }

    @Test void anIncompleteNewScrapeCannotBorrowAnOlderColdCounter() {
        var f = new Samples();
        f.add("a", "0", 1, 100, 100, 190);
        f.point("a", "0", 2, NestStateMetricNames.READ_KEYS, 200);
        f.point("a", "0", 2, NestStateMetricNames.WRITE_KEYS, 200);
        assertThat(NestStateSamples.keyAccess(f.metrics()).get("ns").available()).isFalse();
    }

    @Test void missingMemberCountersAreNotHealthyZeros() {
        var f = new Samples(); f.add("a", "0", 1, 100, 100, 150);
        f.point("b", "0", 1, NestStateMetricNames.ENTRIES, 10);
        assertThat(NestStateSamples.keyAccess(f.metrics()).get("ns").available()).isFalse();
    }

    @Test void changedMembershipChangesTheDifferencingGeneration() {
        var f = new Samples(); f.add("a", "0", 1, 100, 100, 150);
        var before = NestStateSamples.keyAccess(f.metrics()).get("ns");
        f.add("b", "0", 1, 10, 10, 0);
        assertThat(NestStateSamples.keyAccess(f.metrics()).get("ns").follows(before)).isFalse();
    }

    @Test void anUnreportedPartitionOwnerCannotDisappearFromTheDenominatorScope() {
        var f = new Samples(); f.add("a", "0", 1, 100, 100, 0);
        assertThat(NestStateSamples.keyAccess(f.metrics(), Set.of("a", "b")).get("ns").available()).isFalse();
    }

    @Test void commitHistogramsAndRenderedRootsAreSummedAcrossMembersWithoutDuplicatingProcessors() {
        var f = new Samples();
        f.commit("a", "0", 2, 4, 10);
        f.commit("a", "1", 2, 4, 10);
        f.commit("b", "0", 2, 3, 5);
        // A partial scrape cannot be combined with older buckets.
        f.point("a", "0", 3, NestStateMetricNames.COMMIT_GROUPS, 8);
        var result = NestStateSamples.diagnostics(f.metrics()).get("ns");
        assertThat(result.rootsRendered()).isEqualTo(15);
        assertThat(result.commits().groups()).isEqualTo(7);
        assertThat(result.commits().keys().count()).isEqualTo(7);
        assertThat(result.commits().keys().sum()).isEqualTo(14.0);
        assertThat(result.commits().keys().bucketCounts().stream().mapToLong(Long::longValue).sum()).isEqualTo(7);
    }

    @Test void weightDistributionsAddAcrossMembersAndTheLargestDocumentIsTheLargerOfThem() {
        var f = new Samples();
        f.weight("a", "0", 2, 3, 900_000L, 2);
        f.weight("a", "1", 2, 3, 900_000L, 2);   // a sibling processor's copy of the same member's counters
        f.weight("b", "0", 2, 4, 1_500_000L, 0); // a member that folded nothing
        f.point("a", "0", 3, NestStateMetricNames.COMMIT_BYTES_COUNT, 99); // a partial newer scrape
        var weight = NestStateSamples.diagnostics(f.metrics()).get("ns").weight();
        assertThat(weight.documentBytes().count()).isEqualTo(7);
        assertThat(weight.commitBytes().count()).isEqualTo(7);
        assertThat(weight.largestDocumentBytes()).isEqualTo(1_500_000L);
        assertThat(weight.foldDuration().count()).isEqualTo(2);
        assertThat(weight.documentBytes().bucketCounts().stream().mapToLong(Long::longValue).sum()).isEqualTo(7);
    }

    @Test void aLevelThatFoldsNothingCarriesNoFoldDistribution() {
        var f = new Samples();
        f.weight("a", "0", 1, 2, 10_000L, 0);
        assertThat(NestStateSamples.diagnostics(f.metrics()).get("ns").weight().foldDuration()).isNull();
    }

    private static final class Samples {
        final Map<String, List<Measurement>> values = new HashMap<>();
        void add(String member, String processor, long at, long reads, long writes, long cold) {
            point(member, processor, at, NestStateMetricNames.READ_KEYS, reads);
            point(member, processor, at, NestStateMetricNames.WRITE_KEYS, writes);
            point(member, processor, at, NestStateMetricNames.COLD_KEYS, cold);
        }
        void point(String member, String processor, long at, String kind, long value) {
            String metric = NestStateMetricNames.nameOf(kind, "ns");
            values.computeIfAbsent(metric, ignored -> new ArrayList<>()).add(Measurement.of(metric, value, at,
                    Map.of(MetricTags.MEMBER, member, MetricTags.EXECUTION, "execution", MetricTags.VERTEX, "nest", MetricTags.PROCESSOR, processor)));
        }
        void commit(String member, String processor, long at, long groups, long roots) {
            point(member, processor, at, NestStateMetricNames.ROOTS_RENDERED, roots);
            point(member, processor, at, NestStateMetricNames.ROOTS_RENDERED_SINCE, 1);
            point(member, processor, at, NestStateMetricNames.COMMIT_SINCE, 1);
            point(member, processor, at, NestStateMetricNames.COMMIT_GROUPS, groups);
            point(member, processor, at, NestStateMetricNames.COMMIT_KEYS_SUM, groups * 2);
            point(member, processor, at, NestStateMetricNames.COMMIT_ROUND_TRIPS_SUM, groups * 2);
            point(member, processor, at, NestStateMetricNames.COMMIT_NANOS_SUM, groups * 1000);
            buckets(member, processor, at, NestStateMetricNames.COMMIT_KEYS_BUCKET, HistogramBounds.STATE_COMMIT_KEYS, groups);
            buckets(member, processor, at, NestStateMetricNames.COMMIT_ROUND_TRIPS_BUCKET, HistogramBounds.STATE_COMMIT_ROUND_TRIPS, groups);
            buckets(member, processor, at, NestStateMetricNames.COMMIT_NANOS_BUCKET, HistogramBounds.STATE_COMMIT_DURATION, groups);
        }
        void buckets(String member, String processor, long at, String kind, HistogramBounds bounds, long groups) {
            for (int i = 0; i < bounds.buckets(); i++) point(member, processor, at, kind + i, i == 1 ? groups : 0);
        }
        void weight(String member, String processor, long at, long written, long largest, long folds) {
            point(member, processor, at, NestStateMetricNames.WEIGHT_SINCE, 1);
            point(member, processor, at, NestStateMetricNames.DOCUMENT_COUNT, written);
            point(member, processor, at, NestStateMetricNames.DOCUMENT_BYTES_SUM, written * 1000);
            point(member, processor, at, NestStateMetricNames.LARGEST_DOCUMENT_BYTES, largest);
            point(member, processor, at, NestStateMetricNames.COMMIT_BYTES_COUNT, written);
            point(member, processor, at, NestStateMetricNames.COMMIT_BYTES_SUM, written * 1000);
            point(member, processor, at, NestStateMetricNames.FOLD_COUNT, folds);
            point(member, processor, at, NestStateMetricNames.FOLD_NANOS_SUM, folds * 50_000);
            buckets(member, processor, at, NestStateMetricNames.DOCUMENT_BYTES_BUCKET, HistogramBounds.STATE_DOCUMENT_BYTES, written);
            buckets(member, processor, at, NestStateMetricNames.COMMIT_BYTES_BUCKET, HistogramBounds.STATE_COMMIT_BYTES, written);
            buckets(member, processor, at, NestStateMetricNames.FOLD_NANOS_BUCKET, HistogramBounds.ROLLUP_FOLD_DURATION, folds);
        }
        JobMetrics metrics() { return JobMetrics.of(values); }
    }
}
