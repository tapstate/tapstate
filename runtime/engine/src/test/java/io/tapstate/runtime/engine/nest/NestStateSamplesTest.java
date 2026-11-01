package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
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
        JobMetrics metrics() { return JobMetrics.of(values); }
    }
}
