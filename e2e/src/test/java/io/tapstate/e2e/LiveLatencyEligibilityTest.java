package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LiveLatencyEligibilityTest {
    @Test
    void onlyCorrelatedAndComparableSamplesProduceP99() {
        var samples = IntStream.rangeClosed(1, 10000)
                .mapToObj(index -> new LiveLatencyEligibility.Sample("event-" + index, index)).toList();
        var rawAggregate = LiveLatencyEligibility.assess(10000, List.of(), false, 10000, 0.999);
        assertNull(rawAggregate.p99Nanos());
        assertNull(LiveLatencyEligibility.assess(10010, samples, false, 10000, 0.999).p99Nanos());
        assertNull(LiveLatencyEligibility.assess(10000, samples.subList(0, 9999), true, 10000, 0.999).p99Nanos());
        assertNull(LiveLatencyEligibility.assess(10100, samples, true, 10000, 0.999).p99Nanos());
        assertEquals(9900L, LiveLatencyEligibility.assess(10010, samples, true, 10000, 0.999).p99Nanos());
        assertThrows(IllegalArgumentException.class, () -> LiveLatencyEligibility.assess(10000,
                List.of(samples.getFirst(), samples.getFirst()), true, 10000, 0.999));
    }
}
