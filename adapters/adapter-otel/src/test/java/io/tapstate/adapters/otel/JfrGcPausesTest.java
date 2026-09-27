package io.tapstate.adapters.otel;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class JfrGcPausesTest {

    @Test
    void sumsObservedPausesFromTheStreamEpochAndDropsIncompleteEpochs() {
        Instant startedAt = Instant.parse("2026-09-27T00:00:00Z");
        try (JfrGcPauses pauses = new JfrGcPauses(null, startedAt)) {
            assertThat(pauses.snapshot()).isEmpty();
            pauses.record(Duration.ofMillis(4));
            pauses.record(Duration.ofNanos(500));
            assertThat(pauses.snapshot()).contains(new JfrGcPauses.Reading(startedAt, 4_000_500));
            pauses.invalidate("test data loss", null);
            assertThat(pauses.snapshot()).isEmpty();
            pauses.record(Duration.ofMillis(9));
            assertThat(pauses.snapshot()).isEmpty();
        }
    }
}
