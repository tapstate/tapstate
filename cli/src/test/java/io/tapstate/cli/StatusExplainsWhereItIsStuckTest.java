package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StatusExplainsWhereItIsStuckTest {

    @Test
    void aMinuteScalePauseRemainsAStoppedChain() {
        MetricsFacts metrics = MetricsFacts.of(Map.of(
                "recordCount", 128L,
                "frontierStalledMillis.orders", 96_000L));

        assertThat(metrics.stalledChains()).containsExactly(Map.entry("orders", 96_000L));
    }

    @Test
    void millisecondScalePausesAreNotCalledStoppedChains() {
        MetricsFacts metrics = MetricsFacts.of(Map.of(
                "reconcileFailuresInARow", 0L,
                "recordCount", 11L,
                "frontierStalledMillis.shipments", 9L,
                "frontierStalledMillis.orders", 196L));

        assertThat(metrics.stalledChains()).isEmpty();
    }
}
