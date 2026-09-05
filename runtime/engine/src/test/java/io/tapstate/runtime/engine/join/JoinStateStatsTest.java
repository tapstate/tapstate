package io.tapstate.runtime.engine.join;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which rebuilds of a dimension key's fan-out are put in front of anybody.
 *
 * <p>The two cases here are a pair, and neither means anything alone. Asserting only that a large
 * rebuild is reported passes an implementation that reports every one of them - which is the noisy
 * version, and noise is exactly how a number meant to catch a rare, minutes-long wait comes to be
 * scrolled past. Asserting only that a small one is ignored passes an implementation that reports
 * nothing at all.
 */
class JoinStateStatsTest {

    private static final String NAMESPACE = "join.orders.c";

    @Test
    @DisplayName("a rebuild small enough to be over in an instant is not reported")
    void aSmallFanOutIsNotWorthReporting() {
        JoinStateStats stats = new JoinStateStats();

        stats.recomputing(NAMESPACE, "customer-7", 0, 3);
        stats.recomputing(NAMESPACE, "customer-7", 3, 3);

        assertThat(stats.recomputeKey(NAMESPACE)).isNull();
        assertThat(stats.recomputeDone(NAMESPACE)).isZero();
        assertThat(stats.recomputeExpected(NAMESPACE)).isZero();
    }

    @Test
    @DisplayName("a rebuild large enough to be a wait is reported, and its progress advances")
    void aLargeFanOutIsReportedAsItGoes() {
        JoinStateStats stats = new JoinStateStats();

        stats.recomputing(NAMESPACE, "customer-7", 0, JoinStateStats.REPORT_FANOUT_ABOVE);
        assertThat(stats.recomputeKey(NAMESPACE)).isEqualTo("customer-7");
        assertThat(stats.recomputeDone(NAMESPACE)).isZero();

        stats.recomputing(NAMESPACE, "customer-7", 4_000, JoinStateStats.REPORT_FANOUT_ABOVE);

        assertThat(stats.recomputeDone(NAMESPACE)).isEqualTo(4_000);
        assertThat(stats.recomputeExpected(NAMESPACE))
                .isEqualTo(JoinStateStats.REPORT_FANOUT_ABOVE);
    }

    @Test
    @DisplayName("one namespace's rebuild is not reported under another's")
    void namespacesAreKeptApart() {
        JoinStateStats stats = new JoinStateStats();

        stats.recomputing(NAMESPACE, "customer-7", 0, JoinStateStats.REPORT_FANOUT_ABOVE);

        assertThat(stats.recomputeKey("join.orders.d")).isNull();
    }
}
