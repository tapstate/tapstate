package io.tapstate.runtime.engine.join;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.within;

class JoinBenchComparisonTest {
    @Test
    void fivefoldSlowdownDoesNotBecomeAnOperatorRegression() {
        var sample = comparison(300, 30, 10, 10, 50, 50);
        assertThat(sample.ratio()).isCloseTo(10.0, within(1e-9));
    }

    @Test
    void reversingTheDriftDoesNotHideARealDoubling() {
        var sample = comparison(600, 30, 50, 10, 10, 50);
        assertThat(sample.ratio()).isCloseTo(20.0, within(1e-9));
    }

    @Test
    void unequalSetupAndShutdownTimesDoNotPutTheCarrierHalfwayBetweenControls() {
        var sample = comparison(200, 20, 10, 10, 50, 50);
        assertThat(sample.ratio()).isCloseTo(10.0, within(1e-9));
    }

    @Test
    void eachCarrierHasControlsOnBothSidesAndKeepsTheirActualTimes() {
        List<String> order = new ArrayList<>();
        AtomicInteger position = new AtomicInteger();
        var sample = JoinBenchComparison.measure(() -> {
            order.add("heap");
            int slot = position.incrementAndGet();
            return new JoinBenchComparison.Timing(slot % 2 == 0 ? 10 : 50, slot);
        }, () -> {
            order.add("carrier");
            return new JoinBenchComparison.Timing(100, position.incrementAndGet());
        }, 2);
        assertThat(order).containsExactly("heap", "heap", "carrier", "heap", "heap");
        assertThat(sample.before()).isEqualTo(new JoinBenchComparison.Timing(10, 2));
        assertThat(sample.after()).isEqualTo(new JoinBenchComparison.Timing(10, 4));
        assertThat(sample.ratio()).isEqualTo(10.0);
    }

    @Test
    void fastestRatioKeepsTheControlsFromThatTrial() {
        var early = comparison(100, 20, 5, 10, 5, 30);
        var late = comparison(200, 50, 20, 40, 20, 60);
        assertThat(JoinBenchComparison.best(List.of(early, late))).isSameAs(late);
        assertThat(JoinBenchComparison.best(List.of(early, late)).ratio()).isEqualTo(10.0);
    }

    @Test
    void anUnbracketedOrEmptyMeasurementCannotProduceAPlausibleRatio() {
        assertThatIllegalArgumentException().isThrownBy(() -> comparison(100, 5, 10, 10, 10, 20));
        assertThatIllegalArgumentException().isThrownBy(() -> comparison(100, 30, 10, 10, 10, 20));
        assertThatIllegalArgumentException().isThrownBy(() -> comparison(100, 10, 10, 10, 10, 10));
        assertThatIllegalArgumentException().isThrownBy(() -> comparison(0, 15, 10, 10, 10, 20));
        assertThatIllegalArgumentException().isThrownBy(() -> JoinBenchComparison.measure(
                () -> new JoinBenchComparison.Timing(10, 10),
                () -> new JoinBenchComparison.Timing(100, 20), 0));
    }

    private static JoinBenchComparison comparison(long carrier, long carrierAt,
            long before, long beforeAt, long after, long afterAt) {
        return new JoinBenchComparison(new JoinBenchComparison.Timing(carrier, carrierAt),
                new JoinBenchComparison.Timing(before, beforeAt),
                new JoinBenchComparison.Timing(after, afterAt));
    }
}
