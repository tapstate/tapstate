package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TakeoverBoundTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Test
    void theStoreDefaultAgainstTheDefaultLeaseSaysNothing() {
        assertThat(TakeoverBound.warning(Optional.of(Duration.ofSeconds(60)), LEASE)).isEmpty();
    }

    @Test
    void aStoreThatDoesNotSaySaysNothing() {
        assertThat(TakeoverBound.warning(Optional.empty(), LEASE)).isEmpty();
    }

    @Test
    void aLifetimeWellAboveTheLeaseIsNamedWithBothNumbers() {
        assertThat(TakeoverBound.warning(Optional.of(Duration.ofSeconds(600)), LEASE))
                .hasValueSatisfying(text -> assertThat(text).contains("600 s").contains("30 s")
                        .contains("transactionLifetimeLimitSeconds"));
    }
}
