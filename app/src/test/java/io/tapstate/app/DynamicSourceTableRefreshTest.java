package io.tapstate.app;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class DynamicSourceTableRefreshTest {
    @Test void aBlockedDatabaseInventoryCannotBlockConvergence() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var reads = new AtomicInteger();
        try (var refresh = new DynamicSourceTableRefresh(id -> {
            reads.incrementAndGet(); entered.countDown();
            try { release.await(); } catch (InterruptedException ending) { Thread.currentThread().interrupt(); }
        }, Duration.ofNanos(1))) {
            assertTimeoutPreemptively(Duration.ofSeconds(1), () -> refresh.consider(List.of("second", "first")));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
                for (int i = 0; i < 10_000; i++) refresh.consider(List.of("second", "first"));
            });
            assertThat(reads).hasValue(1);
        } finally { release.countDown(); }
    }
}
