package io.tapstate.runtime.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class StopWaitLogTest {
    @Test void oneNodeAndCloseSiteReportsOneWarningAndOneSuccessfulExit() throws Exception {
        var notices = new LinkedBlockingQueue<StopWaitLog.Notice>();
        var log = new StopWaitLog(Duration.ofMillis(200), notices::add);
        var site = new StopWaitLog.Site("orders", "job/execution", "member-1", "SinkWriter.close");
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        Runnable close = () -> log.track(site, () -> hold(entered, release));
        try (var closing = Executors.newFixedThreadPool(2)) {
            try {
                var first = closing.submit(close);
                var second = closing.submit(close);
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var warning = notices.poll(5, TimeUnit.SECONDS);
                assertThat(warning).isNotNull();
                assertThat(warning.site()).isEqualTo(site);
                assertThat(warning.recovered()).isFalse();
                assertThat(warning.stack()).contains("StopWaitLogTest.hold");
                assertThat(notices.poll(300, TimeUnit.MILLISECONDS)).isNull();
                release.countDown();
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
                var recovered = notices.poll(5, TimeUnit.SECONDS);
                assertThat(recovered).isNotNull();
                assertThat(recovered.site()).isEqualTo(site);
                assertThat(recovered.recovered()).isTrue();
                assertThat(recovered.elapsedMillis()).isGreaterThan(warning.elapsedMillis());
                assertThat(notices).isEmpty();
            } finally { release.countDown(); }
        }
    }

    private static void hold(CountDownLatch entered, CountDownLatch release) {
        entered.countDown();
        try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
