package io.tapstate.app;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PipelineStartWorkTest {
    @Test void twoBusyHelpersLeaveTheNextBranchInTheCaller() throws InterruptedException {
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var caller = Thread.currentThread();
        try (var first = new PipelineStartWork<Thread>(); var second = new PipelineStartWork<Thread>();
                var saturated = new PipelineStartWork<Thread>()) {
            try {
                first.tryStart(() -> awaitRelease(entered, release));
                second.tryStart(() -> awaitRelease(entered, release));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                saturated.tryStart(() -> { throw new AssertionError("a third helper was admitted"); });
                assertThat(saturated.awaitOrRun(Thread::currentThread)).isSameAs(caller);
            } finally {
                release.countDown();
            }
            assertThat(first.awaitOrRun(Thread::currentThread)).isNotSameAs(caller);
            assertThat(second.awaitOrRun(Thread::currentThread)).isNotSameAs(caller);
        }
    }

    private static Thread awaitRelease(CountDownLatch entered, CountDownLatch release) {
        entered.countDown();
        try {
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
        return Thread.currentThread();
    }
}
