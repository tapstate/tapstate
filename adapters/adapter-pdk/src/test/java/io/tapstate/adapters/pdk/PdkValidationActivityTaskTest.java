package io.tapstate.adapters.pdk;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkValidationActivityTaskTest {
    @Test void cancelledOrRepeatedFutureRunCannotAnnounceExitWhileItsBodyIsStillActive() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var task = new PdkValidationActivityTask(() -> { entered.countDown(); uninterruptibly(release); return null; });
            executor.execute(task);
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            task.cancel(true);
            assertThat(task.isDone()).isTrue();
            assertThat(task.exited().toCompletableFuture().isDone()).isFalse();
            task.run();
            boolean premature = task.exited().toCompletableFuture().isDone();
            release.countDown(); executor.submit(() -> {}).get(3, TimeUnit.SECONDS);
            assertThat(premature).isFalse();
            assertThat(task.exited().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
        } finally { release.countDown(); }
    }
    private static void uninterruptibly(CountDownLatch latch) {
        while (true) {
            try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture timed out"); return; }
            catch (InterruptedException ignored) { }
        }
    }
}
