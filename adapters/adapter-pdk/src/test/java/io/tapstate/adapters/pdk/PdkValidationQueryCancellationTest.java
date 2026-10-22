package io.tapstate.adapters.pdk;

import io.tapstate.core.validation.ValidationValues;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkValidationQueryCancellationTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 12);

    @Test void cancellationAfterCleanupStartsDoesNotInterruptItOrPretendTheWorkerExited() throws Exception {
        var cancellation = new PdkValidationQueryCancellation();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var interrupted = new AtomicBoolean(); var completed = new AtomicBoolean();
        try (var io = Executors.newSingleThreadExecutor(); var cursor = new PdkValidationCursor("records", rows -> {
            boolean prior = cancellation.enterCleanup();
            entered.countDown();
            try { release.await(); completed.set(true); }
            catch (InterruptedException failure) { interrupted.set(true); throw failure; }
            finally { if (prior) Thread.currentThread().interrupt(); }
        }, row -> { throw new AssertionError("No business row"); }, () -> {}, () -> {}, io, 1, ROW, cancellation)) {
            try {
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                cursor.close();
                assertThat(cursor.workersExited().toCompletableFuture()).isNotDone();
                assertThat(completed).isFalse();
            } finally { release.countDown(); }
            cursor.workersExited().handle((value, failure) -> null).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(interrupted).isFalse(); assertThat(completed).isTrue();
        } finally { release.countDown(); }
    }

    @Test void cancellationStillInterruptsAnActiveQueryBeforeCleanup() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var interrupted = new AtomicBoolean();
        try (var io = Executors.newSingleThreadExecutor(); var cursor = new PdkValidationCursor("records", rows -> {
            entered.countDown();
            try { release.await(); }
            catch (InterruptedException failure) { interrupted.set(true); throw failure; }
        }, row -> { throw new AssertionError("No business row"); }, () -> {}, () -> {}, io, 1, ROW)) {
            try {
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                cursor.close();
                cursor.workersExited().handle((value, failure) -> null).toCompletableFuture().get(3, TimeUnit.SECONDS);
                assertThat(interrupted).isTrue();
            } finally { release.countDown(); }
        } finally { release.countDown(); }
    }
}
