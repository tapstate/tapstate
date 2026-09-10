package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchActionCoordinatorTest {

    @Test
    void closeSuppressesACompletionAlreadyQueuedForTheRenderThread() throws Exception {
        BlockingQueue<Runnable> callbacks = new LinkedBlockingQueue<>();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), callbacks::add, () -> true, event -> {
                });
        WorkbenchActionCoordinator coordinator = new WorkbenchActionCoordinator(runtime);
        CountDownLatch actionCompleted = new CountDownLatch(1);
        AtomicBoolean delivered = new AtomicBoolean();
        coordinator.submit(() -> {
            actionCompleted.countDown();
            return "done";
        }, failure -> "failed", result -> delivered.set(true));
        assertThat(actionCompleted.await(2, TimeUnit.SECONDS)).isTrue();
        Runnable callback = callbacks.poll(2, TimeUnit.SECONDS);
        assertThat(callback).isNotNull();

        coordinator.close();
        callback.run();

        assertThat(delivered).isFalse();
    }

    @Test
    void deliversTypedFailureWhenAnActionThrows() throws Exception {
        BlockingQueue<Runnable> callbacks = new LinkedBlockingQueue<>();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), callbacks::add, () -> true, event -> {
                });
        WorkbenchActionCoordinator coordinator = new WorkbenchActionCoordinator(runtime);
        try {
            coordinator.submit(
                    () -> {
                        throw new IllegalStateException("unavailable");
                    },
                    failure -> "unavailable",
                    result -> assertThat(result).isEqualTo("unavailable"));

            Runnable callback = callbacks.poll(2, TimeUnit.SECONDS);
            assertThat(callback).isNotNull();
            callback.run();
        } finally {
            coordinator.close();
        }
    }
}
