package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NativeWorkerShutdownTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aWorkerMustExitBeforeMemberStorageCloses(boolean convergence) throws Exception {
        var root = mock(StorageRoot.class);
        when(root.runtime()).thenReturn(mock(StorageRuntime.class));
        var nativeContext = new NativeBusinessContext(root, null, null);
        var storage = mock(NativeMemberStorage.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var child = new GenericApplicationContext();
                var observations = new PeriodicObservations(1, 1);
                var convergences = new PipelineConvergences(1, 1)) {
            child.getBeanFactory().registerSingleton("periodicObservations", observations);
            child.getBeanFactory().registerSingleton("pipelineConvergences", convergences);
            child.refresh();
            ReflectionTestUtils.setField(nativeContext, "context", child);
            ReflectionTestUtils.setField(nativeContext, "memberStorage", storage);

            Runnable held = () -> {
                entered.countDown();
                try {
                    assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException stopped) {
                    // Model I/O that needs to finish its cleanup after receiving an interrupt.
                    try {
                        assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException interruptedAgain) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            if (convergence) convergences.offer("orders", held);
            else observations.offer("orders", "run", ignored -> held.run());
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                long deadline = System.nanoTime() + Duration.ofMillis(200).toNanos();
                assertThatThrownBy(() -> nativeContext.close(deadline)).isInstanceOf(TapstateException.class);
                verify(storage, never()).close();
            } finally {
                release.countDown();
            }
            nativeContext.close();
            verify(storage).close();
        }
    }
}
