package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

class RetiredPipelinePayloadShutdownTest {
    @Test void uncertainWorkerShutdownEscapesTheSpringDestroyCallbackAndCanBeRetried() throws Exception {
        var scope = new CacheBusinessScope("cluster", 1, "cache");
        var fence = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"),
                new WorkloadOwner("node", "boot"), 1, 0, 1);
        var bound = mock(StorageRuntime.Bound.class, RETURNS_DEEP_STUBS);
        when(bound.preparation().businessScope()).thenReturn(scope);
        var store = mock(RetiredPipelinePayloads.class);
        when(bound.cache().retiredPipelinePayloads(eq(scope), any())).thenReturn(store);
        var node = mock(NodeSessionLease.class); when(node.active()).thenReturn(true); when(node.currentFence()).thenReturn(Optional.of(fence));
        var owner = new NativeBusinessContext(mock(StorageRoot.class, RETURNS_DEEP_STUBS), bound, mock(ConfigurableApplicationContext.class));
        ((AtomicBoolean) ReflectionTestUtils.getField(owner, "active")).set(true);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        when(store.listAfter(isNull(), eq(1))).thenAnswer(call -> {
            entered.countDown();
            while (release.getCount() != 0) {
                try { release.await(); } catch (InterruptedException ignored) { /* Model an interruption-resistant storage call. */ }
            }
            return List.of();
        });
        var worker = new AtomicReference<RetiredPipelinePayloadCleanup>();
        try (var context = new GenericApplicationContext()) {
            context.registerBean("retiredPipelinePayloadCleanup", RetiredPipelinePayloadCleanup.class, () -> {
                var value = new NativeBusinessContext.Wiring().retiredPipelinePayloadCleanup(owner, node); worker.set(value); return value;
            }, definition -> { definition.setInitMethodName("start"); definition.setDestroyMethodName("close"); });
            ReflectionTestUtils.setField(owner, "context", context);
            context.refresh();
            try {
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(owner::close).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                assertThatThrownBy(owner::close).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                release.countDown(); owner.close();
            } finally {
                release.countDown(); if (worker.get() != null) worker.get().close();
            }
            owner.close();
        }
    }
}
