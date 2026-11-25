package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.*;

class NativeBusinessShutdownTest {
    @Test
    void retryAttemptsMemberStopAgainAfterAStopFailure() {
        var root = mock(StorageRoot.class, RETURNS_DEEP_STUBS);
        try (var child = new GenericApplicationContext()) {
            var owner = new NativeBusinessContext(root, null, child);
            var member = mock(HazelcastInstance.class);
            var lifecycle = mock(LifecycleService.class);
            var running = new AtomicBoolean(true);
            when(member.getLifecycleService()).thenReturn(lifecycle);
            when(lifecycle.isRunning()).thenAnswer(call -> running.get());
            doThrow(new IllegalStateException("first member stop failed"))
                    .doAnswer(call -> { running.set(false); return null; }).when(member).shutdown();
            var periodic = mock(PeriodicObservations.class);
            var drainFailure = StorageRuntime.unavailable();
            doThrow(drainFailure).when(periodic).awaitStopped(anyLong());
            child.getBeanFactory().registerSingleton("hazelcastMember", member);
            child.getBeanFactory().registerSingleton("periodicObservations", periodic);
            child.refresh();
            ReflectionTestUtils.setField(owner, "context", child);
            var storage = mock(NativeMemberStorage.class);
            ReflectionTestUtils.setField(owner, "memberStorage", storage);

            assertThat(catchThrowable(owner::close)).isSameAs(drainFailure);
            assertThat(running).isTrue();
            assertThat(catchThrowable(owner::close)).isSameAs(drainFailure);
            assertThat(running).isFalse();
            verify(member, times(2)).shutdown();
            verify(storage, never()).close();
            doNothing().when(periodic).awaitStopped(anyLong());
            owner.close();
            verify(storage).close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedWorkerDrainStillClosesTheMemberAndPreservesTheFailure(boolean drainError) throws InterruptedException {
        var root = mock(StorageRoot.class, RETURNS_DEEP_STUBS);
        var worker = Executors.newSingleThreadExecutor();
        var release = new CountDownLatch(1);
        worker.submit(() -> { release.await(); return null; });
        try (var child = new GenericApplicationContext()) {
            var owner = new NativeBusinessContext(root, null, child);
            var member = mock(HazelcastInstance.class);
            var lifecycle = mock(LifecycleService.class);
            when(member.getLifecycleService()).thenReturn(lifecycle);
            var running = new AtomicBoolean(true);
            when(lifecycle.isRunning()).thenAnswer(call -> running.get());
            doAnswer(call -> { running.set(false); return null; }).when(member).shutdown();
            WorkloadClaimStore claims = spy(new InMemoryWorkloadClaimStore());
            var key = new WorkloadClaimKey("shutdown-test", WorkloadClaimType.NODE_SESSION, "node-a");
            var ttl = Duration.ofSeconds(30);
            long askedAt = System.nanoTime();
            var claim = claims.acquire(key, new WorkloadOwner("node-a", "old-boot"), 0, ttl).claim();
            doAnswer(call -> {
                assertThat(running).isFalse();
                return call.callRealMethod();
            }).when(claims).release(any());
            var startup = new NodeSessionStartup(claims, claim, askedAt, ttl, Duration.ofSeconds(5));
            startup.handOff(member);
            var periodic = mock(PeriodicObservations.class);
            doAnswer(call -> { worker.shutdown(); return null; }).when(periodic).close();
            var failure = new AtomicReference<Throwable>();
            var error = new AssertionError("worker settlement failed");
            doAnswer(call -> {
                try {
                    ShutdownAwait.executor(worker, call.getArgument(0));
                    return null;
                } catch (RuntimeException timeout) {
                    if (drainError) {
                        error.addSuppressed(timeout);
                        failure.set(error);
                        throw error;
                    }
                    failure.set(timeout);
                    throw timeout;
                }
            }).when(periodic).awaitStopped(anyLong());
            child.registerBean("hazelcastMember", HazelcastInstance.class, () -> member,
                    definition -> ((org.springframework.beans.factory.support.AbstractBeanDefinition) definition)
                            .setDestroyMethodName("shutdown"));
            child.registerBean("nodeSessionLease", NodeSessionLease.class, startup::lease,
                    definition -> definition.setDependsOn("hazelcastMember"));
            child.getBeanFactory().registerSingleton("periodicObservations", periodic);
            child.refresh();
            ReflectionTestUtils.setField(owner, "context", child);
            var storage = mock(NativeMemberStorage.class);
            ReflectionTestUtils.setField(owner, "memberStorage", storage);

            var thrown = catchThrowable(() -> ShutdownAwait.within(Duration.ofMillis(250), owner::close));
            assertThat(thrown).isNotNull().isSameAs(failure.get());
            assertThat(running).isFalse();
            assertThat(child.isActive()).isTrue();
            verify(storage, never()).close();
            verify(claims).release(argThat(ending -> ending.key().equals(key)
                    && ending.owner().equals(claim.owner()) && ending.claimGeneration() == claim.claimGeneration()));
            assertThat(claims.read(key).orElseThrow().leased()).isFalse();

            var retry = catchThrowable(() -> ShutdownAwait.within(Duration.ofMillis(250), owner::close));
            assertThat(retry).isNotNull().isSameAs(failure.get());
            release.countDown();
            assertThat(worker.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
            owner.close();
            verify(storage).close();
            assertThat(child.isActive()).isFalse();
        } finally {
            release.countDown();
            worker.shutdownNow();
        }
    }
}
