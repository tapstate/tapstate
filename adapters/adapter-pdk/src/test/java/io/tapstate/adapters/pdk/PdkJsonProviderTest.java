package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.JsonParser;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkJsonProviderTest {
    @Test
    void concurrentColdAccessCannotObserveThePartiallyInitializedFactory() throws Exception {
        Object factoryMonitor = new Object();
        CountDownLatch published = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger initializations = new AtomicInteger();
        JsonParser parser = parser();
        PdkJsonProvider provider = new PdkJsonProvider(() -> {
            if (initializations.incrementAndGet() != 1) {
                throw new IllegalStateException("factory methods have not been published");
            }
            published.countDown();
            await(finish);
            return parser;
        }, factoryMonitor);
        AtomicReference<Thread> contender = new AtomicReference<>();
        CompletableFuture<JsonParser> first = CompletableFuture.supplyAsync(provider::get);
        CompletableFuture<JsonParser> second = null;
        try {
            assertThat(published.await(5, TimeUnit.SECONDS)).isTrue();
            second = CompletableFuture.supplyAsync(() -> {
                contender.set(Thread.currentThread());
                return provider.get();
            });
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (!second.isDone() && (contender.get() == null
                    || contender.get().getState() != Thread.State.BLOCKED) && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(second).isNotDone();
            assertThat(contender.get().getState()).isEqualTo(Thread.State.BLOCKED);
            assertThat(initializations).hasValue(1);
        } finally {
            finish.countDown();
        }
        assertThat(first.get(5, TimeUnit.SECONDS)).isSameAs(parser);
        assertThat(second.get(5, TimeUnit.SECONDS)).isSameAs(parser);
        assertThat(provider.get()).isSameAs(parser);
        assertThat(initializations).hasValue(1);
    }

    @Test
    void aFailedInitializationIsNotPublishedAsReady() {
        AtomicInteger attempts = new AtomicInteger();
        JsonParser parser = parser();
        PdkJsonProvider provider = new PdkJsonProvider(() -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("not initialized");
            return parser;
        }, new Object());
        assertThatThrownBy(provider::get).isInstanceOf(IllegalStateException.class).hasMessage("not initialized");
        assertThat(provider.get()).isSameAs(parser);
        assertThat(provider.get()).isSameAs(parser);
        assertThat(attempts).hasValue(2);
    }

    private static JsonParser parser() {
        return (JsonParser) Proxy.newProxyInstance(JsonParser.class.getClassLoader(),
                new Class<?>[]{JsonParser.class}, (proxy, method, arguments) -> {
                    throw new AssertionError("provider lookup must not serialize");
                });
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("initializer was not released");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
