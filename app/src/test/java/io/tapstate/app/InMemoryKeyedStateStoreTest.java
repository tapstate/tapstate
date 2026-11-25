package io.tapstate.app;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryKeyedStateStoreTest {
    @Test void parallelCaptureWritersKeepEveryCountAndOneAtomicWinner() throws Exception {
        var store = new InMemoryKeyedStateStore();
        store.save("neighbor", "keep", new byte[] {99});
        var ready = new CountDownLatch(8);
        var start = new CountDownLatch(1);
        var winners = new AtomicInteger();
        var winner = new AtomicReference<byte[]>();
        try (var workers = Executors.newFixedThreadPool(8)) {
            var writes = new ArrayList<Future<?>>();
            for (int writer = 0; writer < 8; writer++) {
                int identity = writer;
                writes.add(workers.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                    byte[] value = {(byte) identity};
                    for (int row = 0; row < 128; row++) store.save("shared", identity + ":" + row, value);
                    if (store.saveIfAbsent("once", "winner", value).isEmpty()) {
                        winners.incrementAndGet(); winner.set(value);
                    }
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            for (var write : writes) write.get(5, TimeUnit.SECONDS);
        } finally { start.countDown(); }
        assertThat(store.count("shared")).isEqualTo(1024);
        for (int writer = 0; writer < 8; writer++)
            for (int row = 0; row < 128; row++)
                assertThat(store.load("shared", writer + ":" + row)).contains(new byte[] {(byte) writer});
        assertThat(winners).hasValue(1);
        assertThat(store.load("once", "winner")).contains(winner.get());
        store.dropNamespace("shared");
        assertThat(store.count("shared")).isZero();
        assertThat(store.load("neighbor", "keep")).contains(new byte[] {99});
        assertThat(store.count("once")).isOne();
    }
}
