package io.tapstate.runtime.srs;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.ringbuffer.Ringbuffer;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A held empty ring read is not business projection, and every poll releases its timing boundary. */
class AnEmptySourcePollDoesNotBecomeActiveWorkTest {

    @Test
    @SuppressWarnings("unchecked")
    void aBlockedEmptyReadIsInactiveAndAFailedReadReleasesItsSlot() throws Exception {
        Address address = Address.createUnresolvedAddress("127.0.0.1", 5701);
        ProcessorMetaSupplier meta = SrsSourceProcessor.metaSupplier("flow", "srs.empty", "orders",
                StartFrom.earliest(), 1, SrsReadCursorPublisherFactory.NONE, SourcePlacement.on(address));
        SrsSourceProcessor source = (SrsSourceProcessor) meta.get(List.of(address)).apply(address).get(1).iterator().next();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean();
        Ringbuffer<SrsItem> ring = (Ringbuffer<SrsItem>) Proxy.newProxyInstance(Ringbuffer.class.getClassLoader(),
                new Class<?>[] { Ringbuffer.class }, (proxy, method, arguments) -> {
                    if (method.getName().equals("tailSequence")) {
                        entered.countDown();
                        if (!released.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("empty read was not released");
                        }
                        if (failed.get()) {
                            throw new IllegalStateException("ring read failed");
                        }
                        return -1L;
                    }
                    throw new AssertionError("unexpected ring call: " + method.getName());
                });
        var reader = SrsSourceProcessor.class.getDeclaredField("reader");
        reader.setAccessible(true);
        reader.set(source, new SrsRingReader(new SrsRingbuffer(ring), 0));
        CompletableFuture<Boolean> empty = CompletableFuture.supplyAsync(source::complete);
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(source.timing().isActive()).isFalse();
            assertThat(source.timing().value().count()).isZero();
        } finally {
            released.countDown();
        }
        assertThat(empty.get(5, TimeUnit.SECONDS)).isFalse();
        source.complete();
        assertThat(source.timing().isActive()).isFalse();
        assertThat(source.timing().value().count()).isZero();
        failed.set(true);
        assertThatThrownBy(source::complete).isInstanceOf(IllegalStateException.class)
                .hasMessage("ring read failed");
        assertThat(source.timing().isActive()).isFalse();
        assertThat(source.timing().value().count()).isZero();
        failed.set(false);
        source.complete();
        assertThat(source.timing().value().count()).isZero();
    }
}
