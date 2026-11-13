package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeferredSinkAckTest {
    @Test void flushKeepsEachStreamsHighestConfirmedOrderAndLastToken() {
        var clock = new AtomicLong();
        var writes = new ArrayList<String>();
        var ack = new DeferredSinkAck(recording(writes), () -> true, clock::get);
        ack.advance("orders", at(1));
        ack.advance("orders", at(2));
        ack.bounded("orders", new SourceOrder(1, 3));
        ack.advance("items", at(1));
        clock.set(TimeUnit.SECONDS.toNanos(9));
        ack.flush(false);
        assertThat(writes).isEmpty();

        clock.set(TimeUnit.SECONDS.toNanos(10));
        ack.flush(false);
        ack.flush(true);
        assertThat(writes).containsExactly("orders:token-2", "orders:bound-3", "items:token-1");
    }

    @Test void closeFlushesOnlyWhileTheOriginalWriterIsAuthorized() {
        var active = new AtomicBoolean(true);
        var writes = new ArrayList<String>();
        var ack = new DeferredSinkAck(recording(writes), active::get);
        ack.advance("orders", at(1));
        ack.flushOnClose();
        ack.advance("orders", at(2));
        active.set(false);
        ack.flushOnClose();
        assertThat(writes).containsExactly("orders:token-1");
    }

    @Test void failedFlushSurfacesAndDoesNotRetryOnClose() {
        var writes = new AtomicLong();
        var failure = new IllegalStateException("store refused the original writer");
        var ack = new DeferredSinkAck((stream, position) -> { writes.incrementAndGet(); throw failure; }, () -> true);
        ack.advance("orders", at(1));
        assertThatThrownBy(() -> ack.flush(true)).isSameAs(failure);
        ack.flushOnClose();
        assertThat(writes).hasValue(1);
    }

    private static ChainPosition at(long seq) {
        return new ChainPosition(new SourceOrder(1, seq), "token-" + seq);
    }

    private static SinkAck recording(List<String> writes) {
        return new SinkAck() {
            @Override public void advance(String stream, ChainPosition position) { writes.add(stream + ":" + position.token()); }
            @Override public void bounded(String stream, SourceOrder through) { writes.add(stream + ":bound-" + through.seq()); }
        };
    }
}
