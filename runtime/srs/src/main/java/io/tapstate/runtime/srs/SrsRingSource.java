package io.tapstate.runtime.srs;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.Objects;

/**
 * The self-built Jet source over one per-table change ring. Jet has no native Hazelcast-Ringbuffer
 * source, so this builds one: a stream source whose per-member reader ({@link SrsRingReader}) tails the
 * ring by sequence and feeds each change into the Jet pipeline, bounded per fill so it honours Jet
 * backpressure.
 *
 * <p>The source is deliberately not fault-tolerant — it sets no snapshot or restore function. Its read
 * position never enters a Jet snapshot: the offset truth lives in the durable coordination store, and an
 * L1 restart re-mines the ring and replays it from the head rather than resuming from execution state.
 * The reader resolves the ring from the member it runs on, so nothing but the ring name crosses the wire.
 */
public final class SrsRingSource {

    /** The most changes one fill drains before yielding — a bounded batch that lets Jet pace the source. */
    private static final int FILL_BATCH = 256;

    private SrsRingSource() {
    }

    /**
     * A Jet stream source that tails the named change ring in sequence order from the {@code start} point,
     * reporting no read progress. As {@link #create(String, StartFrom, SrsReadCursorPublisherFactory)} with
     * no cursor wiring.
     */
    public static StreamSource<SrsItem> create(String ringName, StartFrom start) {
        return create(ringName, start, SrsReadCursorPublisherFactory.NONE);
    }

    /**
     * A Jet stream source that tails the named change ring in sequence order from the {@code start} point
     * and reports its read progress through {@code publisherFactory}. The start point is resolved against
     * the ring on the member that owns it — {@code earliest} replays from the head, {@code latest} takes
     * only changes from now on, and an instant starts at the first change at or after it. As the per-member
     * reader drains the ring it publishes the last sequence it read, the signal the write-side headroom gate
     * reads back as this consumer's cursor.
     *
     * <p>The factory is serialized onto the source and resolves the read-cursor sink on the member the
     * reader runs on: it holds only serializable coordinates, never the durable store itself (which is not
     * serializable), and binds the store member-side. That keeps the offset truth in the coordination store
     * and out of Jet state.
     */
    public static StreamSource<SrsItem> create(
            String ringName, StartFrom start, SrsReadCursorPublisherFactory publisherFactory) {
        Objects.requireNonNull(ringName, "ringName");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(publisherFactory, "publisherFactory");
        return SourceBuilder
                .stream("srs-source-" + ringName, ctx -> {
                    Ringbuffer<SrsItem> rb = ctx.hazelcastInstance().getRingbuffer(ringName);
                    SrsRingbuffer ring = new SrsRingbuffer(rb);
                    return SrsRingReader.from(ring, start, publisherFactory.resolve(ctx.hazelcastInstance()));
                })
                .fillBufferFn((SrsRingReader reader, SourceBuilder.SourceBuffer<SrsItem> buffer) ->
                        reader.fill((item, seq) -> buffer.add(item), FILL_BATCH))
                .build();
    }
}
