package io.tapstate.runtime.srs;

import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import io.tapstate.spi.store.SrsLogStore;

import java.util.Objects;
import java.util.Properties;

/**
 * Binds the durable change log to each change ring as that ring is created.
 *
 * <p>A factory rather than a single store instance, because the ring's store hook is told a sequence and
 * an item but never which ring is asking -- only this call is given the name. One store instance shared
 * across the rings would write every table's changes under one identity, which is the same as having no
 * ring in the key at all.
 *
 * <p>Typed over {@code Object} for the reason the store it builds is: the ring's batch path hands the
 * store an array that is not of the item type, and a store declared over that type dies on every batch
 * write.
 */
public final class SrsLogRingbufferStoreFactory implements RingbufferStoreFactory<Object> {

    private final SrsLogStore log;

    public SrsLogRingbufferStoreFactory(SrsLogStore log) {
        this.log = Objects.requireNonNull(log, "log");
    }

    @Override
    public RingbufferStore<Object> newRingbufferStore(String name, Properties properties) {
        return new SrsLogRingbufferStore(log, name);
    }
}
