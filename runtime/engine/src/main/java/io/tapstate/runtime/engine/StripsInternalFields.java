package io.tapstate.runtime.engine;

import com.hazelcast.function.SupplierEx;
import io.tapstate.spi.sink.SinkWriter;
import java.util.Objects;

/**
 * The writers of a target that asked to receive nothing the product adds to a row, not even the
 * markers meant for consumers. It opens exactly the writers of the factory it wraps; what it adds is
 * the answer the sink reads when it puts each writer behind the serving gate.
 */
final class StripsInternalFields implements IndexedSinkWriterFactory {

    private static final long serialVersionUID = 1L;

    private final SupplierEx<? extends SinkWriter> writers;

    private StripsInternalFields(SupplierEx<? extends SinkWriter> writers) {
        this.writers = Objects.requireNonNull(writers, "writers");
    }

    /** {@code writers} for a target that opted out when {@code strip}, and {@code writers} itself otherwise. */
    static SupplierEx<? extends SinkWriter> when(boolean strip, SupplierEx<? extends SinkWriter> writers) {
        return strip ? new StripsInternalFields(writers) : writers;
    }

    @Override
    public SinkWriter getEx() throws Exception {
        return forWriter(0, 0);
    }

    @Override
    public SinkWriter forWriter(int memberIndex, int localIndex) throws Exception {
        return writers instanceof IndexedSinkWriterFactory indexed
                ? indexed.forWriter(memberIndex, localIndex)
                : writers.get();
    }
}
