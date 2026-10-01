package io.tapstate.runtime.engine;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Objects;

/** Emits a pre-fetched finite sample and then completes; it never opens a capture or CDC source. */
public final class FiniteEnvelopeSourceProcessor extends AbstractProcessor {

    private final List<Envelope> rows;
    private int next;

    public FiniteEnvelopeSourceProcessor(List<Envelope> rows) {
        this.rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
    }

    public static ProcessorMetaSupplier metaSupplier(String vertexName, List<Envelope> rows) {
        Objects.requireNonNull(vertexName, "vertexName");
        List<Envelope> snapshot = List.copyOf(Objects.requireNonNull(rows, "rows"));
        SupplierEx<Processor> supplier = () -> new FiniteEnvelopeSourceProcessor(snapshot);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier), vertexName);
    }

    @Override
    public boolean complete() {
        while (next < rows.size()) {
            if (!tryEmit(rows.get(next))) {
                return false;
            }
            next++;
        }
        return true;
    }
}
