package io.tapstate.runtime.srs;

import com.hazelcast.jet.core.Processor;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceKeyMovePreflight;
import java.io.Serializable;

/** Runs on the source's non-cooperative processor, before any stateful downstream delivery. */
public interface SourceChangeGuard {
    void beforeBatch(Iterable<Envelope> rows);
    void poll();

    @FunctionalInterface
    interface Factory extends Serializable {
        SourceChangeGuard open(Processor.Context context);

        default SourceChangeGuard open(Processor.Context context,
                SourceKeyMovePreflight.Check preflight) {
            return open(context);
        }
    }
}
