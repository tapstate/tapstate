package io.tapstate.runtime.engine;

import com.hazelcast.function.SupplierEx;
import io.tapstate.spi.sink.SinkWriter;

/** Member-local opening with the original topology's stable writer coordinates. */
public interface IndexedSinkWriterFactory extends SupplierEx<SinkWriter> {
    SinkWriter forWriter(int memberIndex,int localIndex) throws Exception;
}
