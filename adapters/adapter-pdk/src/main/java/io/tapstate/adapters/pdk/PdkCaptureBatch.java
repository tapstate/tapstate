package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureBatch;

import java.util.Iterator;
import java.util.List;

/**
 * A bounded snapshot batch over the rows already read from the connector, holding the connector open
 * until closed. Every event is a snapshot read (op {@code r}). Closing stops the connector and closes
 * its loader; it is idempotent and may be called before the batch is drained.
 */
final class PdkCaptureBatch implements CaptureBatch {

    private final Iterator<Envelope> rows;
    private final PdkConnector connector;
    private boolean closed;

    PdkCaptureBatch(List<Envelope> rows, PdkConnector connector) {
        this.rows = rows.iterator();
        this.connector = connector;
    }

    /** The connector this batch was read from, and the state scope it was opened under. */
    PdkConnector connector() {
        return connector;
    }

    @Override
    public boolean hasNext() {
        return rows.hasNext();
    }

    @Override
    public Envelope next() {
        return rows.next();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        connector.stopQuietly();
        connector.close();
    }
}
