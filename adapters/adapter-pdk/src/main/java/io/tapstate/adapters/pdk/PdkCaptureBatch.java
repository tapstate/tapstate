package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.SourcePosition;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * A bounded snapshot batch over the rows already read from the connector, holding the connector open
 * until closed. Every event is a snapshot read (op {@code r}). Closing stops the connector and closes
 * its loader; it is idempotent and may be called before the batch is drained.
 */
final class PdkCaptureBatch implements CaptureBatch {

    private final Iterator<Envelope> rows;
    private final Optional<SourcePosition> seam;
    private final PdkConnector connector;
    private boolean closed;

    PdkCaptureBatch(List<Envelope> rows, Optional<SourcePosition> seam, PdkConnector connector) {
        this.rows = rows.iterator();
        this.seam = seam;
        this.connector = connector;
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
    public Optional<SourcePosition> seam() {
        return seam;
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
