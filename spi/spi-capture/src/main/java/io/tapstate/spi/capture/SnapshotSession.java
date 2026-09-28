package io.tapstate.spi.capture;

import java.util.List;

/**
 * One bounded snapshot round over several selected tables. Each table still has a batch of its own, so
 * the caller can report that table only after its last row has been handed over. The session owns any
 * connector shared by those batches and is closed after the round or when it is abandoned.
 */
@FunctionalInterface
public interface SnapshotSession extends AutoCloseable {

    /** An optional read-side capability; the existing capture port contract remains unchanged. */
    interface Provider {
        SnapshotSession snapshotSession(CaptureConfig config);
    }

    /**
     * Dispatches to a shared connector when the port offers one. Other ports keep their ordinary
     * per-table reads, preserving the same table completion and failure boundary.
     */
    static SnapshotSession open(CapturePort port, CaptureConfig config) {
        if (port instanceof Provider provider) {
            return provider.snapshotSession(config);
        }
        return table -> port.snapshot(new CaptureConfig(
                config.connectorId(), config.settings(), List.of(table), config.node()));
    }

    /** Opens the next selected table's bounded read. The caller closes the batch after draining it. */
    CaptureBatch read(String table);

    /** Closes a connector shared across the round, if this session owns one. */
    @Override
    default void close() {
    }
}
