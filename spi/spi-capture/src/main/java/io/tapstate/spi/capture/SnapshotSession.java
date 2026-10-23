package io.tapstate.spi.capture;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

    /** A session whose host must accept its actual metadata and seam before the connector reads rows. */
    interface PreparedProvider extends Provider {
        SnapshotSession snapshotSession(CaptureConfig config, SnapshotPreparation preparation);
    }

    /** No per-table fallback may start source reads when the caller requires preparation first. */
    static Optional<SnapshotSession> openPrepared(CapturePort port, CaptureConfig config, SnapshotPreparation preparation) {
        Objects.requireNonNull(port, "port"); Objects.requireNonNull(config, "config"); Objects.requireNonNull(preparation, "preparation");
        return port instanceof PreparedProvider provider ? Optional.of(provider.snapshotSession(config, preparation)) : Optional.empty();
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
                config.connectorId(), config.settings(), List.of(table), config.node(), config.sharedNotes()));
    }

    /** Opens the next selected table's bounded read. The caller closes the batch after draining it. */
    CaptureBatch read(String table);

    /** Closes a connector shared across the round, if this session owns one. */
    @Override
    default void close() {
    }
}
