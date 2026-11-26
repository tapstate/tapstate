package io.tapstate.spi.sink;

/**
 * The outcome of a batch write.
 *
 * @param written the writer's reported write count, which can count physical changes
 * @param acknowledgedRows confirmed row events, including accepted no-ops and excluding DDL
 */
public record WriteResult(long written, long acknowledgedRows) {

    /** For writers whose reported count already counts confirmed row events. */
    public WriteResult(long written) {
        this(written, written);
    }

    public WriteResult {
        if (written < 0) {
            throw new IllegalArgumentException("written must be non-negative: " + written);
        }
        if (acknowledgedRows < 0) {
            throw new IllegalArgumentException("acknowledgedRows must be non-negative: " + acknowledgedRows);
        }
    }
}
