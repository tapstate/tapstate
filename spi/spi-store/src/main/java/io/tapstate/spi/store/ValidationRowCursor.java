package io.tapstate.spi.store;

import io.tapstate.core.validation.ValidationValues;
import java.util.Optional;

/**
 * A single independent read invocation. Empty means the invocation returned normally and all data was
 * consumed, never an empty callback, timeout, cancellation or failure. The cursor has no total-row
 * preview limit. Native ordering and key projection are separate reader obligations.
 */
public interface ValidationRowCursor extends AutoCloseable {
    /** The consumer closes each row before retaining further work beyond its admitted budget. */
    Optional<Row> next();

    /** Cancels only this read and releases queued rows; already borrowed rows remain charged. */
    @Override void close();

    interface Row extends AutoCloseable {
        /** Zero-based delivery ordinal of this invocation, not a source position or acknowledgment. */
        long ordinal();
        ValidationValues.Row values();
        @Override void close();
    }
}
