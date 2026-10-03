package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;

import java.time.Instant;
import java.util.Objects;

/**
 * Where a restart of a chain's reader resumes, and when that point was written down.
 *
 * <p>{@code position} is the source's own position, with the order the engine reached it at where one was
 * recorded -- a point put there by hand carries none. {@code recordedAt} is when it was written, or null on a
 * record that predates the stamp. The age is the point's own and not that of how far the chain has been read
 * since: a read can carry on past the last change, through runs that carried none, and a restart still
 * begins at the change. Whether the source's change log reaches back far enough to resume at all is a
 * question about this point.
 *
 * <p>A pure value over {@code java..} only (rule R2).
 */
public record ResumePoint(ChainPosition position, Instant recordedAt) {

    public ResumePoint {
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.token(), "a resume point names a position a read can resume from");
    }
}
