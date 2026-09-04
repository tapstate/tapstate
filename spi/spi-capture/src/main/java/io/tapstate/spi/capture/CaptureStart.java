package io.tapstate.spi.capture;

import java.time.Instant;
import java.util.Objects;

/**
 * Where a change stream begins, stated by the caller. Four cases, and every one of them is a start the
 * caller asked for: {@link Resume} names the position the stream picks up from, {@link At} names an
 * instant the source resolves to a position of its own, {@link Earliest} asks for the oldest change the
 * source still retains, and {@link Present} asks for changes made from the source's present moment
 * onward.
 *
 * <p>The absence of a recorded position is deliberately not expressible here. A caller that has none has
 * to decide what it means before it reaches the port, because the two readings are not the same run: a
 * first run over a source that is also being snapshotted must begin at the snapshot's seam, while a run
 * asked for only new changes begins at the present. Collapsed into one nullable position, the port picks
 * for both — and the run that should have started at the seam silently begins after it, dropping every
 * change made while the snapshot was reading, with nothing thrown and nothing logged.
 *
 * <p>{@link At} and {@link Earliest} are starts the source has to work out, which is why they are cases
 * here rather than positions the caller resolves first: only the source can say which of its positions
 * corresponds to an instant, or which is the oldest it still holds. Both are asks the source may be
 * unable to meet — an instant older than what it retains, or a source that cannot map instants at all —
 * and a port that cannot meet one refuses rather than starting somewhere else, for the same reason the
 * paragraph above gives.
 *
 * <p>A position crosses this boundary as its opaque token and nothing else (rule R2): the port names no
 * connector type, and rendering a connector's own offset as a token is the adapter's affair. An instant
 * crosses as an instant, and carries its own offset from the moment it was parsed, so no part of this
 * boundary reads a wall clock or a time zone.
 */
public sealed interface CaptureStart
        permits CaptureStart.Resume, CaptureStart.At, CaptureStart.Earliest, CaptureStart.Present {

    /**
     * Resume at {@code position}: the stream delivers the changes after it. The position came from a
     * previous run of this same source — a snapshot's seam, or a position that run had reached.
     */
    record Resume(SourcePosition position) implements CaptureStart {

        public Resume {
            Objects.requireNonNull(position, "position");
        }
    }

    /**
     * Begin at the first change at or after {@code instant}, as the source resolves it. The instant is an
     * absolute point on the timeline, not a local reading of one: whoever parsed it has already applied
     * the offset it was written with, so the same moment written in two zones resolves to one position.
     */
    record At(Instant instant) implements CaptureStart {

        public At {
            Objects.requireNonNull(instant, "instant");
        }
    }

    /**
     * Begin at the oldest change the source still retains. What that is moves on its own as the source
     * discards what has aged out, so this names a boundary rather than a fixed point.
     */
    record Earliest() implements CaptureStart {
    }

    /**
     * Begin at the source's present moment: only changes made from now on are delivered, and whatever the
     * source holds from before is not.
     */
    record Present() implements CaptureStart {
    }

    /** Resume at a position a previous run reached. */
    static CaptureStart resume(SourcePosition position) {
        return new Resume(position);
    }

    /** Begin at the first change at or after {@code instant}. */
    static CaptureStart at(Instant instant) {
        return new At(instant);
    }

    /** Begin at the oldest change the source still retains. */
    static CaptureStart earliest() {
        return new Earliest();
    }

    /** Begin at the source's present moment. */
    static CaptureStart present() {
        return new Present();
    }
}
