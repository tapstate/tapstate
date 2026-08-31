package io.tapstate.spi.capture;

import java.util.Objects;

/**
 * Where a change stream begins, stated by the caller. Two cases, and no third: {@link Resume} names the
 * position the stream picks up from, {@link Present} asks for changes made from the source's present
 * moment onward.
 *
 * <p>The absence of a recorded position is deliberately not expressible here. A caller that has none has
 * to decide what it means before it reaches the port, because the two readings are not the same run: a
 * first run over a source that is also being snapshotted must begin at the snapshot's seam, while a run
 * asked for only new changes begins at the present. Collapsed into one nullable position, the port picks
 * for both — and the run that should have started at the seam silently begins after it, dropping every
 * change made while the snapshot was reading, with nothing thrown and nothing logged.
 *
 * <p>A position crosses this boundary as its opaque token and nothing else (rule R2): the port names no
 * connector type, and rendering a connector's own offset as a token is the adapter's affair.
 */
public sealed interface CaptureStart permits CaptureStart.Resume, CaptureStart.Present {

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
     * Begin at the source's present moment: only changes made from now on are delivered, and whatever the
     * source holds from before is not.
     */
    record Present() implements CaptureStart {
    }

    /** Resume at a position a previous run reached. */
    static CaptureStart resume(SourcePosition position) {
        return new Resume(position);
    }

    /** Begin at the source's present moment. */
    static CaptureStart present() {
        return new Present();
    }
}
