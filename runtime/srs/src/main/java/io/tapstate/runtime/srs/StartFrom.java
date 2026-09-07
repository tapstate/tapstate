package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;

/**
 * Where a pipeline starts consuming a mining chain's incremental tail — the typed reading of its
 * {@code start_from} setting. Three forms: {@link Earliest} replays every change still buffered,
 * {@link Latest} takes only changes from now on, and {@link At} starts from the first change at or after
 * an instant.
 *
 * <p>What it points into depends on whether this pipeline buffers through the shared replay ring.
 * Buffered, it positions this one pipeline's consumer cursor into the ring and never moves the shared
 * mining chain's own read offset -- the chain is mined once and each consumer finds its own start in
 * what was mined. Read directly, there is no ring to point into, so it names a position in the
 * source's own log and the tail begins there. Either way it decides a first run only: a recorded
 * position outranks it, or a restart would re-read the same stretch every time.
 *
 * <p>The authoring layer holds {@code start_from} as a free string and does not constrain its format, so
 * an unrecognized value is caught here at consumption time rather than by the validate layer.
 *
 * <p>It is {@link Serializable}: parsed once at pipeline assembly, it is captured by the Jet source's
 * create function and shipped to the member that resolves it against the ring there.
 */
public sealed interface StartFrom extends Serializable permits StartFrom.Earliest, StartFrom.Latest, StartFrom.At {

    /** Start from the oldest change still in the ring — replay everything currently buffered. */
    record Earliest() implements StartFrom {
    }

    /** Start after the newest change — take only changes appended from now on. */
    record Latest() implements StartFrom {
    }

    /**
     * Start from the first change whose event time is at or after {@code instant}.
     *
     * <p>{@code epochMilli} is that same instant in the form changes are timestamped and addressed by,
     * converted once on the way in instead of again at each point of use. Converting is also the range
     * check, and it is why the pair is carried rather than the instant alone: an instant spans years far
     * beyond the epoch milliseconds a start is addressed by, so a value can read cleanly and then overflow
     * later, on whichever member ran the read, as a bare arithmetic failure naming neither the setting nor
     * the value that caused it. Converting while the written text is still in hand keeps the diagnosis
     * attached to the input.
     *
     * <p>Build one through {@link StartFrom#at(Instant)}, which derives the pair. The canonical constructor
     * refuses a pair that disagrees, so the two can never drift apart.
     */
    record At(Instant instant, long epochMilli) implements StartFrom {
        public At {
            Objects.requireNonNull(instant, "instant");
            if (epochMilli != instant.toEpochMilli()) {
                throw new IllegalArgumentException(
                        "epochMilli must be the conversion of instant: got " + epochMilli + " for " + instant);
            }
        }
    }

    static StartFrom earliest() {
        return new Earliest();
    }

    static StartFrom latest() {
        return new Latest();
    }

    /**
     * The start point at {@code instant}, carrying the epoch-millisecond form alongside it.
     *
     * @throws ArithmeticException if the instant is too far out for that form to hold it
     */
    static StartFrom at(Instant instant) {
        return new At(instant, instant.toEpochMilli());
    }

    /**
     * Parses a {@code start_from} value: the keyword {@code earliest} or {@code latest}, or an ISO-8601
     * instant carrying the offset it was written with. A value that is neither keyword nor an instant this
     * build can address is rejected, with the value that was written.
     *
     * <p>An offset is required rather than assumed, which is what the instant form inherits by being read
     * as an instant: a bare local reading like {@code 2026-09-01T10:00:00} is refused instead of being
     * taken as the server's own zone, so one pipeline cannot begin at two different points depending on
     * which machine parsed it. The same moment written in two zones is the same instant and so the same
     * start.
     */
    static StartFrom parse(String raw) {
        Objects.requireNonNull(raw, "raw");
        switch (raw) {
            case "earliest":
                return earliest();
            case "latest":
                return latest();
            default:
                try {
                    // Building the At converts, and the conversion is the range check -- see At. An instant
                    // out of that range fails here, holding the text the author wrote, rather than later as
                    // a bare arithmetic failure on whichever member ran the read.
                    return at(Instant.parse(raw));
                } catch (DateTimeParseException | ArithmeticException e) {
                    throw new TapstateException(CaptureError.START_FROM_UNPARSABLE, Map.of("value", raw), e);
                }
        }
    }
}
