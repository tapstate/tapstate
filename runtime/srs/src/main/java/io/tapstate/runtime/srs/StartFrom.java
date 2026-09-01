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
 * an instant. It positions this one pipeline's consumer cursor into the change ring; it never moves the
 * shared mining chain's own read offset.
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

    /** Start from the first change whose event time is at or after {@code instant}. */
    record At(Instant instant) implements StartFrom {
        public At {
            Objects.requireNonNull(instant, "instant");
        }
    }

    static StartFrom earliest() {
        return new Earliest();
    }

    static StartFrom latest() {
        return new Latest();
    }

    static StartFrom at(Instant instant) {
        return new At(instant);
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
                    Instant instant = Instant.parse(raw);
                    // Range check, and it has to happen here. An instant spans years far beyond the epoch
                    // milliseconds every consumer of a start addresses it by, so a value can parse cleanly
                    // and then overflow at the point of use -- which is a bare arithmetic failure on
                    // whichever member ran the read, naming neither the setting nor the value that caused
                    // it. Converting here turns that into a refusal holding the text the author wrote.
                    instant.toEpochMilli();
                    return at(instant);
                } catch (DateTimeParseException | ArithmeticException e) {
                    throw new TapstateException(CaptureError.START_FROM_UNPARSABLE, Map.of("value", raw), e);
                }
        }
    }
}
