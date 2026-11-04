package io.tapstate.core.lifecycle;

import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * What one namespace of a nest's state did between two readings, as opposed to what it has done since the
 * run began.
 *
 * <p>A reading carries counts that run from when the member came up, and the ratio that matters here cannot
 * be taken from one of them: it would be an average over the whole run, which is the one window in which a
 * namespace that fell off its cliff a minute ago still reads as healthy. A day of serving everything from
 * memory drowns an hour of serving nothing from it. So the counted halves are differenced across two
 * readings and only the interval between them is described.
 *
 * <p>The two gauges are not differenced. {@code entries} and {@code stored} are how much there is now, not
 * how much there has been; their difference would say how much the namespace grew, which is a different
 * question from how much of it can be answered without going to disk.
 *
 * <p>Counters that came back smaller than last time are read as a run that started over rather than as
 * negative traffic. They live on the member and are dropped when a pipeline lets go of its namespace, so a
 * pipeline stopped and started again genuinely does report smaller numbers than before; subtracting would
 * produce a negative over a negative, which is a ratio that can read like anything at all.
 *
 * @param accesses how many times the state was reached for during the window, read or written
 * @param backfills how many of those found nothing in memory and went to the layer behind
 * @param backfillMillis how long those took in total
 * @param entries how many keys the namespace holds in memory as of the later reading
 * @param stored how many keys there are altogether as of the later reading, where there is a layer to ask
 */
public record NestStateWindow(long accesses, long backfills, long backfillMillis, long entries,
        OptionalLong stored, NestKeyAccess keyAccess) {

    public NestStateWindow(long accesses, long backfills, long backfillMillis, long entries, OptionalLong stored) {
        this(accesses, backfills, backfillMillis, entries, stored, null);
    }

    public NestStateWindow {
        Objects.requireNonNull(stored, "stored");
    }

    /**
     * The window between two readings of the same namespace, {@code previous} being the earlier. Where the
     * counts went backwards this is the whole of {@code current} instead, since that is what a run which
     * started over in between has actually served.
     */
    public static NestStateWindow between(NestStateReading previous, NestStateReading current) {
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(current, "current");
        if (startedOver(previous, current)) {
            return fromStart(current);
        }
        return new NestStateWindow(current.accesses() - previous.accesses(),
                current.backfills() - previous.backfills(),
                current.backfillMillis() - previous.backfillMillis(),
                current.entries(), current.stored(), current.keyAccess() == null ? null : current.keyAccess().since(previous.keyAccess()));
    }

    /**
     * The window covering everything {@code current} has counted, for a namespace being read for the first
     * time. Its counts already start at the beginning of the run, so there is nothing to difference against
     * and nothing to wait for — which matters, because the first interval is where a namespace given far
     * less memory than its working set needs announces itself soonest.
     */
    public static NestStateWindow fromStart(NestStateReading current) {
        Objects.requireNonNull(current, "current");
        return new NestStateWindow(current.accesses(), current.backfills(), current.backfillMillis(),
                current.entries(), current.stored(), current.keyAccess());
    }

    /**
     * How much of the reaching during this window had to go to the layer behind memory, between 0 and 1.
     * Absent where nothing was reached for: zero would be the healthy end of the same scale, so an idle
     * namespace would read as the one thing this is watched for.
     */
    public OptionalDouble servedFromCold() {
        long attempts = pressureAccesses();
        return attempts <= 0 || pressureBackfills() < 0 ? OptionalDouble.empty()
                : OptionalDouble.of((double) pressureBackfills() / attempts);
    }

    public long pressureAccesses() { return keyAccess == null ? accesses : keyAccess.attempts(); }
    public long pressureBackfills() { return keyAccess == null ? backfills : keyAccess.coldLoads(); }

    /**
     * How much of the namespace is in memory, between 0 and 1. Absent where there is no layer behind the
     * memory to ask, and where there is nothing stored at all — in both cases there is no denominator, and
     * answering "all of it" would be one number wearing the name of a ratio.
     */
    public OptionalDouble resident() {
        if (stored.isEmpty() || stored.getAsLong() == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of((double) entries / stored.getAsLong());
    }

    /**
     * What one trip to the layer behind memory cost on average during this window. This is what turns the
     * ratio into a time: a tenth of the reaching going to a layer that answers in microseconds and a tenth
     * going to one that answers in tens of milliseconds are not the same finding. Absent where no trip was
     * made, which would otherwise be a cost invented for a namespace that paid none.
     */
    public OptionalDouble millisPerBackfill() {
        return backfills == 0 ? OptionalDouble.empty() : OptionalDouble.of((double) backfillMillis / backfills);
    }

    /**
     * Whether the counts came back smaller than last time, which no run does while it goes on running.
     * All three are checked rather than just the one that is compared: they are incremented independently,
     * and a restart that happened to leave one of them level would otherwise be read as ordinary traffic.
     */
    private static boolean startedOver(NestStateReading previous, NestStateReading current) {
        return current.accesses() < previous.accesses()
                || current.backfills() < previous.backfills()
                || current.backfillMillis() < previous.backfillMillis();
    }
}
