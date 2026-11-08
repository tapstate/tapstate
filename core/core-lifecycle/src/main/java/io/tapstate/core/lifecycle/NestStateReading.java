package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * What one namespace of a nest's state looks like while the run is going: how much of it there is, how
 * often it was reached for, and how much of that reaching had to go to the layer behind it.
 *
 * <p>Logical accesses and loader callbacks retain their separate units. The optional key counters count
 * requested keys, including absent values, and supply the two like units for the cold share. Keys are
 * distinct within one operation, not across the run; a later visit to the same key counts again.
 *
 * <p>They are counts since the run began rather than rates, deliberately. A rate computed here would be an
 * average over the whole run - the one window in which a cliff that started five minutes ago is invisible.
 * Two readings and the time between them give any window a reader actually wants; a single ratio gives
 * only the one nobody asked for.
 *
 * <p>{@code entries} is what is in memory, which is not how much there is: past what a namespace is given
 * to hold, the rest is on the layer behind it and still belongs to the namespace. {@code stored} is that
 * whole, and the two are worth reading together - {@code entries} is what the memory costs, {@code stored}
 * is how much there is to serve, and their ratio is how much of the serving is a trip to the cold layer
 * waiting to happen. It is absent rather than zero where there is no cold layer to ask, since a run
 * holding its state in memory alone has no second number and reporting one would invent it.
 *
 * <p>{@code pendingHighWater} is the odd one out and is a mark rather than a level: the deepest any one
 * key has been seen holding for something that has not arrived, since the run began. It is here because
 * nothing else in this record can see that quantity - what waits lives <em>inside</em> one entry, so a
 * single key holding a queue of ten thousand moves {@code entries} by one, moves {@code stored} not at all,
 * and moves {@code backfills} least of all, the key being hot enough to stay in memory. The run then stops
 * on the limit that bounds it with every number here flat behind it. A mark rather than a level because a
 * queue that filled and drained is exactly what a reader wants to know about, and because two readings
 * cannot recover it: a maximum is not a difference. It does not fall, and a restart begins it again.
 *
 * @param entries how many keys the namespace holds in memory now
 * @param accesses how many times the state was reached for since the run began, read or written
 * @param backfills how many cold loader callbacks completed
 * @param backfillMillis how long those took in total
 * @param pendingHighWater the deepest one key of this namespace has been seen holding for something that
 *     has not arrived, since the run began
 * @param stored how many keys the layer behind the memory holds, where there is one to ask
 * @param origin where in the pipeline's definition the namespace comes from, or null where the reporter
 *     could not say
 * @param keyAccesses the number of requested key accesses, where measured
 * @param coldKeys the number of keys requested by completed cold loaders, where measured
 * @param rootsRendered whole documents sent from this namespace since {@code rootsRenderedSince}; present
 *     only for the root level of a nest, which is the one level that renders documents
 * @param rootsRenderedSince when that count began, present exactly when the count is
 */
public record NestStateReading(long entries, long accesses, long backfills, long backfillMillis,
        long pendingHighWater, OptionalLong stored, NestOrigin origin,
        OptionalLong keyAccesses, OptionalLong coldKeys, OptionalLong rootsRendered,
        Instant rootsRenderedSince) {

    public NestStateReading {
        rootsRendered = rootsRendered == null ? OptionalLong.empty() : rootsRendered;
        if (rootsRendered.isPresent() != (rootsRenderedSince != null)) {
            throw new IllegalArgumentException(
                    "a count of documents sent says what it counts from: a count without a start hands every"
                            + " reader a stream in which a restart and a decrease are the same observation");
        }
    }

    /** A reading without a count of documents sent: every level but the root, and older readers. */
    public NestStateReading(long entries, long accesses, long backfills, long backfillMillis,
            long pendingHighWater, OptionalLong stored, NestOrigin origin,
            OptionalLong keyAccesses, OptionalLong coldKeys) {
        this(entries, accesses, backfills, backfillMillis, pendingHighWater, stored, origin, keyAccesses,
                coldKeys, OptionalLong.empty(), null);
    }

    /** Compatibility reading without key instrumentation. Missing counters are not measured zeroes. */
    public NestStateReading(long entries, long accesses, long backfills, long backfillMillis,
            long pendingHighWater, OptionalLong stored, NestOrigin origin) {
        this(entries, accesses, backfills, backfillMillis, pendingHighWater, stored, origin,
                OptionalLong.empty(), OptionalLong.empty());
    }

    /** A reading whose namespace's place in the definition is not known to the reporter. */
    public NestStateReading(long entries, long accesses, long backfills, long backfillMillis,
            long pendingHighWater, OptionalLong stored) {
        this(entries, accesses, backfills, backfillMillis, pendingHighWater, stored, null);
    }

    /** A reading from a run with no cold layer to ask, whose state is however much is in memory. */
    public NestStateReading(long entries, long accesses, long backfills, long backfillMillis) {
        this(entries, accesses, backfills, backfillMillis, 0L, OptionalLong.empty());
    }

    /** A reading from a run where nothing has been reported waiting under any one key. */
    public NestStateReading(long entries, long accesses, long backfills, long backfillMillis,
            OptionalLong stored) {
        this(entries, accesses, backfills, backfillMillis, 0L, stored);
    }

    /** The same reading, placed in the pipeline's definition. */
    public NestStateReading withOrigin(NestOrigin origin) {
        return new NestStateReading(entries, accesses, backfills, backfillMillis, pendingHighWater, stored,
                origin, keyAccesses, coldKeys, rootsRendered, rootsRenderedSince);
    }

    /** The same reading with independently measured key counters. */
    public NestStateReading withKeyCounts(long keyAccesses, long coldKeys) {
        return new NestStateReading(entries, accesses, backfills, backfillMillis, pendingHighWater, stored,
                origin, OptionalLong.of(keyAccesses), OptionalLong.of(coldKeys), rootsRendered, rootsRenderedSince);
    }

    /** The same reading with the count of whole documents the root level has sent. */
    public NestStateReading withRootsRendered(long rendered, Instant since) {
        return new NestStateReading(entries, accesses, backfills, backfillMillis, pendingHighWater, stored,
                origin, keyAccesses, coldKeys, OptionalLong.of(rendered), Objects.requireNonNull(since, "since"));
    }

    /** A decrease in either measured key counter identifies a new counting lifetime. */
    public boolean keyCountsRestartedSince(NestStateReading previous) {
        return decreased(keyAccesses, previous.keyAccesses) || decreased(coldKeys, previous.coldKeys);
    }

    private static boolean decreased(OptionalLong current, OptionalLong previous) {
        return current.isPresent() && previous.isPresent() && current.getAsLong() < previous.getAsLong();
    }
}
