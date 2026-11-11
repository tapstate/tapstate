package io.tapstate.spi.store;

/**
 * Counts the round trips a store makes on behalf of one operation, on the thread making it. A store adapter
 * reports each command it sends to its backend with {@link #observed()}; a caller that wants to know what an
 * operation cost opens a count around it with {@link #count()}. Outside an open count, a report costs one
 * thread-local read and records nothing.
 *
 * <p>Same-thread by construction: the adapters this serves issue their commands synchronously on the
 * caller's thread, so a count is never mixed with another operation's. An adapter that issues commands on
 * other threads reports nothing here, which reads as an operation that cost no round trips - it is not
 * measured, rather than measured wrongly.
 */
public final class StoreRoundTrips {

    private static final ThreadLocal<long[]> OPEN = new ThreadLocal<>();

    private StoreRoundTrips() {
    }

    /** Reports one command sent to the backend by the operation running on this thread. */
    public static void observed() {
        long[] open = OPEN.get();
        if (open != null) {
            open[0]++;
        }
    }

    /** Opens a count on this thread; close it to read how many round trips were reported while it was open. */
    public static Count count() {
        long[] outer = OPEN.get();
        long[] mine = new long[1];
        OPEN.set(mine);
        return new Count(outer, mine);
    }

    /** One open count. Closing it restores whatever count was open before, and adds this one's total to it. */
    public static final class Count implements AutoCloseable {

        private final long[] outer;
        private final long[] mine;
        private boolean closed;

        private Count(long[] outer, long[] mine) {
            this.outer = outer;
            this.mine = mine;
        }

        /** How many round trips were reported so far while this count was open. */
        public long roundTrips() {
            return mine[0];
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (outer == null) {
                OPEN.remove();
            } else {
                outer[0] += mine[0];
                OPEN.set(outer);
            }
        }
    }
}
