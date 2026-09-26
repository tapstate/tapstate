package io.tapstate.runtime.engine;

/** A sink's bounded batch, pending-write and backpressure readings. */
interface SinkUseGauge {

    void issued(long batches, long records, long largestBatch, int pending, int limit, long sinceMillis);

    void settled(long writeNanos, int pending);

    void backpressured(boolean active);

    void waited(long waitNanos);

    default boolean readableOnlyOnAJobThread() {
        return false;
    }

    static SinkUseGauge none() {
        return new SinkUseGauge() {
            @Override public void issued(long batches, long records, long largestBatch,
                    int pending, int limit, long sinceMillis) { }
            @Override public void settled(long writeNanos, int pending) { }
            @Override public void backpressured(boolean active) { }
            @Override public void waited(long waitNanos) { }
        };
    }
}
