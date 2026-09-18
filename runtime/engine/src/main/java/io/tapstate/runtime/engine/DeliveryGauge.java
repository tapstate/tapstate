package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.HistogramValue;

import java.util.Map;

/**
 * Where a sink reports what has reached its target durably: how many rows, broken out by the table and the
 * operation they came from, and how recent the newest of them is. A seam for the same reason
 * {@link FrontierGauge} is one - the reading is taken inside the sink and read outside the run.
 *
 * <p>Both readings are taken at the moment a write settles, and at no earlier one. A row handed to a
 * writer has not reached anything yet: the write can still fail and be retried, and a count taken on
 * hand-off runs ahead of the target by however much is in flight - which is exactly the amount by which it
 * is wrong on the occasion anybody is looking, when writes are piling up unacknowledged.
 *
 * <p>What is reported for recency is the newest <em>event time</em> settled, not a lag. How far behind a
 * pipeline is has to be worked out against the clock at the moment somebody asks, because it keeps growing
 * while nothing arrives: a lag computed here would be frozen at the last settle and would read as healthy
 * for as long as a stalled pipeline stayed stalled. That is the one shape this measurement must not have.
 */
interface DeliveryGauge {

    /**
     * Takes a reading of how many rows have settled since the run began, keyed by the table they belong to
     * and, within it, the source operation that produced them. Cumulative, not a delta: what a consumer
     * wants is a rate over whatever window it chooses, and only a running total gives every window.
     *
     * <p>A table that has settled nothing is absent rather than present at zero, so a sink that never had
     * anything to deliver for it and a sink whose deliveries are not wired look different from outside.
     */
    void delivered(Map<String, Map<String, Long>> rowsByTableAndOp);

    /**
     * Takes the reading beside that one: how many bytes of payload have settled, by table. Cumulative in
     * the same way, taken at the same moment and off the same batch, so the two cannot come to cover
     * different writes.
     *
     * <p>Broken out by the table alone and not also by operation, unlike the counts. What a reader does
     * with bytes is compare the two ends or divide by the rows, and the operation a row came from says
     * nothing about how much of it there was -- a dimension carried because the neighbour has one is a
     * dimension nothing reads and nothing would notice going wrong.
     */
    void carried(Map<String, Long> bytesByTable);

    /**
     * Takes the other reading: for each table, the event time of the newest row of it that has settled, as
     * epoch milliseconds. Tables with nothing settled are absent.
     */
    void reached(Map<String, Long> newestEventTimeByTable);

    /**
     * Takes the reading of how long settled rows took, per table: from the moment the source stamped a
     * row to the moment its write was confirmed, bucketed over the registered bounds, accumulated since
     * the run began. Taken at the same moment as the counts and off the same rows, so a row is in this
     * distribution exactly when it is in the count beside it.
     *
     * <p>A distribution rather than an average, because an average is where the slow rows disappear, and
     * the slow rows are what anybody reading a delivery time came to see.
     *
     * <p>A table absent from {@code durationByTable} is one whose distribution has not moved since the
     * last reading, and what was taken for it stands. These are values under a name, not deltas, so a
     * reading not taken again is the reading still there — which is what already makes a count read
     * correctly through a lull in which nothing settles at all.
     */
    void took(Map<String, HistogramValue> durationByTable);

    /**
     * Takes the reading the two above are only readable against: the moment this sink began counting, as
     * epoch milliseconds. A running total with no start is a stream in which a restart and a decrease are
     * the same observation, so whoever reads the counts needs to know what they accumulate from — and the
     * only start that is true of them is the one taken where they are taken. A start read off the job's
     * submission would outlive the counters: an execution that restarts inside a job resets them and
     * leaves that start standing, which is the exact shape it was supposed to rule out.
     */
    void countingSince(long epochMillis);

    /**
     * Whether taking a reading requires the thread of a running job. A gauge that writes into a job's own
     * statistics does; one that keeps the readings itself does not.
     *
     * <p>It is asked rather than assumed because a sink is driven two ways. In a job its processors run on
     * the job's threads and the answer is yes; driven by hand - which is how its behaviour is pinned at all
     * - there is no job and no statistics to write into, and asking for a handle there fails outright. A
     * sink that could not be driven by hand would be a sink whose behaviour nothing could pin.
     */
    default boolean readableOnlyOnAJobThread() {
        return false;
    }

    /**
     * A gauge nothing reads, for a sink driven outside a running job. Never a way to opt a real sink out:
     * a delivery no one counts is the state this seam exists to end.
     */
    static DeliveryGauge none() {
        return new DeliveryGauge() {

            @Override
            public void delivered(Map<String, Map<String, Long>> rowsByTableAndOp) {
            }

            @Override
            public void carried(Map<String, Long> bytesByTable) {
            }

            @Override
            public void reached(Map<String, Long> newestEventTimeByTable) {
            }

            @Override
            public void took(Map<String, HistogramValue> durationByTable) {
            }

            @Override
            public void countingSince(long epochMillis) {
            }
        };
    }
}
