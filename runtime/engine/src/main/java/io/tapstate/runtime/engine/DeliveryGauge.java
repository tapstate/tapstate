package io.tapstate.runtime.engine;

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
     * Takes the other reading: for each table, the event time of the newest row of it that has settled, as
     * epoch milliseconds. Tables with nothing settled are absent.
     */
    void reached(Map<String, Long> newestEventTimeByTable);

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
     * A gauge nothing reads, for a sink driven outside a running job. Never a way to opt a real sink out:
     * a delivery no one counts is the state this seam exists to end.
     */
    static DeliveryGauge none() {
        return new DeliveryGauge() {

            @Override
            public void delivered(Map<String, Map<String, Long>> rowsByTableAndOp) {
            }

            @Override
            public void reached(Map<String, Long> newestEventTimeByTable) {
            }

            @Override
            public void countingSince(long epochMillis) {
            }
        };
    }
}
