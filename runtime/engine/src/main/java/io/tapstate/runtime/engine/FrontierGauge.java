package io.tapstate.runtime.engine;

import java.util.Map;

/**
 * Where a sink reports how far each chain's frontier trails the bound combined for it. The reading is taken
 * inside the sink and read outside the run, so it is a seam rather than a call: what publishes it belongs to
 * the engine the vertex runs on, while everything that decides what to publish belongs to the frontier.
 *
 * <p>It exists because a frontier that has stopped moving does not say why, and the two reasons are worked
 * on from opposite ends - one upstream, on whatever is holding changes pending, and one at the sink, which
 * has run out of positions to advance to. Left unmeasured they are one symptom, and which of them is
 * happening cannot be settled by looking harder at the frontier itself.
 *
 * <p>Nothing here is durable. A bound is in flight only - never written down, never outliving a run - so the
 * distance measured from it is a property of the run and belongs with the run's statistics, not in the store
 * that holds what a restart resumes from.
 */
interface FrontierGauge {

    /**
     * Takes a reading: for each chain, how far its bound runs ahead of the position its frontier reached.
     * Chains with no such distance are absent, and a reading taken while none has one is empty rather than
     * skipped - the frontier having nothing to report is itself current.
     */
    void trailing(Map<String, Long> gapsByChain);

    /**
     * Takes the other reading: for each chain that is pinned, how long its durable position has been where
     * it is, in milliseconds. Chains that are not pinned are absent.
     *
     * <p>Reported alongside the distance rather than folded into it because the two are different
     * questions and each is useless without the other. The distance cannot say a frontier has stopped - it
     * reads zero both for a chain held back by pending changes and for one keeping up - and the duration
     * cannot say which of the two stalls is happening. Only one of them is in the unit the consequence is
     * measured in: what a pinned position races is a retention window, kept in time.
     */
    void pinned(Map<String, Long> millisByChain);

    /**
     * Whether this gauge can only take a reading from a thread of the running job it belongs to. A sink
     * driven by hand, outside any job, swaps such a gauge for one nothing reads - there is no job and no
     * statistics to write into, and asking for a handle there fails outright. A sink that could not be driven
     * by hand would be a sink whose behaviour nothing could pin.
     */
    default boolean readableOnlyOnAJobThread() {
        return false;
    }

    /**
     * A gauge nothing reads. This is for a sink whose readings have nowhere to go - one driven outside a
     * running job - and never a way to opt a real sink out: a reading no one takes is the state this whole
     * seam exists to end.
     */
    static FrontierGauge none() {
        return new FrontierGauge() {

            @Override
            public void trailing(Map<String, Long> gapsByChain) {
            }

            @Override
            public void pinned(Map<String, Long> millisByChain) {
            }
        };
    }
}
