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
@FunctionalInterface
interface FrontierGauge {

    /**
     * Takes a reading: for each chain, how far its bound runs ahead of the position its frontier reached.
     * Chains with no such distance are absent, and a reading taken while none has one is empty rather than
     * skipped - the frontier having nothing to report is itself current.
     */
    void trailing(Map<String, Long> gapsByChain);

    /**
     * A gauge nothing reads. This is for a sink whose readings have nowhere to go - one driven outside a
     * running job - and never a way to opt a real sink out: a reading no one takes is the state this whole
     * seam exists to end.
     */
    static FrontierGauge none() {
        return gapsByChain -> { };
    }
}
