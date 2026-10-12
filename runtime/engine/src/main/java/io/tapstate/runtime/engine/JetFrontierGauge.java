package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
import java.util.HashMap;
import java.util.Map;

/**
 * Publishes a sink's frontier readings as run statistics of the job it belongs to, one per chain. The engine
 * already collects a job's statistics on a cadence of its own and hands them out with the job, so a reading
 * left here is readable from outside the run without a second channel to keep alive - and the reading is of
 * something in flight only, which is the one thing that must not be answered from the durable store.
 *
 * <p>Readings can only be taken from the job's own threads: obtaining one elsewhere fails outright rather
 * than quietly recording nothing, which is why a sink driven outside a running job is given a gauge that
 * reads nothing instead of this one. The handle for a chain is kept once obtained - it belongs to the sink
 * rather than to whichever thread ran it - so a reading costs a lookup and a write.
 */
final class JetFrontierGauge implements FrontierGauge {

    private final Map<String, Metric> gaugesByChain = new HashMap<>();
    private final Map<String, Metric> stallsByChain = new HashMap<>();

    @Override
    public void trailing(Map<String, Long> gapsByChain) {
        gapsByChain.forEach((chain, gap) ->
                gaugesByChain.computeIfAbsent(chain, JetFrontierGauge::gapMetricFor).set(gap));
    }

    @Override
    public void pinned(Map<String, Long> millisByChain) {
        millisByChain.forEach((chain, millis) ->
                stallsByChain.computeIfAbsent(chain, JetFrontierGauge::stallMetricFor).set(millis));
    }

    @Override
    public boolean readableOnlyOnAJobThread() {
        return true;
    }

    /**
     * The chain is in each reading's name, as {@link FrontierMetricNames} spells it, because a run's statistics
     * carry numbers and not the names of things: one number covering every chain would average a chain that is
     * keeping up with one that has stalled and read as neither.
     */
    private static Metric gapMetricFor(String chain) {
        return Metrics.metric(FrontierMetricNames.gapNameOf(chain));
    }

    private static Metric stallMetricFor(String chain) {
        return Metrics.metric(FrontierMetricNames.stallNameOf(chain));
    }
}
