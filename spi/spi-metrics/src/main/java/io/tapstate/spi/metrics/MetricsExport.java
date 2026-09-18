package io.tapstate.spi.metrics;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * Where the facts a convergence pass measured are offered for export. The second projection of the
 * instruments: the observation document is the first, and both are read off the same facts, neither off
 * the other. An implementation keeps the latest facts of each pipeline and hands them to whatever backend
 * it carries them to, on that backend's own cadence; this port says nothing about the backend, and the
 * runtime and the assembly offer through it without a dependency on any SDK.
 *
 * <p>Offering is cheap and never throws into the pass that offered: a backend that is down is the
 * exporter's problem to report, not the convergence loop's to stop over.
 */
public interface MetricsExport extends AutoCloseable {

    /**
     * The facts of one pipeline as of one observation, replacing whatever this export held for it. The
     * state rides beside the facts so an exporter can project it the one way the metrics contract allows a
     * state on the metrics side: as a gauge per state, never encoded into the facts themselves.
     */
    void offer(String pipelineId, PipelineState state, Instant observedAt, List<MetricFact> facts);

    /** Drops what is held for every pipeline outside {@code pipelineIds}, which is the set that still exists. */
    void forgetPipelinesOutside(Collection<String> pipelineIds);

    /** Stops the backend, if one was started; the default releases nothing because it holds nothing. */
    @Override
    default void close() {
    }

    /**
     * The export that carries nothing anywhere: what an assembly with no export endpoint configured
     * offers through, so the offering side is the same code whether or not anyone is listening.
     */
    static MetricsExport none() {
        return NONE;
    }

    /** One instance, so "nothing configured" compares equal to itself across the assembly. */
    MetricsExport NONE = new MetricsExport() {
        @Override
        public void offer(String pipelineId, PipelineState state, Instant observedAt, List<MetricFact> facts) {
        }

        @Override
        public void forgetPipelinesOutside(Collection<String> pipelineIds) {
        }

        @Override
        public String toString() {
            return "MetricsExport.none()";
        }
    };
}
