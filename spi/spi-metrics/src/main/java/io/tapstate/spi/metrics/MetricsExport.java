package io.tapstate.spi.metrics;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

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

    /** Internal current-resource owner; it never becomes an exported point attribute. */
    record ScopeToken(String incarnationId, long executionGeneration) {
        public ScopeToken {
            Objects.requireNonNull(incarnationId, "incarnationId");
            if (incarnationId.isBlank() || executionGeneration <= 0) {
                throw new IllegalArgumentException("an export scope needs an incarnation and generation");
            }
        }
    }

    /**
     * The facts of one pipeline as of one observation, replacing whatever this export held for it. The
     * state rides beside the facts so an exporter can project it the one way the metrics contract allows a
     * state on the metrics side: as a gauge per state, never encoded into the facts themselves.
     */
    void offer(String pipelineId, PipelineState state, Instant observedAt, List<MetricFact> facts);

    /**
     * Offers facts already folded by the observation publisher. An exporter may apply its process-wide
     * series ceiling, but must preserve this per-pipeline reduction. Other implementations can treat it
     * as an ordinary offer.
     */
    default void offerFolded(String pipelineId, PipelineState state, Instant observedAt, List<MetricFact> facts) {
        offer(pipelineId, state, observedAt, facts);
    }

    /** Offers a folded frame with its internal owner so a reused id cannot expose old current points. */
    default void offerFoldedScoped(String pipelineId, ScopeToken scope, PipelineState state,
            Instant observedAt, List<MetricFact> facts) {
        offerFolded(pipelineId, state, observedAt, facts);
    }

    /** Binds a local, nonblocking owner lookup used to filter current points during collection. */
    default void bindCurrentScopes(Function<String, Optional<ScopeToken>> current) {
    }

    /** Forgets only the deleted resource's current points; a new incarnation under the id survives. */
    default void forgetIncarnation(String pipelineId, String incarnationId) {
    }

    /** Registers a cheap, store-independent process reading sampled by the exporter's own pull cadence. */
    default void observeProcess(Supplier<List<MetricFact>> facts) {
    }

    /** Replaces one named process reading without replacing other local health sources. */
    default void observeProcess(String source, Supplier<List<MetricFact>> facts) {
        observeProcess(facts);
    }

    /** Drops what is held for every pipeline outside {@code pipelineIds}, which is the set that still exists. */
    void forgetPipelinesOutside(Collection<String> pipelineIds);

    /** Releases one current pipeline's local series when its resource or execution identity changes. */
    default void forgetPipeline(String pipelineId) {
    }

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
