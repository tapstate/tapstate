package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import java.util.Collection;

/** Non-blocking assembly boundary, separate from optional telemetry export. */
interface PipelineAlertFeed {
    void expecting(PipelineConvergencePass pass);
    void published(PipelineConvergencePass pass, Observation observation, Throwable originalFailure);
    default void published(PipelineConvergencePass pass, Observation observation, Throwable originalFailure,
            boolean measured) { published(pass, observation, originalFailure); }
    void unavailable(PipelineConvergencePass pass);
    void retain(Collection<String> pipelineIds);
    /** Unknown consumers retain ordinary publication cadence. This is never an actuation proof. */
    default boolean needsFrequentTerminalSamples(io.tapstate.spi.store.PipelineIntentScope scope) { return true; }
    default boolean needsFrequentTerminalSamples(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.core.lifecycle.PipelineState actual) { return needsFrequentTerminalSamples(scope); }
    /** Unknown consumers require fresh data; lifecycle-only rules may reuse measured terminal data. */
    default boolean needsFreshTerminalMetrics(io.tapstate.spi.store.PipelineIntentScope scope) { return true; }
    static PipelineAlertFeed none() {
        return new PipelineAlertFeed() {
            public boolean needsFreshTerminalMetrics(io.tapstate.spi.store.PipelineIntentScope scope) { return false; }
            public void expecting(PipelineConvergencePass pass) { }
            public void published(PipelineConvergencePass pass, Observation observation, Throwable failure) { }
            public void unavailable(PipelineConvergencePass pass) { }
            public void retain(Collection<String> ids) { }
            public boolean needsFrequentTerminalSamples(io.tapstate.spi.store.PipelineIntentScope scope) { return false; }
        };
    }
}
