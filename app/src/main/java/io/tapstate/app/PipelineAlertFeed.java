package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import java.util.Collection;

/** Non-blocking assembly boundary, separate from optional telemetry export. */
interface PipelineAlertFeed {
    void expecting(PipelineConvergencePass pass);
    void published(PipelineConvergencePass pass, Observation observation, Throwable originalFailure);
    void unavailable(PipelineConvergencePass pass);
    void retain(Collection<String> pipelineIds);
    static PipelineAlertFeed none() {
        return new PipelineAlertFeed() {
            public void expecting(PipelineConvergencePass pass) { }
            public void published(PipelineConvergencePass pass, Observation observation, Throwable failure) { }
            public void unavailable(PipelineConvergencePass pass) { }
            public void retain(Collection<String> ids) { }
        };
    }
}
