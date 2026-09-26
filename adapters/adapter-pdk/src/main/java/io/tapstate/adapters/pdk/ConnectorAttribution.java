package io.tapstate.adapters.pdk;

import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.logging.LogSink;

import org.slf4j.MDC;

/**
 * Says which pipeline execution the work about to run belongs to, for as long as it runs.
 *
 * <p>A connector's output is filed against a pipeline by the thread that writes it, and a thread says so
 * by carrying the attribution while it works. The threads this runs on are shared and long-lived -- a
 * pooled executor, the tail's own thread, whatever member ran the vertex -- so all three slots are
 * replaced with this handle's captured owner and handed back afterwards. Retaining an inherited scope
 * would file an old handle's late line under the execution currently using the thread.
 */
final class ConnectorAttribution {

    private ConnectorAttribution() {
    }

    /** Binds exactly the owner captured by this handle, clearing inherited identity when it has none. */
    static Previous claim(String pipelineId, LogSink.Scope scope) {
        if (pipelineId == null && scope != null) {
            throw new IllegalArgumentException("a log scope requires a pipeline");
        }
        Previous previous = new Previous(MDC.get(PipelineAttribution.MDC_KEY),
                MDC.get(PipelineAttribution.INCARNATION_MDC_KEY),
                MDC.get(PipelineAttribution.EXECUTION_MDC_KEY));
        putOrRemove(PipelineAttribution.MDC_KEY, pipelineId);
        putOrRemove(PipelineAttribution.INCARNATION_MDC_KEY,
                scope == null ? null : scope.pipelineIncarnationId());
        putOrRemove(PipelineAttribution.EXECUTION_MDC_KEY,
                scope == null ? null : Long.toString(scope.executionGeneration()));
        return previous;
    }

    /** Returns every diagnostic slot to the caller's previous owner. */
    static void restore(Previous previous) {
        putOrRemove(PipelineAttribution.MDC_KEY, previous.pipelineId());
        putOrRemove(PipelineAttribution.INCARNATION_MDC_KEY, previous.incarnationId());
        putOrRemove(PipelineAttribution.EXECUTION_MDC_KEY, previous.generation());
    }

    private static void putOrRemove(String key, String value) {
        if (value == null) {
            MDC.remove(key);
        } else {
            MDC.put(key, value);
        }
    }

    record Previous(String pipelineId, String incarnationId, String generation) {
    }
}
