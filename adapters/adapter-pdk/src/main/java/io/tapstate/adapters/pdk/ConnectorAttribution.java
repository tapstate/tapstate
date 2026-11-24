package io.tapstate.adapters.pdk;

import io.tapstate.core.logging.PipelineAttribution;

import org.slf4j.MDC;

/**
 * Says which pipeline the work about to run belongs to, for as long as it runs.
 *
 * <p>A connector's output is filed against a pipeline by the thread that writes it, and a thread says so
 * by carrying the attribution while it works. The threads this runs on are shared and long-lived -- a
 * pooled executor, the tail's own thread, whatever member ran the vertex -- so whatever was in the slot
 * is handed back afterwards rather than cleared: a drive that cleared it would silently unattribute
 * everything the caller logged after it returned.
 */
final class ConnectorAttribution {

    private ConnectorAttribution() {
    }

    static Scope claim(String pipelineId, long stateGeneration) {
        var previous = new Scope(MDC.get(PipelineAttribution.MDC_KEY),
                MDC.get(PipelineAttribution.STATE_GENERATION_MDC_KEY));
        set(PipelineAttribution.MDC_KEY, pipelineId);
        set(PipelineAttribution.STATE_GENERATION_MDC_KEY, stateGeneration > 0 ? Long.toString(stateGeneration) : null);
        return previous;
    }

    private static void set(String key, String value) {
        if (value == null) {
            MDC.remove(key);
        } else {
            MDC.put(key, value);
        }
    }

    record Scope(String pipelineId, String stateGeneration) implements AutoCloseable {
        @Override
        public void close() {
            set(PipelineAttribution.MDC_KEY, pipelineId);
            set(PipelineAttribution.STATE_GENERATION_MDC_KEY, stateGeneration);
        }
    }
}
