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

    /**
     * Claims the slot for {@code pipelineId}, answering what was in it. Pass that answer back to
     * {@link #restore(String)} when the work is done; call neither for a drive naming no pipeline.
     */
    static String claim(String pipelineId) {
        String previous = MDC.get(PipelineAttribution.MDC_KEY);
        MDC.put(PipelineAttribution.MDC_KEY, pipelineId);
        return previous;
    }

    /** Puts back what {@link #claim(String)} found, leaving the slot empty when it found nothing. */
    static void restore(String previous) {
        if (previous == null) {
            MDC.remove(PipelineAttribution.MDC_KEY);
        } else {
            MDC.put(PipelineAttribution.MDC_KEY, previous);
        }
    }
}
