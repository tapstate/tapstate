package io.tapstate.app;

import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.logging.LogSink;
import io.tapstate.spi.store.ObservationStore;
import org.slf4j.MDC;

/** Carries a pipeline's log owner through a bounded unit of lifecycle work. */
record PipelineLogContext(String pipelineId, String incarnationId, String generation) {

    static PipelineLogContext capture() {
        return new PipelineLogContext(MDC.get(PipelineAttribution.MDC_KEY),
                MDC.get(PipelineAttribution.INCARNATION_MDC_KEY),
                MDC.get(PipelineAttribution.EXECUTION_MDC_KEY));
    }

    /** Returns the current complete log owner only when it names this exact pipeline. */
    static LogSink.Scope scopeFor(String pipelineId) {
        PipelineLogContext context = capture();
        if (pipelineId == null || !pipelineId.equals(context.pipelineId())
                || context.incarnationId() == null || context.generation() == null) {
            return null;
        }
        try {
            return new LogSink.Scope(context.incarnationId(), Long.parseLong(context.generation()));
        } catch (IllegalArgumentException malformedContext) {
            return null;
        }
    }

    static void bindScope(ObservationStore.Scope scope) {
        putOrRemove(PipelineAttribution.INCARNATION_MDC_KEY,
                scope == null ? null : scope.pipelineIncarnationId());
        putOrRemove(PipelineAttribution.EXECUTION_MDC_KEY,
                scope == null ? null : Long.toString(scope.executionGeneration()));
    }

    void restore() {
        putOrRemove(PipelineAttribution.MDC_KEY, pipelineId);
        putOrRemove(PipelineAttribution.INCARNATION_MDC_KEY, incarnationId);
        putOrRemove(PipelineAttribution.EXECUTION_MDC_KEY, generation);
    }

    private static void putOrRemove(String key, String value) {
        if (value == null) {
            MDC.remove(key);
        } else {
            MDC.put(key, value);
        }
    }
}
