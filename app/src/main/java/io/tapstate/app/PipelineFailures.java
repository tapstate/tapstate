package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.runtime.engine.EngineError;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps the Throwable that killed a pipeline's run to the coded failure its observation carries, so a dead
 * job reports a reason the read faces can render rather than a bare state change with the cause left in a
 * log line. This lives in the assembly layer because it is the one place that sees both the engine's codes
 * and the observation the scheduler publishes; neither ring depends on the other.
 *
 * <p>A cause the product already coded at its throw site keeps that code: it is more specific than any
 * wrapper around it and is the one that tells the user what to fix. Everything else reports the generic
 * engine failure — a reason is always named, but an uncoded fault is never laundered into a specific code
 * it does not deserve.
 */
final class PipelineFailures {

    /** How deep to look for a coded cause before giving up; a chain longer than this is a cycle or a bug. */
    private static final int MAX_CAUSE_DEPTH = 32;

    /**
     * How much of an uncoded cause's description this carries. Bounding matters specifically because
     * Jet can hand back a "failure" whose message is not a message at all: once a job's live context is
     * gone, {@code JobResult.getFailureAsThrowable()} reconstructs a mock throwable from the stored
     * failure text, which is a full {@code printStackTrace()} dump (measured: 1400+ characters, 17+
     * lines) — every other free-text detail in this codebase bounds itself (e.g. {@code
     * TransformErrors.detail}), this is the one path that read the raw driver/framework message
     * unbounded.
     */
    private static final int MAX_DESCRIPTION_LENGTH = 200;

    private PipelineFailures() {
    }

    /**
     * The coded failure to publish for {@code pipelineId}, given the throwable that ended its run. A coded
     * cause keeps its own code and named arguments, but the argument values are bounded the same way an
     * uncoded description is: not every argument is product-authored — a sink's write failure carries the
     * driver's own message as its detail, which can be as long and as multiline as any raw cause — and
     * whatever rides here is persisted and printed verbatim by the read faces.
     */
    static ObservationFailure of(String pipelineId, Throwable failure) {
        TapstateException coded = codedCause(failure);
        if (coded != null) {
            Map<String, String> params = new LinkedHashMap<>();
            coded.args().forEach((name, value) -> params.put(name, bound(String.valueOf(value))));
            return new ObservationFailure(coded.code().code(), params);
        }
        return new ObservationFailure(EngineError.JOB_FAILED.code(),
                Map.of("pipeline", pipelineId, "cause", describe(failure)));
    }

    /** Recognizes the coded sink failure before Jet can reduce it to a generic wrapper. */
    static boolean isSinkWriteFailure(String pipelineId, Throwable failure) {
        return failure != null
                && ConnectorError.WRITE_FAILED.code().equals(of(pipelineId, failure).code());
    }

    /**
     * The first coded exception in the throwable's cause chain, or {@code null} when nothing in it carries a
     * code. The data plane wraps what a sink or a connector threw before it reaches the converge loop, so the
     * coded fault is usually not the outermost throwable.
     */
    private static TapstateException codedCause(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof TapstateException coded) {
                return coded;
            }
            current = current.getCause() == current ? null : current.getCause();
        }
        return null;
    }

    /**
     * What the run died of, in one bounded line: the throwable's message, or its type when it has none.
     * A declared placeholder always gets a value, and an empty string would tell the user nothing. Bounded
     * to its first line and {@link #MAX_DESCRIPTION_LENGTH} characters, so a message that is itself a
     * multi-line stack-trace dump never rides through whole.
     */
    private static String describe(Throwable failure) {
        if (failure == null) {
            return "unknown";
        }
        String message = failure.getMessage();
        String description = message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
        return bound(description);
    }

    /** Caps free text to its first line and {@link #MAX_DESCRIPTION_LENGTH} characters, marking either cut. */
    private static String bound(String text) {
        int newline = text.indexOf('\n');
        boolean multiline = newline >= 0;
        String firstLine = multiline ? text.substring(0, newline) : text;
        boolean overlong = firstLine.length() > MAX_DESCRIPTION_LENGTH;
        String capped = overlong ? firstLine.substring(0, MAX_DESCRIPTION_LENGTH) : firstLine;
        return multiline || overlong ? capped + " …" : capped;
    }
}
