package io.tapstate.adapters.transform;

import io.tapstate.core.common.TapstateException;

import java.util.Map;

/**
 * Builds the coded {@link TapstateException}s the transform ports raise from the {@code transform}
 * domain codes. One place, so every throw site supplies exactly the named arguments its code declares
 * and the evaluator diagnostic is extracted from the cause the same way.
 */
final class TransformErrors {

    private TransformErrors() {
    }

    /** A row expression that failed to evaluate; carries the expression text and the diagnostic. */
    static TapstateException expressionFailed(String expr, Throwable cause) {
        return new TapstateException(TransformError.EXPRESSION_FAILED,
                Map.of("expr", expr, "detail", detail(cause)), cause);
    }

    /** A js script that would not compile; carries the script-engine diagnostic. */
    static TapstateException scriptCompileFailed(Throwable cause) {
        return new TapstateException(TransformError.SCRIPT_COMPILE_FAILED, Map.of("detail", detail(cause)), cause);
    }

    /** A js script with no {@code process(record, ctx)} entry point. */
    static TapstateException scriptNoProcess() {
        return new TapstateException(TransformError.SCRIPT_NO_PROCESS, Map.of(), null);
    }

    /** A js script that threw while processing an event; carries the guest-side diagnostic. */
    static TapstateException scriptFailed(Throwable cause) {
        return new TapstateException(TransformError.SCRIPT_FAILED, Map.of("detail", detail(cause)), cause);
    }

    /** A js script whose output is not a valid record; {@code detail} names what was wrong. */
    static TapstateException scriptOutputInvalid(String detail) {
        return new TapstateException(TransformError.SCRIPT_OUTPUT_INVALID, Map.of("detail", detail), null);
    }

    /**
     * An expansion handed an update or a delete whose earlier row is half a row; {@code detail} says
     * which way it fell short, because the setting that fixes it differs between the two.
     */
    static TapstateException unwindNeedsACompleteBeforeImage(String path, String detail) {
        return new TapstateException(TransformError.UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE,
                Map.of("path", path, "detail", detail), null);
    }

    /**
     * Two elements of one row expanding onto the same key. Built and handed to the alert rather than
     * thrown: the rows are still sent, and this severity means the run carries on.
     */
    static TapstateException unwindRowsShareAKey(String path, Object key) {
        return new TapstateException(TransformError.UNWIND_ROWS_SHARE_A_KEY,
                Map.of("path", path, "key", String.valueOf(key)), null);
    }

    // The developer-facing detail carried as the {detail} argument: the cause's own message, or its
    // type when it carries none, so the argument map always satisfies the code's placeholder contract.
    private static String detail(Throwable cause) {
        String message = cause.getMessage();
        return message != null ? message : cause.getClass().getSimpleName();
    }
}
