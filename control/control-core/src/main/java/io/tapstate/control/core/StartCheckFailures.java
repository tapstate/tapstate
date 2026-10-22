package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;

import java.util.concurrent.ExecutionException;

/** One failure policy for checks and their nested probes: only coded failures may become warnings. */
final class StartCheckFailures {
    private StartCheckFailures() { }

    static TapstateException coded(ExecutionException failed) {
        Throwable cause = failed.getCause();
        if (cause instanceof TapstateException coded) {
            return coded;
        }
        if (cause instanceof RuntimeException defect) {
            throw defect;
        }
        if (cause instanceof Error fatal) {
            throw fatal;
        }
        throw new IllegalStateException(cause);
    }
}
