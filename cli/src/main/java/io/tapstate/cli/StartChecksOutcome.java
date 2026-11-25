package io.tapstate.cli;

import java.util.Map;

/** The outcome of {@code GET /api/pipelines/{id}/start-checks}. */
sealed interface StartChecksOutcome {

    /** The start checks a start would be asked. */
    record Found(StartChecks checks) implements StartChecksOutcome {
    }

    /** The server has no start checks to read: one released before them. */
    record NotSupported() implements StartChecksOutcome {
    }

    /** The server refused the read with a coded reason. */
    record Rejected(String code, Map<String, Object> params, String message) implements StartChecksOutcome {

        public Rejected {
            params = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(params));
        }
    }

    /** The server could not be reached, or did not answer in time. */
    record Unreachable() implements StartChecksOutcome {
    }
}
