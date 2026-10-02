package io.tapstate.cli;

import java.util.List;
import java.util.Map;

/**
 * The outcome of {@code POST /api/pipelines/{id}:start}, read for what its start checks said. Sealed like
 * every other remote outcome, so a caller renders each branch without try/catch.
 */
sealed interface StartAttempt {

    /**
     * The start went ahead.
     *
     * @param checks           what the start checks said, null from a server that runs none
     * @param decisionsApplied the answers the server took, as it reported them
     * @param staleDecisions   the answers it ignored because their questions are no longer asked
     * @param raw              the whole answer, for {@code -o json|yaml}
     */
    record Started(String pipelineId, String targetState, String revision, StartChecks checks,
            List<Map<String, Object>> decisionsApplied, List<Map<String, Object>> staleDecisions,
            Map<String, Object> raw) implements StartAttempt {

        public Started {
            decisionsApplied = List.copyOf(decisionsApplied);
            staleDecisions = List.copyOf(staleDecisions);
            raw = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(raw));
        }
    }

    /** The start checks stopped the start: a question to answer, or a refusal. */
    record Stopped(String code, Map<String, Object> params, String message, StartChecks checks)
            implements StartAttempt {

        public Stopped {
            params = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(params));
        }
    }

    /**
     * The server refused the start for another coded reason. {@code decisionsApplied} is non-empty when it had
     * already changed the definition as answered before refusing, which the person has to be told.
     */
    record Rejected(String code, Map<String, Object> params, String message, List<Map<String, Object>> decisionsApplied)
            implements StartAttempt {

        public Rejected {
            params = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(params));
            decisionsApplied = List.copyOf(decisionsApplied);
        }
    }

    /**
     * The server could not be reached, or did not answer in time.
     *
     * @param sent whether the start may have reached the server: true after a timeout, when it may have gone
     *             ahead and must not be sent again; false when no connection was made at all
     */
    record Unreachable(boolean sent) implements StartAttempt {
    }
}
