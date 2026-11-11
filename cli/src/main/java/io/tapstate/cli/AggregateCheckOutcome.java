package io.tapstate.cli;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The outcome of checking, or repairing, a running aggregate step
 * ({@code POST /api/pipelines/{id}/aggregates/{step}:check} and {@code ...:repair}). Either the step
 * answered for one page of its groups, or the server refused with a coded reason, or it could not be
 * reached. Sealed so the caller renders each branch without try/catch.
 */
sealed interface AggregateCheckOutcome {

    /**
     * One group that was not simply clean.
     *
     * @param group   the group, as the text of each of its key values by name; a null value is a null
     * @param verdict {@code mismatch}, {@code inconclusive}, or {@code clean} for a group listed only
     *                because it cannot be given exactly now
     * @param fields  the fields whose stored state differed from the group's rows
     * @param code    the code under which the group cannot be given exactly now, or null
     * @param field   the field that code is about, or null
     */
    record Finding(Map<String, String> group, String verdict, List<String> fields, String code, String field) {

        public Finding {
            group = new LinkedHashMap<>(group);
            fields = List.copyOf(fields);
        }
    }

    /**
     * The step answered.
     *
     * @param groups    how many groups this answer covers
     * @param next      where the next page starts, or null when the last group has been covered
     * @param conflicts input events the step has not applied since it started
     * @param withheld  times since it started that the step sent a group at its last good values
     */
    record Found(int groups, List<Finding> findings, String next, long conflicts, long withheld)
            implements AggregateCheckOutcome {

        public Found {
            findings = List.copyOf(findings);
        }
    }

    /** The server refused with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements AggregateCheckOutcome {
    }

    /** The server could not be reached, or answered with a body that is not the promised shape. */
    record Unreachable() implements AggregateCheckOutcome {
    }
}
