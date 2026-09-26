package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * One evidence-backed explanation projected from one current observation.
 *
 * <p>{@code plan} is the plan the pipeline's current run was submitted on - how wide each node runs and why - or
 * {@code null} when no run has one recorded. It answers beside the diagnosis rather than as part of it: no rule
 * reads it, and no conclusion is drawn from a width. {@code awaitingRebalance} are the members of the cluster that
 * plan was not worked out for, by stable id - members that joined after the run was planned - and is empty where
 * there are none or no plan.
 */
public record PipelineExplanation(
        String pipelineId,
        PipelineState state,
        Kind kind,
        String message,
        Instant observedAt,
        Long observedAgeMillis,
        Freshness freshness,
        List<Evidence> evidence,
        List<String> cannotSay,
        Next next,
        Pending pending,
        ExecutionPlan plan,
        List<String> awaitingRebalance) {

    public PipelineExplanation {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(freshness, "freshness");
        evidence = List.copyOf(Objects.requireNonNull(evidence, "evidence"));
        cannotSay = List.copyOf(Objects.requireNonNull(cannotSay, "cannotSay"));
        // Absent reads as none: the wire omits an empty list, and an explanation read back from it names nobody.
        awaitingRebalance = awaitingRebalance == null ? List.of() : List.copyOf(awaitingRebalance);
        if ((observedAt == null) != (observedAgeMillis == null)) {
            throw new IllegalArgumentException("observation time and age are both present or both absent");
        }
        if (kind == Kind.NO_MATCH && cannotSay.isEmpty()) {
            throw new IllegalArgumentException("a no-match explanation names what it cannot say");
        }
    }

    /** An explanation of a run with no plan recorded. */
    public PipelineExplanation(String pipelineId, PipelineState state, Kind kind, String message,
            Instant observedAt, Long observedAgeMillis, Freshness freshness, List<Evidence> evidence,
            List<String> cannotSay, Next next, Pending pending) {
        this(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness, evidence, cannotSay, next,
                pending, null, List.of());
    }

    /** The same explanation, beside the plan the pipeline's current run was submitted on. */
    public PipelineExplanation withPlan(ExecutionPlan plan) {
        return withPlan(plan, List.of());
    }

    /**
     * The same explanation, beside the plan the pipeline's current run was submitted on and the members of the
     * cluster it was not worked out for.
     */
    public PipelineExplanation withPlan(ExecutionPlan plan, List<String> awaitingRebalance) {
        return new PipelineExplanation(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness,
                evidence, cannotSay, next, pending, plan, awaitingRebalance);
    }

    public enum Kind {
        OBSERVATION_STALE,
        CODED_FAILURE,
        RECONCILE_FAILURES,
        NO_MOVEMENT,
        FRONTIER_STALLED,
        NO_MATCH
    }

    public enum Freshness {
        FRESH,
        STALE,
        UNKNOWN
    }

    public enum Source {
        STATUS,
        METRICS,
        SNAPSHOT,
        LIFECYCLE
    }

    public enum NextAction {
        OPEN_PIPELINE_LOGS,
        CHECK_SERVER,
        CHECK_TARGET
    }

    public enum PendingReason {
        START_CAPACITY,
        STOP_CAPACITY,
        START_PENDING,
        STOP_PENDING
    }

    public record Evidence(Source source, String field, Object value) {
        public Evidence {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(field, "field");
            value = immutable(value);
        }

        private static Object immutable(Object value) {
            if (value instanceof Map<?, ?> map) {
                return Collections.unmodifiableMap(new LinkedHashMap<>(map));
            }
            if (value instanceof List<?> list) {
                return List.copyOf(list);
            }
            return value;
        }
    }

    public record Failure(String code, Map<String, String> params, String message) {
        public Failure {
            Objects.requireNonNull(code, "code");
            params = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(params, "params")));
            Objects.requireNonNull(message, "message");
        }
    }

    public record Next(NextAction action, String message) {
        public Next {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(message, "message");
        }
    }

    public record Pending(PendingReason reason) {
        public Pending {
            Objects.requireNonNull(reason, "reason");
        }
    }
}
