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
        List<String> awaitingRebalance,
        List<Rollup> rollups,
        List<Aggregate> aggregates) {

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
        rollups = rollups == null ? List.of() : List.copyOf(rollups);
        aggregates = aggregates == null ? List.of() : List.copyOf(aggregates);
        if ((observedAt == null) != (observedAgeMillis == null)) {
            throw new IllegalArgumentException("observation time and age are both present or both absent");
        }
        if (kind == Kind.NO_MATCH && cannotSay.isEmpty()) {
            throw new IllegalArgumentException("a no-match explanation names what it cannot say");
        }
    }

    /** An explanation that lists no aggregate steps. */
    public PipelineExplanation(String pipelineId, PipelineState state, Kind kind, String message,
            Instant observedAt, Long observedAgeMillis, Freshness freshness, List<Evidence> evidence,
            List<String> cannotSay, Next next, Pending pending, ExecutionPlan plan, List<String> awaitingRebalance,
            List<Rollup> rollups) {
        this(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness, evidence, cannotSay, next,
                pending, plan, awaitingRebalance, rollups, List.of());
    }

    /** An explanation that lists no rollup fields. */
    public PipelineExplanation(String pipelineId, PipelineState state, Kind kind, String message,
            Instant observedAt, Long observedAgeMillis, Freshness freshness, List<Evidence> evidence,
            List<String> cannotSay, Next next, Pending pending, ExecutionPlan plan, List<String> awaitingRebalance) {
        this(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness, evidence, cannotSay, next,
                pending, plan, awaitingRebalance, List.of());
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
                evidence, cannotSay, next, pending, plan, awaitingRebalance, rollups, aggregates);
    }

    /** The same explanation, listing the rollup fields the pipeline declares. */
    public PipelineExplanation withRollups(List<Rollup> declared) {
        return new PipelineExplanation(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness,
                evidence, cannotSay, next, pending, plan, awaitingRebalance, declared, aggregates);
    }

    /** The same explanation, listing what each aggregate step of the pipeline keeps. */
    public PipelineExplanation withAggregates(List<Aggregate> steps) {
        return new PipelineExplanation(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness,
                evidence, cannotSay, next, pending, plan, awaitingRebalance, rollups, steps);
    }

    /**
     * What one aggregate step keeps in state. The step stores every input row it has seen and, on the
     * group side, the row again and one more entry for each field that needs its inputs kept, so the cost
     * per input row is known from the definition alone.
     *
     * @param step             the step
     * @param entriesPerRow    state entries kept per input row: two, and one per field that keeps its inputs
     * @param keptInputFields  the fields that keep their inputs, by name
     * @param inputRows        how many rows the step's inputs hold by their last load's estimate, or null
     *                         when any of them is not known
     * @param estimatedEntries {@code entriesPerRow} times {@code inputRows}, or null when that is not known;
     *                         an upper estimate, since a row failing {@code where:} keeps one entry only
     * @param storedEntries    what the running step last counted, or null when it has not finished a count
     */
    public record Aggregate(String step, int entriesPerRow, List<String> keptInputFields, Long inputRows,
            Long estimatedEntries, Long storedEntries) {
        public Aggregate {
            Objects.requireNonNull(step, "step");
            keptInputFields = List.copyOf(keptInputFields);
        }
    }

    /**
     * One rollup field the pipeline declares: the nest step, the level it lands on, the embed it is computed
     * over, its function and argument as written, its filter, and the references it reads - the ones whose
     * absence makes it null rather than a smaller number.
     */
    public record Rollup(String step, String level, String embed, String field, String function, String argument,
            String filter, List<String> references) {
        public Rollup {
            Objects.requireNonNull(step, "step");
            Objects.requireNonNull(level, "level");
            Objects.requireNonNull(embed, "embed");
            Objects.requireNonNull(field, "field");
            Objects.requireNonNull(function, "function");
            references = references == null ? List.of() : List.copyOf(references);
        }
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
