package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Selects bounded history candidates from a complete, original controller snapshot. Pending cache
 * mirrors and durable reports occupy the same per-task/per-mode pool. This is no expiry authority:
 * stores must still verify the original control revision, irreversible terminal facts and retention
 * fence before sealing or deleting. Selection does not release storage bytes or permit late upserts.
 * Input reading/materialization and this component's conservative metadata budget are pre-admitted.
 */
public final class ValidationHistoryRetention {
    public record Policy(int full, int incremental) {
        public static final Policy DEFAULT = new Policy(10, 10);
        public Policy {
            if (full < 1 || full > 100 || incremental < 1 || incremental > 100) throw invalid("history.retention");
        }
        int forMode(ValidationMode mode) { return mode == ValidationMode.FULL ? full : incremental; }
    }
    public record Scope(String cluster, String pipeline, String task) {
        public Scope {
            cluster = ValidationError.identifier(cluster, "history.cluster");
            pipeline = ValidationError.identifier(pipeline, "history.pipeline");
            task = ValidationError.identifier(task, "history.task");
        }
        boolean contains(ValidationRunIdentity run) {
            return cluster.equals(run.owner().clusterId()) && pipeline.equals(run.owner().pipelineId()) && task.equals(run.taskId());
        }
    }
    public enum Copy { CACHE_PENDING, CONFIG_REPORT }

    /** A determined terminal fact, never a best-effort status projection or an unknown CAS outcome. */
    public record Fact(ValidationRunIdentity run, ValidationRunState state, Instant endedAt, Copy copy) {
        public Fact {
            Objects.requireNonNull(run, "run"); Objects.requireNonNull(state, "state");
            Objects.requireNonNull(endedAt, "endedAt"); Objects.requireNonNull(copy, "copy");
            if (!state.terminal() || run.mode() == ValidationMode.INCREMENTAL && state == ValidationRunState.COMPLETED)
                throw invalid("history.terminalFact");
        }
    }
    public record EndedRun(ValidationRunIdentity run, ValidationRunState state, Instant endedAt, Set<Copy> copies) {
        public EndedRun { copies = Set.copyOf(copies); }
    }
    public record Limits(int facts, long bytes) {
        public Limits {
            if (facts < 1 || facts > 8192 || bytes < 1 || bytes > 32L * 1024 * 1024) throw invalid("history.limits");
        }
    }
    public record Selection(Scope scope, long controlRevision, List<EndedRun> retained,
                            List<EndedRun> expirationCandidates, Set<String> protectedRunIds, long accountedBytes) {
        public Selection {
            retained = List.copyOf(retained); expirationCandidates = List.copyOf(expirationCandidates);
            protectedRunIds = Set.copyOf(protectedRunIds);
        }
    }
    private static final Comparator<EndedRun> NEWEST_FIRST = Comparator.comparing(EndedRun::endedAt).reversed()
            .thenComparing(entry -> entry.run().runId());
    private ValidationHistoryRetention() {}

    /**
     * Active runs and undetermined terminal outcomes are protected separately, never counted as ended.
     * Contradictory copies or a protected/terminal collision fail closed; the caller must reconcile them.
     * The two complete input reads must belong to this fixed scope and controller revision.
     */
    public static Selection select(Scope scope, long controlRevision, Policy policy, List<Fact> facts,
            Set<String> protectedRunIds, Limits limits) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(facts, "facts"); Objects.requireNonNull(protectedRunIds, "protectedRunIds");
        Objects.requireNonNull(limits, "limits");
        if (controlRevision < 1) throw invalid("history.controlRevision");
        if (facts.size() > limits.facts() || protectedRunIds.size() > limits.facts() - facts.size()) throw limit("facts");
        long bytes = charge(0, 512L + textBytes(scope.cluster()) + textBytes(scope.pipeline()) + textBytes(scope.task()), limits);
        for (String id : protectedRunIds) {
            ValidationError.identifier(id, "history.protectedRunId");
            bytes = charge(bytes, 96L + textBytes(id), limits);
        }
        var distinct = new LinkedHashMap<String, Entry>();
        for (var fact : facts) {
            Objects.requireNonNull(fact, "fact");
            if (!scope.contains(fact.run())) throw invalid("history.scope");
            if (protectedRunIds.contains(fact.run().runId())) throw invalid("history.terminalUnknown");
            bytes = charge(bytes, factBytes(fact), limits);
            var existing = distinct.get(fact.run().runId());
            if (existing == null) distinct.put(fact.run().runId(), new Entry(fact));
            else {
                if (!existing.fact.run().equals(fact.run()) || existing.fact.state() != fact.state()
                        || !existing.fact.endedAt().equals(fact.endedAt())) throw invalid("history.copyConflict");
                existing.copies.add(fact.copy());
            }
        }
        var retained = new ArrayList<EndedRun>();
        var expired = new ArrayList<EndedRun>();
        for (var mode : ValidationMode.values()) {
            var ended = new ArrayList<EndedRun>();
            for (var entry : distinct.values()) if (entry.fact.run().mode() == mode)
                ended.add(new EndedRun(entry.fact.run(), entry.fact.state(), entry.fact.endedAt(), entry.copies));
            ended.sort(NEWEST_FIRST);
            int keep = Math.min(policy.forMode(mode), ended.size());
            retained.addAll(ended.subList(0, keep)); expired.addAll(ended.subList(keep, ended.size()));
        }
        return new Selection(scope, controlRevision, retained, expired, protectedRunIds, bytes);
    }

    private static final class Entry {
        private final Fact fact;
        private final EnumSet<Copy> copies;
        private Entry(Fact fact) { this.fact = fact; copies = EnumSet.of(fact.copy()); }
    }
    private static long factBytes(Fact fact) {
        var run = fact.run(); var owner = run.owner();
        return 1024L + textBytes(owner.clusterId()) + textBytes(owner.pipelineId()) + textBytes(owner.nodeId())
                + textBytes(owner.bootId()) + textBytes(run.taskId()) + textBytes(run.runId())
                + textBytes(run.specHash()) + textBytes(run.comparisonRulesVersion());
    }
    private static long textBytes(String text) { return 48L + 2L * text.length(); }
    private static long charge(long used, long amount, Limits limits) {
        if (amount > limits.bytes() - used) throw limit("bytes");
        return used + amount;
    }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit(String dimension) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "history." + dimension), null); }
}
