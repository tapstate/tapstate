package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationMethod;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Method-specific structural restrictions over selected declarations. Separate branches do not become
 * a multiple-expansion path merely because the pipeline contains two Unwinds. Dynamic references
 * remain obligations of the declaration stage; this report never declares a graph executable.
 * Count requires an explicit root-preservation/native-condition rule where expansion or union occurs;
 * this component neither rejects a proven child-only exception nor invents that proof from topology.
 */
public final class ValidationStructuralRestrictions {
    private ValidationStructuralRestrictions() {}
    public enum Kind { UNSUPPORTED, ROOT_COUNT_RULE_REQUIRED }
    public enum Rule { UNION, ELEMENT_KEY, PRESERVE_EMPTY, ORDINAL_REQUIRED, ORDINAL_COLUMN_CONFLICT, MULTIPLE_UNWINDS, UNWIND_AFTER_NEST, COUNT_ROOT_RULE }
    public record Reason(Kind kind, String node, String option, Rule rule, Optional<String> relatedNode) {
        public Reason {
            Objects.requireNonNull(kind, "kind"); Objects.requireNonNull(rule, "rule");
            Objects.requireNonNull(relatedNode, "relatedNode");
            name(node); name(option); relatedNode.ifPresent(ValidationStructuralRestrictions::name);
        }
    }
    public record Limits(int nodes, int visits, int reasons, long bytes) {
        public Limits {
            if (nodes < 1 || nodes > 4096 || visits < 1 || visits > 131072 || reasons < 1 || reasons > 4096
                    || bytes < 1 || bytes > 16L * 1024 * 1024) throw invalid("structuralRestrictions.limits");
        }
        public static Limits defaults() { return new Limits(1024, 16384, 256, 1024 * 1024); }
    }
    public record Report(ValidationMethod method, List<Reason> reasons, int visitedSteps, long accountedBytes) {
        public Report { Objects.requireNonNull(method, "method"); reasons = List.copyOf(reasons); }
    }

    /** The common JS/Join/Computed exclusions and all model/native/version checks remain mandatory. */
    public static Report inspect(ValidationDeclarations.Inspection declarations, ValidationMethod method, Limits limits) {
        Objects.requireNonNull(declarations, "declarations"); Objects.requireNonNull(method, "method");
        Objects.requireNonNull(limits, "limits");
        if (declarations.steps().size() > limits.nodes()) throw limit("nodes");
        var budget = new Budget(limits);
        budget.charge(256L + 32L * declarations.steps().size());
        var steps = new ArrayList<>(declarations.steps().values());
        steps.sort(Comparator.comparing(Step.Inline::id));
        for (Step.Inline step : steps) {
            budget.visit();
            if (step.body() instanceof TransformBody.Union) {
                budget.add(method == ValidationMethod.COUNT ? countRule(step.id()) : unsupported(step.id(), "type", Rule.UNION, null));
            } else if (step.body() instanceof TransformBody.Unwind unwind) {
                if (method == ValidationMethod.COUNT) { budget.add(countRule(step.id())); continue; }
                if (unwind.elementKey() != null) budget.add(unsupported(step.id(), "elementKey", Rule.ELEMENT_KEY, null));
                if (Boolean.TRUE.equals(unwind.preserveNullAndEmptyArrays())) budget.add(unsupported(step.id(), "preserveNullAndEmptyArrays", Rule.PRESERVE_EMPTY, null));
                if (unwind.includeArrayIndex() == null || unwind.includeArrayIndex().isBlank())
                    budget.add(unsupported(step.id(), "includeArrayIndex", Rule.ORDINAL_REQUIRED, null));
                else if (unwind.includeArrayIndex().equals(unwind.path()))
                    budget.add(unsupported(step.id(), "includeArrayIndex", Rule.ORDINAL_COLUMN_CONFLICT, null));
            }
        }
        if (method != ValidationMethod.COUNT) {
            for (var route : declarations.routes()) path(route.steps(), budget);
            for (var pending : declarations.pending()) path(pending.downstreamSteps(), budget);
        }
        return new Report(method, new ArrayList<>(budget.reasons), budget.visits, budget.bytes);
    }

    private static void path(List<Step.Inline> steps, Budget budget) {
        String firstUnwind = null;
        String lastNest = null;
        for (Step.Inline step : steps) {
            budget.visit();
            if (step.body() instanceof TransformBody.Nest) lastNest = step.id();
            if (step.body() instanceof TransformBody.Unwind) {
                if (firstUnwind != null) budget.add(unsupported(step.id(), "from", Rule.MULTIPLE_UNWINDS, firstUnwind));
                if (lastNest != null) budget.add(unsupported(step.id(), "from", Rule.UNWIND_AFTER_NEST, lastNest));
                if (firstUnwind == null) firstUnwind = step.id();
            }
        }
    }
    private static Reason countRule(String node) { return new Reason(Kind.ROOT_COUNT_RULE_REQUIRED, node, "type", Rule.COUNT_ROOT_RULE, Optional.empty()); }
    private static Reason unsupported(String node, String option, Rule rule, String related) {
        return new Reason(Kind.UNSUPPORTED, node, option, rule, Optional.ofNullable(related));
    }
    private static final class Budget {
        private final Limits limits;
        private final LinkedHashSet<Reason> reasons = new LinkedHashSet<>();
        private int visits;
        private long bytes;
        private Budget(Limits limits) { this.limits = limits; }
        private void visit() { if (visits++ == limits.visits()) throw limit("visits"); }
        private void add(Reason reason) {
            if (reasons.contains(reason)) return;
            if (reasons.size() == limits.reasons()) throw limit("reasons");
            charge(256L + 2L * (reason.node().length() + reason.option().length() + reason.relatedNode().orElse("").length()));
            reasons.add(reason);
        }
        private void charge(long amount) { if (amount > limits.bytes() - bytes) throw limit("bytes"); bytes += amount; }
    }
    private static void name(String name) {
        if (name == null || name.isBlank()) throw invalid("structuralRestrictions.location");
        ValidationError.fieldName(name, "structuralRestrictions.location");
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String field) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "structuralRestrictions." + field), null);
    }
}
