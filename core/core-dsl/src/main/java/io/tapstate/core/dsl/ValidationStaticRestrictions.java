package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Certain restrictions visible directly in selected, normalized declarations. All validation methods
 * share these exclusions. This component neither parses source text to guess a transform kind nor
 * executes expressions. An empty report is not admission: finite predicates, other operators and
 * combinations, fixed versions, models, keys, native reads and run conditions still require checks.
 */
public final class ValidationStaticRestrictions {
    private ValidationStaticRestrictions() {}
    public enum Kind { UNSUPPORTED, RESOLUTION_REQUIRED }
    public enum Rule { JAVASCRIPT, JOIN, COMPUTED_MAP, UNRESOLVED_REFERENCE }

    /** Field path segments retain literal field names, including dots, without ambiguity. */
    public record Reason(Kind kind, String node, List<String> fieldPath, Rule rule, Optional<FromRef> reference) {
        public Reason {
            Objects.requireNonNull(kind, "kind"); Objects.requireNonNull(rule, "rule");
            Objects.requireNonNull(reference, "reference");
            if (node == null || node.isBlank() || fieldPath == null || fieldPath.isEmpty() || fieldPath.size() > 2)
                throw invalid("staticRestrictions.location");
            ValidationError.fieldName(node, "staticRestrictions.node");
            for (String segment : fieldPath) ValidationError.fieldName(segment, "staticRestrictions.field");
            fieldPath = List.copyOf(fieldPath);
        }
    }
    public record Limits(int nodes, int fields, int reasons, long bytes) {
        public Limits {
            if (nodes < 1 || nodes > 4096 || fields < 1 || fields > 65536 || reasons < 1 || reasons > 4096
                    || bytes < 1 || bytes > 16L * 1024 * 1024) throw invalid("staticRestrictions.limits");
        }
        public static Limits defaults() { return new Limits(1024, 16384, 256, 1024 * 1024); }
    }
    public record Report(List<Reason> reasons, int examinedNodes, int examinedFields, long accountedBytes) {
        public Report { reasons = List.copyOf(reasons); }
    }

    /** The caller already admitted the stable declaration models; limits bound this stage's work and metadata. */
    public static Report inspect(ValidationDeclarations.Inspection declarations, Limits limits) {
        Objects.requireNonNull(declarations, "declarations"); Objects.requireNonNull(limits, "limits");
        var budget = new Budget(limits);
        int nodes = declarations.steps().size();
        if (nodes > limits.nodes()) throw limit("nodes");
        budget.charge(256L + 32L * nodes);
        var steps = new ArrayList<>(declarations.steps().values());
        steps.sort(Comparator.comparing(Step.Inline::id));
        for (Step.Inline step : steps) {
            switch (step.body()) {
                case TransformBody.Js ignored -> budget.add(new Reason(Kind.UNSUPPORTED, step.id(), List.of("type"), Rule.JAVASCRIPT, Optional.empty()));
                case TransformBody.Join ignored -> budget.add(new Reason(Kind.UNSUPPORTED, step.id(), List.of("type"), Rule.JOIN, Optional.empty()));
                case TransformBody.MapProjection map -> {
                    if (map.fields().size() > limits.fields() - budget.fields) throw limit("fields");
                    budget.fields += map.fields().size();
                    for (var field : map.fields().entrySet()) {
                        if (field.getValue() instanceof FieldRule.Computed)
                            budget.add(new Reason(Kind.UNSUPPORTED, step.id(), List.of("fields", field.getKey()), Rule.COMPUTED_MAP, Optional.empty()));
                    }
                }
                default -> { /* Remaining operator admission is deliberately separate. */ }
            }
        }
        for (ValidationDeclarations.Pending pending : declarations.pending()) {
            String node = pending.downstreamSteps().isEmpty() ? declarations.output().id() : pending.downstreamSteps().getFirst().id();
            budget.add(new Reason(Kind.RESOLUTION_REQUIRED, node, List.of("from"), Rule.UNRESOLVED_REFERENCE, Optional.of(pending.reference())));
        }
        return new Report(budget.reasons, nodes, budget.fields, budget.bytes);
    }

    private static final class Budget {
        private final Limits limits;
        private final List<Reason> reasons = new ArrayList<>();
        private int fields;
        private long bytes;
        private Budget(Limits limits) { this.limits = limits; }
        private void add(Reason reason) {
            if (reasons.size() == limits.reasons()) throw limit("reasons");
            long amount = 256L + 2L * reason.node().length();
            for (String segment : reason.fieldPath()) amount += 48L + 2L * segment.length();
            if (reason.reference().isPresent()) {
                String selector = reason.reference().orElseThrow() instanceof FromRef.Literal literal ? literal.ref()
                        : ((FromRef.Regex)reason.reference().orElseThrow()).pattern();
                amount += 64L + 2L * selector.length();
            }
            charge(amount);
            reasons.add(reason);
        }
        private void charge(long amount) {
            if (amount > limits.bytes() - bytes) throw limit("bytes");
            bytes += amount;
        }
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String field) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "staticRestrictions." + field), null);
    }
}
