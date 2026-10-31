package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ValidationDeclarations;
import io.tapstate.core.dsl.ValidationPredicateCompiler;
import io.tapstate.core.dsl.ValidationStaticRestrictions;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationPredicate;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationRootCheck;
import io.tapstate.core.validation.ValidationRelationRows;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.runtime.engine.validation.ValidationStaticMap;
import io.tapstate.runtime.engine.validation.ValidationRelationReader;
import io.tapstate.runtime.engine.validation.ValidationRootReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Assembles an independently evaluated Map/Filter chain from one resolved declaration route.
 * This is the linear row-rule component, not admission for count, physical key mapping, repair,
 * native reads or a complete validation run. Other graph shapes use their own rule components.
 * No production transform, expression runtime, target row or stored operator state is consulted.
 *
 * <p>The caller reserves compiler scratch space and execution working copies before use, in addition
 * to this component's bounded retained program. Evaluation accepts complete owned source rows only;
 * partial Nest projections must retain their knowledge and source evidence in the Nest component.
 */
public final class ValidationLinearRules {
    public record Limits(ValidationPredicate.Limits predicate, int maxSteps, long programBytes) {
        public Limits {
            Objects.requireNonNull(predicate, "predicate");
            if (maxSteps < 1 || maxSteps > 128 || programBytes < 1 || programBytes > 16L * 1024 * 1024)
                throw invalid("linear.limits");
        }
        public ValidationValues.Limits row() { return predicate.row(); }
    }

    public record SourceProblem(String node, ValidationPredicate.Problem predicate) {
        public SourceProblem { Objects.requireNonNull(node, "node"); Objects.requireNonNull(predicate, "predicate"); }
    }

    public record Evaluation(ValidationRelationRows.Disposition disposition, ValidationValues.Row working,
                             SourceProblem problem) {
        public Evaluation {
            Objects.requireNonNull(disposition, "disposition"); Objects.requireNonNull(working, "working");
            if ((disposition == ValidationRelationRows.Disposition.INDETERMINATE) != (problem != null))
                throw invalid("linear.evaluation");
        }
    }

    /** Explicit endpoint bindings evaluated on source facts; no intermediate field correspondence is inferred. */
    public record Identity(ValidationRunIdentity run, String mappingId, String outputId,
                           ValidationKeyBinding source, ValidationKeyBinding projected) {
        public Identity {
            Objects.requireNonNull(run, "run"); Objects.requireNonNull(source, "source"); Objects.requireNonNull(projected, "projected");
            if (mappingId == null || mappingId.isBlank() || outputId == null || outputId.isBlank()) throw invalid("linear.identityScope");
            ValidationError.fieldName(mappingId, "linear.identityScope"); ValidationError.fieldName(outputId, "linear.identityScope");
            if (source.components().size() != projected.components().size()) throw invalid("linear.identityArity");
            for (int i = 0; i < source.components().size(); i++) {
                if (source.components().get(i).domain() != projected.components().get(i).domain()) throw invalid("linear.identityDomains");
            }
        }
    }

    private sealed interface Operation {
        record Projection(ValidationStaticMap map) implements Operation {}
        record Predicate(String node, ValidationPredicate predicate) implements Operation {}
    }
    private final List<Operation> operations;
    private final Limits limits;
    private final long retainedBytes;
    private final String output;

    private ValidationLinearRules(List<Operation> operations, Limits limits, long retainedBytes, String output) {
        this.operations = List.copyOf(operations); this.limits = limits; this.retainedBytes = retainedBytes; this.output = output;
    }

    /** Every selected step is compiled before any row can be evaluated, including unreachable data branches. */
    public static ValidationLinearRules compile(ValidationDeclarations.Inspection declarations, Limits limits) {
        Objects.requireNonNull(declarations, "declarations"); Objects.requireNonNull(limits, "limits");
        if (!declarations.pending().isEmpty() || declarations.routes().size() != 1)
            throw unsupported(declarations.output().id(), "from", "linear.single-resolved-route-required");
        List<Step.Inline> steps = declarations.routes().getFirst().steps();
        if (steps.size() > limits.maxSteps()) throw limit("steps");
        var restrictions = ValidationStaticRestrictions.inspect(declarations, ValidationStaticRestrictions.Limits.defaults());
        for (var reason : restrictions.reasons()) {
            if (reason.kind() == ValidationStaticRestrictions.Kind.UNSUPPORTED) {
                String rule = switch (reason.rule()) {
                    case JAVASCRIPT -> "js";
                    case JOIN -> "join";
                    case COMPUTED_MAP -> "map.computed";
                    case UNRESOLVED_REFERENCE -> throw new IllegalStateException("an unresolved reference is not an operator restriction");
                };
                throw unsupported(reason.node(), reason.fieldPath().getLast(), rule);
            }
        }
        long bytes = 256L + 2L * declarations.output().id().length();
        if (bytes > limits.programBytes()) throw limit("programBytes");
        var operations = new ArrayList<Operation>();
        for (Step.Inline step : steps) {
            long operationBytes;
            Operation operation;
            switch (step.body()) {
                case TransformBody.MapProjection projection -> {
                    ValidationStaticMap map = ValidationStaticMap.compile(step.id(), projection, limits.row());
                    operation = new Operation.Projection(map);
                    operationBytes = 64L + map.retainedBytes();
                }
                case TransformBody.Filter filter -> {
                    ValidationPredicate predicate = ValidationPredicateCompiler.compile(step.id(), filter.expr(), limits.predicate());
                    operation = new Operation.Predicate(step.id(), predicate);
                    operationBytes = 64L + 2L * step.id().length() + predicate.retainedBytes();
                }
                default -> throw unsupported(step.id(), "type", "linear." + step.body().type());
            }
            if (operationBytes > limits.programBytes() - bytes) throw limit("programBytes");
            bytes += operationBytes;
            operations.add(operation);
        }
        return new ValidationLinearRules(operations, limits, bytes, declarations.output().id());
    }

    public long retainedBytes() { return retainedBytes; }

    /** Includes the original source row, the retained program, and successive bounded projection copies. */
    public long executionReservationBytes(ValidationValues.Row source) {
        checkInput(source);
        return retainedBytes + source.accountedBytes() + 4L * limits.row().bytes();
    }

    /** Adds both logical-key constructions and bounded intermediate key/diagnostic metadata. Bindings are separately admitted configuration. */
    public long executionReservationBytes(ValidationValues.Row source, Identity identity) {
        Objects.requireNonNull(identity, "identity");
        return executionReservationBytes(source) + 4L * ValidationKey.MAX_KEY_BYTES + 1024L * ValidationKey.MAX_COMPONENTS;
    }

    /** An indeterminate source predicate remains a source problem; it is never treated as an excluded row. */
    public Evaluation evaluate(ValidationValues.Row source, Runnable requireAuthorized) {
        Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        requireAuthorized.run();
        checkInput(source);
        ValidationValues.Row working = source;
        for (Operation operation : operations) {
            requireAuthorized.run();
            switch (operation) {
                case Operation.Projection projection -> working = projection.map().apply(working);
                case Operation.Predicate predicate -> {
                    var result = predicate.predicate().evaluate(working);
                    if (result.problem() != null) {
                        requireAuthorized.run();
                        return new Evaluation(ValidationRelationRows.Disposition.INDETERMINATE, working,
                                new SourceProblem(predicate.node(), result.problem()));
                    }
                    if (!result.matches()) {
                        requireAuthorized.run();
                        return new Evaluation(ValidationRelationRows.Disposition.EXCLUDED, working, null);
                    }
                }
            }
        }
        requireAuthorized.run();
        return new Evaluation(ValidationRelationRows.Disposition.INCLUDED, working, null);
    }

    /** Adapts a complete source observation without deriving or changing its separately admitted physical key. */
    public ValidationRootReader.Source classify(ValidationValues.Row source, ValidationPhysicalIdentity physical,
            ValidationObservation observation, Runnable requireAuthorized) {
        Objects.requireNonNull(physical, "physical"); Objects.requireNonNull(observation, "observation");
        return sourceOf(evaluate(source, requireAuthorized), physical, observation);
    }

    /**
     * Checks actual endpoint key facts before a participating source can be paired with the target.
     * Losing or changing the configured root identity cannot safely advance the ordered comparison;
     * it is a coded execution failure, not a fabricated target missing/extra or a repairable finding.
     * This does not replace configuration-time identity checks or prove business equivalence of a user mapping.
     */
    public ValidationRootReader.Source classify(ValidationValues.Row source, ValidationPhysicalIdentity physical,
            ValidationObservation observation, Identity identity, Runnable requireAuthorized) {
        Objects.requireNonNull(source, "source"); Objects.requireNonNull(physical, "physical");
        Objects.requireNonNull(observation, "observation"); Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        if (!identity.run().equals(observation.run()) || !identity.mappingId().equals(observation.root().mappingId())
                || !identity.outputId().equals(observation.root().outputId()) || !output.equals(identity.outputId())
                || !observation.root().ancestors().isEmpty()) throw invalid("linear.identityRunScope");
        requireAuthorized.run();
        checkInput(source);
        ValidationKey original = bound(identity.source(), source, "identity.source-key-unavailable");
        if (!original.equals(observation.root().rootKey())) throw invalid("linear.sourceKeyScope");
        requireAuthorized.run();
        Evaluation result = evaluate(source, requireAuthorized);
        if (result.disposition() == ValidationRelationRows.Disposition.INCLUDED) {
            requireAuthorized.run();
            ValidationKey projected = bound(identity.projected(), result.working(), "identity.projected-key-unavailable");
            for (int i = 0; i < original.parts().size(); i++) {
                if (!original.parts().get(i).equals(projected.parts().get(i)))
                    throw identityFailure(identity.projected().components().get(i).path(), "identity.projected-key-changed");
            }
        }
        requireAuthorized.run();
        return sourceOf(result, physical, observation);
    }

    private ValidationKey bound(ValidationKeyBinding binding, ValidationValues.Row values, String rule) {
        var result = binding.read(values);
        if (result instanceof ValidationKeyBinding.Bound bound) return bound.key();
        throw identityFailure(((ValidationKeyBinding.Invalid)result).problems().getFirst().path(), rule);
    }
    private TapstateException identityFailure(List<String> path, String rule) {
        return new TapstateException(ValidationError.RULE_UNSUPPORTED, Map.of("node", output, "field", path, "rule", rule), null);
    }
    private static ValidationRootReader.Source sourceOf(Evaluation result, ValidationPhysicalIdentity physical, ValidationObservation observation) {
        var row = new ValidationRelationReader.Evaluation(result.disposition(), result.working(), Optional.of(physical));
        if (result.problem() == null) return new ValidationRootReader.Source(row, List.of());
        var problem = result.problem().predicate();
        var kind = problem.kind() == ValidationPredicate.ProblemKind.MISSING_FIELD
                ? ValidationRootCheck.Kind.SOURCE_FIELD_MISSING : ValidationRootCheck.Kind.SOURCE_FIELD_INVALID;
        var finding = new ValidationRootCheck.Finding(kind, new ValidationRootCheck.Location(observation.root(), problem.path()));
        return new ValidationRootReader.Source(row, List.of(finding));
    }

    private void checkInput(ValidationValues.Row input) {
        Objects.requireNonNull(input, "source");
        if (input.accountedBytes() > limits.row().bytes() || input.nodes() > limits.row().nodes()
                || input.depth() > limits.row().depth()) throw limit("input");
    }
    private static TapstateException unsupported(String node, String field, String rule) {
        return new TapstateException(ValidationError.RULE_UNSUPPORTED,
                Map.of("node", node, "field", field, "rule", rule), null);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "validation.linear." + dimension), null);
    }
}
