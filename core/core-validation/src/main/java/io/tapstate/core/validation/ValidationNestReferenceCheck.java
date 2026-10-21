package io.tapstate.core.validation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Classifies an independently read parent's reference and checks the no-reference output shape.
 * Complete non-NULL references are returned for a separately admitted lookup; no query is issued here.
 * Base projection/path conflicts, model admission and parent uniqueness are caller obligations.
 */
public final class ValidationNestReferenceCheck {
    public enum Shape { OBJECT, ARRAY }
    private ValidationNestReferenceCheck() {}

    public static ValidationReferenceFields.Result check(ValidationObservation parent, List<String> outputPath,
            Shape shape, ValidationReferenceFields fields, ValidationCheckEvent.Side source,
            Optional<ValidationCheckEvent.Side> target, Builder root) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(parent, "parent");
            Objects.requireNonNull(shape, "shape");
            Objects.requireNonNull(fields, "fields");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
            if (outputPath == null || outputPath.isEmpty() || outputPath.size() > 32) {
                throw ValidationError.invalid("referenceCheck.outputPath");
            }
            for (String segment : outputPath) ValidationError.identifier(segment, "referenceCheck.outputPath");
            outputPath = List.copyOf(outputPath);
            requireScope(parent, root.observation());
            requireUniqueParent(source);
            root.read(Side.SOURCE, source.boundary());
            target.ifPresent(side -> {
                requireUniqueParent(side);
                root.read(Side.TARGET, side.boundary());
            });
            var reference = fields.read(source.row());
            Location output = new Location(parent.root(), List.copyOf(outputPath));
            SampledSide sourceSample = sample(source);
            if (reference instanceof ValidationReferenceFields.Missing missing) {
                for (String field : missing.fields()) {
                    Finding cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(parent.root(), List.of(field)));
                    root.finding(cause, Optional.of(sourceSample), Optional.empty()).notCompared(Check.STRUCTURE, output, cause);
                    if (root.method() == ValidationMethod.FULL_COLUMN) root.notCompared(Check.VALUES, output, cause);
                }
                return reference;
            }
            if (reference instanceof ValidationReferenceFields.Complete) return reference;
            if (root.participation() != ValidationCheckEvent.Participation.SHOULD_EXIST
                    || root.targetPresence() != TargetPresence.PRESENT || target.isEmpty()) {
                throw ValidationError.invalid("referenceCheck.targetParent");
            }
            Slot actual = slot(target.orElseThrow().row(), outputPath);
            boolean matches = shape == Shape.OBJECT ? !actual.present() : actual.present() && emptyArray(actual.value());
            if (!matches) {
                Finding cause = new Finding(Kind.TARGET_STRUCTURE, output);
                root.finding(cause, Optional.of(sourceSample), Optional.of(sample(target.orElseThrow())))
                        .notApplicable(Check.STRUCTURE, output, cause);
                if (root.method() == ValidationMethod.FULL_COLUMN) root.notApplicable(Check.VALUES, output, cause);
            }
            return reference;
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
        }
    }

    private static void requireScope(ValidationObservation actual, ValidationObservation expected) {
        if (!expected.run().equals(actual.run()) || expected.revision() != actual.revision()
                || !expected.root().mappingId().equals(actual.root().mappingId())
                || !expected.root().outputId().equals(actual.root().outputId())
                || !expected.root().rootKey().equals(actual.root().rootKey())) {
            throw ValidationError.invalid("referenceCheck.observation");
        }
    }
    private static void requireUniqueParent(ValidationCheckEvent.Side side) {
        if (side.groupStatus() != ValidationCheckEvent.GroupStatus.COMPLETE || side.observedOccurrences() != 1) {
            throw ValidationError.invalid("referenceCheck.parent");
        }
    }
    private static SampledSide sample(ValidationCheckEvent.Side side) {
        return new SampledSide(side.identity(), side.row(), side.boundary().readId());
    }
    private static Slot slot(ValidationValues.Row row, List<String> path) {
        Object current = row.fields();
        for (String segment : path) {
            while (current instanceof ValidationValues.Typed typed) current = typed.value();
            // An omitted object slot contributes no prefix; base fields are checked independently.
            if (!(current instanceof java.util.Map<?, ?> fields) || !fields.containsKey(segment)) return new Slot(false, null);
            current = fields.get(segment);
        }
        return new Slot(true, current);
    }
    private static boolean emptyArray(Object value) {
        while (value instanceof ValidationValues.Typed typed) {
            if (!typed.logicalType().equals("array") && !typed.logicalType().equals("json")) return false;
            value = typed.value();
        }
        return value instanceof List<?> list && list.isEmpty();
    }
    private record Slot(boolean present, Object value) {}
}
