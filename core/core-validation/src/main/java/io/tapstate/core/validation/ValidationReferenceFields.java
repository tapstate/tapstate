package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Presence of explicitly bound, literal reference columns in an already owned source row.
 * This does not encode query keys or establish a model's type, query or relationship contract.
 * The caller must admit those contracts independently, including when the reference is absent.
 */
public final class ValidationReferenceFields {
    public sealed interface Result permits Complete, NoReference, Missing {}

    /** Ordered, non-NULL portable components; their type provenance remains intact for key binding. */
    public static final class Complete implements Result {
        private final List<Object> components;
        private Complete(List<Object> components) { this.components = List.copyOf(components); }
        public List<Object> components() { return components; }
    }
    public enum NoReference implements Result { INSTANCE }
    public record Missing(List<String> fields) implements Result {
        public Missing {
            if (fields == null || fields.isEmpty() || fields.size() > ValidationKey.MAX_COMPONENTS) {
                throw ValidationError.invalid("reference.missing");
            }
            for (String field : fields) ValidationError.identifier(field, "reference.missing");
            fields = List.copyOf(fields);
        }
    }

    private final List<String> fields;
    private final ValidationValues.Limits rowLimits;

    public ValidationReferenceFields(List<String> fields, ValidationValues.Limits rowLimits) {
        if (fields == null || fields.isEmpty() || fields.size() > ValidationKey.MAX_COMPONENTS) {
            throw ValidationError.invalid("reference.fields");
        }
        var seen = new HashSet<String>();
        for (String field : fields) {
            ValidationError.identifier(field, "reference.fields");
            if (!seen.add(field)) throw ValidationError.invalid("reference.duplicateField");
        }
        this.fields = List.copyOf(fields);
        this.rowLimits = Objects.requireNonNull(rowLimits, "rowLimits");
    }

    public List<String> fields() { return fields; }

    /** Missing any component takes precedence over NULL, regardless of declaration order. */
    public Result read(ValidationValues.Row row) {
        Objects.requireNonNull(row, "row");
        if (row.accountedBytes() > rowLimits.bytes() || row.nodes() > rowLimits.nodes() || row.depth() > rowLimits.depth()) {
            throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "reference.row"), null);
        }
        var missing = new ArrayList<String>();
        for (String field : fields) {
            if (!row.fields().containsKey(field)) missing.add(field);
        }
        if (!missing.isEmpty()) return new Missing(missing);
        var components = new ArrayList<Object>(fields.size());
        for (String field : fields) {
            Object value = row.fields().get(field);
            if (isNull(value)) return NoReference.INSTANCE;
            components.add(value);
        }
        return new Complete(components);
    }

    private static boolean isNull(Object value) {
        // Owned rows have a bounded depth; retain an explicit guard at this boundary as well.
        int wrappers = 0;
        while (value instanceof ValidationValues.Typed typed) {
            if (++wrappers > 64) throw ValidationError.invalid("reference.typedDepth");
            value = typed.value();
        }
        return value == null;
    }
}
