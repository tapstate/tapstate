package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationValues;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Independent single-level ordinal expansion over an owned working row. Only one output is leased
 * at a time; later filters keep its original ordinal. The caller separately retains original read
 * facts and physical query identity. The emitted key is a comparison identity, never a source key.
 *
 * <p>The caller admits unique/non-null parent identity, compatible native ordering, complete facts,
 * type/model rules and the entire selected path before opening this component. It must reserve the
 * working set before materialization. This component does not admit multiple unwinds, a preceding
 * Nest, repair, or the whole pipeline merely because this single declaration is supported.
 */
public final class ValidationOrdinalExpansion {
    public sealed interface Preparation permits Ready, Conflict {}
    public record Ready(Cursor cursor) implements Preparation { public Ready { Objects.requireNonNull(cursor); } }
    /** A parent-scope input fact, not EOF or a complete final-root observation. */
    public record Conflict(String node, String field, ValidationKey parentKey) implements Preparation {
        public Conflict { node = name(node, "unwind.node"); field = name(field, "unwind.index"); Objects.requireNonNull(parentKey); }
        public ValidationError code() { return ValidationError.SOURCE_FIELD_CONFLICT; }
        public Map<String, Object> args() { return Map.of("node", node, "field", field); }
    }

    private final String node;
    private final String path;
    private final String ordinalField;
    private final ValidationValues.Limits limits;
    private final int maxElements;
    private final long retainedBytes;

    private ValidationOrdinalExpansion(String node, String path, String ordinalField, ValidationValues.Limits limits, int maxElements) {
        this.node = node; this.path = path; this.ordinalField = ordinalField; this.limits = limits; this.maxElements = maxElements;
        retainedBytes = 256L + 2L * (node.length() + path.length() + ordinalField.length());
        if (retainedBytes > limits.bytes()) throw limit("unwind.rules.bytes");
    }

    public static ValidationOrdinalExpansion compile(String node, TransformBody.Unwind declaration,
            ValidationValues.Limits limits, int maxElements) {
        node = name(node, "unwind.node"); Objects.requireNonNull(declaration); Objects.requireNonNull(limits);
        String path = name(declaration.path(), "unwind.path");
        if (maxElements < 1 || maxElements > 65536) throw invalid("unwind.maxElements");
        if (declaration.includeArrayIndex() == null || declaration.includeArrayIndex().isBlank()) throw unsupported(node, path, "unwind.ordinalRequired");
        String ordinal = name(declaration.includeArrayIndex(), "unwind.index");
        if (declaration.elementKey() != null) throw unsupported(node, path, "unwind.elementKey");
        if (Boolean.TRUE.equals(declaration.preserveNullAndEmptyArrays())) throw unsupported(node, path, "unwind.preserveEmpty");
        if (path.equals(ordinal)) throw unsupported(node, ordinal, "unwind.columnConflict");
        return new ValidationOrdinalExpansion(node, path, ordinal, limits, maxElements);
    }

    /** Includes retained input/program/key facts and bounded output construction; not measured heap. */
    public long workingSetBytes(ValidationValues.Row input, ValidationKey parentKey) {
        checkInput(input, parentKey);
        return retainedBytes + input.accountedBytes() + 3L * limits.bytes() + 2L * parentKey.retainedBytes() + 512;
    }

    public Preparation prepare(ValidationValues.Row input, ValidationKey parentKey) {
        checkInput(input, parentKey);
        // A collision is explicit even for an otherwise empty expansion; it must not look like EOF.
        if (input.fields().containsKey(ordinalField)) return new Conflict(node, ordinalField, parentKey);
        Object original = input.fields().get(path);
        Object shape = original;
        boolean arrayCarrier = false;
        boolean compatibleListCarrier = true;
        while (shape instanceof ValidationValues.Typed typed) {
            arrayCarrier |= typed.logicalType().equals("array");
            compatibleListCarrier &= typed.logicalType().equals("array") || typed.logicalType().equals("json");
            shape = typed.value();
        }
        List<?> elements;
        if (shape == null) elements = List.of();
        else if (shape instanceof List<?> list) {
            if (!compatibleListCarrier) throw unsupported(node, path, "unwind.containerType");
            elements = list;
        } else {
            if (arrayCarrier) throw unsupported(node, path, "unwind.containerType");
            // Preserve the original scalar/object carrier, including an empty string or object.
            elements = List.of(original);
        }
        if (elements.size() > maxElements) throw limit("unwind.elements");
        if (!elements.isEmpty()) {
            if (input.fields().size() == 4096) throw limit("unwind.outputFields");
            key(parentKey, elements.size() - 1L); // Refuse key-size overflow before any output is emitted.
        }
        return new Ready(new Cursor(input, parentKey, elements));
    }

    private void checkInput(ValidationValues.Row input, ValidationKey parentKey) {
        Objects.requireNonNull(input); Objects.requireNonNull(parentKey);
        if (input.fields().size() > 4096 || input.accountedBytes() > limits.bytes() || input.nodes() > limits.nodes() || input.depth() > limits.depth())
            throw limit("unwind.input");
        if (parentKey.parts().size() >= ValidationKey.MAX_COMPONENTS)
            throw new TapstateException(ValidationError.KEY_LIMIT_EXCEEDED, Map.of("limit", "components"), null);
    }

    /** Drive on admitted I/O/non-cooperative resources; cancellation cleanup must not block a lease callback. */
    public final class Cursor implements AutoCloseable {
        private ValidationValues.Row input;
        private ValidationKey parentKey;
        private List<?> elements;
        private int next;
        private Output outstanding;
        private boolean closed;
        private boolean complete;
        private Throwable failure;

        private Cursor(ValidationValues.Row input, ValidationKey parentKey, List<?> elements) {
            this.input = input; this.parentKey = parentKey; this.elements = elements;
        }

        public synchronized Optional<Output> next() {
            if (complete) return Optional.empty();
            checkActive();
            if (outstanding != null && !outstanding.closed) throw new IllegalStateException("close the ordinal output before advancing");
            outstanding = null;
            if (next == elements.size()) {
                complete = true; close(); return Optional.empty();
            }
            try {
                var fields = new LinkedHashMap<String, Object>(input.fields());
                fields.put(path, elements.get(next));
                fields.put(ordinalField, (long)next);
                var values = ValidationValues.ownRow(fields, limits);
                var output = new Output(next, key(parentKey, next), values);
                next++; outstanding = output;
                return Optional.of(output);
            } catch (RuntimeException | Error reported) { failure = reported; close(); throw reported; }
        }

        private void checkActive() {
            if (failure instanceof RuntimeException runtime) throw runtime;
            if (failure instanceof Error fatal) throw fatal;
            if (closed && !complete) throw new TapstateException(ValidationError.EXECUTION_CANCELLED, Map.of("node", node), null);
        }

        @Override public synchronized void close() {
            if (closed) return;
            closed = true;
            if (outstanding != null) { outstanding.close(); outstanding = null; }
            input = null; parentKey = null; elements = null;
        }

        public final class Output implements AutoCloseable {
            private final long ordinal;
            private ValidationKey comparisonKey;
            private ValidationValues.Row values;
            private boolean closed;
            private Output(long ordinal, ValidationKey comparisonKey, ValidationValues.Row values) {
                this.ordinal = ordinal; this.comparisonKey = comparisonKey; this.values = values;
            }
            public long ordinal() { synchronized (Cursor.this) { active(); return ordinal; } }
            public ValidationKey comparisonKey() { synchronized (Cursor.this) { active(); return comparisonKey; } }
            public ValidationValues.Row values() { synchronized (Cursor.this) { active(); return values; } }
            private void active() {
                Cursor.this.checkActive();
                if (closed) throw new IllegalStateException("ordinal output is closed");
            }
            @Override public void close() { synchronized (Cursor.this) { closed = true; comparisonKey = null; values = null; } }
        }
    }

    private static ValidationKey key(ValidationKey parent, long ordinal) {
        var parts = new ArrayList<>(parent.parts()); parts.add(ValidationKey.Part.number(ordinal)); return new ValidationKey(parts);
    }
    private static String name(String name, String field) {
        if (name == null || name.isBlank() || name.length() > 512) throw invalid(field);
        for (int i = 0; i < name.length(); i++) {
            char next = name.charAt(i);
            if (Character.isHighSurrogate(next)) {
                if (++i == name.length() || !Character.isLowSurrogate(name.charAt(i))) throw invalid(field);
            } else if (Character.isLowSurrogate(next)) throw invalid(field);
        }
        return name;
    }
    private static TapstateException unsupported(String node, String field, String rule) {
        return new TapstateException(ValidationError.RULE_UNSUPPORTED, Map.of("node", node, "field", field, "rule", rule), null);
    }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit(String limit) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", limit), null); }
}
