package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Explicit, ordered physical fields mapped to already admitted logical key domains.
 * No inference, parsing, timezone conversion, production transformation or query-key substitution.
 * Model provenance, cross-side compatibility and native collation are separate admission obligations.
 */
public final class ValidationKeyBinding {
    public enum NumericPolicy { EXACT_ONLY, EXACT_BINARY_FLOAT }
    public enum ProblemKind { MISSING_FIELD, NULL_COMPONENT, INVALID_PARENT, TYPE_MISMATCH, NON_FINITE, INVALID_TEXT }

    /** One path segment denotes a literal column name, including a name containing a dot. */
    public record Component(List<String> path, ValidationKey.Kind domain, Set<String> logicalTypes,
                            NumericPolicy numericPolicy) {
        public Component {
            if (path == null || path.isEmpty() || path.size() > 32) throw ValidationError.invalid("keyBinding.path");
            for (String field : path) ValidationError.identifier(field, "keyBinding.path");
            path = List.copyOf(path);
            Objects.requireNonNull(domain, "domain");
            Objects.requireNonNull(numericPolicy, "numericPolicy");
            if (domain != ValidationKey.Kind.EXACT_NUMBER && numericPolicy != NumericPolicy.EXACT_ONLY) {
                throw ValidationError.invalid("keyBinding.numericPolicy");
            }
            if (logicalTypes == null || logicalTypes.size() > 32) throw ValidationError.invalid("keyBinding.logicalTypes");
            for (String type : logicalTypes) ValidationError.identifier(type, "keyBinding.logicalTypes");
            logicalTypes = Set.copyOf(logicalTypes);
        }
        public Component(List<String> path, ValidationKey.Kind domain, Set<String> logicalTypes) {
            this(path, domain, logicalTypes, NumericPolicy.EXACT_ONLY);
        }
    }

    public sealed interface Result permits Bound, Invalid {}
    public record Bound(ValidationKey key) implements Result {
        public Bound { Objects.requireNonNull(key, "key"); }
    }
    public record Problem(int component, List<String> path, ProblemKind kind) {
        public Problem {
            if (component < 0 || component >= ValidationKey.MAX_COMPONENTS) throw ValidationError.invalid("keyBinding.component");
            if (path == null || path.isEmpty() || path.size() > 32) throw ValidationError.invalid("keyBinding.problemPath");
            for (String field : path) ValidationError.identifier(field, "keyBinding.problemPath");
            path = List.copyOf(path);
            Objects.requireNonNull(kind, "kind");
        }
    }
    public record Invalid(List<Problem> problems) implements Result {
        public Invalid {
            if (problems == null || problems.isEmpty() || problems.size() > ValidationKey.MAX_COMPONENTS) {
                throw ValidationError.invalid("keyBinding.problems");
            }
            problems = List.copyOf(problems);
        }
    }

    private final String id;
    private final List<Component> components;
    private final Set<String> containerTypes;
    private final ValidationValues.Limits rowLimits;

    /** Accepted wrapper types come from the fixed model/rule binding, never from the current sample. */
    public ValidationKeyBinding(String id, List<Component> components, Set<String> containerTypes,
                                ValidationValues.Limits rowLimits) {
        this.id = ValidationError.identifier(id, "keyBinding.id");
        if (components == null || components.isEmpty() || components.size() > ValidationKey.MAX_COMPONENTS) {
            throw ValidationError.invalid("keyBinding.components");
        }
        var paths = new HashSet<List<String>>();
        for (Component component : components) {
            Objects.requireNonNull(component, "component");
            if (!paths.add(component.path())) throw ValidationError.invalid("keyBinding.duplicatePath");
        }
        if (containerTypes == null || containerTypes.size() > 32) throw ValidationError.invalid("keyBinding.containerTypes");
        for (String type : containerTypes) ValidationError.identifier(type, "keyBinding.containerTypes");
        this.components = List.copyOf(components);
        this.containerTypes = Set.copyOf(containerTypes);
        this.rowLimits = Objects.requireNonNull(rowLimits, "rowLimits");
    }

    public List<Component> components() { return components; }

    /** Data violations need a separately locatable root; they never authorize synthesizing a key. */
    public Result read(ValidationValues.Row row) {
        Objects.requireNonNull(row, "row");
        if (row.accountedBytes() > rowLimits.bytes() || row.nodes() > rowLimits.nodes() || row.depth() > rowLimits.depth()) {
            throw limit("row");
        }
        var parts = new ArrayList<ValidationKey.Part>(components.size());
        var problems = new ArrayList<Problem>();
        long keyBytes = 32;
        for (int i = 0; i < components.size(); i++) {
            Component component = components.get(i);
            Field field = field(row, component);
            if (field.problem() != null) {
                problems.add(new Problem(i, component.path(), field.problem()));
                continue;
            }
            Object value = unwrap(field.value(), component.logicalTypes(), component.path());
            if (value == null) {
                problems.add(new Problem(i, component.path(), ProblemKind.NULL_COMPONENT));
                continue;
            }
            if (value instanceof Float || value instanceof Double) {
                if (component.domain() == ValidationKey.Kind.EXACT_NUMBER
                        && component.numericPolicy() == NumericPolicy.EXACT_BINARY_FLOAT
                        && !Double.isFinite(((Number) value).doubleValue())) {
                    problems.add(new Problem(i, component.path(), ProblemKind.NON_FINITE));
                    continue;
                }
            }
            if (component.domain() == ValidationKey.Kind.TEXT && value instanceof String text && !validText(text)) {
                problems.add(new Problem(i, component.path(), ProblemKind.INVALID_TEXT));
                continue;
            }
            if (component.domain() == ValidationKey.Kind.BINARY && value instanceof ValidationValues.Binary binary
                    && binary.subtype() < 0) {
                throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                        Map.of("node", id, "field", component.path().toString(), "rule", "key binary subtype unavailable"), null);
            }
            ValidationKey.Part part = part(value, component);
            if (part == null) problems.add(new Problem(i, component.path(), ProblemKind.TYPE_MISMATCH));
            else {
                keyBytes += part.retainedBytes();
                if (keyBytes > ValidationKey.MAX_KEY_BYTES) {
                    throw new TapstateException(ValidationError.KEY_LIMIT_EXCEEDED, Map.of("limit", "keyBytes"), null);
                }
                parts.add(part);
            }
        }
        // Inspect every component, so an earlier data issue cannot hide an unknown wrapper or hard limit.
        return problems.isEmpty() ? new Bound(new ValidationKey(parts)) : new Invalid(problems);
    }

    private Field field(ValidationValues.Row row, Component component) {
        Object current = row.fields();
        for (int i = 0; i < component.path().size(); i++) {
            current = unwrap(current, containerTypes, component.path().subList(0, i));
            if (!(current instanceof Map<?, ?> fields)) return new Field(null, ProblemKind.INVALID_PARENT);
            String segment = component.path().get(i);
            if (!fields.containsKey(segment)) return new Field(null, ProblemKind.MISSING_FIELD);
            current = fields.get(segment);
        }
        return new Field(current, null);
    }

    private Object unwrap(Object value, Set<String> accepted, List<String> path) {
        int depth = 0;
        while (value instanceof ValidationValues.Typed typed) {
            if (++depth > 64) throw limit("typedDepth");
            if (!accepted.contains(typed.logicalType())) {
                throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                        Map.of("node", id, "field", path.toString(), "rule", "key logical type " + typed.logicalType()), null);
            }
            value = typed.value();
        }
        return value;
    }

    private static ValidationKey.Part part(Object value, Component component) {
        return switch (component.domain()) {
            case EXACT_NUMBER -> number(value, component.numericPolicy());
            case TEXT -> value instanceof String text ? ValidationKey.Part.text(text) : null;
            case BOOLEAN -> value instanceof Boolean bool ? ValidationKey.Part.bool(bool) : null;
            case BINARY -> binary(value);
            case DATE -> value instanceof LocalDate date ? ValidationKey.Part.date(date) : null;
            case TIME -> value instanceof LocalTime time ? ValidationKey.Part.time(time) : null;
            case LOCAL_DATE_TIME -> value instanceof LocalDateTime dateTime ? ValidationKey.Part.localDateTime(dateTime) : null;
            case INSTANT -> value instanceof Instant instant ? ValidationKey.Part.instant(instant) : null;
        };
    }

    private static boolean validText(String text) {
        if (48L + 2L * text.length() > ValidationKey.MAX_COMPONENT_BYTES) {
            throw new TapstateException(ValidationError.KEY_LIMIT_EXCEEDED, Map.of("limit", "componentBytes"), null);
        }
        for (int i = 0; i < text.length(); i++) {
            char next = text.charAt(i);
            if (Character.isHighSurrogate(next)) {
                if (++i == text.length() || !Character.isLowSurrogate(text.charAt(i))) return false;
            } else if (Character.isLowSurrogate(next)) return false;
        }
        return true;
    }

    private static ValidationKey.Part number(Object value, NumericPolicy policy) {
        if (value instanceof BigDecimal decimal) return ValidationKey.Part.number(decimal);
        if (value instanceof BigInteger integer) return ValidationKey.Part.number(integer);
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return ValidationKey.Part.number(((Number) value).longValue());
        }
        if (policy == NumericPolicy.EXACT_BINARY_FLOAT && (value instanceof Float || value instanceof Double)) {
            return ValidationKey.Part.number(new BigDecimal(((Number) value).doubleValue()));
        }
        return null;
    }

    private static ValidationKey.Part binary(Object value) {
        if (!(value instanceof ValidationValues.Binary binary)) return null;
        // Check before bytes() allocates another payload copy.
        if (48L + binary.length() > ValidationKey.MAX_COMPONENT_BYTES) throw limit("binaryComponent");
        return ValidationKey.Part.binary(binary.subtype(), binary.bytes());
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "keyBinding." + dimension), null);
    }
    private record Field(Object value, ProblemKind problem) {}
}
