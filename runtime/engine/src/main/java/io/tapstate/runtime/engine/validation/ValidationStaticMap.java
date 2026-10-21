package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Independent static projection over owned validation facts. It interprets parsed field rules, never
 * a production operator, expression program or target row. Every rename in one node reads the same
 * input. Applying another compiled map consumes the preceding working row, without changing the
 * original read facts or the separately fixed physical/comparison identities.
 *
 * <p>The caller admits compilation and execution reservations before invoking these methods. Local
 * limits bound this component's copies, not driver allocation, overall run memory or model validity.
 * Compilation requires four rule-sized reservations for borrowed configuration, literals and rule
 * metadata. The retained charge is conservative accounting, not a measured Java heap size.
 */
public final class ValidationStaticMap {
    private static final int MAX_FIELDS = 4096;
    private sealed interface Assignment {
        record Rename(String output, String source) implements Assignment {}
        record Literal(String output) implements Assignment {}
    }

    private final List<Assignment> assignments;
    private final Set<String> consumed;
    private final ValidationValues.Row literals;
    private final ValidationValues.Limits limits;
    private final long retainedBytes;

    private ValidationStaticMap(List<Assignment> assignments, Set<String> consumed,
            ValidationValues.Row literals, ValidationValues.Limits limits, long retainedBytes) {
        this.assignments = List.copyOf(assignments);
        this.consumed = Set.copyOf(consumed);
        this.literals = literals;
        this.limits = limits;
        this.retainedBytes = retainedBytes;
    }

    /** Compiles only static rules; even a constant computed expression is refused before row reads. */
    public static ValidationStaticMap compile(String node, TransformBody.MapProjection projection,
            ValidationValues.Limits limits) {
        name(node, "map.node");
        Objects.requireNonNull(projection, "projection");
        Objects.requireNonNull(limits, "limits");
        if (projection.fields().size() > MAX_FIELDS) throw limit("map.ruleCount");
        var assignments = new ArrayList<Assignment>();
        var consumed = new HashSet<String>();
        var constants = new LinkedHashMap<String, Object>();
        long metadata = 256L + 2L * node.length();
        for (var entry : projection.fields().entrySet()) {
            String output = name(entry.getKey(), "map.output");
            FieldRule rule = Objects.requireNonNull(entry.getValue(), "field rule");
            String source = rule instanceof FieldRule.Rename rename ? name(rename.sourceField(), "map.source") : null;
            metadata += 96L + 2L * output.length() + (source == null ? 0 : 2L * source.length());
            if (metadata > limits.bytes()) throw limit("map.rules.bytes");
            switch (rule) {
                case FieldRule.Computed ignored -> throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                        Map.of("node", node, "field", output, "rule", "map.computed"), null);
                case FieldRule.Drop ignored -> consumed.add(output);
                case FieldRule.Rename ignored -> {
                    consumed.add(source);
                    assignments.add(new Assignment.Rename(output, source));
                }
                case FieldRule.Literal literal -> {
                    constants.put(output, literal.value());
                    assignments.add(new Assignment.Literal(output));
                }
            }
        }
        // One aggregate bound prevents many individually legal constants from filling the process.
        var literals = ValidationValues.ownRow(constants, limits);
        long retained = metadata + literals.accountedBytes();
        if (retained > limits.bytes()) throw limit("map.rules.bytes");
        return new ValidationStaticMap(assignments, consumed, literals, limits, retained);
    }

    public long retainedBytes() { return retainedBytes; }

    /** Includes the input, compiled constants/metadata and bounded output construction working set. */
    public long executionReservationBytes(ValidationValues.Row input) {
        checkInput(input);
        return retainedBytes + input.accountedBytes() + 3L * limits.bytes();
    }

    /** Returns a new owned working row; an error cannot return a partial projection. */
    public ValidationValues.Row apply(ValidationValues.Row input) {
        checkInput(input);
        Map<String, Object> original = input.fields();
        var output = new LinkedHashMap<String, Object>();
        for (Assignment assignment : assignments) {
            switch (assignment) {
                case Assignment.Rename rename -> {
                    if (original.containsKey(rename.source())) put(output, rename.output(), original.get(rename.source()));
                }
                case Assignment.Literal literal -> put(output, literal.output(), literals.fields().get(literal.output()));
            }
        }
        // A missing rename produced nothing: its output may still pass through from the input.
        for (var field : original.entrySet()) {
            if (!output.containsKey(field.getKey()) && !consumed.contains(field.getKey()))
                put(output, field.getKey(), field.getValue());
        }
        return ValidationValues.ownRow(output, limits);
    }

    /** A partial working view; unresolved source causes and physical identities remain with the caller. */
    public static final class Projection {
        private final ValidationValues.Row row;
        private final List<List<Object>> unavailablePaths;
        private final long accountedBytes;
        private Projection(ValidationValues.Row row, List<List<Object>> paths, long accountedBytes) {
            this.row = row;
            this.unavailablePaths = List.copyOf(paths);
            this.accountedBytes = accountedBytes;
        }
        public ValidationValues.Row row() { return row; }
        public List<List<Object>> unavailablePaths() { return unavailablePaths; }
        public long accountedBytes() { return accountedBytes; }
    }

    /**
     * Carries unresolved paths through these explicit forward rules. A literal replaces uncertainty;
     * a rename reads the original view, including its unavailable descendants, and a drop removes its
     * field contribution. This does not infer physical keys, trace key lineage or resolve source issues.
     * A whole unknown view remains conservatively unknown because its unknown fields cannot be listed.
     * Reserve input and output masks in addition to the ordinary projection's working copies.
     */
    public Projection apply(ValidationValues.Row input, List<List<Object>> unavailablePaths) {
        checkInput(input);
        var inputPaths = new PathSet(input.accountedBytes());
        if (unavailablePaths == null || unavailablePaths.size() > 256) throw invalid("map.unavailablePaths");
        for (var path : unavailablePaths) inputPaths.add(path);
        var projected = apply(input);
        var outputPaths = new PathSet(projected.accountedBytes());
        if (inputPaths.paths.contains(List.of())) {
            outputPaths.add(List.of());
            return new Projection(projected, new ArrayList<>(outputPaths.paths), outputPaths.bytes);
        }
        var byField = new LinkedHashMap<String, List<List<Object>>>();
        for (var path : inputPaths.paths) byField.computeIfAbsent((String)path.getFirst(), ignored -> new ArrayList<>()).add(path);
        var assigned = new HashSet<String>();
        for (Assignment assignment : assignments) {
            switch (assignment) {
                case Assignment.Literal literal -> assigned.add(literal.output());
                case Assignment.Rename rename -> {
                    var paths = byField.getOrDefault(rename.source(), List.of());
                    if (input.fields().containsKey(rename.source()) || !paths.isEmpty()) assigned.add(rename.output());
                    for (var path : paths) {
                        var renamed = new ArrayList<Object>(path); renamed.set(0, rename.output());
                        outputPaths.add(renamed);
                    }
                }
            }
        }
        for (var path : inputPaths.paths) {
            String field = (String)path.getFirst();
            if (!consumed.contains(field) && !assigned.contains(field)) outputPaths.add(path);
        }
        return new Projection(projected, new ArrayList<>(outputPaths.paths), outputPaths.bytes);
    }

    private final class PathSet {
        private final Set<List<Object>> paths = new LinkedHashSet<>();
        private long bytes;
        private PathSet(long rowBytes) { bytes = rowBytes; }
        private void add(List<Object> path) {
            if (path == null || path.size() > 64 || !path.isEmpty() && !(path.getFirst() instanceof String))
                throw invalid("map.unavailablePath");
            long amount = 64L + 8L * path.size();
            for (Object segment : path) {
                if (segment instanceof String field) {
                    ValidationError.fieldName(field, "map.unavailablePath");
                    amount += 48L + 2L * field.length();
                } else if (segment instanceof Integer ordinal && ordinal >= 0) amount += 24L;
                else throw invalid("map.unavailablePath");
            }
            var owned = List.copyOf(path);
            if (paths.contains(owned)) return;
            if (paths.size() == 256) throw limit("map.unavailablePaths");
            if (amount > limits.bytes() - bytes) throw limit("map.unavailableBytes");
            bytes += amount;
            paths.add(owned);
        }
    }

    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }

    private void checkInput(ValidationValues.Row input) {
        Objects.requireNonNull(input, "input");
        if (input.fields().size() > MAX_FIELDS) throw limit("map.inputFields");
        if (input.accountedBytes() > limits.bytes() || input.nodes() > limits.nodes() || input.depth() > limits.depth())
            throw limit("map.input");
    }

    private static void put(Map<String, Object> output, String field, Object value) {
        if (!output.containsKey(field) && output.size() == MAX_FIELDS) throw limit("map.outputFields");
        output.put(field, value);
    }

    private static String name(String value, String field) {
        if (value == null || value.isEmpty() || value.length() > 512)
            throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
        return value;
    }

    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }
}
