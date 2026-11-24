package io.tapstate.core.dsl.aggregate;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * What an aggregate step's stored state was built from, as one digest for each part of the step a user
 * writes: {@code from}, {@code key}, {@code where}, {@code groupBy} and {@code fields}.
 *
 * <p>The stored rows and totals are valid only for the definition that built them. A total computed
 * under one {@code where:} and continued under another mixes two answers that nothing can separate
 * afterwards, so every difference counts, including one that only edits a default. Keeping a digest per
 * part lets a refusal say which part changed without storing the definition itself.
 *
 * @param parts part name to the digest of its content, in a fixed order
 */
public record AggregateDefinition(Map<String, String> parts) implements Serializable {
    private static final long serialVersionUID = 1L;

    public AggregateDefinition {
        parts = Collections.unmodifiableMap(new LinkedHashMap<>(parts));
    }

    /**
     * The definition of {@code spec} reading {@code from}.
     *
     * @param from what the step reads, as written
     */
    public static AggregateDefinition of(List<String> from, AggregateStepSpec spec) {
        Map<String, String> parts = new LinkedHashMap<>();
        parts.put("from", digest(from));
        parts.put("key", digest(spec.key()));
        parts.put("where", digest(List.of(String.valueOf(spec.where()))));
        List<String> groups = new ArrayList<>();
        spec.groupBy().forEach((name, expression) -> {
            groups.add(name);
            groups.add(expression);
            groups.add(String.valueOf(spec.groupTypes().get(name)));
        });
        parts.put("groupBy", digest(groups));
        List<String> fields = new ArrayList<>();
        for (RollupSpec field : spec.fields()) {
            fields.add(field.field());
            fields.add(field.function().name());
            fields.add(String.valueOf(field.argument()));
            fields.add(String.valueOf(field.by().size()));
            fields.addAll(field.by());
            fields.add(String.valueOf(field.filter()));
            fields.add(String.valueOf(field.argumentType()));
            fields.add(String.valueOf(field.resultType()));
            fields.add(String.valueOf(field.convert()));
            Object value = field.defaultValue();
            fields.add(value == null ? "null" : value.getClass().getName() + ":" + value);
            fields.add(String.join(",", new TreeSet<>(field.references())));
        }
        parts.put("fields", digest(fields));
        return new AggregateDefinition(parts);
    }

    /** The parts in which this definition differs from {@code stored}, in the fixed order. */
    public List<String> changedSince(AggregateDefinition stored) {
        List<String> changed = new ArrayList<>();
        for (Map.Entry<String, String> part : parts.entrySet()) {
            if (!part.getValue().equals(stored.parts().get(part.getKey()))) {
                changed.add(part.getKey());
            }
        }
        return changed;
    }

    /** The form kept beside the state: one {@code name=digest} line for each part. */
    public byte[] stored() {
        StringBuilder text = new StringBuilder();
        parts.forEach((name, digest) -> text.append(name).append('=').append(digest).append('\n'));
        return text.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Reads what {@link #stored()} wrote. */
    public static AggregateDefinition read(byte[] stored) {
        Map<String, String> parts = new LinkedHashMap<>();
        for (String line : new String(stored, StandardCharsets.UTF_8).split("\n")) {
            int at = line.indexOf('=');
            if (at > 0) {
                parts.put(line.substring(0, at), line.substring(at + 1));
            }
        }
        return new AggregateDefinition(parts);
    }

    /** Each item is written with its length, so no two lists of items share a digest by concatenation. */
    private static String digest(List<String> items) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            for (String item : items) {
                byte[] bytes = item.getBytes(StandardCharsets.UTF_8);
                sha.update(Integer.toString(bytes.length).getBytes(StandardCharsets.UTF_8));
                sha.update((byte) ':');
                sha.update(bytes);
            }
            return HexFormat.of().formatHex(sha.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every Java platform", e);
        }
    }
}
