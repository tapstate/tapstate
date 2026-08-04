package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Reads a test specification. Anything unknown, empty, duplicated or lossy is rejected rather than
 * absorbed: a specification that silently drops a step an author wrote, or that holds while
 * asserting nothing, would pass while testing something other than what it says.
 *
 * <p>Enum words are read case-insensitively; the state word is the product's own enum constant and
 * the rest are lower-case by convention.
 */
public final class EnvelopeParser {



    private EnvelopeParser() {}

    public static Envelope parse(String yaml) {
        Map<String, Object> root = mapping(load(yaml), "envelope");
        rejectUnknownKeys(root.keySet(), Set.copyOf(Vocabulary.TOP_LEVEL_KEYS), "envelope");
        return new Envelope(
                requiredString(root, "name"),
                setup(root.get("setup")),
                requiredString(root, "pipeline"),
                seed(root.get("seed")),
                steps(root.get("steps")));
    }

    private static Object load(String yaml) {
        LoaderOptions options = new LoaderOptions();
        // A duplicate key otherwise last-wins with only a log line, quietly discarding whichever
        // step, table or count the author wrote first.
        options.setAllowDuplicateKeys(false);
        try {
            return new Yaml(new SafeConstructor(options)).load(yaml);
        } catch (YAMLException e) {
            throw new EnvelopeException("envelope is not well-formed YAML: " + e.getMessage(), e);
        }
    }

    private static Setup setup(Object node) {
        if (node == null) {
            return Setup.NONE;
        }
        Map<String, Object> mapping = mapping(node, "setup");
        rejectUnknownKeys(mapping.keySet(), Set.copyOf(Vocabulary.SETUP_KEYS), "setup");
        return new Setup(
                databases(mapping.get("databases")),
                stringList(mapping.get("connectors"), "setup.connectors"),
                stringList(mapping.get("apply"), "setup.apply"),
                stringList(mapping.get("discover"), "setup.discover"));
    }

    /**
     * The stores a specification asks the harness to provide, keyed by the name its resources
     * interpolate an address out of. Nothing here names a driver or a container: the kind settles both,
     * so a specification stays a description of what it needs rather than of how to get it.
     */
    private static Map<String, DatabaseRequest> databases(Object node) {
        if (node == null) {
            return Map.of();
        }
        Map<String, DatabaseRequest> requests = new LinkedHashMap<>();
        mapping(node, "setup.databases")
                .forEach(
                        (name, request) -> {
                            String where = "setup.databases." + name;
                            Map<String, Object> fields = mapping(request, where);
                            rejectUnknownKeys(fields.keySet(), Vocabulary.DATABASE_KEYS, where);
                            requests.put(
                                    name,
                                    new DatabaseRequest(
                                            databaseKind(string(fields.get("kind"), where + ".kind"))));
                        });
        return requests;
    }

    private static DatabaseKind databaseKind(String word) {
        return word(DatabaseKind.values(), DatabaseKind::word, word,
                "unknown store kind: " + word + "; the harness can provide " + Vocabulary.DATABASE_KINDS);
    }

    private static List<Seed> seed(Object node) {
        if (node == null) {
            return List.of();
        }
        List<Seed> seeds = new ArrayList<>();
        mapping(node, "seed")
                .forEach(
                        (alias, spec) -> {
                            Map<String, Object> entry = mapping(spec, "seed." + alias);
                            rejectUnknownKeys(entry.keySet(), Vocabulary.SEED_KEYS, "seed." + alias);
                            seeds.add(new Seed(alias(alias), seedRows(entry, "seed." + alias)));
                        });
        return seeds;
    }

    /**
     * The rows one seed entry lays down. {@code rows: N} is sugar for the generated shape - ids 1..N,
     * each with a sequence equal to its id - expanded here so no driver ever decides what a row looks
     * like. {@code values} is the general form: the rows themselves, columns and all.
     */
    private static List<Map<String, Object>> seedRows(Map<String, Object> entry, String at) {
        Object rows = entry.get("rows");
        Object values = entry.get("values");
        if ((rows == null) == (values == null)) {
            throw new EnvelopeException(at + " carries exactly one of rows or values");
        }
        if (rows != null) {
            return SeedRows.generated(rowCount(rows, at + ".rows"));
        }
        return valueRows(values, at + ".values");
    }

    /**
     * Explicit rows: each carries {@code id} (the key everything seeds and upserts by), no two rows
     * carry the same one, every row carries the same columns (one table, one shape), and every value is
     * an integer or a string - the two scalars whose crossing every store in this vocabulary spells the
     * same way. Wider value types are a widening of this word, not a loosening of this check.
     *
     * <p>Uniqueness is held here rather than left to the store, because the stores disagree about it: a
     * relational table refuses the second row outright, and a document store accepts it, so the same
     * specification would be an authoring error against one and a silently doubled seed against
     * another - surfacing far away as an ambiguous document read, or as a count that agrees by
     * accident.
     */
    private static List<Map<String, Object>> valueRows(Object node, String at) {
        if (!(node instanceof List<?> sequence) || sequence.isEmpty()) {
            throw new EnvelopeException(at + " must be a non-empty sequence of rows, found: " + describe(node));
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        Set<String> shape = null;
        Set<Object> ids = new LinkedHashSet<>();
        for (int i = 0; i < sequence.size(); i++) {
            String rowAt = at + "[" + i + "]";
            Map<String, Object> row = mapping(sequence.get(i), rowAt);
            if (!row.containsKey(SeedRows.ID)) {
                throw new EnvelopeException(rowAt + " carries no id, the key rows are seeded and upserted by");
            }
            if (!ids.add(normalizedId(row.get(SeedRows.ID)))) {
                throw new EnvelopeException(
                        rowAt + " carries the id " + row.get(SeedRows.ID)
                                + " a row before it already carries; one row, one id");
            }
            row.forEach((column, value) -> requireScalar(value, rowAt + "." + column));
            if (shape == null) {
                shape = row.keySet();
            } else if (!shape.equals(row.keySet())) {
                throw new EnvelopeException(
                        rowAt + " does not carry the same columns as the rows before it: one table, one shape");
            }
            rows.add(new LinkedHashMap<>(row));
        }
        return rows;
    }

    /**
     * The id as identity rather than as written: a YAML parser hands back 1 as an Integer and 1 as a
     * Long depending on nothing an author controls, and two rows keyed the same way are the same row
     * whichever width each arrived as.
     */
    private static Object normalizedId(Object id) {
        return id instanceof Number number ? number.longValue() : id;
    }

    /**
     * A path into a document: dotted field names, each optionally followed by list indices, as in
     * {@code items[0].sku}. Held here rather than where it is walked, because a path is written by an
     * author and a mistake in one is an authoring mistake: unchecked, a missing bracket or a negative
     * index reaches a polling wait as a raw index or substring fault, naming neither the specification
     * nor the path, and a wait that dies that way records no witness at all.
     */
    private static void requirePath(String path, String at) {
        String where = at + "." + path;
        if (path.isBlank()) {
            throw new EnvelopeException(where + " must name a field");
        }
        for (String segment : path.split("\\.", -1)) {
            int bracket = segment.indexOf('[');
            String field = bracket < 0 ? segment : segment.substring(0, bracket);
            if (field.isBlank()) {
                throw new EnvelopeException(where + " must name a field before each index");
            }
            while (bracket >= 0) {
                int close = segment.indexOf(']', bracket);
                if (close < 0) {
                    throw new EnvelopeException(where + " leaves an index unclosed: expected a ] after " + segment);
                }
                String index = segment.substring(bracket + 1, close);
                if (!index.matches("\\d+")) {
                    throw new EnvelopeException(
                            where + " indexes a list with '" + index
                                    + "'; an index is a whole number counting from zero");
                }
                bracket = segment.indexOf('[', close);
                if (bracket > close + 1) {
                    throw new EnvelopeException(where + " carries text between indices: " + segment);
                }
                if (bracket < 0 && close != segment.length() - 1) {
                    throw new EnvelopeException(where + " carries text after an index: " + segment);
                }
            }
        }
    }

    private static void requireScalar(Object value, String at) {
        if (!(value instanceof Integer || value instanceof Long || value instanceof String)) {
            throw new EnvelopeException(
                    at + " must be an integer or a string, found: " + describe(value));
        }
    }

    private static List<Step> steps(Object node) {
        if (node == null) {
            throw new EnvelopeException("envelope is missing the required key: steps");
        }
        if (!(node instanceof List<?> sequence)) {
            throw new EnvelopeException("steps must be a sequence, found: " + describe(node));
        }
        if (sequence.isEmpty()) {
            throw new EnvelopeException("steps must name at least one step");
        }
        List<Step> steps = new ArrayList<>();
        for (Object element : sequence) {
            steps.add(step(element));
        }
        return steps;
    }

    private static Step step(Object element) {
        if (element instanceof String verb) {
            return new Step.Lifecycle(lifecycleVerb(verb));
        }
        Map<String, Object> mapping = mapping(element, "step");
        if (mapping.size() != 1) {
            throw new EnvelopeException("a step carries exactly one verb, found: " + mapping.keySet());
        }
        Map.Entry<String, Object> only = mapping.entrySet().iterator().next();
        // Exhaustive over the keyword enum: a keyword added to the vocabulary does not compile until
        // it means something here.
        return switch (keyword(only.getKey())) {
            case AWAIT -> new Step.Await(matcher(only.getValue()));
            case ASSERT -> new Step.Assertion(matcher(only.getValue()));
            case CDC -> cdc(only.getValue());
        };
    }

    private static LifecycleVerb lifecycleVerb(String verb) {
        if (!Vocabulary.LIFECYCLE_STEPS.contains(verb.toLowerCase(Locale.ROOT))) {
            throw new EnvelopeException(
                    "unknown step verb: " + verb + "; a step on its own is one of "
                            + Vocabulary.LIFECYCLE_STEPS);
        }
        return LifecycleVerb.valueOf(verb.toUpperCase(Locale.ROOT));
    }

    private static StepKeyword keyword(String word) {
        return word(StepKeyword.values(), StepKeyword::word, word,
                "unknown step verb: " + word + "; a step with a body is one of " + Vocabulary.BODIED_STEPS);
    }

    private static MatcherWord matcherWord(String word) {
        return word(MatcherWord.values(), MatcherWord::word, word,
                "unknown matcher: " + word + "; known matchers are " + Vocabulary.MATCHERS);
    }

    /** Resolves a written word to its enum, refusing anything the vocabulary does not hold. */
    private static <T> T word(T[] values, java.util.function.Function<T, String> spelling,
            String written, String refusal) {
        for (T value : values) {
            if (spelling.apply(value).equals(written)) {
                return value;
            }
        }
        throw new EnvelopeException(refusal);
    }

    private static Step.Cdc cdc(Object node) {
        Map<String, Object> mapping = mapping(node, "cdc");
        if (mapping.size() != 1) {
            throw new EnvelopeException("a cdc step names exactly one table, found: " + mapping.keySet());
        }
        Map.Entry<String, Object> only = mapping.entrySet().iterator().next();
        String change = string(only.getValue(), "cdc." + only.getKey());
        String[] parts = change.trim().split("\\s+");
        if (parts.length != 2) {
            throw new EnvelopeException("a cdc change reads '<op> <rows>', found: " + change);
        }
        return new Step.Cdc(alias(only.getKey()), cdcOp(parts[0]), cdcRows(parts[1]));
    }

    private static CdcOp cdcOp(String op) {
        try {
            return CdcOp.valueOf(op.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new EnvelopeException(
                    "unknown cdc operation: " + op + "; known operations are " + Vocabulary.CDC_OPERATIONS);
        }
    }

    private static long cdcRows(String rows) {
        long value;
        try {
            value = Long.parseLong(rows);
        } catch (NumberFormatException e) {
            throw new EnvelopeException("a cdc change must name a whole number of rows, found: " + rows);
        }
        if (value < 0) {
            throw new EnvelopeException("a cdc change must not name negative rows, found: " + rows);
        }
        return value;
    }

    private static Matcher matcher(Object node) {
        Map<String, Object> mapping = mapping(node, "matcher");
        if (mapping.size() != 1) {
            throw new EnvelopeException("a matcher carries exactly one word, found: " + mapping.keySet());
        }
        Map.Entry<String, Object> only = mapping.entrySet().iterator().next();
        return switch (matcherWord(only.getKey())) {
            case COUNT -> count(only.getValue());
            case DOC -> doc(only.getValue());
            case ERROR_COUNT -> new Matcher.ErrorCount(rowCount(only.getValue(), "error_count"));
            case STATE -> new Matcher.State(pipelineState(only.getValue()));
        };
    }

    /** One table, one document: located by {@code where}, held to {@code expect} and {@code size}. */
    private static Matcher doc(Object node) {
        Map<String, Object> mapping = mapping(node, "doc");
        if (mapping.size() != 1) {
            throw new EnvelopeException("doc names exactly one table, found: " + mapping.keySet());
        }
        Map.Entry<String, Object> only = mapping.entrySet().iterator().next();
        String at = "doc." + only.getKey();
        Map<String, Object> body = mapping(only.getValue(), at);
        rejectUnknownKeys(body.keySet(), Vocabulary.DOC_KEYS, at);

        Map<String, Object> where = mapping(body.get("where"), at + ".where");
        if (where.isEmpty()) {
            throw new EnvelopeException(at + ".where must name at least one setting to locate the document by");
        }
        where.forEach((setting, value) -> requireScalar(value, at + ".where." + setting));

        Map<String, Object> expect = body.get("expect") == null
                ? Map.of()
                : mapping(body.get("expect"), at + ".expect");
        expect.forEach(
                (path, value) -> {
                    requirePath(path, at + ".expect");
                    requireScalar(value, at + ".expect." + path);
                });

        Map<String, Long> size = new LinkedHashMap<>();
        if (body.get("size") != null) {
            mapping(body.get("size"), at + ".size")
                    .forEach(
                            (path, length) -> {
                                requirePath(path, at + ".size");
                                size.put(path, rowCount(length, at + ".size." + path));
                            });
        }
        if (expect.isEmpty() && size.isEmpty()) {
            throw new EnvelopeException(at + " holds the document to nothing: carry expect or size");
        }
        return new Matcher.Doc(alias(only.getKey()), where, expect, size);
    }

    private static Matcher count(Object node) {
        Map<String, Object> mapping = mapping(node, "count");
        if (mapping.isEmpty()) {
            throw new EnvelopeException("count must name at least one table");
        }
        Map<TableAlias, Long> expected = new LinkedHashMap<>();
        mapping.forEach((alias, rows) -> expected.put(alias(alias), rowCount(rows, "count." + alias)));
        return new Matcher.Count(expected);
    }

    private static PipelineState pipelineState(Object node) {
        String state = string(node, "state");
        try {
            return PipelineState.valueOf(state.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new EnvelopeException("unknown pipeline state: " + state);
        }
    }

    /**
     * Splits at the first dot, matching how the product resolves a qualified table reference. A
     * table name may itself contain dots; a resource id may not.
     */
    private static TableAlias alias(String alias) {
        int dot = alias.indexOf('.');
        if (dot <= 0 || dot == alias.length() - 1) {
            throw new EnvelopeException("a table alias reads '<resourceId>.<table>', found: " + alias);
        }
        return new TableAlias(alias.substring(0, dot), alias.substring(dot + 1));
    }

    private static void rejectUnknownKeys(Set<String> present, Set<String> known, String where) {
        for (String key : present) {
            if (!known.contains(key)) {
                throw new EnvelopeException("unknown " + where + " key: " + key + "; known keys are " + known);
            }
        }
    }

    private static String requiredString(Map<String, Object> mapping, String key) {
        Object value = mapping.get(key);
        if (value == null) {
            throw new EnvelopeException("envelope is missing the required key: " + key);
        }
        return string(value, key);
    }

    /** Copies into a string-keyed map, naming any key YAML resolved to something else. */
    private static Map<String, Object> mapping(Object node, String where) {
        if (!(node instanceof Map<?, ?> raw)) {
            throw new EnvelopeException(where + " must be a mapping, found: " + describe(node));
        }
        Map<String, Object> mapping = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new EnvelopeException(
                        where + " keys must be strings, found: " + entry.getKey() + " (" + describe(entry.getKey()) + ")");
            }
            mapping.put(key, entry.getValue());
        }
        return mapping;
    }

    private static List<String> stringList(Object node, String where) {
        if (node == null) {
            return List.of();
        }
        if (!(node instanceof List<?> sequence)) {
            throw new EnvelopeException(where + " must be a sequence, found: " + describe(node));
        }
        List<String> values = new ArrayList<>();
        for (Object element : sequence) {
            values.add(string(element, where));
        }
        return values;
    }

    private static String string(Object node, String where) {
        if (!(node instanceof String value)) {
            throw new EnvelopeException(where + " must be a string, found: " + describe(node));
        }
        return value;
    }

    /** Whole numbers only: a fractional row count would otherwise truncate in silence. */
    private static long rowCount(Object node, String where) {
        long value;
        switch (node) {
            case Integer number -> value = number;
            case Long number -> value = number;
            case BigInteger number -> throw new EnvelopeException(
                    where + " is out of range for a row count: " + number);
            case null -> throw new EnvelopeException(where + " must be a whole number, found: nothing");
            default -> throw new EnvelopeException(
                    where + " must be a whole number, found: " + node + " (" + describe(node) + ")");
        }
        if (value < 0) {
            throw new EnvelopeException(where + " must not be negative, found: " + value);
        }
        return value;
    }

    private static String describe(Object node) {
        return node == null ? "nothing" : node.getClass().getSimpleName();
    }
}
