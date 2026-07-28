package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates the specification schema and the vocabulary listing from the executor's own vocabulary.
 *
 * <p>These artifacts exist so that whoever writes a specification - a person or a model - reads what
 * the parser actually accepts rather than a guide someone kept up to date by hand. That only holds
 * if the artifacts cannot lag: every word below comes from {@link Vocabulary}, and every shape is
 * emitted from an exhaustive switch over the keyword enums the parser dispatches on. A word added to
 * the vocabulary stops this class compiling until it says what the word looks like, so a schema that
 * silently omits a facet is not a mistake anyone can make quietly.
 *
 * <p>What that does not cover: a change to the shape of a word that already exists. Nothing here
 * forces this description to follow the parser's own reading of, say, a cdc change - that pairing is
 * held by the tests, not the compiler.
 */
final class SpecGenerator {

    private static final String SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema";
    private static final String SCHEMA_ID = "https://tapstate.io/schema/e2e/spec-v1.json";
    private static final String ALIAS_PATTERN = "^[^.]+\\..+$";

    private SpecGenerator() {
    }

    /** The JSON Schema for a specification envelope. */
    static String schema() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("$schema", SCHEMA_DIALECT);
        root.put("$id", SCHEMA_ID);
        root.put("title", "Tapstate e2e test specification");
        root.put(
                "description",
                "A declarative end-to-end specification. The same document runs on every tier; "
                        + "generated from the executor, so it says what the parser accepts.");
        root.put("type", "object");
        root.put("additionalProperties", false);
        root.put("required", List.of("name", "pipeline", "steps"));
        root.put("properties", properties());
        root.put("$defs", defs());
        return SpecJson.write(root);
    }

    /** The vocabulary listing: every word an author may write, and where each one comes from. */
    static String vocabulary() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("$id", "https://tapstate.io/schema/e2e/matchers-v1.json");
        root.put(
                "description",
                "Every word a specification may use. Generated from the executor; a word is admitted "
                        + "only once something real answers it.");
        root.put("matchers", listing(Vocabulary.MATCHERS, SpecGenerator::matcherDescription));
        root.put("steps", stepListing());
        root.put("cdcOperations", listing(Vocabulary.CDC_OPERATIONS, op -> "Produces " + op + " changes."));
        root.put("pipelineStates", List.copyOf(Vocabulary.PIPELINE_STATES));
        root.put("topLevelKeys", List.copyOf(Vocabulary.TOP_LEVEL_KEYS));
        root.put("setupKeys", List.copyOf(Vocabulary.SETUP_KEYS));
        root.put("databaseKinds", listing(Vocabulary.DATABASE_KINDS, kind -> "Provides a " + kind + " store."));
        return SpecJson.write(root);
    }

    private static Map<String, Object> properties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("name", scalar("string", "What this specification is called."));
        properties.put("setup", ref("setup", "The bootstrap a real endpoint needs before a pipeline can name it."));
        properties.put(
                "pipeline",
                scalar("string", "Path to the product pipeline document, read with the product's own parser."));
        properties.put("seed", ref("seed", "Initial data laid down before the first step runs."));
        Map<String, Object> steps = new LinkedHashMap<>();
        steps.put("type", "array");
        steps.put("description", "The scenario, in order. A specification with no steps checks nothing.");
        steps.put("minItems", 1);
        
        steps.put("items", Map.of("$ref", "#/$defs/step"));
        properties.put("steps", steps);
        return properties;
    }

    private static Map<String, Object> defs() {
        Map<String, Object> defs = new LinkedHashMap<>();
        defs.put("setup", setupDef());
        defs.put("seed", seedDef());
        defs.put("step", stepDef());
        defs.put("matcher", matcherDef());
        return defs;
    }

    private static Map<String, Object> setupDef() {
        Map<String, Object> properties = new LinkedHashMap<>();
        for (String key : Vocabulary.SETUP_KEYS) {
            // Every setup key but one is a list of names; databases is a mapping, because each store
            // carries a kind and is referenced by the name its author gave it.
            properties.put(key, "databases".equals(key) ? databasesDef() : stringArray(setupDescription(key)));
        }
        Map<String, Object> setup = new LinkedHashMap<>();
        setup.put("type", "object");
        setup.put("description", "What the harness brings up, then the three product verbs run against it, in dependency order.");
        setup.put("additionalProperties", false);
        setup.put("properties", properties);
        return setup;
    }

    private static Map<String, Object> seedDef() {
        Map<String, Object> rows = new LinkedHashMap<>();
        rows.put("type", "object");
        rows.put("additionalProperties", false);
        rows.put("required", List.of("rows"));
        Map<String, Object> rowCount = scalar("integer", "How many rows to lay down.");
        rowCount.put("minimum", 0);
        rows.put("properties", Map.of("rows", rowCount));

        Map<String, Object> seed = new LinkedHashMap<>();
        seed.put("type", "object");
        seed.put("description", "Rows per table, addressed as <resourceId>.<table>.");
        seed.put("propertyNames", Map.of("pattern", ALIAS_PATTERN));
        seed.put("additionalProperties", rows);
        return seed;
    }

    private static Map<String, Object> stepDef() {
        List<Object> forms = new ArrayList<>();
        Map<String, Object> lifecycle = new LinkedHashMap<>();
        lifecycle.put("type", "string");
        lifecycle.put(
                "description",
                "A lifecycle verb, spelled as the product spells it. There is no rewind: re-snapshotting "
                        + "is stop then start.");
        lifecycle.put("enum", List.copyOf(Vocabulary.LIFECYCLE_STEPS));
        forms.add(lifecycle);
        // Exhaustive: a keyword added to the vocabulary does not compile until its shape is here.
        for (StepKeyword keyword : StepKeyword.values()) {
            forms.add(
                    switch (keyword) {
                        case CDC -> keyed(keyword.word(), cdcBody());
                        case AWAIT -> keyed(keyword.word(), Map.of("$ref", "#/$defs/matcher"));
                        case ASSERT -> keyed(keyword.word(), Map.of("$ref", "#/$defs/matcher"));
                    });
        }
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("description", "One stage. Steps run in declaration order; the order is the scenario.");
        step.put("oneOf", forms);
        return step;
    }

    private static Map<String, Object> matcherDef() {
        List<Object> forms = new ArrayList<>();
        // Exhaustive for the same reason as the step keywords above.
        for (MatcherWord word : MatcherWord.values()) {
            forms.add(
                    switch (word) {
                        case COUNT -> keyed(word.word(), countBody());
                        case ERROR_COUNT -> keyed(word.word(), errorCountBody());
                        case STATE -> keyed(word.word(), stateBody());
                    });
        }
        Map<String, Object> matcher = new LinkedHashMap<>();
        matcher.put(
                "description",
                "A condition over observable state. One vocabulary serves both timings: assert reads "
                        + "once, await polls the same matcher until it holds or the bound expires.");
        matcher.put("oneOf", forms);
        return matcher;
    }

    private static Map<String, Object> cdcBody() {
        Map<String, Object> change = new LinkedHashMap<>();
        change.put("type", "string");
        change.put("description", "A change, written '<operation> <rows>'.");
        change.put("pattern", "^(" + String.join("|", Vocabulary.CDC_OPERATIONS) + ")\\s+\\d+$");

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", "object");
        body.put("description", "Exactly one table, addressed as <resourceId>.<table>.");
        body.put("minProperties", 1);
        body.put("maxProperties", 1);
        body.put("propertyNames", Map.of("pattern", ALIAS_PATTERN));
        body.put("additionalProperties", change);
        return body;
    }

    private static Map<String, Object> countBody() {
        Map<String, Object> rows = scalar("integer", "Rows the table is expected to hold.");
        rows.put("minimum", 0);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", "object");
        body.put("description", "Expected rows per table, read from the endpoint itself.");
        body.put("minProperties", 1);
        body.put("propertyNames", Map.of("pattern", ALIAS_PATTERN));
        body.put("additionalProperties", rows);
        return body;
    }

    private static Map<String, Object> errorCountBody() {
        Map<String, Object> count = scalar("integer", "The published error count the pipeline is expected to show.");
        count.put("minimum", 0);
        return count;
    }

    private static Map<String, Object> stateBody() {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("type", "string");
        state.put(
                "description",
                "The lifecycle state of the pipeline this specification names. A specification names "
                        + "exactly one, so the state is written on its own.");
        state.put("enum", List.copyOf(Vocabulary.PIPELINE_STATES));
        return state;
    }

    private static Map<String, Object> keyed(String word, Object body) {
        Map<String, Object> form = new LinkedHashMap<>();
        form.put("type", "object");
        form.put("additionalProperties", false);
        form.put("required", List.of(word));
        form.put("properties", Map.of(word, body));
        return form;
    }

    private static List<Object> stepListing() {
        List<Object> steps = new ArrayList<>();
        for (String verb : Vocabulary.LIFECYCLE_STEPS) {
            steps.add(word(verb, "A lifecycle verb, driven on the pipeline. Written on its own."));
        }
        for (String keyword : Vocabulary.BODIED_STEPS) {
            steps.add(word(keyword, bodiedStepDescription(keyword)));
        }
        return steps;
    }

    private static List<Object> listing(Iterable<String> words, java.util.function.Function<String, String> describe) {
        List<Object> listing = new ArrayList<>();
        words.forEach(w -> listing.add(word(w, describe.apply(w))));
        return listing;
    }

    private static Map<String, Object> word(String word, String description) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("word", word);
        entry.put("description", description);
        return entry;
    }

    private static Map<String, Object> scalar(String type, String description) {
        Map<String, Object> scalar = new LinkedHashMap<>();
        scalar.put("type", type);
        scalar.put("description", description);
        return scalar;
    }

    private static Map<String, Object> stringArray(String description) {
        Map<String, Object> array = new LinkedHashMap<>();
        array.put("type", "array");
        array.put("description", description);
        array.put("items", Map.of("type", "string"));
        return array;
    }

    private static Map<String, Object> ref(String def, String description) {
        Map<String, Object> ref = new LinkedHashMap<>();
        ref.put("$ref", "#/$defs/" + def);
        ref.put("description", description);
        return ref;
    }

    private static String matcherDescription(String word) {
        return switch (MatcherWord.valueOf(word.toUpperCase(java.util.Locale.ROOT))) {
            case COUNT -> "Rows present at an endpoint, read from the endpoint itself rather than from "
                    + "the product's record of what it wrote.";
            case ERROR_COUNT -> "The pipeline's published error count, read from the metrics face: one "
                    + "while it is FAILED, zero otherwise.";
            case STATE -> "The pipeline's published lifecycle state, read from the observation face.";
        };
    }

    private static String bodiedStepDescription(String word) {
        return switch (StepKeyword.valueOf(word.toUpperCase(java.util.Locale.ROOT))) {
            case CDC -> "Produces changes against a seeded table while the pipeline runs.";
            case AWAIT -> "Polls a matcher until it holds or the bound expires.";
            case ASSERT -> "Checks a matcher once, now.";
        };
    }

    /** A named store per entry, each saying only what kind it is; the harness settles the rest. */
    private static Map<String, Object> databasesDef() {
        Map<String, Object> kind = new LinkedHashMap<>();
        kind.put("type", "string");
        kind.put("description", "What sort of store, which settles provisioning, address shape and driver.");
        kind.put("enum", List.copyOf(Vocabulary.DATABASE_KINDS));

        Map<String, Object> request = new LinkedHashMap<>();
        request.put("type", "object");
        request.put("additionalProperties", false);
        request.put("required", List.of("kind"));
        request.put("properties", Map.of("kind", kind));

        Map<String, Object> databases = new LinkedHashMap<>();
        databases.put("type", "object");
        databases.put("description", setupDescription("databases"));
        databases.put("additionalProperties", request);
        return databases;
    }

    private static String setupDescription(String key) {
        return switch (key) {
            case "databases" -> "Stores the harness provisions first, keyed by the name whose address the "
                    + "resources interpolate.";
            case "connectors" -> "Connector ids whose runtime jars are registered; idempotent by content hash.";
            case "apply" -> "Product resource files, applied as one batch: the product resolves references "
                    + "within the submitted set.";
            case "discover" -> "Resource ids whose source model is discovered, feeding target-table creation.";
            default -> throw new IllegalStateException(
                    "setup gained the key '" + key + "' with nothing to say about it");
        };
    }
}
