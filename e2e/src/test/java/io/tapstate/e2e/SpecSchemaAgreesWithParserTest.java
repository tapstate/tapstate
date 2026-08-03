package io.tapstate.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Holds the generated schema to the parser it claims to describe.
 *
 * <p>Byte-locking the schema to the generator proves the file is not hand-edited; it proves nothing
 * about whether the description is true. A schema that quietly disagrees with the parser is worse
 * than no schema, because an author - especially a model reading it as ground truth - would take its
 * word.
 *
 * <p>The obligation is deliberately asymmetric. Every specification the parser accepts <em>must</em>
 * validate: a false rejection is the harmful direction, telling an author their legal document is
 * wrong. The reverse is tolerated - the schema may be the looser of the two, because the parser is
 * what actually refuses a run. Where the schema cannot express a rule at all, that is stated here
 * rather than left for someone to discover.
 */
class SpecSchemaAgreesWithParserTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonSchema SCHEMA = JsonSchemaFactory
            .getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(SpecGenerator.schema());

    @Test
    void theSchemaAcceptsEverySpecificationTheParserAccepts() {
        for (String specification : accepted()) {
            assertThat(catchThrowable(() -> EnvelopeParser.parse(specification)))
                    .as("this sample is meant to be valid:\n%s", specification)
                    .isNull();
            assertThat(validate(specification))
                    .as("the parser accepts this but the schema rejects it — the schema is lying to "
                            + "whoever reads it as ground truth:\n%s", specification)
                    .isEmpty();
        }
    }

    @Test
    void theSchemaRejectsWhatItIsAbleToReject() {
        for (String specification : refused()) {
            assertThat(catchThrowable(() -> EnvelopeParser.parse(specification)))
                    .as("this sample is meant to be invalid:\n%s", specification)
                    .isInstanceOf(EnvelopeException.class);
            assertThat(validate(specification))
                    .as("the parser refuses this and the schema does not — an author would be told it "
                            + "is fine right up until the run:\n%s", specification)
                    .isNotEmpty();
        }
    }

    @Test
    void theRulesTheSchemaCannotExpressAreTheParsersAlone() {
        // A duplicate key is a YAML-level fact: by the time a document is a JSON tree the first value
        // is already gone, so no schema can see it. The parser refuses it; nothing here can.
        String duplicate = """
                name: n
                pipeline: p.tap.yml
                steps:
                  - assert: { count: { t.o: 1 } }
                  - assert: { count: { t.o: 1, t.o: 2 } }
                """;
        assertThat(catchThrowable(() -> EnvelopeParser.parse(duplicate)))
                .as("the parser is the only thing that can refuse a duplicate key")
                .isInstanceOf(EnvelopeException.class);
    }

    private static Set<ValidationMessage> validate(String yaml) {
        Object tree = new Yaml(new SafeConstructor(new LoaderOptions())).load(yaml);
        return SCHEMA.validate(JSON.valueToTree(tree));
    }

    private static java.util.List<String> accepted() {
        return java.util.List.of(
                // The smallest thing worth running.
                """
                name: minimal
                pipeline: p.tap.yml
                steps:
                  - start
                """,
                // Every facet at once, which is the shape the first example takes.
                """
                name: everything
                setup:
                  connectors: [mongodb]
                  apply: [src.tap.yml, tgt.tap.yml]
                  discover: [src]
                pipeline: p.tap.yml
                seed:
                  src.orders: { rows: 100 }
                steps:
                  - start
                  - await: { state: RUNNING }
                  - assert: { count: { tgt.orders: 100 } }
                  - cdc: { src.orders: insert 10 }
                  - await: { count: { tgt.orders: 110 } }
                  - pause
                  - resume
                  - stop
                """,
                // A table name may carry dots; only the first one separates the resource id.
                """
                name: dotted-table
                pipeline: p.tap.yml
                seed:
                  src.orders.2026: { rows: 1 }
                steps:
                  - assert: { count: { src.orders.2026: 1 } }
                """,
                // Counting several tables in one matcher.
                """
                name: many-tables
                pipeline: p.tap.yml
                steps:
                  - assert: { count: { a.one: 0, b.two: 3 } }
                """,
                // The error-count matcher, a whole number written on its own like the state matcher.
                """
                name: error-count
                pipeline: p.tap.yml
                steps:
                  - await: { state: FAILED }
                  - assert: { error_count: 1 }
                """);
    }

    private static java.util.List<String> refused() {
        return java.util.List.of(
                // A key nobody serves.
                """
                name: n
                pipeline: p.tap.yml
                expect: { count: { t.o: 1 } }
                steps:
                  - start
                """,
                // A word with no source behind it.
                """
                name: n
                pipeline: p.tap.yml
                steps:
                  - await: { synced: true }
                """,
                // A document located but held to nothing: it would hold for any document at all,
                // including the wrong one, so locating it proves nothing.
                """
                name: n
                pipeline: p.tap.yml
                steps:
                  - assert: { doc: { t.o: { where: { id: 1 } } } }
                """,
                // A verb the product does not have.
                """
                name: n
                pipeline: p.tap.yml
                steps:
                  - restart
                """,
                // A lane word. Nothing selects a lane from a specification, so naming one is not a
                // harmless leftover: it would be a choice the author believes is being taken.
                """
                name: n
                tier: smoke
                pipeline: p.tap.yml
                steps:
                  - start
                """,
                // A change no cdc step can produce.
                """
                name: n
                pipeline: p.tap.yml
                steps:
                  - cdc: { t.orders: truncate 1 }
                """,
                // Rows cannot run backwards.
                """
                name: n
                pipeline: p.tap.yml
                steps:
                  - assert: { count: { t.o: -5 } }
                """,
                // Neither can an error count.
                """
                name: n
                pipeline: p.tap.yml
                steps:
                  - assert: { error_count: -1 }
                """,
                // A specification with no steps checks nothing.
                """
                name: n
                pipeline: p.tap.yml
                steps: []
                """,
                // A name is not optional.
                """
                pipeline: p.tap.yml
                steps:
                  - start
                """,
                // Neither is the pipeline it exercises.
                """
                name: n
                steps:
                  - start
                """,
                // An alias without a resource id addresses nothing.
                """
                name: n
                pipeline: p.tap.yml
                steps:
                  - assert: { count: { orders: 1 } }
                """);
    }
}
