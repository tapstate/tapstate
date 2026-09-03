package io.tapstate.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Holds the published examples to the parser and the schema that are published beside them.
 *
 * <p>An example is the one artifact an author - especially a model reading the repository as ground
 * truth - copies before writing anything of their own. That makes a stale example worse than none: it
 * teaches a shape the executor no longer runs, and it does so with the authority of a checked-in file.
 *
 * <p>The example is held from two sides on purpose. The executor running it proves the shape still
 * works; this test proves the same bytes are what the published parser and the published schema
 * accept. Neither implies the other: an example the executor never loads can rot unnoticed, and one
 * that runs may still contradict the schema an author is told to trust.
 *
 * <p>The sweep is asserted to find something. A sweep over a directory that has been moved or renamed
 * finds no file, has nothing to disagree with, and reports the silence as success.
 */
class CheckedInExamplesTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The published schema as it sits on disk, which is the copy an author would open. */
    private static final JsonSchema SCHEMA = JsonSchemaFactory
            .getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(Examples.read(Path.of("spec", "e2e-spec.schema.json")));

    static List<Path> specifications() {
        return Examples.specifications();
    }

    @Test
    void theSweepFindsThePublishedExamples() {
        assertThat(Examples.specifications())
                .as("no specification under %s/ - a sweep that finds nothing agrees with everything", Examples.ROOT)
                .isNotEmpty();
    }

    @ParameterizedTest
    @MethodSource("specifications")
    void theParserAcceptsThePublishedExample(Path specification) {
        String yaml = Examples.read(specification);
        assertThat(catchThrowable(() -> EnvelopeParser.parse(yaml)))
                .as("the executor must be able to run what is published as an example:%n%s", yaml)
                .isNull();
    }

    @ParameterizedTest
    @MethodSource("specifications")
    void theSchemaAcceptsThePublishedExample(Path specification) {
        String yaml = Examples.read(specification);
        assertThat(validate(yaml))
                .as("the published schema must accept the published example, or one of them is lying:%n%s", yaml)
                .isEmpty();
    }

    /**
     * An assembled array is awaited before it is asserted.
     *
     * <p>Reaching a count of documents does not mean the children are on them. The roots are written
     * when they arrive and the arrays fill as the child rows are placed, so a count is true strictly
     * before the assembly is. Reading an array on that instant with a one-shot assert passes while its
     * example runs alone and fails once another real-connector example runs beside it, which is load
     * deciding a result rather than the product. It also fails in the one lane that runs nightly rather
     * than on a pull request, so it surfaces against a release rather than against the change that
     * wrote it.
     *
     * <p>A size read is therefore held to one of two shapes: an await, which carries its own bound, or
     * an assert placed after an await on the same target has already held. Awaiting costs no
     * discrimination - an implementation that never assembles still runs the bound out and fails on the
     * array exactly as the one-shot read would.
     *
     * <p>Only size reads are held to it. A scalar is on the root the moment the document exists, which
     * is why the flat crossings that assert one the instant their count holds are right to.
     *
     * <p>This is a rule rather than a note on an example because the note was tried: the shape was
     * diagnosed and removed from one example, and the next example written that same day carried it
     * straight back in.
     */
    @ParameterizedTest
    @MethodSource("specifications")
    void anAssembledArrayIsAwaitedBeforeItIsAsserted(Path specification) {
        Set<String> settled = new LinkedHashSet<>();
        List<String> raced = new ArrayList<>();
        for (Object step : steps(Examples.read(specification))) {
            if (!(step instanceof Map<?, ?> keyword)) {
                continue;
            }
            settled.addAll(documentReads(keyword.get("await")).keySet());
            documentReads(keyword.get("assert")).forEach((target, matcher) -> {
                if (matcher.containsKey("size") && !settled.contains(target)) {
                    raced.add(target);
                }
            });
        }

        assertThat(raced)
                .as("%s asserts an array size on a target no await has settled yet. The count such an"
                                + " assert follows is true strictly before the assembly is, so it passes"
                                + " alone and fails under load. Await the shape instead; it loses no"
                                + " discrimination.",
                        specification)
                .isEmpty();
    }

    /** The steps of a specification, in order, or nothing when it has none to read. */
    private static List<?> steps(String yaml) {
        Object tree = new Yaml(new SafeConstructor(new LoaderOptions())).load(yaml);
        return tree instanceof Map<?, ?> specification && specification.get("steps") instanceof List<?> steps
                ? steps
                : List.of();
    }

    /** The per-target matchers of a step's {@code doc} read, or nothing when it is not one. */
    private static Map<String, Map<?, ?>> documentReads(Object stepBody) {
        if (!(stepBody instanceof Map<?, ?> body) || !(body.get("doc") instanceof Map<?, ?> reads)) {
            return Map.of();
        }
        Map<String, Map<?, ?>> byTarget = new LinkedHashMap<>();
        reads.forEach((target, matcher) -> byTarget.put(
                String.valueOf(target), matcher instanceof Map<?, ?> fields ? fields : Map.of()));
        return byTarget;
    }

    private Set<ValidationMessage> validate(String yaml) {
        Object tree = new Yaml(new SafeConstructor(new LoaderOptions())).load(yaml);
        return SCHEMA.validate(JSON.valueToTree(tree));
    }
}
