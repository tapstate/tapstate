package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

/**
 * What identifies one row of a level is the author's to say, in the level that holds those rows, and the
 * word is {@code key} wherever the level sits - the root has always used it and an embed now does too.
 *
 * <p>It is not the same question as {@code arrayKey}, which asks which element of an array this is and is
 * allowed to answer with something unique only inside that array - an order's line numbers. They coincide
 * often enough to look like one field and are not: a column that tells two lines of one order apart does
 * not identify a line among every line ever written, and only the second can be pointed at from elsewhere.
 *
 * <p>Absent is not the default written down. A level that says nothing takes what its stream declares, and
 * says so by carrying nothing: the canonical form of every pipeline written before this field existed has
 * to come back byte for byte, because it is what their identity is computed from.
 */
class ANestCarriesTheRowIdentityItsAuthorWroteTest {

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    /** The same tree every time, with the embed's own extra lines the only thing that differs. */
    private static String pipeline(String embedLines) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: doc
                    type: nest
                    from: { order: orders, item: order_items }
                    root:
                      from: order
                      key: [id]
                      embed:
                        - from: item
                          on: { order_id: id }
                          as: array
                          path: items
                """
                + embedLines
                + """
                serve:
                  from: doc
                  query: [ { type: rest } ]
                """;
    }

    private static Embed embedOf(PipelineResource p) {
        TransformBody.Nest nest = (TransformBody.Nest) ((Step.Inline) p.transforms().get(0)).body();
        return nest.root().embed().get(0);
    }

    private Embed parseEmbed(String embedLines) {
        return embedOf((PipelineResource) parser.parse(pipeline(embedLines)));
    }

    private String canonicalOf(String embedLines) {
        return writer.write(parser.parse(pipeline(embedLines)));
    }

    /** How many levels in the written form name a key of their own. The root always does. */
    private static long keyLines(String canonical) {
        return canonical.lines().filter(line -> line.strip().startsWith("key:")).count();
    }

    @Test
    void anEmbedThatWritesNoRowIdentityCarriesNone() {
        // Not the default resolved early. Resolving it here would freeze whatever the catalog says today
        // into every artifact, so a level that never named a key would start carrying columns nobody wrote.
        assertThat(parseEmbed("").key()).isNull();
    }

    @Test
    void anEmbedCarriesTheRowIdentityItsAuthorWrote() {
        assertThat(parseEmbed("          key: [sku_code]\n").key()).containsExactly("sku_code");
    }

    @Test
    void aRowIdentityAndAnArrayElementKeyAreCarriedSeparately() {
        // The pair that says these are two questions: this level's rows are identified by sku_code, while
        // an element of the array is told apart by line_no, which repeats across orders.
        Embed embed = parseEmbed("          key: [sku_code]\n          arrayKey: [line_no]\n");

        assertThat(embed.key()).containsExactly("sku_code");
        assertThat(embed.arrayKey()).containsExactly("line_no");
    }

    @Test
    void whatTheAuthorWroteSurvivesBeingWrittenBackOut() {
        String canonical = canonicalOf("          key: [sku_code]\n");

        assertThat(canonical).contains("sku_code");
        assertThat(keyLines(canonical)).describedAs("the root's and the embed's").isEqualTo(2);
    }

    @Test
    void anEmbedThatWroteNoRowIdentityEmitsNone() {
        // The half that matters. A pipeline written before this field existed has to canonicalize to
        // exactly what it did before: the canonical form is the artifact's identity, so emitting a
        // resolved default here would change the form and the hash of every workspace at once, for every
        // pipeline already running - and no gate in this repository would catch it.
        String canonical = canonicalOf("");

        assertThat(canonical).doesNotContain("sku_code");
        assertThat(keyLines(canonical)).describedAs("the root's alone, which was always written").isEqualTo(1);
    }

    @Test
    void arrayKeyAloneStillMeansWhatItAlwaysMeant() {
        // The existing field is untouched by the new one: writing it names an element key and no row
        // identity, which is what every embed carrying an arrayKey today already says.
        Embed embed = parseEmbed("          arrayKey: [line_no]\n");

        assertThat(embed.arrayKey()).containsExactly("line_no");
        assertThat(embed.key()).isNull();
        assertThat(canonicalOf("          arrayKey: [line_no]\n")).contains("arrayKey:");
    }
}
