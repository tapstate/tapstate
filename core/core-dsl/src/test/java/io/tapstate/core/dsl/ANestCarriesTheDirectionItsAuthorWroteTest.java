package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

/**
 * Which side of a join carries the other's identity is the author's to say, in the embed that joins them.
 *
 * <p>The two directions are written identically. {@code on:} is a field pair either way - the child row
 * carrying the parent's identity and the parent row carrying the child's produce the same two names in the
 * same order - so nothing in the declaration distinguishes them and nothing in the metadata can be asked
 * instead: a table that declares no key, or one not discovered at all, answers neither way, and those are
 * exactly the cases where the tree is assembled wrong without a word.
 *
 * <p>Absent is not the default written down. An embed that says nothing means what every embed written
 * before this one meant, and says it by carrying nothing: the canonical form of those embeds has to come
 * back byte for byte, because it is what their identity is computed from.
 */
class ANestCarriesTheDirectionItsAuthorWroteTest {

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    /** The same tree every time, with the embed's {@code ref:} line the only thing that differs. */
    private static String pipeline(String ref) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: doc
                    type: nest
                    from: { order: orders, customer: customers }
                    root:
                      from: order
                      key: [id]
                      embed:
                        - from: customer
                          on: { id: customer_id }
                          as: object
                          path: customer
                """
                + ref
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

    private Embed parseEmbed(String ref) {
        return embedOf((PipelineResource) parser.parse(pipeline(ref)));
    }

    @Test
    void anEmbedThatWritesNoDirectionCarriesNone() {
        // Not the default resolved early. Resolving it here would freeze today's direction into every
        // artifact, so an embed that never mentioned one would start carrying a word nobody wrote.
        assertThat(parseEmbed("").ref()).isNull();
    }

    @Test
    void anEmbedCarriesTheDirectionItsAuthorWrote() {
        assertThat(parseEmbed("          ref: parent\n").ref()).isEqualTo(EmbedRef.PARENT);
        assertThat(parseEmbed("          ref: child\n").ref()).isEqualTo(EmbedRef.CHILD);
    }

    @Test
    void whatTheAuthorWroteSurvivesBeingWrittenBackOut() {
        String canonical = writer.write(parser.parse(pipeline("          ref: parent\n")));

        // After path, which is where the emitted order puts the embed's own scalars.
        assertThat(canonical).contains("""
                          as: object
                          path: customer
                          ref: parent
                """);
    }

    @Test
    void anEmbedThatWroteNoDirectionEmitsNone() {
        // The half that matters. A pipeline written before this field existed has to canonicalize to
        // exactly what it did before: the canonical form is the artifact's identity, so emitting the
        // default here would change the form and the hash of every workspace at once, for every
        // pipeline already running.
        String canonical = writer.write(parser.parse(pipeline("")));

        assertThat(canonical).doesNotContain("ref");
    }

    @Test
    void aDirectionThatIsNeitherSideIsRefusedWithACode() {
        Throwable thrown = catchThrowable(() -> parseEmbed("          ref: sideways\n"));

        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).code()).isEqualTo(DslError.ILLEGAL_VALUE);
    }
}
