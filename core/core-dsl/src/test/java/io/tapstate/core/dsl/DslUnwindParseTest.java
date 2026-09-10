package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The parse surface of {@code type: unwind}, the one transform that turns one row into several.
 *
 * <p>Its payload is one required key and four optional ones, and the naming is deliberately not this
 * project's invention: {@code path}, {@code include_array_index} and
 * {@code preserve_null_and_empty_arrays} are the three options a document store's own unwind stage
 * carries, spelled the way every other key here is spelled. The remaining two describe things that
 * stage never has to answer for - what type the expanded column is declared as, and which field
 * inside an element identifies the row it becomes - because it does not write into a keyed, typed
 * table. Those two carry the {@code element_} prefix so a reader can see at a glance which half of
 * the vocabulary they are in.
 *
 * <p>Nothing here judges what the step then does to a row; that is the port's own suite. What is
 * checked is only that the declaration is accepted where it should be and refused where it should
 * be, since a payload key silently dropped by the parser reads to an author exactly like one that
 * had no effect.
 */
class DslUnwindParseTest {

    private static final String SOURCES = """
            version: tapstate/v1
            kind: source
            id: src_a
            connector: mysql
            mode: cdc
            tables: [ orders ]
            """;

    private static String pipeline(String payload) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: explode
                    from: [orders]
                    type: unwind
                %s
                serve:
                  from: explode
                  sync: [ { id: s, source: src_a } ]
                """.formatted(payload.indent(4).stripTrailing());
    }

    private static void batch(String payload) {
        DslParser parser = new DslParser();
        Workspace.of(Stream.of(SOURCES, pipeline(payload)).map(parser::parse).toList());
    }

    private static DslException refused(String payload) {
        Throwable thrown = catchThrowable(() -> batch(payload));
        assertThat(thrown).isInstanceOf(DslException.class);
        return (DslException) thrown;
    }

    /**
     * A path and nothing else is the whole of what the grammar demands, and it parses into a body
     * whose four optional keys are simply absent. Asserted on the parsed body rather than on the
     * batch loading clean: a payload key the parser silently dropped would satisfy "nothing was
     * thrown" exactly as well as one it read.
     *
     * <p>Such a declaration does not go on to <em>load</em>, and that is a different layer's
     * verdict - naming nothing that varies per element is refused by the write-key rule, in
     * {@code UnwindWriteKeyRulesTest}. Parsing and being allowed to run are kept apart here on
     * purpose: this suite is what catches a payload key that never reaches the model at all.
     */
    @Test
    @DisplayName("the smallest unwind - a path and nothing else - parses to a body carrying only it")
    void aPathIsTheWholeOfTheRequiredPayload() {
        TransformBody.Unwind body = unwindIn(new DslParser().parse(pipeline("path: items")));

        assertThat(body.path()).isEqualTo("items");
        assertThat(body.elementKey()).isNull();
        assertThat(body.includeArrayIndex()).isNull();
        assertThat(body.preserveNullAndEmptyArrays()).isNull();
        assertThat(body.elementType()).isNull();
    }

    private static TransformBody.Unwind unwindIn(Resource parsed) {
        Step step = ((PipelineResource) parsed).transforms().get(0);
        return (TransformBody.Unwind) ((Step.Inline) step).body();
    }

    @Test
    @DisplayName("all four optional keys are accepted alongside the path")
    void theOptionalKeysAreAccepted() {
        assertThatCode(() -> batch("""
                path: items
                include_array_index: item_no
                preserve_null_and_empty_arrays: true
                element_key: sku
                element_type: string
                """)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("an unwind with no path is refused as a missing field, not accepted as a no-op")
    void thePathIsRequired() {
        DslException ex = refused("element_key: sku");

        assertThat(ex.code()).isEqualTo(DslError.MISSING_FIELD);
        assertThat(ex.path()).isEqualTo("transforms[0].path");
        assertThat(ex.args()).containsEntry("field", "path");
    }

    /**
     * The camelCase spelling is the one an author arrives with, having read the document store's own
     * documentation. Refusing it by name is the whole point: accepted-and-ignored would expand
     * without the ordinal column the author asked for, and the rows would look right until somebody
     * counted them.
     */
    @Test
    @DisplayName("the camelCase spelling of an option is refused rather than ignored")
    void anOptionSpelledTheWayTheDocumentStoreSpellsItIsRefused() {
        DslException ex = refused("""
                path: items
                includeArrayIndex: item_no
                """);

        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(ex.path()).isEqualTo("transforms[0].includeArrayIndex");
    }

    /**
     * A payload key that belongs to another type is refused here too. The step-level keys every type
     * shares stay allowed, so this is checking the boundary between the two sets rather than that
     * the parser refuses anything unfamiliar.
     */
    @Test
    @DisplayName("a type name outside the vocabulary is refused, not read as leaving it out")
    void aMisspelledElementTypeIsRefused() {
        // Leaving element_type out is a real answer - it hands the choice to the target, the same
        // way any column nobody resolved a type for does - so a misspelling has to be refused
        // rather than folded into that answer. Read as "unresolved", `strng` is indistinguishable
        // from the omission the author never made, and the column silently becomes whatever the
        // target would have guessed anyway.
        DslException ex = refused("""
                path: items
                element_key: sku
                element_type: strng
                """);

        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("transforms[0].element_type");
        assertThat(ex.args()).containsEntry("value", "strng");
    }

    @Test
    @DisplayName("a payload key belonging to another transform type is refused on an unwind")
    void anotherTypesPayloadKeyIsNotAllowedHere() {
        DslException ex = refused("""
                path: items
                expr: "op != 'd'"
                """);

        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(ex.path()).isEqualTo("transforms[0].expr");
    }
}
