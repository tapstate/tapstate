package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

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

    @Test
    @DisplayName("the smallest unwind - a path and nothing else - loads clean")
    void aPathIsTheWholeOfTheRequiredPayload() {
        assertThatCode(() -> batch("path: items")).doesNotThrowAnyException();
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
