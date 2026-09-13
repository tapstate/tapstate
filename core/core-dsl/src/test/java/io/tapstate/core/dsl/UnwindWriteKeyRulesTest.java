package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What an unwind has to say about the rows it makes, judged before anything runs.
 *
 * <p>Expanding a row is the first thing in this grammar that changes how many rows there are, and
 * the target still has to tell them apart. The parent's own key cannot: it is the same value on
 * every row the expansion produces, so an upsert matches all of them to one row and the last one
 * written is the only one that survives. That outcome is indistinguishable from the step having no
 * effect at all - the pipeline is green, the target holds rows, and nothing anywhere counts what
 * was overwritten - which is why the declaration is refused here rather than left to be discovered
 * from a row count somebody happens to check.
 *
 * <p>The other half is the write mode. An append target never matches a write to an existing row,
 * so deleting a parent appends its old elements again instead of removing the rows it produced.
 * Nothing about the expansion is wrong there; the combination simply cannot converge, so it is
 * refused rather than promised.
 *
 * <p><b>Both assertions are on the code, never on "it threw".</b> These two refusals sit next to a
 * dozen others that a malformed pipeline can raise first - a bad reference, an unknown field, a
 * missing mode - and any of those would satisfy a test that only asked whether something was
 * thrown, while saying nothing about the rule under test.
 */
class UnwindWriteKeyRulesTest {

    private static final String SOURCES = """
            version: tapstate/v1
            kind: source
            id: src_a
            connector: mysql
            mode: cdc
            tables: [ orders ]
            """;

    /** A pipeline whose one step is an unwind carrying {@code payload}, served by {@code sync}. */
    private static String pipeline(String payload, String sync) {
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
                  sync: [ %s ]
                """.formatted(payload.indent(4).stripTrailing(), sync);
    }

    private static final String UPSERT = "{ id: s, source: src_a }";
    private static final String APPEND = "{ id: s, source: src_a, write_mode: append }";

    private static void batch(String payload, String sync) {
        DslParser parser = new DslParser();
        Workspace.of(Stream.of(SOURCES, pipeline(payload, sync)).map(parser::parse).toList());
    }

    private static DslException refused(String payload, String sync) {
        Throwable thrown = catchThrowable(() -> batch(payload, sync));
        assertThat(thrown).isInstanceOf(DslException.class);
        return (DslException) thrown;
    }

    // ---- the element must be locatable -------------------------------------------------

    @Test
    @DisplayName("an unwind naming nothing that varies per element is refused before it runs")
    void anUnwindWithNoElementLocatorIsRefused() {
        DslException refusal = refused("path: items", UPSERT);

        assertThat(refusal.code()).isEqualTo(DslError.UNWIND_NEEDS_AN_ELEMENT_KEY);
        assertThat(refusal.path()).isEqualTo("transforms[0]");
        assertThat(refusal.args()).containsEntry("step", "explode");
    }

    @Test
    @DisplayName("a field inside the element identifies the row it becomes")
    void anElementKeyLocatesTheRow() {
        assertThatCode(() -> batch("""
                path: items
                element_key: sku
                """, UPSERT)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("the element's ordinal within the array locates the row just as well")
    void anArrayIndexLocatesTheRow() {
        assertThatCode(() -> batch("""
                path: items
                include_array_index: item_no
                """, UPSERT)).doesNotThrowAnyException();
    }

    /**
     * Both given is a legal declaration, not a conflict: the element's own field is the key and the
     * ordinal is carried as an ordinary column, which is how an author asks to keep the array's
     * order without keying on a position that shifts whenever an element is inserted.
     */
    @Test
    @DisplayName("declaring both keeps the element's own field as the key")
    void bothKeysDeclaredIsLegal() {
        assertThatCode(() -> batch("""
                path: items
                element_key: sku
                include_array_index: item_no
                """, UPSERT)).doesNotThrowAnyException();
    }

    /**
     * The rule reads the step, so a pipeline with no unwind in it is not judged. Without this the
     * refusal would land on every keyless upsert in the product, which is a different rule with a
     * different verdict - it needs a discovered model to know whether the table has a key at all.
     */
    @Test
    @DisplayName("a pipeline with no unwind in it is not judged by this rule")
    void aPipelineWithoutAnUnwindIsUntouched() {
        String noUnwind = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: keep
                    from: [orders]
                    type: filter
                    expr: "true"
                serve:
                  from: keep
                  sync: [ { id: s, source: src_a } ]
                """;
        DslParser parser = new DslParser();
        assertThatCode(() -> Workspace.of(Stream.of(SOURCES, noUnwind).map(parser::parse).toList()))
                .doesNotThrowAnyException();
    }

    // ---- the target has to be able to converge -----------------------------------------

    @Test
    @DisplayName("an unwind writing an append-only target is refused")
    void anUnwindWritingAnAppendTargetIsRefused() {
        DslException refusal = refused("""
                path: items
                element_key: sku
                """, APPEND);

        assertThat(refusal.code()).isEqualTo(DslError.UNWIND_NEEDS_AN_UPSERT_TARGET);
        assertThat(refusal.path()).isEqualTo("serve.sync[0].write_mode");
        assertThat(refusal.args())
                .containsEntry("step", "explode")
                .containsEntry("sync", "s");
    }

    /**
     * Appending is an ordinary thing to do; it is only the combination that cannot converge. A rule
     * that refused append outright would pass this suite and break every append pipeline shipped.
     */
    @Test
    @DisplayName("appending without an unwind is left alone")
    void appendingWithoutAnUnwindIsLeftAlone() {
        String noUnwind = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: keep
                    from: [orders]
                    type: filter
                    expr: "true"
                serve:
                  from: keep
                  sync: [ { id: s, source: src_a, write_mode: append } ]
                """;
        DslParser parser = new DslParser();
        assertThatCode(() -> Workspace.of(Stream.of(SOURCES, noUnwind).map(parser::parse).toList()))
                .doesNotThrowAnyException();
    }

    /**
     * An unwind on a branch the serve block never reads writes nothing, so it converges nowhere and
     * there is nothing to refuse. Judging it would refuse a pipeline on the strength of a step whose
     * rows never arrive at the append target at all.
     */
    @Test
    @DisplayName("an unwind the serve block does not read does not make the target's mode illegal")
    void anUnwindOffThePathIsNotJudgedAgainstTheTarget() {
        String offPath = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - id: keep
                    from: [orders]
                    type: filter
                    expr: "true"
                  - id: explode
                    from: [orders]
                    type: unwind
                    path: items
                    element_key: sku
                serve:
                  from: keep
                  sync: [ { id: s, source: src_a, write_mode: append } ]
                """;
        DslParser parser = new DslParser();
        assertThatCode(() -> Workspace.of(Stream.of(SOURCES, offPath).map(parser::parse).toList()))
                .doesNotThrowAnyException();
    }
}
