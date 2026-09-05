package io.tapstate.core.dsl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A field the model declares required and the parser cannot supply is refused with a code, at the
 * position the block starts. Before this the model's own {@code Objects.requireNonNull} was the only
 * thing standing there, so an author who left a field out got a {@code NullPointerException} with a
 * stack trace, no file, no line and no code — the shape reserved for a defect on this side.
 *
 * <p>Required here means required <em>in the document</em>, which is narrower than the model's
 * non-null contract: an id the parser generates and a {@code from:} it derives from natural order are
 * both non-null in the model and both legitimately absent from the YAML.
 */
class DslRequiredFieldTest {

    private final DslParser parser = new DslParser();

    private static DslException refusal(Throwable thrown) {
        assertThat(thrown).isInstanceOf(DslException.class);
        return (DslException) thrown;
    }

    @ParameterizedTest(name = "{0} -> missing-field at {1}")
    @DisplayName("a required field left out is refused with its code, field and path")
    @CsvSource(delimiter = '|', textBlock = """
            source-without-connector | connector          | connector
            source-without-id        | id                 | id
            sync-without-source      | source             | serve.sync[0].source
            push-without-source      | source             | serve.push[0].source
            query-without-type       | type               | serve.query[0].type
            table-spec-without-name  | name               | tables[0].name
            filter-without-expr      | expr               | transforms[0].expr
            js-without-script        | script             | transforms[0].script
            join-without-sql         | sql                | transforms[0].sql
            nest-without-root        | root               | transforms[0].root
            inline-view-without-key  | primary_key        | view.primary_key
            view-def-without-key     | primary_key        | primary_key
            """)
    void aRequiredFieldLeftOutIsRefused(String scenario, String field, String path) {
        Throwable thrown = catchThrowable(() -> parser.parse(document(scenario)));

        DslException ex = refusal(thrown);
        assertThat(ex.code()).isEqualTo(DslError.MISSING_FIELD);
        assertThat(ex.args()).containsEntry("field", field);
        assertThat(ex.path()).isEqualTo(path);
    }

    @Test
    @DisplayName("the refusal is located, so an author is told where to write the field")
    void theRefusalCarriesTheBlocksPosition() {
        // A missing key has no node of its own to point at, so the position is the block that should
        // have carried it. Reported at 0:0 the diagnostic names a field and no place to put it, which
        // in a directory of artifacts is most of the answer missing.
        DslException ex = refusal(catchThrowable(() -> parser.parse(document("sync-without-source"))));

        assertThat(ex.line()).isPositive();
        assertThat(ex.column()).isPositive();
    }

    @Test
    @DisplayName("a field the parser supplies itself is not required in the document")
    void aParserSuppliedFieldIsNotRequiredInTheDocument() {
        // Three fields required in the model are absent here: the sync element's id (generated as
        // sync_1), the view block's id (generated as view), and the view's from: (taken from natural
        // order). A check derived from the model's non-null contract alone would refuse this document,
        // and it is the valid corpus's most ordinary shape.
        assertThatCode(() -> parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms: [ { type: filter, from: t1, expr: "true" } ]
                view: { primary_key: k }
                serve: { sync: [ { source: tgt } ] }
                """)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("an unknown field is still an unknown field, not a missing one")
    void anUnknownKeyIsUnaffected() {
        // The two checks sit at the same place and read the same map. Closing the keys must still come
        // first: a typo'd 'mod' is a key the author wrote, and reporting it as an absent 'mode' would
        // send them looking for the wrong thing.
        DslException ex = refusal(catchThrowable(() -> parser.parse("""
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                mod: cdc
                """)));

        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
    }

    private static String document(String scenario) {
        return switch (scenario) {
            case "source-without-connector" -> """
                    version: tapstate/v1
                    kind: source
                    id: src_a
                    config: { host: 10.0.0.1 }
                    mode: cdc
                    """;
            case "source-without-id" -> """
                    version: tapstate/v1
                    kind: source
                    connector: mysql
                    mode: cdc
                    """;
            case "sync-without-source" -> pipeline("""
                    serve: { from: t1, sync: [ { write_mode: append } ] }
                    """);
            case "push-without-source" -> pipeline("""
                    serve: { from: t1, push: [ { topic: orders } ] }
                    """);
            case "query-without-type" -> pipeline("""
                    serve: { from: t1, query: [ { backend: sync_1 } ] }
                    """);
            case "table-spec-without-name" -> """
                    version: tapstate/v1
                    kind: source
                    id: src_a
                    connector: mysql
                    mode: cdc
                    tables: [ { filter: "amount > 0" } ]
                    """;
            case "filter-without-expr" -> pipeline("""
                    transforms: [ { type: filter, from: t1 } ]
                    serve: { from: t1, sync: [ { source: tgt } ] }
                    """);
            case "js-without-script" -> pipeline("""
                    transforms: [ { type: js, from: t1 } ]
                    serve: { from: t1, sync: [ { source: tgt } ] }
                    """);
            case "join-without-sql" -> pipeline("""
                    transforms: [ { type: join, from: { a: t1 }, engine: duckdb } ]
                    serve: { from: t1, sync: [ { source: tgt } ] }
                    """);
            case "nest-without-root" -> pipeline("""
                    transforms: [ { type: nest, from: { a: t1 }, primary_key: k } ]
                    serve: { from: t1, sync: [ { source: tgt } ] }
                    """);
            case "inline-view-without-key" -> pipeline("""
                    view: { id: v_a, from: t1 }
                    """);
            case "view-def-without-key" -> """
                    version: tapstate/v1
                    kind: view
                    id: v_a
                    """;
            default -> throw new IllegalArgumentException("no such scenario: " + scenario);
        };
    }

    private static String pipeline(String body) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                """ + body;
    }
}
