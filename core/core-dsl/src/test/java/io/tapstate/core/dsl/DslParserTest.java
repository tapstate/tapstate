package io.tapstate.core.dsl;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Parse layer (plan poc1 B3): YAML text -> resource model. The inverse of the canonical
 * writer — parse accepts any legal YAML style (block / flow / any key order / sugar),
 * the model is the post-normalization form (canonical-form.md §1).
 */
class DslParserTest {

    private final DslParser parser = new DslParser();

    @Test
    void parsesMinimalSource() {
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src_ora
                connector: oracle
                config: { host: 10.20.0.15, port: 1521, service_name: ORCL }
                mode: cdc
                tables: [ ORDERS, ORDER_ITEMS, CUSTOMERS ]
                """;

        Resource r = parser.parse(yaml);

        assertThat(r).isInstanceOf(SourceResource.class);
        SourceResource s = (SourceResource) r;
        assertThat(s.id()).isEqualTo("src_ora");
        assertThat(s.connector()).isEqualTo("oracle");
        assertThat(s.mode()).isEqualTo(SourceMode.CDC);
        assertThat(s.tables()).containsExactly(
                TableRef.literal("ORDERS"), TableRef.literal("ORDER_ITEMS"), TableRef.literal("CUSTOMERS"));
        assertThat(s.config())
                .containsEntry("host", "10.20.0.15")
                .containsEntry("port", 1521)
                .containsEntry("service_name", "ORCL");
    }

    @Test
    void rejectsRelocatedSourceReadOption() {
        // read_mode / start_from moved to pipeline settings; the old source-level option names are
        // rejected as unknown fields rather than silently passed through the free options map.
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src_ora
                connector: oracle
                config: { host: 10.20.0.15 }
                mode: cdc
                tables: [ ORDERS ]
                options: { snapshot_mode: initial, include_ddl: true }
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(ex.path()).isEqualTo("options.snapshot_mode");
        assertThat(ex.args()).containsEntry("field", "snapshot_mode");
    }

    @Test
    void parsesADocumentDeclaringTheSupportedVersion() {
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src_ora
                connector: oracle
                config: { host: 10.20.0.15 }
                """;

        assertThat(parser.parse(yaml)).isInstanceOf(SourceResource.class);
    }

    @Test
    void refusesAVersionItDoesNotSupport() {
        // version selects the grammar, so a version this build does not know cannot be parsed by
        // guessing. Until now it was accepted and the document was rewritten as v1 on the way out.
        String yaml = """
                version: tapstate/v2
                kind: source
                id: src_ora
                connector: oracle
                config: { host: 10.20.0.15 }
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNSUPPORTED_VERSION);
        assertThat(ex.args()).containsEntry("got", "tapstate/v2").containsEntry("supported", "tapstate/v1");
    }

    @Test
    void refusesADocumentWithNoVersionUnderTheSameCode() {
        // Same code, different argument: to whoever wrote the document these are one situation --
        // it does not say which grammar it is written in -- so they get one code to look up.
        String yaml = """
                kind: source
                id: src_ora
                connector: oracle
                config: { host: 10.20.0.15 }
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNSUPPORTED_VERSION);
        assertThat(ex.args()).containsEntry("got", "(absent)");
    }

    @Test
    void reportsAnUnknownKindAsAKindProblemWhenTheVersionIsFine() {
        // A supported version and an unknown kind is a kind problem, not a version one. Worth its own
        // case because the version check runs first: were it to report on anything it did not like,
        // every malformed document would come back as a version error.
        String yaml = """
                version: tapstate/v1
                kind: sink
                id: x
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("kind");
    }

    @Test
    void reportsTheVersionFirstWhenBothTheVersionAndTheKindAreWrong() {
        // The version decides which grammar reads the rest, so nothing downstream means anything
        // until it is settled -- including what counts as a kind.
        String yaml = """
                version: tapstate/v2
                kind: sink
                id: x
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(((DslException) t).code()).isEqualTo(DslError.UNSUPPORTED_VERSION);
    }

    @Test
    void rejectsEverySourceOptionKey() {
        // options is the engine's own configuration, so its keys are a closed vocabulary owned by
        // the product - not a free map like config, whose keys belong to the connector. No engine
        // option exists yet: every key that was here had no reader anywhere in the product, so the
        // vocabulary is empty and any key is unknown. A key that does nothing must say so rather
        // than be accepted in silence.
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src_ora
                connector: oracle
                config: { host: 10.20.0.15 }
                mode: cdc
                tables: [ ORDERS ]
                options: { include_ddl: true }
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(ex.path()).isEqualTo("options.include_ddl");
        assertThat(ex.args()).containsEntry("field", "include_ddl");
    }

    @Test
    void rejectsRelocatedStartFromOption() {
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src_kfk
                connector: kafka
                config: { brokers: k1:9092 }
                mode: stream
                tables: [ orders_topic ]
                options: { start_from: earliest }
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(ex.path()).isEqualTo("options.start_from");
    }

    @Test
    void parsesSrsEnabledFalse() {
        // read amendment: srs.enabled: false is the SRS off switch (default true).
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src
                connector: mysql
                mode: cdc
                tables: [ orders ]
                srs: { enabled: false }
                """;

        SourceResource s = (SourceResource) parser.parse(yaml);

        assertThat(s.srs().enabled()).isFalse();
    }

    @Test
    void rejectsUnknownTopLevelField() {
        // mirrors corpus invalid/s01: `mod` is a typo of `mode`; §11.5 rejects, never ignores
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src_typo
                connector: mysql
                config: { host: 10.0.0.1 }
                mod: cdc
                tables: [ orders ]
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(ex.path()).isEqualTo("mod");
        assertThat(ex.line()).isEqualTo(6);
    }

    @Test
    void rejectsUnknownKind() {
        // A top-level kind outside the closed set is a coded value error, not a bare crash.
        String yaml = """
                version: tapstate/v1
                kind: bogus
                id: x
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("kind");
        assertThat(ex.args()).containsEntry("value", "bogus");
    }

    @Test
    void rejectsMissingKind() {
        // No kind: at all is likewise a coded value error, not an UnsupportedOperationException.
        String yaml = """
                version: tapstate/v1
                id: x
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("kind");
    }

    @Test
    void bindsAStoredDocumentWithoutParsingAnyText() {
        // What a document store hands back: plain maps and already-typed scalars, no text anywhere.
        // The port stays an int through the binding, which is the half a text round trip cannot show --
        // there every scalar arrives as characters and the type is re-guessed from them.
        Map<String, Object> stored = new LinkedHashMap<>();
        stored.put("version", "tapstate/v1");
        stored.put("kind", "source");
        stored.put("id", "src_ora");
        stored.put("connector", "oracle");
        stored.put("config", new LinkedHashMap<>(Map.of("port", 1521)));

        Resource r = parser.fromTree(stored);

        assertThat(r).isInstanceOf(SourceResource.class);
        assertThat(r.id()).isEqualTo("src_ora");
        assertThat(((SourceResource) r).config()).containsEntry("port", 1521);
    }

    @Test
    void refusesAStoredDocumentDeclaringAGrammarThisBuildCannotRead() {
        // Reading a stored document skips the text, not the grammar gate. A document a later build
        // wrote is refused whole, rather than bound field by field under a grammar it does not claim --
        // which would read some fields correctly and quietly drop the ones that moved.
        Map<String, Object> stored = new LinkedHashMap<>();
        stored.put("version", "tapstate/v2");
        stored.put("kind", "source");
        stored.put("id", "src_ora");
        stored.put("connector", "oracle");

        Throwable t = catchThrowable(() -> parser.fromTree(stored));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNSUPPORTED_VERSION);
        assertThat(ex.args()).containsEntry("got", "tapstate/v2");
    }

    @Test
    void namesTheFieldWhenAStoredDocumentHasTheWrongShapeThere() {
        // A stored document has no line to point at, so the field path is the whole of the position.
        Map<String, Object> stored = new LinkedHashMap<>();
        stored.put("version", "tapstate/v1");
        stored.put("kind", "source");
        stored.put("id", "src_ora");
        stored.put("connector", "oracle");
        stored.put("srs", List.of("not", "a", "mapping"));

        Throwable t = catchThrowable(() -> parser.fromTree(stored));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.path()).isEqualTo("srs");
        assertThat(ex.line()).isZero();
    }
}
