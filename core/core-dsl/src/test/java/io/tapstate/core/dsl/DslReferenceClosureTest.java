package io.tapstate.core.dsl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * B3-6: batch reference closure. Offline, the closure is the batch: every
 * reference inside a pipeline must resolve within the loaded directory. This test drives the
 * directory loader ({@link WorkspaceLoader}) — the realistic entry that parses every
 * {@code *.tap.yml}, attributes parse errors to their file, and runs closure validation.
 *
 * <p>The acceptance bar is twofold: (a) every §14 valid scenario loads clean — the
 * real false-positive guard, since the valid corpus exercises the full addressing surface
 * (bare table / step id / view id / regex / source-id list / nest+join alias maps / use
 * references); (b) the four B3-6 invalid cases raise the right rule at the right path.
 */
class DslReferenceClosureTest {

    private static final Path CORPUS = Path.of("src", "test", "resources", "corpus");
    private static final Path VALID = CORPUS.resolve("valid");
    private static final Path INVALID = CORPUS.resolve("invalid");

    // ---- (a) the false-positive guard: every valid scenario loads clean ----------------

    @ParameterizedTest(name = "valid/{0} loads clean")
    @MethodSource("validScenarios")
    @DisplayName("every §14 valid workspace passes closure with no false positive")
    void validScenarioLoadsClean(String dir) {
        assertThatCode(() -> WorkspaceLoader.load(VALID.resolve(dir))).doesNotThrowAnyException();
    }

    static Stream<String> validScenarios() throws IOException {
        try (Stream<Path> list = Files.list(VALID)) {
            return list.filter(Files::isDirectory).map(p -> p.getFileName().toString()).sorted().toList().stream();
        }
    }

    // ---- (b) the four B3-6 invalid cases: right rule, right path ------------------------

    @ParameterizedTest(name = "invalid/{0} -> {1}")
    @MethodSource("closureViolations")
    @DisplayName("each closure violation raises its expected rule at its expected path")
    void closureViolationRaisesExpectedRule(String dir, String expectedRule, String expectedPath) {
        Throwable thrown = catchThrowable(() -> WorkspaceLoader.load(INVALID.resolve(dir)));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code().symbol()).isEqualTo(expectedRule);
        assertThat(ex.path()).isEqualTo(expectedPath);
    }

    /** The batch-validation subset of invalid/ (closure + composition + internal-id uniqueness); rule/path read from expected.yml. */
    static Stream<org.junit.jupiter.params.provider.Arguments> closureViolations() {
        return Stream.of("s02-missing-source-ref", "s03-unknown-step-ref",
                        "s07-pipeline-no-output", "s08-ambiguous-table-ref",
                        "g03-duplicate-pipeline-internal-id",
                        "g04-step-id-shadows-table", "g05-step-id-shadows-source")
                .map(dir -> {
                    Map<?, ?> expected = expectedSidecar(INVALID.resolve(dir));
                    return org.junit.jupiter.params.provider.Arguments.of(
                            dir, expected.get("rule"), String.valueOf(expected.get("path")));
                });
    }

    // ---- directory loading: parse errors carry their source file -----------------------

    @Test
    @DisplayName("a per-file parse error is attributed to its source filename")
    void parseErrorCarriesSourceFile() {
        // s01 is a parse-layer violation (mode typo); loading the directory must surface it
        // with the offending file attached — the B3-6 attribution deliverable.
        Throwable thrown = catchThrowable(() -> WorkspaceLoader.load(INVALID.resolve("s01-unknown-field-mode-typo")));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(ex.source()).isEqualTo("src_typo.tap.yml");
    }

    // ---- branch coverage the corpus does not witness -----------------------------------

    @Test
    @DisplayName("an open-universe source (tables omitted) cannot prove a bare table missing")
    void openUniverseAcceptsBareTable() {
        // tables omitted = the whole source (§4, X9): offline we cannot enumerate it,
        // so a bare from-token that is neither a step id nor a known literal is accepted, not
        // flagged missing (deferred to connect-time validation).
        String src = """
                version: tapstate/v1
                kind: source
                id: src_open
                connector: mysql
                mode: cdc
                """;
        String pipe = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_open
                transforms:
                  - { id: f, from: [some_unlisted_table], type: filter, expr: "op != 'd'" }
                serve:
                  from: f
                  sync: [ { id: s, source: src_open } ]
                """;

        assertThatCode(() -> batch(src, pipe)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a pipeline cannot reference a table outside its Source selection")
    void selectedSourceRejectsAnUnselectedTableReference() {
        String source = source("src_orders", "mysql", "orders");
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_orders
                transforms:
                  - { id: f, from: [payments], type: filter, expr: "op != 'd'" }
                serve:
                  from: f
                  sync: [ { id: s, source: src_orders } ]
                """;

        Throwable thrown = catchThrowable(() -> batch(source, pipeline));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException error = (DslException) thrown;
        assertThat(error.code()).isEqualTo(DslError.MISSING_REFERENCE);
        assertThat(error.path()).isEqualTo("transforms[0].from");
    }

    @Test
    @DisplayName("a <source_id>.<table> prefix resolves the otherwise-ambiguous bare name")
    void qualifiedPrefixDisambiguates() {
        // Same collision as invalid/s08, but the from-token carries the disambiguating prefix
        // (§4); it must resolve, proving the qualified addressing path is correct.
        String crm = source("src_crm", "mysql", "customers");
        String erp = source("src_erp", "postgres", "customers");
        String pipe = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: [ src_crm, src_erp ]
                transforms:
                  - { id: f, from: [src_crm.customers], type: filter, expr: "op != 'd'" }
                view:
                  id: v
                  from: f
                  primary_key: id
                  storage: { warm: { collection: v } }
                """;

        assertThatCode(() -> batch(crm, erp, pipe)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a sync element referencing an absent connection source is missing-reference")
    void missingSyncSourceIsMissingReference() {
        String src = source("src_a", "mysql", "orders");
        String pipe = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                serve:
                  from: orders
                  sync: [ { id: s, source: tgt_absent } ]
                """;

        Throwable thrown = catchThrowable(() -> batch(src, pipe));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.MISSING_REFERENCE);
        assertThat(ex.path()).isEqualTo("serve.sync[0].source");
    }

    @Test
    @DisplayName("a use: reference with no matching definition body is missing-reference")
    void missingUseReferenceIsMissingReference() {
        String src = source("src_a", "mysql", "orders");
        String pipe = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - { id: f, from: [orders], type: filter, expr: "op != 'd'" }
                  - { use: no_such_transform, from: [f] }
                serve:
                  from: no_such_transform
                  sync: [ { id: s, source: src_a } ]
                """;

        Throwable thrown = catchThrowable(() -> batch(src, pipe));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.MISSING_REFERENCE);
        assertThat(ex.path()).isEqualTo("transforms[1].use");
    }

    @Test
    @DisplayName("a source id repeated in the source list is one source, not a false ambiguity")
    void duplicateSourceIdInListIsNotFalseAmbiguity() {
        // The universe is a set of sources: listing src_a twice must not make a bare table in it
        // look like it collides across two sources. (Rejecting the duplicate itself is out of scope.)
        String src = source("src_a", "mysql", "orders");
        String pipe = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: [ src_a, src_a ]
                transforms:
                  - { id: f, from: [orders], type: filter, expr: "op != 'd'" }
                serve:
                  from: f
                  sync: [ { id: s, source: src_a } ]
                """;

        assertThatCode(() -> batch(src, pipe)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("no-shadowing covers generated step ids too: an anonymous filter_1 over a table filter_1")
    void generatedStepIdShadowingATableIsRejected() {
        // The anonymous filter generates id filter_1 (canonical-form §5); src_a happens to declare a
        // table named filter_1, so the generated id shadows it — caught the same as a declared clash,
        // proving no-shadowing runs on post-generation ids (§5).
        String src = source("src_a", "mysql", "filter_1");
        String pipe = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                transforms:
                  - { from: [filter_1], type: filter, expr: "op != 'd'" }
                serve:
                  from: filter_1
                  sync: [ { id: s, source: src_a } ]
                """;

        Throwable thrown = catchThrowable(() -> batch(src, pipe));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.DUPLICATE_ID);
        assertThat(ex.path()).isEqualTo("transforms[0].id");
    }

    @Test
    @DisplayName("the loader reads only regular files, not directories that happen to end .tap.yml")
    void loaderIgnoresDirectoriesNamedLikeArtifacts(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("src.tap.yml"), source("src_only", "mysql", "orders"));
        Files.createDirectory(dir.resolve("weird.tap.yml"));   // a directory, not an artifact to read

        assertThatCode(() -> WorkspaceLoader.load(dir)).doesNotThrowAnyException();
    }

    // ---- helpers -----------------------------------------------------------------------

    private static void batch(String... yamls) {
        DslParser parser = new DslParser();
        Workspace.of(Stream.of(yamls).map(parser::parse).toList());
    }

    private static String source(String id, String connector, String table) {
        return "version: tapstate/v1\nkind: source\nid: " + id
                + "\nconnector: " + connector + "\nmode: cdc\ntables: [ " + table + " ]\n";
    }

    private static Map<?, ?> expectedSidecar(Path caseDir) {
        try (InputStream in = Files.newInputStream(caseDir.resolve("expected.yml"))) {
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            return (Map<?, ?>) new Yaml(new SafeConstructor(options)).load(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
