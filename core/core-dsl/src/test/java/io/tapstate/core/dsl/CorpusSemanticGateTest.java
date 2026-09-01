package io.tapstate.core.dsl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The B3-7 semantic acceptance gate: the whole ADR-0016 §14 corpus driven end to end through
 * parse + validate (the offline half). It complements {@code CorpusSmokeTest} (the structural
 * gate) by asserting DSL <em>semantics</em>:
 * <ul>
 *   <li>every valid scenario loads clean — the real false-positive guard;</li>
 *   <li>every invalid scenario is rejected with the corpus-declared {@link DslError} code at the
 *       declared field path;</li>
 *   <li>the thrown named arguments exactly satisfy that code's placeholder contract (ADR-0024
 *       D5-4, the runtime half the build gate cannot link statically);</li>
 *   <li>every {@link DslError} is witnessed by at least one case — the corpus rule vocabulary
 *       (corpus/README.md) maps 1:1 to the enum.</li>
 * </ul>
 */
class CorpusSemanticGateTest {

    private static final Path CORPUS = Path.of("src", "test", "resources", "corpus");
    private static final Path VALID = CORPUS.resolve("valid");
    private static final Path INVALID = CORPUS.resolve("invalid");

    @ParameterizedTest(name = "valid/{0} loads clean")
    @MethodSource("validScenarios")
    @DisplayName("every valid scenario parses + validates with no false positive")
    void validScenarioLoadsClean(String dir) {
        assertThatCode(() -> WorkspaceLoader.load(VALID.resolve(dir))).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "invalid/{0} -> {1} @ {2}")
    @MethodSource("invalidCases")
    @DisplayName("every invalid scenario is rejected with the declared code at the declared path")
    void invalidScenarioRejected(String dir, String rule, String path) {
        Throwable thrown = catchThrowable(() -> WorkspaceLoader.load(INVALID.resolve(dir)));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.ofSymbol(rule));
        assertThat(ex.path()).isEqualTo(path);
        // ADR-0024 D5-4 (runtime half): the thrown args exactly cover the code's placeholder contract
        assertThat(ex.args().keySet()).containsExactlyInAnyOrderElementsOf(ex.code().placeholders());
    }

    @Test
    @DisplayName("every semantic DslError code is witnessed by an invalid scenario (vocabulary <-> enum 1:1)")
    void everyCodeIsWitnessed() {
        Set<DslError> witnessed = EnumSet.noneOf(DslError.class);
        invalidCases().forEach(a -> witnessed.add(DslError.ofSymbol((String) a.get()[1])));
        // A corpus artifact witnesses a code by being that code's whole cause. Where it cannot be,
        // the code is exempt here and proven by a direct test instead. Two reasons, and the exemption
        // holds only for as long as its reason does:
        //
        // pre-semantic - the code fires before there is an artifact to serve as the witness.
        //   MALFORMED_YAML: a syntax error cannot be a well-formed corpus artifact (DslMalformedYamlTest).
        //   UNDEFINED_VARIABLE / MALFORMED_INTERPOLATION: interpolation runs on raw text before the
        //   parse, and whether a reference resolves depends on the environment rather than on the
        //   document, so a corpus that tried would pass or fail by ambient state (InterpolatorTest).
        //
        // post-semantic - the artifact is well-formed and complete, but the code needs knowledge that
        // lives outside it.
        //   CONFIG_REQUIRED: the verdict depends on the live connector catalog, which the offline
        //   WorkspaceLoader this corpus exercises does not consult.
        //   ROW_EXPRESSION_NEEDS_DISCOVERY / ROW_EXPRESSION_TYPE_UNSUPPORTED /
        //   ROW_EXPRESSION_TYPE_UNKNOWN: the verdict depends on whether a source has been discovered
        //   and on the column types that discovery resolved, neither of which a document declares nor
        //   an offline check can reach (RowExpressionDiscoveryRulesTest / RowExpressionTypeRulesTest).
        //   UPSERT_NEEDS_KEY: whether a table has a key is a property of the table, which only a
        //   discovered model carries - a document names the table but cannot say what it declares
        //   (WriteKeyRulesTest).
        //
        // inexpressible - the corpus cannot hold the document that would witness it.
        //   UNSUPPORTED_VERSION: CorpusSmokeTest requires every artifact here to declare the
        //   supported version, so an artifact declaring another one cannot exist in this corpus. That
        //   requirement is worth more than the witness -- it is what stops a fixture drifting onto a
        //   stale grammar unnoticed -- so the code is proven directly instead (DslParserTest, which
        //   covers an unreadable version, an absent one, and that a supported version leaves an
        //   unknown kind reported as a kind problem).
        Set<DslError> requiresCorpusWitness = EnumSet.complementOf(EnumSet.of(
                DslError.MALFORMED_YAML, DslError.UNDEFINED_VARIABLE, DslError.MALFORMED_INTERPOLATION,
                DslError.CONFIG_REQUIRED,
                DslError.ROW_EXPRESSION_NEEDS_DISCOVERY, DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED,
                DslError.ROW_EXPRESSION_TYPE_UNKNOWN, DslError.UPSERT_NEEDS_KEY,
                DslError.UNSUPPORTED_VERSION));
        assertThat(witnessed).containsExactlyInAnyOrderElementsOf(requiresCorpusWitness);
    }

    // ---- corpus enumeration ------------------------------------------------------------

    static Stream<String> validScenarios() throws IOException {
        return scenarioDirs(VALID);
    }

    static Stream<Arguments> invalidCases() {
        try {
            return scenarioDirs(INVALID)
                    .map(dir -> {
                        Map<?, ?> expected = sidecar(INVALID.resolve(dir));
                        return Arguments.of(dir, String.valueOf(expected.get("rule")),
                                String.valueOf(expected.get("path")));
                    })
                    .toList().stream();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Stream<String> scenarioDirs(Path root) throws IOException {
        try (Stream<Path> list = Files.list(root)) {
            return list.filter(Files::isDirectory).map(p -> p.getFileName().toString())
                    .sorted().toList().stream();
        }
    }

    private static Map<?, ?> sidecar(Path caseDir) {
        try (InputStream in = Files.newInputStream(caseDir.resolve("expected.yml"))) {
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            return (Map<?, ?>) new Yaml(new SafeConstructor(options)).load(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
