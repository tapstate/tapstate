package io.tapstate.core.dsl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Corpus hygiene gate (plan poc1 B1). The corpus under src/test/resources/corpus is the
 * acceptance baseline for B2-B5; this test only guards its structural contract, it does
 * NOT validate DSL semantics (that is the validate engine built in B3 against this corpus).
 *
 * Contract (documented in corpus/README.md):
 * - valid/ holds one workspace directory per ADR-0016 §14 scenario (s01..s11),
 *   reference-closed within the directory, no expectation sidecars.
 * - invalid/ holds minimal self-contained batches, each with exactly one expected.yml
 *   declaring the violated rule from a fixed vocabulary.
 * - every *.tap.yml is a single-document YAML map with version/kind/id, no duplicate keys.
 */
class CorpusSmokeTest {

    private static final Path CORPUS = Path.of("src", "test", "resources", "corpus");
    private static final Path VALID = CORPUS.resolve("valid");
    private static final Path INVALID = CORPUS.resolve("invalid");

    /** One directory prefix per ADR-0016 §14 scenario (14.1 -> s01 ... 14.11 -> s11). */
    private static final List<String> SCENARIO_PREFIXES = List.of(
            "s01-", "s02-", "s03-", "s04-", "s05-", "s06-",
            "s07-", "s08-", "s09-", "s10-", "s11-");

    /** Violation vocabulary; expected.yml 'rule' must use one of these. */
    private static final Set<String> RULES = Set.of(
            "unknown-field",        // §11.5 strict rejection of fields outside the schema
            "forbidden-field",      // field known to the schema but banned in this position (X18/X19)
            "missing-field",        // required field the parser cannot supply, left out of the document
            "missing-reference",    // batch-closure reference to a nonexistent id / table / step
            "ambiguous-reference",  // bare table name colliding across sources, no id prefix (§4)
            "mode-mismatch",        // option / block illegal for the source mode or boundedness (§4/X7/X10)
            "mode-required-for-read", // a source a pipeline reads declares no mode (X18)
            "illegal-value",        // enum / format constraint violation (§2 id charset, §8 enums)
            "illegal-expression",   // CEL expression field fails to compile or type-check (§12)
            "composition",          // structural rule on resource composition (X17 minimal pipeline)
            "duplicate-id",         // id collision: top-level / pipeline-internal uniqueness, or step-id shadowing (§2/F8, §5)
            "unsupported-mode",     // source mode outside the connector's capability matrix (§4 / C3)
            "config-type-mismatch", // connector config value of the wrong declared type (C3)
            "invalid-config-value");// connector config value outside the declared enum choices (C3)

    private static final Set<String> KINDS = Set.of("source", "pipeline", "transform", "view", "serve");

    private static Yaml yaml;

    @BeforeAll
    static void strictYamlLoader() {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        yaml = new Yaml(new SafeConstructor(options));
    }

    @Test
    @DisplayName("valid/ covers every ADR-0016 §14 scenario with a non-empty workspace directory")
    void validCoversAllScenarios() throws IOException {
        assertThat(VALID).isDirectory();
        List<Path> dirs = subDirectories(VALID);
        for (String prefix : SCENARIO_PREFIXES) {
            List<Path> matches = dirs.stream().filter(d -> d.getFileName().toString().startsWith(prefix)).toList();
            assertThat(matches)
                    .withFailMessage("expected exactly one valid/ scenario directory with prefix '%s', found %s", prefix, matches)
                    .hasSize(1);
            assertThat(artifactFiles(matches.get(0)))
                    .withFailMessage("scenario directory %s holds no .tap.yml artifacts", matches.get(0))
                    .isNotEmpty();
        }
    }

    @Test
    @DisplayName("every corpus *.tap.yml is a single-document YAML map with version/kind/id")
    void everyArtifactIsWellFormed() throws IOException {
        List<Path> artifacts = allArtifacts();
        assertThat(artifacts).isNotEmpty();
        for (Path artifact : artifacts) {
            Object doc = loadSingleDocument(artifact);
            assertThat(doc).withFailMessage("%s is not a YAML map", artifact).isInstanceOf(Map.class);
            Map<?, ?> map = (Map<?, ?>) doc;
            assertThat(map.get("version")).withFailMessage("%s: version must be 'tapstate/v1'", artifact).isEqualTo("tapstate/v1");
            assertThat(map.get("kind")).withFailMessage("%s: kind must be one of %s", artifact, KINDS).isIn(KINDS);
            assertThat(map.get("id")).withFailMessage("%s: top-level id is required", artifact)
                    .isInstanceOf(String.class).asString().isNotBlank();
        }
    }

    @Test
    @DisplayName("every invalid/ case declares its violated rule in expected.yml")
    void invalidCasesDeclareExpectedRule() throws IOException {
        assertThat(INVALID).isDirectory();
        List<Path> cases = subDirectories(INVALID);
        assertThat(cases).isNotEmpty();
        for (Path caseDir : cases) {
            assertThat(artifactFiles(caseDir))
                    .withFailMessage("invalid case %s holds no .tap.yml artifacts", caseDir)
                    .isNotEmpty();
            Path expectation = caseDir.resolve("expected.yml");
            assertThat(expectation).withFailMessage("invalid case %s lacks expected.yml", caseDir).isRegularFile();
            Object doc = loadSingleDocument(expectation);
            assertThat(doc).withFailMessage("%s is not a YAML map", expectation).isInstanceOf(Map.class);
            Object rule = ((Map<?, ?>) doc).get("rule");
            assertThat(rule)
                    .withFailMessage("%s: rule '%s' is not in the vocabulary %s", expectation, rule, RULES)
                    .isIn(RULES);
        }
    }

    @Test
    @DisplayName("valid/ workspaces carry no expectation sidecars")
    void validBatchesCarryNoExpectations() throws IOException {
        try (Stream<Path> walk = Files.walk(VALID)) {
            List<Path> sidecars = walk.filter(p -> p.getFileName().toString().equals("expected.yml")).toList();
            assertThat(sidecars).isEmpty();
        }
    }

    private static List<Path> subDirectories(Path root) throws IOException {
        try (Stream<Path> list = Files.list(root)) {
            return list.filter(Files::isDirectory).sorted().toList();
        }
    }

    private static List<Path> artifactFiles(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(p -> p.getFileName().toString().endsWith(".tap.yml")).sorted().toList();
        }
    }

    private static List<Path> allArtifacts() throws IOException {
        return artifactFiles(CORPUS);
    }

    private static Object loadSingleDocument(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            return yaml.load(in);
        }
    }
}
