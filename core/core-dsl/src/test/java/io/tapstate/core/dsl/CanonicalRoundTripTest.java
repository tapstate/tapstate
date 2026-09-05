package io.tapstate.core.dsl;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks the canonical serialization of the whole valid corpus two ways.
 *
 * <ul>
 *   <li><b>Fixed point</b>: canonical output is a fixed point of parse-then-write —
 *       {@code write(parse(write(r))) == write(r)}. This is the locally testable projection of the
 *       store invariant that re-importing an exported artifact is a no-op; a stable canonical form
 *       is what makes that no-op possible.</li>
 *   <li><b>Golden</b>: each resource's canonical text is pinned byte-for-byte to a checked-in golden
 *       file. The canonical form is a long-term compatibility promise, so a diff here is a real
 *       behavior change: regenerate with {@code -Dtapstate.golden.update=true}, then review the diff.</li>
 *   <li><b>Structured tree</b>: the same corpus survives the other serialization —
 *       {@code fromTree(tree(r)) == r} — which is what lets the store hold a resource as a document
 *       and read it back without parsing any text. The two directions share one binder, so this is a
 *       round trip through a different carrier rather than through a second mapping.</li>
 * </ul>
 *
 * <p>Golden files live outside {@code corpus/} on purpose: the workspace loader walks every
 * {@code *.tap.yml} under a scenario directory, so a golden placed inside one would be loaded as a
 * duplicate resource. Resource ids repeat across scenarios, so goldens are namespaced by scenario.
 */
class CanonicalRoundTripTest {

    private static final Path VALID = Path.of("src", "test", "resources", "corpus", "valid");
    private static final Path GOLDEN = Path.of("src", "test", "resources", "golden", "valid");
    private static final boolean UPDATE = Boolean.getBoolean("tapstate.golden.update");

    private final CanonicalWriter writer = new CanonicalWriter();
    private final DslParser parser = new DslParser();

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("canonicalResources")
    void canonicalOutputIsAParseWriteFixedPoint(String scenario, String id, String canonical) {
        assertThat(writer.write(parser.parse(canonical))).isEqualTo(canonical);
    }

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("canonicalResources")
    void canonicalOutputMatchesGolden(String scenario, String id, String canonical) throws IOException {
        Path golden = GOLDEN.resolve(scenario).resolve(id + ".tap.yml");
        if (UPDATE) {
            Files.createDirectories(golden.getParent());
            Files.writeString(golden, canonical);
            return;
        }
        assertThat(Files.exists(golden))
                .as("golden %s missing — regenerate with -Dtapstate.golden.update=true", golden)
                .isTrue();
        assertThat(Files.readString(golden)).isEqualTo(canonical);
    }

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("canonicalResources")
    void theStructuredTreeBindsBackToTheSameResource(String scenario, String id, String canonical) {
        Resource fromText = parser.parse(canonical);

        Resource fromTree = parser.fromTree(writer.tree(fromText));

        assertThat(fromTree).isEqualTo(fromText);
    }

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("canonicalResources")
    void theStructuredTreeKeepsTheCanonicalKeyOrder(String scenario, String id, String canonical) {
        // Key order in the tree is the key order in the text, because both come off the same render
        // tree. It matters at the far end: the tree is what a document store keeps, and fields that
        // shuffle between two writes of the same resource make every diff of the stored form noise.
        List<String> inText = canonical.lines()
                .filter(line -> !line.isBlank() && !Character.isWhitespace(line.charAt(0)) && line.charAt(0) != '-')
                .map(line -> line.substring(0, line.indexOf(':')))
                .toList();

        assertThat(List.copyOf(writer.tree(parser.parse(canonical)).keySet())).isEqualTo(inText);
    }

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("canonicalResources")
    void theStructuredTreeIsMadeOfPlainValuesOnly(String scenario, String id, String canonical) {
        // The tree is handed to a document store, which can encode plain values and nothing else. A
        // render node that leaked into it would compile, pass the round trip above (the binder would
        // read it straight back), and fail only at the storage boundary in another module.
        assertOnlyPlainValues(writer.tree(parser.parse(canonical)), "");
    }

    private static void assertOnlyPlainValues(Object value, String path) {
        switch (value) {
            case null -> { }
            case Map<?, ?> map -> map.forEach((k, v) -> {
                assertThat(k).as("key at %s", path).isInstanceOf(String.class);
                assertOnlyPlainValues(v, path.isEmpty() ? String.valueOf(k) : path + "." + k);
            });
            case List<?> list -> {
                for (int i = 0; i < list.size(); i++) {
                    assertOnlyPlainValues(list.get(i), path + "[" + i + "]");
                }
            }
            default -> assertThat(value)
                    .as("value at %s", path.isEmpty() ? "(root)" : path)
                    .isInstanceOfAny(String.class, Integer.class, Long.class, Double.class, Boolean.class);
        }
    }

    @Test
    void everyResourceHashesToItsGolden() throws IOException {
        // The canonical text is pinned above; this pins what the text does not show. The content hash is
        // taken over the structure, so a change to how that structure is encoded moves every stored
        // resource's identity while leaving every canonical byte where it was -- invisible to every
        // other assertion in this file, and to every caller until preconditions start being refused.
        StringBuilder actual = new StringBuilder();
        List<String> lines = new ArrayList<>();
        for (Path dir : scenarioDirs()) {
            String scenario = dir.getFileName().toString();
            for (Resource r : WorkspaceLoader.load(dir).resources()) {
                lines.add(scenario + "/" + r.id() + " " + CanonicalHash.of(r));
            }
        }
        lines.stream().sorted().forEach(line -> actual.append(line).append('\n'));

        Path golden = GOLDEN.getParent().resolve("content-hash.golden");
        if (UPDATE) {
            Files.createDirectories(golden.getParent());
            Files.writeString(golden, actual.toString());
            return;
        }
        assertThat(Files.exists(golden))
                .as("golden %s missing -- regenerate with -Dtapstate.golden.update=true", golden)
                .isTrue();
        assertThat(Files.readString(golden)).isEqualTo(actual.toString());
    }

    @Test
    void everyGoldenIsClaimedByACorpusResource() throws IOException {
        // Symmetric counterpart to the per-resource golden assertion above. That assertion forces
        // the ADD direction (a new corpus resource has no golden -> RED); this one forces the
        // DELETE/RENAME direction, so a removed or renamed resource cannot leave a stale lock file
        // drifting unnoticed. Comparing the two sets catches both an orphan and a missing golden.
        Set<Path> expected = new TreeSet<>();
        for (Path dir : scenarioDirs()) {
            String scenario = dir.getFileName().toString();
            for (Resource r : WorkspaceLoader.load(dir).resources()) {
                expected.add(GOLDEN.resolve(scenario).resolve(r.id() + ".tap.yml"));
            }
        }
        Set<Path> actual;
        try (Stream<Path> walk = Files.walk(GOLDEN)) {
            actual = walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".tap.yml"))
                    .collect(Collectors.toCollection(TreeSet::new));
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void goldenUpdateToggleIsOffDuringNormalRuns() {
        // The regenerate path rewrites goldens and skips the byte assertion. Were the toggle set
        // during a normal run, a real canonical regression would be silently rebaselined and pass —
        // defeating the whole point of the byte-for-byte lock. This guard makes any run with the
        // toggle RED, so regeneration is always deliberate: regenerate (RED), review the diff, then
        // re-run without the toggle (GREEN).
        assertThat(UPDATE)
                .as("tapstate.golden.update must not be set during a normal run — it rewrites goldens and masks regressions")
                .isFalse();
    }

    @Test
    void sourceRegexWithNegativeLookaheadSurvivesCanonicalRoundTrip() {
        String raw = """
                version: tapstate/v1
                kind: source
                id: mysql_feynman
                metadata:
                  description: MySQL source for feynman database
                connector: mysql
                mode: cdc
                tables:
                - /^(?!timezone_test$).*/
                """;

        String canonical = writer.write(parser.parse(raw));

        assertThat(canonical)
                .contains("tables: [\"/^(?!timezone_test$).*/\"]");

        assertThat(writer.write(parser.parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void sourceRegexWithColonSurvivesCanonicalRoundTrip() {
        String raw = """
                version: tapstate/v1
                kind: source
                id: mysql_feynman
                connector: mysql
                mode: cdc
                tables:
                - /^orders:[0-9]+$/
                """;

        String canonical = writer.write(parser.parse(raw));

        assertThat(canonical)
                .contains("tables: [\"/^orders:[0-9]+$/\"]");
        assertThat(writer.write(parser.parse(canonical))).isEqualTo(canonical);
    }

    /** Every resource of every valid scenario, paired with its canonical text. */
    static Stream<Arguments> canonicalResources() throws IOException {
        CanonicalWriter writer = new CanonicalWriter();
        List<Arguments> out = new ArrayList<>();
        for (Path dir : scenarioDirs()) {
            String scenario = dir.getFileName().toString();
            for (Resource r : WorkspaceLoader.load(dir).resources()) {
                out.add(Arguments.of(scenario, r.id(), writer.write(r)));
            }
        }
        return out.stream();
    }

    private static List<Path> scenarioDirs() throws IOException {
        try (Stream<Path> list = Files.list(VALID)) {
            return list.filter(Files::isDirectory).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
