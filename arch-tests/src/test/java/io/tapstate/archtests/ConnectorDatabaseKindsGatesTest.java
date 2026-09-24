package io.tapstate.archtests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The support promise, connector builds, cache validation and live harness must cover the same stores. */
class ConnectorDatabaseKindsGatesTest {

    private static final Path REPOSITORY = Path.of("..");
    private static final String VERIFIED =
            "arch-tests/src/test/java/io/tapstate/archtests/ConnectorAcceptanceGatesTest.java";
    private static final String BUILD = "scripts/build-real-connectors.sh";
    private static final String CACHE = ".github/scripts/connector-cache.sh";
    private static final String HARNESS = "e2e/src/test/java/io/tapstate/e2e/DatabaseKind.java";
    private static final String FIXTURE = ".github/scripts/connector-cache-smoke.sh";
    private static final List<String> MUST_BE_SCANNED = List.of(VERIFIED, BUILD, CACHE, HARNESS);
    private static final Set<String> PRUNED = Set.of(".git", "target", "node_modules", ".claude");

    @Test
    void fourDatabaseKindInventoriesAgree() {
        Map<String, Set<String>> inventories = inventories(REPOSITORY);
        assertThat(inventories).containsOnlyKeys(MUST_BE_SCANNED.toArray(String[]::new));
        Set<String> verified = inventories.get(VERIFIED);
        assertThat(verified).isNotEmpty();
        inventories.forEach((path, kinds) -> assertThat(kinds)
                .as("database kinds in %s must match the independent verified support promise", path)
                .isEqualTo(verified));
    }

    @Test
    void recognisesEachInventorySyntaxAndRejectsEmptyOrMalformedDeclarations() {
        assertThat(parse(VERIFIED, "private static final List<String> VERIFIED_DATABASE_KINDS = "
                + "List.of(\n\"mysql\", \"mongodb\", \"oracle\");"))
                .containsExactlyInAnyOrder("mysql", "mongodb", "oracle");
        assertThat(parse(BUILD, "readonly DEFAULT_MODULES=\"mysql=connectors/mysql-connector,"
                + "sqlserver=connectors/mssql-connector\""))
                .containsExactlyInAnyOrder("mysql", "sqlserver");
        assertThat(parse(CACHE, "MODULES = 'mysql=connectors/mysql-connector,"
                + "sqlserver=connectors/mssql-connector'"))
                .containsExactlyInAnyOrder("mysql", "sqlserver");
        assertThat(parse(HARNESS, "enum DatabaseKind { MYSQL(\"mysql\"), MONGO(\"mongodb\"), "
                + "SQLSERVER(\"sqlserver\"); private final String connectorId; }"))
                .containsExactlyInAnyOrder("mysql", "mongodb", "sqlserver");
        for (String path : MUST_BE_SCANNED) {
            assertThatThrownBy(() -> parse(path, "// declaration removed"))
                    .as("a missing inventory at %s must never be an empty equal set", path)
                    .isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> parse(VERIFIED, "VERIFIED_DATABASE_KINDS = List.of();"))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> parse(CACHE, "MODULES = 'connectors/mysql-connector'"))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> parse(BUILD,
                "DEFAULT_MODULES=\"mysql=connectors/mysql-connector,mysql=connectors/other-connector\""))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> parse(HARNESS, "enum DatabaseKind { MYSQL; }"))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void scanActuallyReadsAllFourPinnedFiles() {
        assertThat(inventories(REPOSITORY).keySet()).containsExactlyInAnyOrderElementsOf(MUST_BE_SCANNED);
    }

    @Test
    void scanReachesScriptsAndTestSourcesWhileIgnoringNestedCopies(@TempDir Path root) throws IOException {
        for (String path : MUST_BE_SCANNED) {
            write(root.resolve(path), "inventory");
            write(root.resolve(".claude/worktrees/copy").resolve(path), "nested copy");
            write(root.resolve("target/copy").resolve(path), "generated copy");
        }
        write(root.resolve(FIXTURE), "fixture");
        assertThat(inventoryFiles(root).stream().map(path -> relative(root, path)).toList())
                .containsExactlyInAnyOrderElementsOf(MUST_BE_SCANNED);
    }

    @Test
    void excludedSmokeFixtureStillExistsAndCreatesIndependentJars() throws IOException {
        Path fixture = REPOSITORY.resolve(FIXTURE);
        assertThat(fixture).isRegularFile();
        String text = Files.readString(fixture);
        assertThat(text).contains("for name in (", "-connector-v1.jar", ".write_bytes(",
                "call('seal'", "call('verify'");
        assertThat(inventoryFiles(REPOSITORY)).doesNotContain(fixture);
    }

    private static Map<String, Set<String>> inventories(Path root) {
        Map<String, Set<String>> result = new LinkedHashMap<>();
        for (Path path : inventoryFiles(root)) {
            String relative = relative(root, path);
            try {
                result.put(relative, parse(relative, Files.readString(path)));
            } catch (IOException e) {
                throw new UncheckedIOException("reading inventory " + path, e);
            }
        }
        return result;
    }

    private static Set<String> parse(String path, String text) {
        String body;
        Pattern entry;
        if (path.equals(VERIFIED)) {
            body = capture(text, "\\bVERIFIED_DATABASE_KINDS\\s*=\\s*List\\.of\\(([^;]*)\\)\\s*;");
            entry = Pattern.compile("\\s*\"([a-z][a-z0-9-]*)\"\\s*");
        } else if (path.equals(BUILD) || path.equals(CACHE)) {
            String variable = path.equals(BUILD) ? "DEFAULT_MODULES" : "MODULES";
            body = capture(text, "(?m)^\\s*(?:readonly\\s+)?" + variable + "\\s*=\\s*['\"]([^'\"\\r\\n]+)['\"]\\s*$");
            entry = Pattern.compile("\\s*([a-z][a-z0-9-]*)=connectors/[a-z0-9-]+\\s*");
        } else {
            assertThat(path).isEqualTo(HARNESS);
            body = capture(text, "\\benum\\s+DatabaseKind\\s*\\{([^;]+);");
            entry = Pattern.compile("\\s*[A-Z][A-Z_0-9]*\\(\\s*\"([a-z][a-z0-9-]*)\"\\s*\\)\\s*");
        }
        Set<String> kinds = new TreeSet<>();
        for (String item : body.split(",", -1)) {
            Matcher matcher = entry.matcher(item);
            assertThat(matcher.matches()).as("unrecognised database kind entry in %s: %s", path, item).isTrue();
            assertThat(kinds.add(matcher.group(1))).as("duplicate database kind in %s: %s", path, item).isTrue();
        }
        assertThat(kinds).as("database kinds read from %s", path).isNotEmpty();
        return kinds;
    }

    private static String capture(String text, String expression) {
        Matcher matcher = Pattern.compile(expression).matcher(text);
        assertThat(matcher.find()).as("inventory declaration must match %s", expression).isTrue();
        String body = matcher.group(1);
        assertThat(matcher.find()).as("inventory declaration must be unique: %s", expression).isFalse();
        return body;
    }

    /** This walk includes scripts and test sources, which the shipped-artifact walk excludes. */
    private static List<Path> inventoryFiles(Path root) {
        List<Path> files = new ArrayList<>();
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes) {
                    return PRUNED.contains(directory.getFileName().toString())
                            ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                    if (MUST_BE_SCANNED.contains(relative(root, file))) {
                        files.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("walking database kind inventories at " + root, e);
        }
        return files;
    }

    private static String relative(Path root, Path file) {
        return root.relativize(file).toString().replace('\\', '/');
    }

    private static void write(Path file, String text) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, text);
    }
}
