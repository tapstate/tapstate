package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Counts the sleeps in this module's own sources and holds the count to an exact, named allowlist.
 *
 * <p>A fixed-duration sleep is the wrong tool everywhere in an end-to-end harness: long enough to be
 * reliable, it wastes that long on every green run, and it is never quite reliable anyway. The harness
 * has three sanctioned uses of {@code Thread.sleep}, and each is a poll interval inside a condition
 * loop - the executor's bounded await, the server launcher's bounded readiness wait, and the synthetic
 * connector's change-stream tail - where the loop's condition, not the sleep, decides what happens
 * next. Everything else is a settle: a guess about how long some unobservable thing takes, checked by
 * nothing. One such guess already shipped here and papered over a real product gap for weeks.
 *
 * <p>The allowlist names each sanctioned call site exactly. A new sleep anywhere in this module -
 * including one more in an allowlisted file - fails this gate and must either become a bounded wait
 * on an observable condition, or argue its way onto the allowlist in review, visibly.
 *
 * <p>This scan found a sleep the day it was written that a plain text search over the same tree had
 * just missed, so the two are not interchangeable: the gate reads every source line itself.
 */
class FixedSleepGateTest {

    /**
     * Keyed by the path under {@code src}, not by file name. A name is not an identity: two files may
     * share one, and then the allowlist stops saying which file is sanctioned - an unsanctioned
     * {@code other/E2eExecutor.java} would satisfy the entry the moment the sanctioned one stopped
     * needing it, and the gate would be green over a sleep nobody allowed.
     */
    private static final Map<String, Long> POLL_PRIMITIVES = Map.of(
            "test/java/io/tapstate/e2e/E2eExecutor.java", 1L,
            "test/java/io/tapstate/e2e/RealProcessServer.java", 1L,
            "test/java/io/tapstate/e2e/connector/CsvConnector.java", 1L);

    private static final Pattern SLEEP = Pattern.compile(
            "Thread\\.sleep\\(|TimeUnit\\.[A-Z_]+\\.sleep\\(");

    @Test
    void everyFixedSleepInThisModuleIsANamedPollPrimitive() {
        assertThat(sleepsUnder(Path.of("src")))
                .as("every Thread.sleep in the e2e module must be a named poll primitive inside a "
                        + "bounded condition loop; a new one is a settle - wait on an observable "
                        + "condition instead")
                .containsExactlyInAnyOrderEntriesOf(POLL_PRIMITIVES);
    }

    /**
     * Two files of the same name in different packages are two files, and the gate has to be able to
     * say which of them is allowed a sleep. Under one key it cannot: an unsanctioned namesake spends
     * the sanctioned file's allowance the moment that file stops needing it, and the gate stays green
     * over a sleep nobody allowed - which is the one thing it exists to prevent.
     */
    @Test
    void sameNamedFilesInDifferentPackagesAreToldApart(@TempDir Path root) throws IOException {
        Files.createDirectories(root.resolve("one"));
        Files.createDirectories(root.resolve("two"));
        // Spelled in pieces on purpose: written whole, these fixtures would be counted by the very walk
        // they are here to exercise, and this file would fail its own gate.
        String sleep = "Thread." + "sleep(";
        Files.writeString(root.resolve("one/Twin.java"), "class Twin { void a() { " + sleep + "1); } }");
        Files.writeString(root.resolve("two/Twin.java"), "class Twin { void b() { " + sleep + "2); } }");

        assertThat(sleepsUnder(root)).containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                "one/Twin.java", 1L,
                "two/Twin.java", 1L));
    }

    /**
     * Every source under the root that holds a sleep, keyed by its path under that root.
     *
     * <p>The path is the identity because a file name is not one. Two packages may hold a
     * {@code Foo.java} each, and keyed by name they are one entry: whichever is walked second used to
     * replace the first, taking its sleeps out of the reckoning, and adding them instead only fixes
     * half of it - a sanctioned file that stops needing its sleep leaves its allowance behind for an
     * unsanctioned namesake to spend. A path says which file is allowed what.
     */
    static Map<String, Long> sleepsUnder(Path root) {
        Map<String, Long> found = new TreeMap<>();
        try (Stream<Path> sources = Files.walk(root)) {
            sources.filter(path -> path.getFileName().toString().endsWith(".java"))
                    .forEach(source -> {
                        long sleeps = countSleeps(source);
                        if (sleeps > 0) {
                            // Separator normalised so the allowlist reads the same on every platform.
                            found.put(root.relativize(source).toString().replace(File.separatorChar, '/'), sleeps);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("cannot walk the sources under " + root, e);
        }
        return found;
    }

    private static long countSleeps(Path source) {
        List<String> lines;
        try {
            lines = Files.readAllLines(source);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + source, e);
        }
        return lines.stream()
                .filter(line -> !line.trim().startsWith("//") && !line.trim().startsWith("*"))
                .filter(line -> SLEEP.matcher(line).find())
                .count();
    }
}
