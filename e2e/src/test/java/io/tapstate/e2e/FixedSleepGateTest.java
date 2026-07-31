package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

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

    /** Sanctioned poll primitives: file name to the exact number of sleep calls it may hold. */
    private static final Map<String, Long> POLL_PRIMITIVES = Map.of(
            "E2eExecutor.java", 1L,
            "RealProcessServer.java", 1L,
            "CsvConnector.java", 1L);

    private static final Pattern SLEEP = Pattern.compile(
            "Thread\\.sleep\\(|TimeUnit\\.[A-Z_]+\\.sleep\\(");

    @Test
    void everyFixedSleepInThisModuleIsANamedPollPrimitive() {
        Map<String, Long> found = new TreeMap<>();
        try (Stream<Path> sources = Files.walk(Path.of("src"))) {
            sources.filter(path -> path.getFileName().toString().endsWith(".java"))
                    .forEach(source -> {
                        long sleeps = countSleeps(source);
                        if (sleeps > 0) {
                            found.put(source.getFileName().toString(), sleeps);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("cannot walk this module's sources", e);
        }
        assertThat(found)
                .as("every Thread.sleep in the e2e module must be a named poll primitive inside a "
                        + "bounded condition loop; a new one is a settle - wait on an observable "
                        + "condition instead")
                .containsExactlyInAnyOrderEntriesOf(POLL_PRIMITIVES);
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
