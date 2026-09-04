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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Counts the sleeps in this module's own sources and holds the count to an exact, named allowlist.
 *
 * <p>A fixed-duration sleep is the wrong tool everywhere in an end-to-end harness: long enough to be
 * reliable, it wastes that long on every green run, and it is never quite reliable anyway. Nearly every
 * sanctioned use is a poll interval inside a condition loop - the executor's bounded await, the server
 * launcher's bounded readiness wait, the synthetic connector's change-stream tail, and one bounded read
 * of its own target per witness class - where the loop's condition, not the sleep, decides what happens
 * next. Everything else is a settle: a guess about how long some unobservable thing takes, checked by
 * nothing. One such guess already shipped here and papered over a real product gap for weeks.
 *
 * <p>The allowlist names each sanctioned call site exactly. A new sleep anywhere in this module -
 * including one more in an allowlisted file - fails this gate and must either become a bounded wait
 * on an observable condition, or argue its way onto the allowlist in review, visibly.
 *
 * <p>One entry is admitted knowingly and is not a poll interval: the throttle witness spaces its quiet
 * phase's changes apart by a fixed gap, to produce the input the case is about rather than to wait for
 * an outcome. The gate counts per file and that file's three call sites share one helper, so admitting
 * the two polls admits the gap with them. It is named here rather than left to look like a poll: a
 * stimulus whose duration has to exceed a collection window is still a duration nothing checks.
 *
 * <p>This scan found a sleep the day it was written that a plain text search over the same tree had
 * just missed, so the two are not interchangeable: the gate reads every source itself.
 *
 * <p><b>What this gate does not see.</b> It matches text, so its reach is the shape of the call and
 * nothing deeper, and saying where that ends is part of what it is worth. Whitespace no longer decides:
 * a call split before its dot was measured slipping through and now counts, and two calls on one line
 * count as two. Still invisible, and deliberately named rather than left to be discovered: a wait that
 * never spells the word, such as {@code LockSupport.parkNanos}, {@code Object.wait(millis)} or an
 * unconditional {@code latch.await(millis, unit)}; a sleep reached through a helper that lives outside
 * this module, where nothing here reads its source; and anything assembled at runtime. Each is a real
 * way to make this gate green over a fixed wait. They are left out because closing them by text would
 * cost more false alarms than the sleeps it would catch, and the honest position is that this gate
 * raises the price of a settle rather than making one impossible. A wait belongs on an observable
 * condition whether or not a gate can see it.
 *
 * <p><b>The other direction is a defect, not a boundary.</b> Reddening over a sleep that is not there
 * blocks work that is not wrong, and teaches people to route around the gate - which costs more than
 * the settles it was built to catch. One such was found and fixed: a call named in a comment after code
 * counted as a call, so a file could fail this gate with nothing in it to fix. Comments are now removed
 * before anything is matched, block comments included. Report any other false alarm as a bug in here.
 */
class FixedSleepGateTest {

    /**
     * Keyed by the path under {@code src}, not by file name. A name is not an identity: two files may
     * share one, and then the allowlist stops saying which file is sanctioned - an unsanctioned
     * {@code other/E2eExecutor.java} would satisfy the entry the moment the sanctioned one stopped
     * needing it, and the gate would be green over a sleep nobody allowed.
     */
    private static final Map<String, Long> POLL_PRIMITIVES = Map.ofEntries(
            // The harness's own four: the specification runner's bounded await, the direct-drive await
            // every bespoke witness shares, the launcher's readiness wait, the synthetic connector's tail.
            entry("test/java/io/tapstate/e2e/Await.java", 1L),
            entry("test/java/io/tapstate/e2e/E2eExecutor.java", 1L),
            entry("test/java/io/tapstate/e2e/RealProcessServer.java", 1L),
            entry("test/java/io/tapstate/e2e/connector/CsvConnector.java", 1L),
            // One bounded read of its own target per witness class, each a poll inside a deadline loop.
            entry("test/java/io/tapstate/e2e/LosslessNumericTypeIsAcceptedIT.java", 1L),
            entry("test/java/io/tapstate/e2e/RealMysqlToMongoSnapshotIT.java", 1L),
            entry("test/java/io/tapstate/e2e/RealMysqlToMongoViewIndexIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestAssemblesParentAndChildrenIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestChildCdcMutatesTheArrayIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestColdRootWakesCorrectlyIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestConvergesWhenChildArrivesBeforeParentIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestFrontierDoesNotOutrunDeferredEventsIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestHoldsMoreThanItsMemoryBudgetIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestIdleInstanceDoesNotStallFrontierIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestIsolatesSameLevelEmbedKeyspacesIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestMiddleRowDeleteDoesNotDropWaitingIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestMigratesSubtreeOnAncestorReparentIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestPendingLimitFailsTheJobIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestPointedAtRowDeleteLeavesTheTargetWithoutItIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestReparentMovesTheChildIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestResolvesFourLevelDeepIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestRootFanoutLimitFailsTheJobIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestSaysWhenItIsServedFromTheColdLayerIT.java", 1L),
            entry("test/java/io/tapstate/e2e/NestTrackingRefusesASourceWithoutBeforeImagesIT.java", 1L),
            // Two polls and one fixed gap that is not one - see the class comment.
            entry("test/java/io/tapstate/e2e/NestThrottleCoalescesHotRootIT.java", 1L),
            // The read face's own: two harness primitives - a wait on what a CLI process has printed,
            // and a wait on what a follow has delivered - and one bounded read per witness class. Each
            // is a single named sleep() called from a `while (nanoTime - deadline < 0)` loop whose
            // condition, not the sleep, decides the outcome.
            entry("test/java/io/tapstate/e2e/CliProcess.java", 1L),
            entry("test/java/io/tapstate/e2e/ControlPlane.java", 1L),
            entry("test/java/io/tapstate/e2e/DataBrowserCollectionsIT.java", 1L),
            entry("test/java/io/tapstate/e2e/DataBrowserDottedFieldIT.java", 1L),
            entry("test/java/io/tapstate/e2e/TailIT.java", 1L),
            entry("test/java/io/tapstate/e2e/WatchRedrawsIT.java", 1L));

    private static final Pattern SLEEP = Pattern.compile(
            "Thread\\s*\\.\\s*sleep\\s*\\(|TimeUnit\\s*\\.\\s*[A-Z_]+\\s*\\.\\s*sleep\\s*\\(");

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
     * A sleep is a sleep however it is typed, so the scan may not be decided by formatting.
     *
     * <p>Reading line by line, the gate used to see {@code Thread.sleep(} and miss the same call with a
     * line break before the dot - which the formatter is free to introduce, and which anyone wanting a
     * settle past the gate can type on purpose. Both were measured: the one-line form fails this gate on
     * an exact per-file count, the split form passed it while sleeping just as long.
     *
     * <p>Two sleeps sharing one line are two sleeps here for the same reason - what is counted is the
     * calls, not the lines that happen to carry them.
     */
    @Test
    void aSleepIsCountedHoweverItIsSpacedAcrossLines(@TempDir Path root) throws IOException {
        // Spelled in pieces for the reason above: written whole, these would be counted by the walk.
        String split = "Thread" + "\n            ." + "sleep(";
        String inline = "Thread." + "sleep(";
        Files.writeString(root.resolve("Split.java"),
                "class Split { void a() throws Exception { " + split + "1); } }");
        Files.writeString(root.resolve("Pair.java"),
                "class Pair { void a() throws Exception { " + inline + "1); " + inline + "2); } }");

        assertThat(sleepsUnder(root)).containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                "Split.java", 1L,
                "Pair.java", 2L));
    }

    /**
     * A sleep the gate only read about is not a sleep, wherever the comment sits on the line.
     *
     * <p>Comments used to be dropped a whole line at a time, and only when the line began with one, so a
     * comment after code survived and was matched: a file whose one mention of the call sat in a trailing
     * comment failed this gate with nothing to fix. That direction matters as much as the misses. A gate
     * that reddens over prose blocks work that is not wrong, and the way people deal with one of those is
     * to stop writing the comment - or to stop trusting the gate, which costs more than the sleeps it was
     * built to catch. This class's own comments discuss the call throughout, and were safe only by the
     * accident of starting their lines with a star.
     *
     * <p>Code that resumes after a block comment closes is code again, so the last file here is counted.
     */
    @Test
    void aSleepOnlyMentionedInACommentIsNotCounted(@TempDir Path root) throws IOException {
        // Spelled in pieces for the reason above: written whole, these would be counted by the walk.
        String call = "Thread." + "sleep(";
        Files.writeString(root.resolve("Trailing.java"),
                "class Trailing { void a() { int x = 1; // a settle such as " + call + "500) is banned\n } }");
        Files.writeString(root.resolve("Block.java"),
                "class Block {\n/* prose about " + call + "500)\n   and more prose */\n void a() { } }");
        Files.writeString(root.resolve("Resumed.java"),
                "class Resumed { void a() throws Exception { /* about it */ " + call + "1); } }");

        assertThat(sleepsUnder(root)).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("Resumed.java", 1L));
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
        // Comments come out first, then what is left is matched as one text rather than line by line: a
        // call split before its dot spans two lines and no single line holds it, and two calls sharing a
        // line are two calls. What is counted is the matches, not the lines carrying them.
        return SLEEP.matcher(withoutComments(lines)).results().count();
    }

    /**
     * The source with its comments taken out, so the gate counts calls rather than mentions of one.
     *
     * <p>Dropping whole lines that begin with a comment marker is not enough and was not: a comment after
     * code stayed, and a file whose only mention of the call sat there failed the gate over prose. Block
     * comments are tracked across lines for the same reason, and code resuming after one closes is code.
     *
     * <p>Not a Java lexer, and the remaining gap is a string holding {@code //} - a URL, say - which ends
     * the line early here and would hide a call written after it on that same line. Worth knowing and not
     * worth a parser: this reads test sources, where that line has never existed. A string holding the
     * call itself is still counted, which is why the fixtures in this file spell theirs in pieces.
     */
    private static String withoutComments(List<String> lines) {
        StringBuilder code = new StringBuilder();
        boolean inBlock = false;
        for (String line : lines) {
            int index = 0;
            while (index < line.length()) {
                if (inBlock) {
                    int close = line.indexOf("*/", index);
                    if (close < 0) {
                        break;
                    }
                    inBlock = false;
                    index = close + 2;
                } else if (line.startsWith("//", index)) {
                    break;
                } else if (line.startsWith("/*", index)) {
                    inBlock = true;
                    index += 2;
                } else {
                    code.append(line.charAt(index));
                    index++;
                }
            }
            code.append('\n');
        }
        return code.toString();
    }
}
