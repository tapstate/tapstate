package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoKeyedStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness for the one rule the durable frontier rests on: a change this level is holding may be passed
 * by the bound it reports only once the write carrying that change has come back from the store.
 *
 * <p>The frontier no longer promises that everything at or below it has been emitted - it promises that
 * everything at or below it has been <em>put somewhere it survives from</em>, which for a child waiting on
 * a parent means the cold layer. That is what lets a dangling foreign key stop pinning a source's read
 * offset. It also means the promise is now only as true as the ordering behind it: let the bound past a
 * change whose write has not landed, and the source is cleared to forget a row that is in neither place.
 * Nothing fails, no count moves, and the document is short an element for good.
 *
 * <p><b>Why this has to kill a process.</b> The violation exists only inside the window between the bound
 * going out and the write landing, so no ordinary run can see it: every assertion available to a test that
 * lives to make it is taken after both have happened, when the two orders look alike. The only observer
 * that can tell them apart is one that stops the process between the two - so the run under test is a real
 * process, and it is sent a real {@code SIGKILL}. A member shut down from inside the test JVM would get to
 * run its own code on the way out, which is the one thing a crash never does.
 *
 * <p><b>What makes it decide rather than guess.</b> Two things, both in {@link NestCrashHarness}: the cold
 * layer is slowed, which stretches that window from microseconds to seconds; and the source stops feeding
 * rows after its burst while going on raising its bound, so a level that settled its drain anywhere but in
 * the drain has nothing left to trigger it and the window stays open. The kill is taken the moment the
 * report says the bound has passed every claim - which, if the ordering holds, cannot happen until all
 * eight slow writes have come back.
 *
 * <p>Both halves are asserted, and they fail for different reasons:
 * <ul>
 *   <li><b>What the bound passed is in the store</b> - read straight out of Mongo after the kill, by a
 *       process that was never part of the run. Reversing the order empties this.</li>
 *   <li><b>And it is still usable</b> - the harness is started again over that state, the policies arrive,
 *       and every claim comes back up into the document. Bytes present but unreadable would pass the first
 *       half and fail here.</li>
 * </ul>
 */
@RequiresDocker
class WhatTheFrontierHasPassedSurvivesAKillIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    /**
     * How long each save is held up for. Long enough that a bound let out ahead of one is still ahead of it
     * when the signal lands, short enough that eight of them are a couple of seconds.
     */
    private static final long SAVE_DELAY_MILLIS = 250;

    /** How much of a forked run's own output a failure carries: enough to name a cause, not the whole run. */
    private static final int EVIDENCE_LINES = 20;

    @TempDir
    private Path work;

    @Test
    void everythingTheFrontierPassedIsInTheStoreWhenTheProcessIsKilledAndCanBeReadBackAfterwards()
            throws Exception {
        Path holdReport = work.resolve("hold.report");
        Files.createFile(holdReport);

        Process holding = fork(holdReport, "hold");
        try {
            awaitBoundPastEveryClaim(holdReport, holding);
        } finally {
            holding.destroyForcibly();
        }
        assertThat(holding.waitFor(30, TimeUnit.SECONDS))
                .describedAs("the run under test did not die to the signal, so nothing here is a crash")
                .isTrue();

        List<String> report = Files.readAllLines(holdReport);
        long highest = highestBound(report);
        int passed = claimsPassedBy(highest);
        assertThat(passed)
                .describedAs("the run was killed before its bound reached any claim, so there is nothing "
                        + "to hold it to; report was %s", report)
                .isEqualTo(NestCrashHarness.CLAIMS);

        // First half: what the frontier was allowed past is on the disk of a process that is now gone.
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            KeyedStateStore cold = new MongoKeyedStateStore(
                    client.getDatabase(MongoProperties.DEFAULT_OPERATOR_STATE_DATABASE)
                            .getCollection(MongoStorePort.OPERATOR_STATE));
            long stored = 0;
            for (String namespace : namespaces(report)) {
                stored += cold.count(namespace);
            }
            assertThat(stored)
                    .describedAs("the bound was let past %d claims, so that many writes must have come "
                            + "back from the store before the kill; %d are there", passed, stored)
                    .isGreaterThanOrEqualTo(passed);
        }

        // Second half: and it is state, not just bytes - a new process assembles the document out of it.
        Path recoverReport = work.resolve("recover.report");
        Files.createFile(recoverReport);
        Process recovering = fork(recoverReport, "recover");
        // Longer than the deadline the run holds itself to, or this fires first and takes the report with it -
        // the run's own verdict on what it recovered is the evidence, and killing it to ask is not asking.
        assertThat(recovering.waitFor(5, TimeUnit.MINUTES))
                .describedAs("the run over the surviving state never finished; report was %s, and the run "
                        + "itself said %s", readQuietly(recoverReport), troubleFrom("recover"))
                .isTrue();

        List<String> recoverLines = Files.readAllLines(recoverReport);
        assertThat(recovered(recoverLines))
                .describedAs("every claim the killed process had been allowed to forget came back out of "
                        + "the store and into the document; what was assembled was %s, and the run itself "
                        + "said %s", linesStartingWith(recoverLines, "document="), troubleFrom("recover"))
                .containsExactlyElementsOf(everyClaim());
    }

    // ---- driving and reading the run under test -----------------------------------------

    /**
     * Waits until the report says the bound has passed every claim. Under the ordering being witnessed that
     * cannot happen until every one of the slow writes has returned, which is exactly why the kill is taken
     * here and not on a timer.
     */
    private static void awaitBoundPastEveryClaim(Path report, Process running) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (System.nanoTime() < deadline) {
            if (!running.isAlive()) {
                throw new AssertionError("the run under test exited on its own; report was "
                        + Files.readAllLines(report));
            }
            if (claimsPassedBy(highestBound(Files.readAllLines(report))) == NestCrashHarness.CLAIMS) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        throw new AssertionError("the bound never reached the claims; report was "
                + Files.readAllLines(report));
    }

    private Process fork(Path report, String mode) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        ProcessBuilder builder = new ProcessBuilder(java, "-cp", classpath,
                NestCrashHarness.class.getName(),
                REPLICA_SET.getReplicaSetUrl(),
                report.toString(),
                mode,
                String.valueOf(SAVE_DELAY_MILLIS));
        builder.redirectErrorStream(true);
        builder.redirectOutput(work.resolve(mode + ".log").toFile());
        return builder.start();
    }

    /** How many claims a bound of {@code value} has been let past. */
    private static int claimsPassedBy(long value) {
        int passed = 0;
        for (int index = 0; index < NestCrashHarness.CLAIMS; index++) {
            if (NestCrashHarness.boundPast(index) <= value) {
                passed++;
            }
        }
        return passed;
    }

    private static long highestBound(List<String> report) {
        long highest = Long.MIN_VALUE;
        for (String line : report) {
            if (line.startsWith(NestCrashHarness.BOUND_LINE)) {
                highest = Math.max(highest,
                        Long.parseLong(line.substring(NestCrashHarness.BOUND_LINE.length()).trim()));
            }
        }
        return highest;
    }

    private static List<String> namespaces(List<String> report) {
        List<String> namespaces = new ArrayList<>();
        for (String line : report) {
            if (line.startsWith(NestCrashHarness.NAMESPACE_LINE)) {
                namespaces.add(line.substring(NestCrashHarness.NAMESPACE_LINE.length()).trim());
            }
        }
        return namespaces;
    }

    private static List<Integer> recovered(List<String> report) {
        List<Integer> claims = new ArrayList<>();
        for (String line : report) {
            if (line.startsWith(NestCrashHarness.RECOVERED_LINE)) {
                claims.add(Integer.parseInt(line.substring(NestCrashHarness.RECOVERED_LINE.length()).trim()));
            }
        }
        return claims;
    }

    /**
     * What the forked run itself said, which is the one thing a failure here has never carried.
     *
     * <p>The report holds the run's answers; this holds its own account of what happened while it was
     * answering, and when a run answers with less than it should it is the second that says why. It is written
     * beside the report and goes away with the working directory, so a failure that does not carry it leaves
     * nothing to read afterwards - measured, at the cost of a whole session spent deriving a cause that had
     * been printed and thrown away.
     *
     * <p>Whatever names a fault, or the tail of it when nothing does: enough to say what went wrong without
     * carrying a run's entire output into an assertion message.
     */
    private List<String> troubleFrom(String mode) {
        List<String> said = readQuietly(work.resolve(mode + ".log"));
        List<String> trouble = new ArrayList<>();
        for (String line : said) {
            if (line.contains("SEVERE") || line.contains("Exception") || line.contains("Caused by")
                    || line.contains("ERROR")) {
                trouble.add(line);
            }
        }
        List<String> evidence = trouble.isEmpty() ? said : trouble;
        return evidence.size() <= EVIDENCE_LINES
                ? evidence
                : evidence.subList(evidence.size() - EVIDENCE_LINES, evidence.size());
    }

    /** The evidence a failure needs, kept out of the thousands of bound lines around it. */
    private static List<String> linesStartingWith(List<String> report, String prefix) {
        List<String> matching = new ArrayList<>();
        for (String line : report) {
            if (line.startsWith(prefix)) {
                matching.add(line);
            }
        }
        return matching;
    }

    private static List<Integer> everyClaim() {
        List<Integer> claims = new ArrayList<>();
        for (int index = 0; index < NestCrashHarness.CLAIMS; index++) {
            claims.add(index);
        }
        return claims;
    }

    private static List<String> readQuietly(Path file) {
        try {
            return Files.readAllLines(file);
        } catch (IOException e) {
            return List.of("<unreadable: " + e.getMessage() + ">");
        }
    }
}
