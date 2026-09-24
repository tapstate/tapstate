package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A source is tailed by one process through one tail, however many members are in the cluster and
 * however many pipelines read it.
 *
 * <p>This is the refutable form of the promise that a cluster is not a multiplier on the databases it
 * reads. Two members each running the capture would double every read the source serves, and on a
 * production database that is the difference between a tool and an incident - while on the target it
 * is invisible, because a sink keyed on the discovered key absorbs the second copy of every row into
 * the same final state. A case that counted rows at the target would pass at full marks against
 * exactly the defect this is about, which is why the count is taken at the far end instead.
 *
 * <p><b>What is not promised, and why saying so is part of the case.</b> The cluster is not a
 * multiplier; the number of pipelines still is, for the initial load alone. A load is owed per
 * pipeline and asked of that pipeline's own record, so a second pipeline over a source already being
 * tailed loads what it has never loaded - deliberately, because a chain leaves the table subset out
 * of its identity and a pipeline that trusted the chain's answer would skip a load it never did. So
 * the tail is shared and the load is not, and the load runs where the pipeline is driven: on the member
 * holding the capture beside its tail, on the other member without one. How a pipeline reads is its own
 * settings' to say and never its member's, so the second pipeline loads exactly once wherever it lands,
 * and the processes that read the source can be as many as the pipelines - while the process that tails
 * it stays one. An earlier form of this case held the second pipeline to a load only where it shared
 * the holder's member, and took the other member reading nothing at all for the design: that was a
 * pipeline left without its rows, healthy and silent, which the harness's connector hid by replaying
 * its whole table through the ring on every run.
 *
 * <p><b>What counts as a read, and who counts it.</b> The harness's own connector notes every batch of
 * rows it hands over into a ledger of its own, one file per process. Nothing in the product is asked,
 * so a product that believed it was reading once would not be believed here; and a second reader
 * cannot hide, because it reads through a connector of its own that writes its own file. A batch
 * handed over is the unit rather than a poll, so the number does not grow with how long the case runs.
 *
 * <p><b>The numbers it is held to are measured, not written down.</b> One member running the same
 * pipeline over the same rows is run first, and what it read is the figure the cluster is held to. A
 * literal would have to be re-derived by hand whenever the connector's batching changed, and the first
 * person to update it would have no way to tell a legitimate change from the defect. The process that
 * tails the source and the tails it opened are held to it exactly, and so are the loads, at one per
 * pipeline; the processes that read the source at all are held to a bound of one per pipeline, because
 * a load runs where its pipeline is driven and that is the product's decision rather than a promise to
 * this case.
 *
 * <p><b>Two fences hold this up, and the case exercises them in turn.</b> A pipeline is driven by one
 * member because its actuation is claimed; a source is captured once because the capture itself is
 * claimed, under an identity derived from the source contract rather than from the pipeline. The first
 * fence alone makes one pipeline on two members read once, so a second pipeline over the same source
 * is added afterwards: nothing but the capture claim keeps that one from opening a second tail on the
 * same table, whichever member picks it up.
 *
 * <p>The ledger is read once it has stopped growing rather than at a moment of the case's choosing. A
 * capture is opened before the run is submitted, so by the time a pipeline is RUNNING a second read,
 * if there is going to be one, has already been asked for; waiting for quiet after that is what keeps
 * the reading from being a race between the assertion and the product.
 *
 * <p><b>Bespoke rather than declarative, and registered as such.</b> The specification envelope has no
 * word for a second member, and no matcher that can say how many times a source was read - its whole
 * set reads what is at an endpoint now. Both gaps are written down where gaps are written down.
 */
class CaptureRunsOnOneMemberOnlyIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    private static final String SOURCE_ID = "capture_once_src";
    private static final String BASELINE_PIPELINE = "capture_once_alone";
    private static final String BASELINE_TARGET = "capture_once_alone_tgt";
    private static final String FIRST_PIPELINE = "capture_once_first";
    private static final String FIRST_TARGET = "capture_once_first_tgt";
    private static final String SECOND_PIPELINE = "capture_once_second";
    private static final String SECOND_TARGET = "capture_once_second_tgt";

    private static final String ADMIN = "e2e";
    private static final String PASSWORD = "e2e-password";

    /** How long the ledger must hold still before it is read. See the class note on why it is this long. */
    private static final Duration QUIET = Duration.ofSeconds(5);

    /** Bound on reaching quiet. A bound only decides how quickly a stuck case says so. */
    private static final Duration SETTLE_BOUND = Duration.ofSeconds(120);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void oneSourceIsReadOnceHoweverManyMembersAndPipelinesReadIt(@TempDir Path directory) throws Exception {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        try (FileEndpoints files = new FileEndpoints()) {
            Reads byOneMemberAlone = whatOneMemberReads(directory.resolve("alone"), connector, files);
            assertThat(byOneMemberAlone.tails())
                    .describedAs("what reading this source once looks like, measured rather than "
                            + "assumed - a zero here would make every comparison below vacuous, since "
                            + "a source nothing ever read is also read no more than once: %s",
                            byOneMemberAlone)
                    .isPositive();
            assertThat(byOneMemberAlone.snapshots())
                    .describedAs("and it loaded, so that the bound on loads below bounds something: %s",
                            byOneMemberAlone)
                    .isPositive();

            Path source = Files.createDirectories(directory.resolve("cluster/src"));
            Path ledger = Files.createDirectories(directory.resolve("cluster/reads"));
            Path firstTarget = Files.createDirectories(directory.resolve("cluster/tgt-first"));
            Path secondTarget = Files.createDirectories(directory.resolve("cluster/tgt-second"));
            files.seed(EndpointAddress.uri(source.toString()), TABLE, SeedRows.generated(SEEDED_ROWS));

            String store = SharedMongo.replicaSetUrl("e2e_capture_once_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-capture-once")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                control.discoverSchema(
                        SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, settings(source, ledger));

                Map<String, String> oneReader = new LinkedHashMap<>(sourceResource(source, ledger));
                oneReader.putAll(pipelineResources(FIRST_PIPELINE, FIRST_TARGET, firstTarget));
                control.apply(oneReader);
                control.lifecycle(FIRST_PIPELINE, LifecycleVerb.START);
                awaitRunning(control, FIRST_PIPELINE);
                awaitTheRowsAtTheTarget(files, firstTarget);

                Reads onePipeline = settle(ledger);
                assertThat(onePipeline.readers())
                        .describedAs("one process read the source, exactly as on the single member. A "
                                + "member that captured alongside the one driving the pipeline would "
                                + "make this two, and nothing at either target would show it: %s",
                                onePipeline)
                        .isEqualTo(byOneMemberAlone.readers());
                assertThat(onePipeline.tails())
                        .describedAs("and one tail was opened on it, not one per member: %s", onePipeline)
                        .isEqualTo(byOneMemberAlone.tails());
                assertThat(onePipeline.snapshots())
                        .describedAs("and it was loaded once. One pipeline owes one load however many "
                                + "members could have run it: %s", onePipeline)
                        .isEqualTo(byOneMemberAlone.snapshots());

                // The whole workspace, not the two new documents alone: what is applied is read as the
                // workspace entire, so a pipeline arriving without the source it names is refused for
                // naming nothing. The first pipeline's documents are unchanged and it keeps running,
                // which is asserted below rather than assumed.
                Map<String, String> twoReaders = new LinkedHashMap<>(oneReader);
                twoReaders.putAll(pipelineResources(SECOND_PIPELINE, SECOND_TARGET, secondTarget));
                control.apply(twoReaders);
                control.lifecycle(SECOND_PIPELINE, LifecycleVerb.START);
                awaitRunning(control, SECOND_PIPELINE);
                assertThat(control.state(FIRST_PIPELINE))
                        .describedAs("the first pipeline is still running, so what is counted below is "
                                + "two readers of one source rather than one that replaced the other")
                        .contains(PipelineState.RUNNING);

                Reads twoPipelines = settle(ledger);
                assertThat(twoPipelines.tailers())
                        .describedAs("a second pipeline over the same source brought no second process "
                                + "tailing it. Its capture's identity is the source contract's, not the "
                                + "pipeline's, so whichever member picked this one up joins the capture "
                                + "already running rather than standing up one of its own: %s",
                                twoPipelines)
                        .isEqualTo(byOneMemberAlone.tailers());
                assertThat(twoPipelines.tails())
                        .describedAs("and opened no second tail on it, which is the half of this that "
                                + "is shared between pipelines rather than owed by each: %s",
                                twoPipelines)
                        .isEqualTo(byOneMemberAlone.tails());
                assertThat(twoPipelines.snapshots())
                        .describedAs("and loaded exactly once more, wherever it landed - a load per "
                                + "pipeline is owed, so one fewer is a pipeline missing the rows that "
                                + "were there before it, and one more is a source re-read for nothing: %s",
                                twoPipelines)
                        .isEqualTo(2 * byOneMemberAlone.snapshots());
                assertThat(twoPipelines.readers())
                        .describedAs("so the processes reading it are at most one per pipeline - the one "
                                + "tailing it, and the one the second pipeline is driven by if that is "
                                + "another: %s", twoPipelines)
                        .isBetween(byOneMemberAlone.readers(), 2 * byOneMemberAlone.readers());

                Map<String, String> behindTheSecond = control.captureOwnersOf(SECOND_PIPELINE);
                assertThat(behindTheSecond)
                        .describedAs("and the cluster says so itself: both pipelines name one capture, "
                                + "held by one member. Without this the count above would also be "
                                + "satisfied by a second pipeline that never got as far as reading")
                        .isEqualTo(control.captureOwnersOf(FIRST_PIPELINE))
                        .hasSize(1);
                assertThat(behindTheSecond.values())
                        .describedAs("the capture is held by a member of this cluster")
                        .containsAnyOf(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            }
        }
    }

    /**
     * Runs the same pipeline over the same rows on a single member, and answers what the source served.
     *
     * <p>Its own store, its own directories and its own pipeline id: the state a run keeps is filed
     * under the pipeline, and the ring it fills under the source's settings, so sharing either with the
     * cluster half would have the two halves reading each other's leavings.
     */
    private static Reads whatOneMemberReads(Path root, byte[] connector, FileEndpoints files)
            throws IOException {
        Path source = Files.createDirectories(root.resolve("src"));
        Path ledger = Files.createDirectories(root.resolve("reads"));
        Path target = Files.createDirectories(root.resolve("tgt"));
        files.seed(EndpointAddress.uri(source.toString()), TABLE, SeedRows.generated(SEEDED_ROWS));

        String store = SharedMongo.replicaSetUrl("e2e_capture_once_one_member");
        try (RealProcessServer server = RealProcessServer.start(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(ADMIN, PASSWORD);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, settings(source, ledger));

            Map<String, String> resources = new LinkedHashMap<>(sourceResource(source, ledger));
            resources.putAll(pipelineResources(BASELINE_PIPELINE, BASELINE_TARGET, target));
            control.apply(resources);
            control.lifecycle(BASELINE_PIPELINE, LifecycleVerb.START);
            awaitRunning(control, BASELINE_PIPELINE);
            awaitTheRowsAtTheTarget(files, target);
            return settle(ledger);
        }
    }

    /** How many batches of rows the source has handed over, across every process that read it. */
    private static long reads(Path ledger) {
        return ledgerLines(ledger).size();
    }

    /**
     * What the source's own ledger says happened: who read it, and which phases they opened.
     *
     * <p>Three numbers rather than one total, because the three are not promised alike. How many
     * processes read the source, and how many tails were opened on it, are what a capture claim exists
     * to hold at one however large the cluster gets. How many initial loads ran is not - a load is owed
     * per pipeline. A single total adds the three together and then cannot say which of them moved,
     * which is how the earlier form of this case came to assert a number that depended on where the
     * second pipeline landed: it failed 5 times in 28 runs, every one of them on a load that was owed.
     */
    private record Reads(int readers, int tailers, long snapshots, long tails, List<String> lines) {

        private static final String SNAPSHOT = "snapshot";
        private static final String TAIL = "tail";

        /** Parses the ledger the harness's own connector writes: pid, phase, table, rows, tab-separated. */
        static Reads of(List<String> lines) {
            Set<String> readers = new LinkedHashSet<>();
            Set<String> tailers = new LinkedHashSet<>();
            long snapshots = 0;
            long tails = 0;
            for (String line : lines) {
                String[] fields = line.split("\t");
                readers.add(fields[0]);
                if (fields.length > 1 && SNAPSHOT.equals(fields[1])) {
                    snapshots++;
                } else if (fields.length > 1 && TAIL.equals(fields[1])) {
                    tailers.add(fields[0]);
                    tails++;
                }
            }
            return new Reads(readers.size(), tailers.size(), snapshots, tails, List.copyOf(lines));
        }

        @Override
        public String toString() {
            return readers + " process(es), " + tailers + " of them tailing, " + snapshots + " load(s), "
                    + tails + " tail(s) in " + lines.size() + " batch(es): " + lines;
        }
    }

    /**
     * Waits until the ledger stops growing and answers what it then held.
     *
     * <p>Quiet rather than a moment chosen by the case: a read that is going to happen has been asked
     * for by the time the pipeline that wants it is running, but it is served on the capture's own
     * thread, and a count taken the instant the state flips would be a race the product usually wins.
     */
    private static Reads settle(Path ledger) {
        long[] last = {reads(ledger)};
        long[] since = {System.nanoTime()};
        Await.until("the source's own ledger of reads to stop growing", SETTLE_BOUND,
                () -> {
                    long now = reads(ledger);
                    if (now != last[0]) {
                        last[0] = now;
                        since[0] = System.nanoTime();
                        return false;
                    }
                    return System.nanoTime() - since[0] >= QUIET.toNanos();
                },
                () -> ledgerDump(ledger));
        return Reads.of(ledgerLines(ledger));
    }

    /** Every line the source's readers wrote, so a failure says who read and what they took. */
    private static List<String> ledgerLines(Path ledger) {
        try (Stream<Path> files = Files.list(ledger)) {
            List<String> lines = new ArrayList<>();
            for (Path file : files.sorted().toList()) {
                lines.addAll(Files.readAllLines(file));
            }
            return lines;
        } catch (IOException unreadable) {
            throw new UncheckedIOException("could not read the source's ledger at " + ledger, unreadable);
        }
    }

    /**
     * The ledger as a failure should print it: every line, each naming the process that wrote it.
     *
     * <p>Which is what tells the two defects apart. One process reading twice and two processes reading
     * once produce the same total and are not the same thing - the first is a capture opened twice on
     * one member, the second is a cluster multiplying its reads by its size.
     */
    private static String ledgerDump(Path ledger) {
        List<String> lines = ledgerLines(ledger);
        return lines.size() + " batch(es) handed over: " + lines;
    }

    private static void awaitRunning(ControlPlane control, String pipelineId) {
        Await.until(
                pipelineId + " to reach " + PipelineState.RUNNING,
                () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    /**
     * Waits for the seeded rows to cross, which is what says the source was read at all.
     *
     * <p>Before any counting: "read no more than once" is satisfied by a run that read nothing, and a
     * run that never started reads nothing in exactly the same way a working one reads once.
     */
    private static void awaitTheRowsAtTheTarget(FileEndpoints files, Path target) {
        EndpointAddress address = EndpointAddress.uri(target.toString());
        Await.until(
                "the seeded rows to reach " + target.getFileName(),
                () -> files.count(address, TABLE) >= SEEDED_ROWS,
                () -> "rows at target = " + files.count(address, TABLE));
    }

    private static Map<String, Object> settings(Path source, Path ledger) {
        return Map.of("uri", source.toString(), "read_witness", ledger.toString());
    }

    /**
     * The source both halves read, carrying the directory it reads and the ledger it notes reads in.
     *
     * <p>Written here rather than taken from the shared documents because of that second setting: the
     * witness is what this case is, and a source without it is a source this case cannot count.
     */
    private static Map<String, String> sourceResource(Path source, Path ledger) {
        return Map.of(SOURCE_ID + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s", read_witness: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, source, ledger, TABLE));
    }

    /** A pipeline carrying the source's changes to a target of its own. */
    private static Map<String, String> pipelineResources(String pipelineId, String targetId, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(targetId + ".tap.yml", Workspaces.targetYaml(targetId, target));
        resources.put(pipelineId + ".tap.yml",
                Workspaces.pipelineYaml(pipelineId, SOURCE_ID, targetId, TABLE));
        return resources;
    }
}
