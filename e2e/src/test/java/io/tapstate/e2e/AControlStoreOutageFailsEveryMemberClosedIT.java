package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.control.core.ClusterError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every member loses the coordination store at once, while the cluster itself is untouched.
 *
 * <p>This is the shape a coordination store actually fails in: not one member losing it, but the
 * store going away from everybody -- a failover, a rolling restart, a network that drops the one
 * thing every member depends on and nothing else. The members can still see each other, their
 * processes are healthy, their sources and targets are reachable, and each of them holds a run it was
 * told it owns. The only thing gone is the ability to prove that it still owns it.
 *
 * <p>Failing closed is the whole product here. A member that carries on writing because everything it
 * can see looks fine is a member writing on an authority that expired while it was not looking, and
 * when the store comes back the run it was carrying may well have been taken over. Failing open does
 * not announce itself either: the rows keep arriving, the pipeline reads healthy, and the damage is
 * only visible later, in a target written by two runs at once.
 *
 * <p>So the cut is made where nothing else is touched: the store's own container is frozen, which
 * takes it away from all three members at the same instant and from nothing else. What is asserted is
 * that a change made at the source <em>after</em> the freeze does not reach the target -- a negative
 * that means something only because a change made just <em>before</em> it did cross, and because the
 * held-back one crosses once the store runs again. The member links that carry the cluster's own
 * traffic are asserted to have turned nobody away, so "the members stopped" can never be this case
 * having quietly partitioned them: that is a different case and a different rule.
 *
 * <p>Freezing rather than proxying, and measured rather than chosen: each member was first given its
 * own relay in front of the store's published port, and in two runs the driver lost the store within
 * fifteen seconds of connecting and never got it back, while the same relay carried an ordinary
 * request by hand. Whatever that is, it is not this product, and a case cannot rest on it.
 *
 * <p><strong>What this settles, and what it does not.</strong> It settles that nothing crosses while
 * no member can reach the store, that every process stays up and every member link stays open while
 * that is true, and that the work resumes under exactly one owner once the members are restarted. It
 * does <em>not</em> settle that the fencing rules are what stopped them. Removing the rule that ends
 * a member's cluster participation when its session cannot be renewed leaves this case green;
 * removing that rule and the majority rule together also leaves it green. With the store frozen the
 * engine has nowhere to write its positions either, so "nothing crossed" is overdetermined here, and
 * a case cannot claim more than it can tell apart.
 *
 * <p>The rules are discriminated where they can be: in the unit cases that take the store away from
 * one member while another still reads it, and in the partition case, where the store stays reachable
 * throughout and only the members' view of each other is cut. What this case adds is the scenario
 * neither of those reaches -- every member losing the store at once, with everything else healthy --
 * and the half that exists only on the way back. A member that took itself out stays out, nothing
 * rejoining on its own, so without the restart the question of one owner versus two could not be
 * asked at all.
 */
class AControlStoreOutageFailsEveryMemberClosedIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    /**
     * One change made while everything works, before anything is cut.
     *
     * <p>Without it the cut below asserts nothing. A run that had already died -- of its own accord, or
     * of a defect nothing here is about -- carries no changes either, and "no change crossed" reads the
     * same from a member failing closed and from a pipeline that was not running to begin with. This
     * row is what tells them apart: it has to cross first, and it crosses only if the run is live at
     * the moment the store is taken away.
     */
    private static final long LIVENESS_ROW = 1;

    private static final long ROWS_ADDED_DURING_THE_OUTAGE = 2;

    private static final String SOURCE_ID = "outage_src";
    private static final String TARGET_ID = "outage_tgt";
    private static final String PIPELINE = "outage_pipe";

    /**
     * Bound on the precondition: rows crossing before anything is cut.
     *
     * <p>Three server processes and a container on one machine, and nothing is discriminated here --
     * the case has not done anything yet -- so a tighter bound only buys a red meaning "busy".
     */
    private static final Duration SEEDING = Duration.ofMinutes(3);

    /**
     * How long the cut cluster is watched for a member that writes anyway.
     *
     * <p>Comfortably past the local authorization window, so a member that was going to carry on has
     * had every chance to: the recovery half below shows that the same changes cross in far less once
     * the store is back.
     */
    private static final Duration WATCHED_FOR_A_WRITE = Duration.ofSeconds(90);

    /** Bound on the store coming back and the run resuming. Re-proving a claim is a lease, not a join. */
    private static final Duration RECOVERY = Duration.ofMinutes(4);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void everyMemberStopsWhenNoneOfThemCanReachTheStoreAndAllOfThemResumeWhenItReturns(
            @TempDir Path directory) throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        List<String> nodes = List.of("node-a", "node-b", "node-c");

        try (FileEndpoints files = new FileEndpoints()) {
            EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
            EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
            files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));

            // Its own store, not the module's shared one: this case takes the store away, and taking
            // away something other cases are using is a different kind of failure entirely.
            try (NetworkedMongo store = NetworkedMongo.start()) {
                try (PartitionableCluster cluster = PartitionableCluster.start(
                        store.uriForThisHost("e2e_store_outage"), "e2e-store-outage", nodes)) {
                    cluster.awaitMembers("node-a", 3);
                    ControlPlane control = cluster.member("node-a");
                    start(control, connector, source, target);
                    Await.until("the seeded rows to cross before the store is cut", SEEDING,
                            () -> files.count(targetAddress, TABLE) >= SEEDED_ROWS,
                            () -> seeding(control, files.count(targetAddress, TABLE)));

                    // Live at the moment the store is taken away, proved by a change made now.
                    files.insert(sourceAddress, TABLE, rows(SEEDED_ROWS, LIVENESS_ROW));
                    Await.until("a change made while everything works to cross", SEEDING,
                            () -> files.count(targetAddress, TABLE) >= SEEDED_ROWS + LIVENESS_ROW,
                            () -> seeding(control, files.count(targetAddress, TABLE)));

                    long rowsAtTheCut = files.count(targetAddress, TABLE);
                    store.freeze();
                    files.insert(sourceAddress, TABLE,
                            rows(SEEDED_ROWS + LIVENESS_ROW, ROWS_ADDED_DURING_THE_OUTAGE));

                    assertThatThrownBy(() -> Await.until(
                            "a change made while the store is unreachable to reach the target",
                            WATCHED_FOR_A_WRITE,
                            () -> files.count(targetAddress, TABLE) > rowsAtTheCut,
                            () -> "rows at target = " + files.count(targetAddress, TABLE)))
                            .describedAs("no member may write for a run none of them can still prove it "
                                    + "owns, however healthy everything each of them can see looks")
                            .isInstanceOf(AssertionError.class)
                            .hasMessageContaining("timed out");

                    // Failing closed, not falling over, and not quietly carrying on either: every
                    // process is up, nothing between the members was ever cut, and every member has
                    // taken itself out of the cluster rather than act on an authority it can no longer
                    // prove.
                    for (String nodeId : nodes) {
                        assertThat(cluster.isAlive(nodeId))
                                .describedAs("%s is still running -- a member that fails closed stays up "
                                        + "and refuses, it does not exit", nodeId)
                                .isTrue();
                        assertThat(cluster.linkTo(nodeId).turnedAway())
                                .describedAs("nothing between the members was ever cut in front of %s, so "
                                        + "this is a store outage and not a partition", nodeId)
                                .isZero();
                        assertThat(clusterFaceRefusalOn(cluster, nodeId))
                                .describedAs("%s stops answering for the cluster rather than answering "
                                        + "from what it last remembers -- which is the read face's half "
                                        + "of the same rule, and is not evidence about why it stopped. "
                                        + "It says so in a code: measured here as io.store-unavailable, "
                                        + "the durable half of the answer being what it cannot reach",
                                        nodeId)
                                .isPresent();
                    }
                    assertThat(cluster.unattributedConnections())
                            .describedAs("and no member connection arrived from a port nobody claims")
                            .isZero();

                    // Put the store back, so what is left behind is a cluster that lost its store and
                    // got it back rather than one still missing it -- and so the case ends having shown
                    // the outage was the store and nothing else.
                    store.thaw();
                    Await.until("the store to answer these members again", RECOVERY,
                            () -> control.state(PIPELINE).isPresent(),
                            () -> "the read face says " + stateOn(control));

                    // With the store answering and nothing rejoined yet, this read reaches the engine
                    // half for the only stretch in this case where it can: the durable half of the
                    // answer is readable again, and the member that would supply the live half shut its
                    // own engine down when its session lapsed and does not restart it. What it must not
                    // answer here is a page carrying no code, which a caller cannot tell from the
                    // product having fallen over -- nor the reading that is worse still, a plain 200
                    // saying this member is in a cluster of nobody.
                    for (String nodeId : nodes) {
                        assertThat(cluster.member(nodeId).clusterReadRefusal())
                                .describedAs("%s says in a code that it cannot read the cluster, rather "
                                        + "than answering an empty one and rather than refusing without "
                                        + "saying why", nodeId)
                                .contains(ClusterError.MEMBERSHIP_UNREADABLE.code());
                    }

                    // Nothing comes back on its own, so the way back is a restart -- and what has to
                    // hold on the way back is that the run resumes under exactly one owner. Keeping a
                    // second one from ever existing is the whole reason for stopping in the first
                    // place, and it is the half a case that only watched the outage never sees.
                    nodes.forEach(cluster::restart);
                    cluster.awaitMembers("node-a", nodes.size());
                    ControlPlane back = cluster.member("node-a");
                    if (back.state(PIPELINE).filter(PipelineState.RUNNING::equals).isEmpty()) {
                        back.lifecycle(PIPELINE, LifecycleVerb.START);
                    }
                    Await.until("the changes made during the outage to cross once the members are back",
                            RECOVERY,
                            () -> files.count(targetAddress, TABLE)
                                    >= SEEDED_ROWS + LIVENESS_ROW + ROWS_ADDED_DURING_THE_OUTAGE,
                            () -> seeding(back, files.count(targetAddress, TABLE)));
                    assertThat(back.captureOwnersOf(PIPELINE).values().stream().distinct().toList())
                            .describedAs("the run came back under one owner: a member that had acted "
                                    + "while it could not prove its claim would be a second one here")
                            .hasSize(1);
                }
            }
        }
    }

    /**
     * The code one member refuses the cluster read with, or nothing when it answered after all.
     *
     * <p>Two different paths refuse on this face and the code is what tells them apart: while the
     * store is away the durable half of the answer cannot be read, and once it is back the member
     * that shut its own engine down cannot supply the live half. Asserting that something was thrown
     * would be satisfied by either, and by an uncoded page as well -- and an uncoded page is the one
     * answer a caller cannot tell from this member having fallen over. So the reading is the code,
     * and a refusal carrying none fails inside this rather than being counted as one.
     */
    private static Optional<String> clusterFaceRefusalOn(PartitionableCluster cluster, String nodeId) {
        return cluster.member(nodeId).clusterReadRefusal();
    }

    /** {@code count} further generated rows, continuing after the {@code alreadyThere} already written. */
    private static List<Map<String, Object>> rows(long alreadyThere, long count) {
        return SeedRows.generated(alreadyThere + count)
                .subList((int) alreadyThere, (int) (alreadyThere + count));
    }

    private static String stateOn(ControlPlane plane) {
        try {
            return String.valueOf(plane.state(PIPELINE));
        } catch (RuntimeException | AssertionError refused) {
            // A diagnosis that throws replaces the failure it was written to explain. The read face
            // refuses an unreachable store with an assertion of its own, which is exactly when this
            // is called.
            return "refused:" + refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }

    private static String ownersOn(ControlPlane plane) {
        try {
            return String.valueOf(plane.captureOwnersOf(PIPELINE).values());
        } catch (RuntimeException | AssertionError refused) {
            // A diagnosis that throws replaces the failure it was written to explain. The read face
            // refuses an unreachable store with an assertion of its own, which is exactly when this
            // is called.
            return "refused:" + refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }

    private static String failureOn(ControlPlane plane) {
        try {
            return String.valueOf(plane.failureCode(PIPELINE));
        } catch (RuntimeException | AssertionError refused) {
            // A diagnosis that throws replaces the failure it was written to explain. The read face
            // refuses an unreachable store with an assertion of its own, which is exactly when this
            // is called.
            return "refused:" + refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }

    /** What a wait reports when nothing crosses, so a reader is not sent to the machine's load first. */
    private static String seeding(ControlPlane control, long rows) {
        return "rows at target = " + rows + "; the pipeline is " + stateOn(control)
                + ", its failure code is " + failureOn(control)
                + ", its captures are owned by " + ownersOn(control);
    }

    private static void start(ControlPlane control, byte[] connector, Path source, Path target) {
        control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
        control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
        control.apply(resources(source, target));
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)));
    }

    private static Map<String, String> resources(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, source));
        resources.put(TARGET_ID + ".tap.yml", Workspaces.targetYaml(TARGET_ID, target));
        resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE_ID, TARGET_ID, TABLE));
        return resources;
    }
}
