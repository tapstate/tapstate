package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A node that comes back under its own stable id waits out the session its dead boot left behind, and
 * then replaces that boot rather than joining beside it.
 *
 * <p>This is the other half of a stable id being stable. One half is that two live processes cannot
 * share one - which is a case of its own. This half is harder to see and easier to get wrong: the
 * process holding the id is <em>gone</em>, every read face has already stopped listing it, and the only
 * thing still standing between its id and the next process to ask for it is a lease nobody is renewing.
 * So the refusal asserted here cannot be explained away by the old member still being there. The case
 * proves it is not there first, and only then tries to take its place.
 *
 * <p><b>Why a restart has to be kept out at all</b>, when the node it is restarting is its own. Because
 * the store cannot tell a restart from an impostor, and must not try. What a coordination store knows
 * is that something claiming to be this node is alive and holding claims; a second process saying "that
 * was me, I am back" is exactly what a split-brained member would also say. The lease is what converts
 * that unanswerable question into a waitable one - and the price of the conversion is that an honest
 * restart waits too. A build that let the honest restart in early would be a build with no answer for
 * the dishonest one.
 *
 * <p><b>Restarted rather than slept.</b> What a supervisor does with a process that exited non-zero is
 * start it again, so that is what this does: it is turned away, it comes back, and eventually it is
 * admitted. Sizing a sleep to the lease instead would make the case assert the clock - it would pass on
 * a build that never refused anything, and fail on a machine that was merely slow.
 *
 * <p><b>What tells a replacement from a second member.</b> Three things about the readmitted process
 * are new - the boot it is on, the engine identity it carries, and the address it advertises - while
 * two are not: its stable node id, and the cluster it belongs to. Asserting both sides is what makes
 * this refutable in both directions. A build that treated the stable id as the runtime identity fails
 * on the id having drifted; a build that only forgot the old member in memory fails on there being two
 * entries under one id, which is the state nothing downstream can unpick.
 *
 * <p><b>What this case cannot witness, and where that lives instead.</b> The old boot not renewing its
 * claims afterwards is not observable here, because the old boot is not running - it was killed, which
 * is the point. That the generation moves on so a returning holder could not renew is held at the store
 * contract by {@code WorkloadClaimStoreIT}, and a live member that keeps acting after losing its lease
 * is the subject of the fencing cases rather than of this one.
 *
 * <p><b>Bespoke rather than declarative.</b> Same registered gap as the other member cases: the
 * specification envelope has no word for a Tapstate member at all, let alone for one being restarted.
 */
class AStableNodeRestartReplacesOnlyItsExpiredBootSessionIT {

    /**
     * The lease this case sizes on purpose, because it is the window the case lives inside.
     *
     * <p>Wide enough that a restart launched right after the kill reaches its own preflight while the
     * dead boot's session is still unambiguously live - a JVM start is seconds, and this leaves tens of
     * them - and narrow enough that waiting the session out is not most of the run.
     */
    private static final Duration NODE_SESSION = Duration.ofSeconds(60);

    /** A killed process drops its connections rather than timing out, so departure is quick. */
    private static final Duration DEPARTURE_BUDGET = Duration.ofSeconds(60);

    /** A refused boot fails in its own preflight: this bounds a process start, not a lease. */
    private static final Duration REFUSAL_BUDGET = Duration.ofSeconds(120);

    /** One restart attempt: long enough to start and be answered, far short of the lease. */
    private static final Duration ATTEMPT_BUDGET = Duration.ofSeconds(60);

    /** Comfortably past the lease, so a case that never gets in says "never" rather than "not yet". */
    private static final Duration READMISSION_BUDGET = NODE_SESSION.multipliedBy(4);

    /** Committing a membership change is a round trip on its own cadence, not an instant. */
    private static final Duration COMMIT_BUDGET = Duration.ofSeconds(60);

    /** A member joins the cluster before its own read face is listening. See where this is used. */
    private static final Duration SERVING_BUDGET = Duration.ofSeconds(60);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRestartedNodeIsRefusedUntilItsOwnDeadSessionExpiresAndThenReplacesIt() {
        String store = SharedMongo.replicaSetUrl("e2e_stable_node_restart");

        try (TwoMemberCluster cluster = TwoMemberCluster.start(
                store, "e2e-stable-node-restart", NODE_SESSION)) {
            cluster.awaitBothMembers();

            // Asked of the member that survives all of this. The one being restarted cannot be the
            // witness to its own restart: its read face goes away in the middle.
            ControlPlane survivor = cluster.second();
            String cluster0 = survivor.clusterId();
            ClusterMemberFacts deadBoot = theMemberCalled(survivor, TwoMemberCluster.NODE_A);

            // Killed rather than closed. A member that shuts down releases its session on the way out,
            // and the id is then free immediately - which is correct, and is not this case: what is
            // being tested is the id of a node that did not get to say goodbye.
            cluster.processCarrying(TwoMemberCluster.NODE_A).kill();

            // Read patiently: while the survivor settles after losing the other member it can refuse to say
            // who is left, with its code, and that refusal is on the way to the departure this waits for.
            // Any other answer that is not a membership still fails here.
            Await.until("the killed member to leave the cluster the other one reports", DEPARTURE_BUDGET,
                    () -> survivor.clusterMemberNodeIdsIfAnswered()
                            .filter(nodeIds -> !nodeIds.contains(TwoMemberCluster.NODE_A))
                            .isPresent(),
                    () -> "the membership was still " + survivor.clusterMemberNodeIdsIfAnswered()
                            .map(String::valueOf)
                            .orElse("unanswered, with the code that says to ask again"));

            List<RealProcessServer> restarts = new ArrayList<>();
            try {
                // The first restart, with the session it left behind still live. Everything readable
                // says this id is free; the lease says it is not, and the lease is what decides.
                RealProcessServer tooSoon = cluster.launching(TwoMemberCluster.NODE_A);
                restarts.add(tooSoon);
                Await.until("the restart to be turned away by the session its own dead boot left",
                        REFUSAL_BUDGET,
                        () -> !tooSoon.isAlive(),
                        () -> "it is still running, so the id was handed out while its lease was still "
                                + "standing; it said:\n" + tooSoon.tail());
                assertThat(tooSoon.exitValue())
                        .describedAs("the status it exited with. A supervisor reads zero as \"finished\" "
                                + "and stops restarting, which is how a node that only needed to wait "
                                + "never comes back at all")
                        .isNotZero();
                assertThat(outputOf(tooSoon))
                        .describedAs("and it says which id it was turned away over, so an operator "
                                + "reading the restart loop can tell waiting from misconfiguration")
                        .contains("boot.node-id-in-use")
                        .contains(TwoMemberCluster.NODE_A);

                RealProcessServer admitted = restartUntilAdmitted(cluster, survivor, restarts);
                assertThat(admitted)
                        .describedAs("the node got back in within %s of its session being left behind. "
                                + "A lease that outlived this would not be a lease, it would be a "
                                + "node id retired by one crash", READMISSION_BUDGET)
                        .isNotNull();

                // Committed, not merely seen. The engine's list has the new process in it as soon as it
                // joins; the committed set is agreed through the coordination store on its own cadence,
                // and a node that is back in the first but never reaches the second is back without
                // being eligible for any work - which looks like a recovery and is not one.
                Await.until("the restarted member to be committed into the active set", COMMIT_BUDGET,
                        () -> survivor.clusterMembers().stream()
                                .anyMatch(member -> TwoMemberCluster.NODE_A.equals(member.nodeId())
                                        && "ACTIVE".equals(member.state())),
                        () -> "the members were " + survivor.clusterMembers());

                List<ClusterMemberFacts> underTheSameId = survivor.clusterMembers().stream()
                        .filter(member -> TwoMemberCluster.NODE_A.equals(member.nodeId()))
                        .toList();
                assertThat(underTheSameId)
                        .describedAs("one entry under the restarted id, not two. A build that removed "
                                + "the old member from memory and left its session standing would have "
                                + "both boots here, and no reader of a claim could say which one it "
                                + "names: %s", survivor.clusterMembers())
                        .hasSize(1);

                ClusterMemberFacts newBoot = underTheSameId.get(0);
                assertThat(newBoot.bootId())
                        .describedAs("a new boot of the same node. The boot id is what a claim records "
                                + "as its owner alongside the stable id, so a restart that kept the old "
                                + "one would let the dead boot's claims be renewed by the new process")
                        .isNotNull()
                        .isNotEqualTo(deadBoot.bootId());
                assertThat(newBoot.memberUuid())
                        .describedAs("and a new engine identity - which is precisely the thing that "
                                + "must never be mistaken for the node's own id, because it changes "
                                + "every time and the node id must not")
                        .isNotNull()
                        .isNotEqualTo(deadBoot.memberUuid());
                assertThat(newBoot.controlUrl())
                        .describedAs("the address it advertises is this boot's own, which a restart is "
                                + "allowed to change - and that it did change is also what says this "
                                + "entry is the new process rather than the old record still standing")
                        .isNotNull()
                        .isNotEqualTo(deadBoot.controlUrl());

                assertThat(survivor.clusterMemberNodeIds())
                        .describedAs("by stable id the cluster is the two it started as. That is the "
                                + "whole promise: a node that restarts is the same node")
                        .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(survivor.clusterId())
                        .describedAs("and it came back to the same cluster")
                        .isEqualTo(cluster0);

                // Asked of the new arrival itself, not only of the member that watched it arrive. A
                // process listed by somebody else has been seen; a process that answers this has joined.
                //
                // Waited for rather than asked once: this process was launched without waiting for
                // health, because what it was launched to find out was whether it would be let in at
                // all. It joins the cluster while its own read face is still coming up, so being in the
                // membership is exactly what it does before it can answer for itself.
                ControlPlane readmitted = awaitSignedInAt(cluster, admitted);
                assertThat(readmitted.clusterMemberNodeIds())
                        .describedAs("and the restarted member answers the same as the one that stayed, "
                                + "rather than standing up a cluster of its own beside it")
                        .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(readmitted.clusterId())
                        .describedAs("out of the same cluster identity")
                        .isEqualTo(cluster0);
            } finally {
                restarts.forEach(RealProcessServer::close);
            }
        }
    }

    /**
     * Restarts the node until one attempt is let in, the way a supervisor would, and answers with it.
     *
     * <p>Each attempt is waited on until it either joins or exits, so nothing here is timed against the
     * lease - the lease is what the attempts are discovering. An attempt that exits is required to have
     * exited over the id: a restart loop that was being stopped by something else entirely would
     * otherwise read exactly like one patiently waiting its turn.
     *
     * @return the attempt that became a member, or null if none did within the budget
     */
    private static RealProcessServer restartUntilAdmitted(
            TwoMemberCluster cluster, ControlPlane survivor, List<RealProcessServer> restarts) {
        long givingUp = System.nanoTime() + READMISSION_BUDGET.toNanos();
        while (System.nanoTime() < givingUp) {
            RealProcessServer attempt = cluster.launching(TwoMemberCluster.NODE_A);
            restarts.add(attempt);
            Await.until("this restart to either join the cluster or be turned away", ATTEMPT_BUDGET,
                    () -> !attempt.isAlive()
                            || survivor.clusterMemberNodeIds().contains(TwoMemberCluster.NODE_A),
                    () -> "it was still starting; it said:\n" + attempt.tail());
            if (attempt.isAlive()) {
                return attempt;
            }
            assertThat(outputOf(attempt))
                    .describedAs("every restart that was turned away was turned away over its own id. "
                            + "Without this, a build where the node could never start again would "
                            + "produce the same patient-looking loop")
                    .contains("boot.node-id-in-use");
        }
        return null;
    }

    /** Signs in on a process that has joined but may not be serving yet, and answers when it does. */
    private static ControlPlane awaitSignedInAt(TwoMemberCluster cluster, RealProcessServer member) {
        AtomicReference<ControlPlane> signedIn = new AtomicReference<>();
        AtomicReference<String> refusal = new AtomicReference<>("it was not asked yet");
        Await.until("the readmitted member's own read face to answer", SERVING_BUDGET,
                () -> {
                    try {
                        signedIn.set(cluster.signedInAt(member));
                        return true;
                    } catch (RuntimeException | AssertionError notServingYet) {
                        refusal.set(String.valueOf(notServingYet));
                        return false;
                    }
                },
                refusal::get);
        return signedIn.get();
    }

    private static ClusterMemberFacts theMemberCalled(ControlPlane control, String nodeId) {
        List<ClusterMemberFacts> named = control.clusterMembers().stream()
                .filter(member -> nodeId.equals(member.nodeId()))
                .toList();
        assertThat(named)
                .describedAs("the premise: exactly one member is %s before anything is killed", nodeId)
                .hasSize(1);
        return named.get(0);
    }

    private static String outputOf(RealProcessServer server) {
        try {
            return Files.readString(server.output());
        } catch (IOException unreadable) {
            throw new UncheckedIOException("could not read what the server said", unreadable);
        }
    }
}
