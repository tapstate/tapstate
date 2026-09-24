package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A stable node id belongs to one running process at a time, and a second process claiming one that is
 * taken is turned away before it becomes a member of anything.
 *
 * <p>The id is not a label. It is what every cluster read face answers with, what a claim records as
 * its owner, and what a durable position is attributed to - so two live processes answering to one id
 * are two members that the store, the read faces and every operator looking at them cannot tell apart.
 * The failure that follows is not a crash: work gets claimed twice under one owner, and the second
 * holder looks exactly like a renewal by the first.
 *
 * <p>Which is why the refusal has to land <em>before</em> the member exists. A process that joins and
 * is then told to leave has already been in the membership, already been offered work, and already had
 * the chance to take some. So what this asserts is not only that the duplicate stops, but that the
 * cluster standing beside it never grew: the membership after the refusal is the same two it was
 * before, by node id.
 *
 * <p>And a third member with an id of its own joins at the end. Without it a case like this passes on a
 * build where no further process can ever join - where the refusal was about being third rather than
 * about the id - and that is the one other explanation for everything above.
 *
 * <p><b>Bespoke rather than declarative.</b> The specification envelope has no word for a member
 * starting, let alone for one being refused at startup, and the subject here is a process that never
 * serves a request at all. Registered where the gaps are registered rather than left as a habit.
 */
class DuplicateStableNodeIdsCannotBothJoinIT {

    /** A refused boot fails in its own preflight, so this is about process start, not about a timeout. */
    private static final Duration REFUSAL_BUDGET = Duration.ofSeconds(120);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aSecondProcessClaimingATakenNodeIdIsRefusedAndTheClusterDoesNotGrow() {
        String store = SharedMongo.replicaSetUrl("e2e_duplicate_node_ids");

        try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-duplicate-node-ids")) {
            List<String> before = cluster.awaitBothMembers();
            assertThat(before)
                    .describedAs("the premise: two members, each under its own id, before anything "
                            + "tries to take one of them")
                    .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);

            RealProcessServer duplicate = cluster.launching(TwoMemberCluster.NODE_A);
            try {
                Await.until("the duplicate to be refused its node id and exit", REFUSAL_BUDGET,
                        () -> !duplicate.isAlive(),
                        () -> "it is still running, so a second process is answering to "
                                + TwoMemberCluster.NODE_A + "; it said:\n" + duplicate.tail());

                // Non-zero, not merely stopped: anything supervising this process reads an exit of 0 as
                // "finished" and moves on, which is how a refusal becomes a silent restart loop nobody
                // is told about.
                assertThat(duplicate.exitValue())
                        .describedAs("the status the duplicate exited with")
                        .isNotZero();
                assertThat(outputOf(duplicate))
                        .describedAs("a process turned away has to say which id it was turned away over; "
                                + "a bare stack trace leaves an operator guessing at a configuration fault")
                        .contains("boot.node-id-in-use")
                        .contains(TwoMemberCluster.NODE_A);
            } finally {
                duplicate.close();
            }

            assertThat(cluster.first().clusterMemberNodeIds())
                    .describedAs("and the cluster beside it never grew. A refusal that arrived after the "
                            + "join would leave this list with the same id twice, which is the state "
                            + "nothing downstream can unpick")
                    .isEqualTo(before);
            assertThat(cluster.second().clusterMemberNodeIds())
                    .describedAs("the other member answers the same, rather than only the one the "
                            + "duplicate would have dialled first")
                    .isEqualTo(before);

            // The control. Same cluster, same seeds, same everything but the id: it must join. Without
            // this, a build where no third process can ever join passes everything above.
            RealProcessServer third = cluster.launching("node-c");
            try {
                assertThat(cluster.awaitMembers(3))
                        .describedAs("a further member with an id of its own joins, so what turned the "
                                + "duplicate away was the id it asked for and not its being third")
                        .containsExactlyInAnyOrder(
                                TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, "node-c");
            } finally {
                third.close();
            }
        }
    }

    private static String outputOf(RealProcessServer server) {
        try {
            return Files.readString(server.output());
        } catch (IOException unreadable) {
            throw new UncheckedIOException("could not read what the server said", unreadable);
        }
    }
}
