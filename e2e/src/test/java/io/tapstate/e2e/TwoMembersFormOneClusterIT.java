package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.testsupport.DockerGate;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Two server processes told about each other become one cluster, and each of them says so.
 *
 * <p>Everything else about clustering rests on this and none of it says whether it happened. Two
 * processes that never find each other are two clusters of one: both answer their health probe, both
 * serve every verb, both report a membership - of themselves. Nothing fails, and every later property
 * built on "the cluster" is then being asserted twice over two separate installations.
 *
 * <p>So what is asserted is the membership each of them reports, by node id, and that the two answers
 * are the same. A case that asked only whether the topology verb answers would pass on two
 * installations that had never heard of one another, which is exactly the shape that has to fail here.
 *
 * <p>Both are launched against one coordination store, given one cluster id and two node ids of their
 * own, and pointed at each other by address. The addresses are reserved before either is launched,
 * because neither can be told the other's afterwards.
 *
 * <p><b>Bespoke rather than declarative, and that is registered rather than assumed.</b> The
 * specification envelope has no word for a second member - its setup speaks of connectors, resources,
 * discovery and databases, and a member is none of those - so this is written in Java against the
 * real-process tier. The gap is written down where gaps are written down rather than left as a habit.
 */
class TwoMembersFormOneClusterIT {

    private static final String ADMIN = "e2e";
    private static final String PASSWORD = "e2e-password";

    /** Membership is committed through the coordination store, so it converges rather than being instant. */
    private static final Duration JOIN_BUDGET = Duration.ofSeconds(90);
    private static final Duration POLL = Duration.ofMillis(500);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void twoProcessesFindEachOtherAndBothAnswerTheSameMembership() {
        String store = SharedMongo.replicaSetUrl("e2e_two_members");
        String bindAddress = routableAddress();
        // An id of this run's own, and it has to be the cluster id rather than anything else: in cluster
        // mode that is what names the member protocol's own cluster. The protocol is unauthenticated and
        // this binds it to a real interface, so a shared id would let two of these runs on one network -
        // or anything else answering to it - join each other, and the membership asserted below would
        // have somebody else in it.
        String cluster = "e2e-two-members-" + UUID.randomUUID();
        int memberPortA = RealProcessServer.reservePort();
        int memberPortB = RealProcessServer.reservePort();
        String seeds = bindAddress + ":" + memberPortA + "," + bindAddress + ":" + memberPortB;

        // Listening everywhere rather than only where it is dialled: each has to answer at the address
        // it advertises to the other, and has to go on answering on the loopback because that is the
        // only address the first admin may be created from.
        try (RealProcessServer first = RealProcessServer.start(store, "0.0.0.0",
                     httpPort -> member(cluster, "node-a", memberPortA, seeds, httpPort, bindAddress));
             RealProcessServer second = RealProcessServer.start(store, "0.0.0.0",
                     httpPort -> member(cluster, "node-b", memberPortB, seeds, httpPort, bindAddress))) {
            ControlPlane a = new ControlPlane(first.baseUrl());
            ControlPlane b = new ControlPlane(second.baseUrl());
            a.bootstrapAndLogin(ADMIN, PASSWORD);
            // The admin lives in the store both of them share, so the second one does not create it again.
            b.login(ADMIN, PASSWORD);

            List<String> asAsees = awaitBothMembers(a);

            assertThat(asAsees)
                    .describedAs("the two processes joined. Told about each other and unable to reach "
                            + "one another, each would answer with itself alone")
                    .containsExactly("node-a", "node-b");
            assertThat(b.clusterMemberNodeIds())
                    .describedAs("and the other one answers the same list rather than its own view of "
                            + "who it has met - which is the whole promise of reading this from any member")
                    .isEqualTo(asAsees);
            assertThat(b.clusterId())
                    .describedAs("one cluster, not two that happen to hold the same members")
                    .isEqualTo(a.clusterId());
        }
    }

    /** Polls one member until it reports both, so the case fails on not joining rather than on timing. */
    private static List<String> awaitBothMembers(ControlPlane member) {
        long deadline = System.nanoTime() + JOIN_BUDGET.toNanos();
        List<String> seen = List.of();
        while (System.nanoTime() < deadline) {
            seen = member.clusterMemberNodeIds();
            if (seen.size() >= 2) {
                return seen;
            }
            try {
                Thread.sleep(POLL.toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for the two to join", interrupted);
            }
        }
        throw new AssertionError("after " + JOIN_BUDGET + " the membership was still " + seen
                + "; the two processes did not become one cluster");
    }

    private static List<String> member(String cluster, String nodeId, int memberPort, String seeds,
            int httpPort, String bindAddress) {
        return List.of(
                // Also the name of the member protocol's cluster, which is why it carries this run's own.
                "--tapstate.cluster.id=" + cluster,
                "--tapstate.cluster.node-id=" + nodeId,
                // Exactly two is what this profile is for, and it is checked against the bootstrap
                // count rather than assumed from the name - the default count is three.
                "--tapstate.cluster.profile=process-failure-only",
                "--tapstate.cluster.bootstrap-min-members=2",
                // Where it really listens, which is also the only kind of URL it is allowed
                // to advertise: a member that named the loopback here is refused at startup.
                "--tapstate.control.advertise-url=http://" + bindAddress + ":" + httpPort,
                "--tapstate.hz.bind-address=" + bindAddress,
                "--tapstate.hz.member-port=" + memberPort,
                "--tapstate.hz.discovery.mode=tcp-ip",
                "--tapstate.hz.discovery.tcp-ip.seeds=" + seeds);
    }

    /**
     * An address of this machine that is not the loopback.
     *
     * <p>Required rather than chosen: a member with discovery on refuses to start bound to the loopback,
     * and it refuses deliberately - two members on one machine reaching each other over 127.0.0.1 would
     * be a cluster whose members could never be reached by a third machine, which is the arrangement the
     * product exists to stop somebody shipping. So this pair binds where a real member binds.
     *
     * <p>Failing here rather than skipping. A machine with no address but the loopback cannot run this,
     * and saying so is the honest answer; skipping would report a pass for a case that never ran.
     */
    private static String routableAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface candidate = interfaces.nextElement();
                if (!candidate.isUp() || candidate.isLoopback()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = candidate.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address && !address.isLoopbackAddress()
                            && !address.isLinkLocalAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (SocketException unreachable) {
            throw new AssertionError("could not read this machine's interfaces", unreachable);
        }
        throw new AssertionError("this machine has no address but the loopback, and a member with "
                + "discovery on is refused there - two members cannot be brought up here");
    }
}
