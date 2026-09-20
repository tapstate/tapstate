package io.tapstate.e2e;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Two server processes of one cluster, brought up together over one coordination store.
 *
 * <p>Three product rules decide the whole of this wiring, and each of them rules out the obvious
 * arrangement. A member with discovery on is refused when it binds the loopback, because two members
 * reaching each other over 127.0.0.1 would be a cluster no third machine could ever join. The address
 * it advertises is held to the same rule. And the first administrator may only be created from the
 * loopback. So each member listens everywhere, is dialled on the loopback, and advertises the address
 * another machine could reach it at - the one arrangement that satisfies all three at once.
 *
 * <p>Each bring-up gets a cluster id of its own. In cluster mode that id is also the name of the member
 * protocol's cluster, the protocol is unauthenticated, and these members bind a real interface - so a
 * shared id would let two runs on one network join each other, and anything asserted about the
 * membership would have somebody else's process in it.
 *
 * <p>Both member addresses are reserved before either process is launched, because neither can be told
 * the other's afterwards: the seed list is a command line argument.
 *
 * <p>What this deliberately does not do is wait for them to find each other. Whether they join is the
 * subject of a case rather than a precondition of one, so {@link #awaitBothMembers()} is asked for
 * explicitly by the cases that need the cluster standing before they begin.
 */
final class TwoMemberCluster implements AutoCloseable {

    /** The stable node ids the two carry, which is what every cluster read face answers with. */
    static final String NODE_A = "node-a";
    static final String NODE_B = "node-b";

    private static final String ADMIN = "e2e";
    private static final String PASSWORD = "e2e-password";

    /** Membership is committed through the coordination store, so it converges rather than being instant. */
    private static final Duration JOIN_BUDGET = Duration.ofSeconds(90);

    private final RealProcessServer first;
    private final RealProcessServer second;
    private final ControlPlane a;
    private final ControlPlane b;
    private final String storeUri;
    private final String clusterId;
    private final String bindAddress;
    private final String seeds;
    private final Duration nodeSessionTtl;

    private TwoMemberCluster(RealProcessServer first, RealProcessServer second, String storeUri,
            String clusterId, String bindAddress, String seeds, Duration nodeSessionTtl) {
        this.first = first;
        this.second = second;
        this.a = new ControlPlane(first.baseUrl());
        this.b = new ControlPlane(second.baseUrl());
        this.storeUri = storeUri;
        this.clusterId = clusterId;
        this.bindAddress = bindAddress;
        this.seeds = seeds;
        this.nodeSessionTtl = nodeSessionTtl;
    }

    /**
     * Launches both members over {@code storeUri} and signs in on each.
     *
     * @param name a word naming the case, carried into the cluster id so a stray process is traceable
     */
    static TwoMemberCluster start(String storeUri, String name) {
        return start(storeUri, name, null);
    }

    /**
     * The same, with the node-session lease sized by the case instead of left at the product's default.
     *
     * <p>Only a case whose subject is the lease itself should pass anything here. The stable id a member
     * holds outlives the process by exactly one lease, so a case about what happens inside that window
     * has to choose how wide it is: the default is a compromise struck for deployments, and a case that
     * took it would be racing a number nobody picked for it. Everything else passes null and is held to
     * what a real member is held to.
     *
     * @param nodeSessionTtl the lease each member's node session is taken under, or null for the default
     */
    static TwoMemberCluster start(String storeUri, String name, Duration nodeSessionTtl) {
        String clusterId = name + "-" + UUID.randomUUID();
        String bindAddress = routableAddress();
        int memberPortA = RealProcessServer.reservePort();
        int memberPortB = RealProcessServer.reservePort();
        String seeds = bindAddress + ":" + memberPortA + "," + bindAddress + ":" + memberPortB;

        RealProcessServer first = RealProcessServer.start(storeUri, "0.0.0.0",
                httpPort -> arguments(
                        clusterId, NODE_A, memberPortA, seeds, httpPort, bindAddress, nodeSessionTtl));
        RealProcessServer second;
        try {
            second = RealProcessServer.start(storeUri, "0.0.0.0",
                    httpPort -> arguments(
                            clusterId, NODE_B, memberPortB, seeds, httpPort, bindAddress, nodeSessionTtl));
        } catch (RuntimeException | Error failure) {
            first.close();
            throw failure;
        }
        TwoMemberCluster cluster = new TwoMemberCluster(
                first, second, storeUri, clusterId, bindAddress, seeds, nodeSessionTtl);
        try {
            cluster.a.bootstrapAndLogin(ADMIN, PASSWORD);
            // The administrator lives in the store both of them share, so the second does not create one.
            cluster.b.login(ADMIN, PASSWORD);
        } catch (RuntimeException | Error failure) {
            cluster.close();
            throw failure;
        }
        return cluster;
    }

    /** The control plane of the member carrying {@link #NODE_A}. Every read face answers the same. */
    ControlPlane first() {
        return a;
    }

    /** The control plane of the member carrying {@link #NODE_B}. */
    ControlPlane second() {
        return b;
    }

    /** The control plane of whichever member is not the one named. */
    ControlPlane memberOtherThan(String nodeId) {
        return NODE_A.equals(requireKnown(nodeId)) ? b : a;
    }

    /**
     * The process carrying one member, for a case whose subject is that process going away.
     *
     * <p>Handed out rather than hidden because a member failing is not something a cluster can be asked
     * to do to itself: the case has to reach the process. Which member to reach for is read off the
     * cluster first - the one holding the work - so the case never assumes where that landed.
     */
    RealProcessServer processCarrying(String nodeId) {
        return NODE_A.equals(requireKnown(nodeId)) ? first : second;
    }

    private static String requireKnown(String nodeId) {
        if (!NODE_A.equals(nodeId) && !NODE_B.equals(nodeId)) {
            throw new AssertionError("this cluster carries " + NODE_A + " and " + NODE_B
                    + ", and was asked about '" + nodeId + "'");
        }
        return nodeId;
    }

    /**
     * Launches a further process into this cluster under {@code nodeId}, and hands it back before it
     * serves.
     *
     * <p>Unstarted on purpose. A case whose subject is a boot being turned away cannot wait for health,
     * because health is the thing that is not going to arrive; what to wait for instead - the member
     * appearing in the membership, or the process exiting and saying why - is the case's own business.
     *
     * <p>It is given this cluster's id and seed list, so a member that is allowed to join joins this
     * cluster rather than standing up one of its own. Its own member port is reserved here; the seed
     * list is not extended with it, and does not need to be - a member dials the seeds, and the members
     * already standing accept what dials them.
     */
    RealProcessServer launching(String nodeId) {
        int memberPort = RealProcessServer.reservePort();
        return RealProcessServer.launching(storeUri, "0.0.0.0",
                httpPort -> arguments(
                        clusterId, nodeId, memberPort, seeds, httpPort, bindAddress, nodeSessionTtl));
    }

    /**
     * A control plane on a process this case launched into the cluster, signed in like the other two.
     *
     * <p>The administrator lives in the store all of them share, so a member that joins later signs in
     * rather than bootstrapping - and a case that wants to ask the new arrival what it can see, rather
     * than only asking the members that were already standing, needs to be able to reach it.
     */
    ControlPlane signedInAt(RealProcessServer member) {
        ControlPlane control = new ControlPlane(member.baseUrl());
        control.login(ADMIN, PASSWORD);
        return control;
    }

    /**
     * Polls one member until it reports both, so a case fails on their not joining rather than on timing.
     *
     * @return the node ids that member answered with, in order
     */
    List<String> awaitBothMembers() {
        return awaitMembers(2);
    }

    /** The same, for a cluster a case has added a further member to. */
    List<String> awaitMembers(int atLeast) {
        AtomicReference<List<String>> seen = new AtomicReference<>(List.of());
        Await.until("the cluster to report " + atLeast + " members", JOIN_BUDGET,
                () -> {
                    seen.set(a.clusterMemberNodeIds());
                    return seen.get().size() >= atLeast;
                },
                () -> "the membership was still " + seen.get());
        return seen.get();
    }

    @Override
    public void close() {
        try {
            second.close();
        } finally {
            first.close();
        }
    }

    private static List<String> arguments(String clusterId, String nodeId, int memberPort, String seeds,
            int httpPort, String bindAddress, Duration nodeSessionTtl) {
        List<String> arguments = new ArrayList<>(List.of(
                // Also the name of the member protocol's cluster, which is why it carries this run's own.
                "--tapstate.cluster.id=" + clusterId,
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
                "--tapstate.hz.discovery.tcp-ip.seeds=" + seeds));
        if (nodeSessionTtl != null) {
            // Renewed three times over the life of a lease. A member that renewed once per lease would
            // lose its session to a single slow round trip, which is a different case's subject.
            arguments.add("--tapstate.cluster.node-session-ttl=" + nodeSessionTtl);
            arguments.add("--tapstate.cluster.node-session-renew-interval=" + nodeSessionTtl.dividedBy(3));
        }
        return List.copyOf(arguments);
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
