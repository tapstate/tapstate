package io.tapstate.e2e;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Server processes of one cluster whose member traffic runs through links a case can cut per peer.
 *
 * <p>Each member reports a {@link CuttableLink} address instead of its own and every seed names a link,
 * so member traffic reaches a member only through the link in front of it. Each member is also told
 * which local ports it may dial from, which is what lets a link tell its callers apart: everything
 * arrives from loopback, so the source port is the only thing carrying the dialler's identity. That is
 * the whole of how a partition is made on one machine here -- no packet filter, and no address anybody
 * needs root to create.
 *
 * <p><strong>Cutting by peer rather than by direction was arrived at by measurement, after the
 * direction-shaped arrangement was tried and failed.</strong> A connection lives in the link of
 * whichever member was dialled, so severing a whole link severs only what was dialled into it;
 * measured on three members, that left the three holding three different answers. Routing each ordered
 * pair through its own link fails too, because a member's entry in the member list is the address it
 * reports and every dial after the first goes there rather than to a seed. Both readings are written up
 * in {@link CuttableLink}, along with what the per-peer cut measured instead.
 *
 * <p>A case that partitions this cluster must also read {@link #unattributedConnections()}. A
 * connection nobody could attribute passes straight through a cut aimed at its dialler, and a
 * partition that holds only because nothing was dialled is indistinguishable from one that holds.
 */
final class PartitionableCluster implements AutoCloseable {

    private static final String ADMIN = "e2e";
    private static final String PASSWORD = "e2e-password";

    /** Membership is committed through the coordination store, so it converges rather than being instant. */
    private static final Duration JOIN_BUDGET = Duration.ofSeconds(90);

    /**
     * Ports per member for dialling out. They are used in turn and one still in its close-wait is
     * skipped, so this is far wider than the handful of connections a member of this size holds.
     */
    private static final int OUTBOUND_RANGE_SIZE = 120;

    /** Below the ephemeral range this machine hands out, so an outbound port is never one it assigned. */
    private static final int OUTBOUND_FLOOR = 20000;

    private static final int OUTBOUND_SLOTS = 160;

    private final List<String> nodeIds;
    private final Map<String, RealProcessServer> servers;
    private final Map<String, CuttableLink> links;
    private final Map<String, ControlPlane> planes;
    private final String clusterId;

    private final String storeUri;
    private final Map<String, IntFunction<List<String>>> launchArguments;

    /**
     * Where every member process this cluster launched wrote what it said, including the incarnations a
     * restart replaced - a member's account of why it left is in the log of the process that left, and
     * the one that came back in its place says nothing about it.
     */
    private final Map<String, Path> logs = new LinkedHashMap<>();

    private PartitionableCluster(List<String> nodeIds, Map<String, RealProcessServer> servers,
            Map<String, CuttableLink> links, String clusterId, String storeUri,
            Map<String, IntFunction<List<String>>> launchArguments) {
        this.nodeIds = List.copyOf(nodeIds);
        this.servers = servers;
        this.links = links;
        this.clusterId = clusterId;
        this.storeUri = storeUri;
        this.launchArguments = launchArguments;
        Map<String, ControlPlane> built = new LinkedHashMap<>();
        servers.forEach((nodeId, server) -> built.put(nodeId, new ControlPlane(server.baseUrl())));
        this.planes = built;
        servers.forEach(this::remember);
    }

    /** Files one launch's log under its node id, or under a numbered one when that id launched before. */
    private void remember(String nodeId, RealProcessServer server) {
        String name = nodeId;
        for (int launch = 2; logs.containsKey(name); launch++) {
            name = nodeId + "-launch-" + launch;
        }
        logs.put(name, server.output());
    }

    /**
     * Launches one member per node id, in that order, each reporting its own link and dialling from
     * its own ports.
     *
     * <p>Every port -- the member ports, the link ports and the outbound ranges -- is settled before
     * any process starts, because a member is told what to report, what to dial and what to dial from
     * on its command line and cannot be told any of it afterwards.
     */
    static PartitionableCluster start(String storeUri, String name, List<String> nodeIds) {
        String clusterId = name + "-" + UUID.randomUUID();
        String bindAddress = RoutableAddress.ofThisMachine();
        Map<String, Integer> memberPorts = new LinkedHashMap<>();
        Map<String, int[]> outbound = outboundRanges(nodeIds);
        Map<String, CuttableLink> links = new LinkedHashMap<>();
        for (String nodeId : nodeIds) {
            memberPorts.put(nodeId, RealProcessServer.reservePort());
        }
        for (String nodeId : nodeIds) {
            links.put(nodeId, CuttableLink.open(
                    RealProcessServer.reservePort(), bindAddress, memberPorts.get(nodeId), outbound));
        }
        String seeds = String.join(",", links.values().stream().map(CuttableLink::address).toList());

        Map<String, RealProcessServer> servers = new LinkedHashMap<>();
        // Kept per member, because what a member is told is fixed on its command line: a member that
        // comes back has to come back as the same member, on the same port, behind the same link.
        Map<String, IntFunction<List<String>>> launchArguments = new LinkedHashMap<>();
        for (String nodeId : nodeIds) {
            int memberPort = memberPorts.get(nodeId);
            String advertised = links.get(nodeId).address();
            int[] range = outbound.get(nodeId);
            launchArguments.put(nodeId, httpPort -> arguments(clusterId, nodeId, memberPort, advertised,
                    seeds, httpPort, bindAddress, nodeIds.size(), range));
        }
        try {
            for (String nodeId : nodeIds) {
                servers.put(nodeId,
                        RealProcessServer.start(storeUri, "0.0.0.0", launchArguments.get(nodeId)));
            }
        } catch (RuntimeException | Error failure) {
            servers.values().forEach(RealProcessServer::close);
            links.values().forEach(CuttableLink::close);
            throw failure;
        }
        PartitionableCluster cluster = new PartitionableCluster(
                nodeIds, servers, links, clusterId, storeUri, launchArguments);
        try {
            cluster.planes.get(nodeIds.getFirst()).bootstrapAndLogin(ADMIN, PASSWORD);
            // The administrator lives in the store they all share, so the rest do not create one.
            nodeIds.stream().skip(1).forEach(nodeId -> cluster.planes.get(nodeId).login(ADMIN, PASSWORD));
        } catch (RuntimeException | Error failure) {
            cluster.close();
            throw failure;
        }
        return cluster;
    }

    /** The control plane of one member. Every read face answers the same while the cluster is whole. */
    ControlPlane member(String nodeId) {
        return planes.get(requireKnown(nodeId));
    }

    /** The link in front of one member -- what it carried, from whom, and what a cut severs. */
    CuttableLink linkTo(String nodeId) {
        return links.get(requireKnown(nodeId));
    }

    /** The cluster id this bring-up runs under; its own, so two runs on one network never merge. */
    String clusterId() {
        return clusterId;
    }

    /**
     * Severs the named members from all the others, in both directions, leaving each side intact.
     *
     * <p>Both directions is the point: a pair is severed by refusing a in front of b and b in front of
     * a, because either of them may be the one that dialled and only the dialled member's link sees the
     * connection.
     */
    void separate(List<String> oneSide) {
        List<String> side = oneSide.stream().map(this::requireKnown).toList();
        List<String> rest = nodeIds.stream().filter(node -> !side.contains(node)).toList();
        if (side.isEmpty() || rest.isEmpty()) {
            throw new AssertionError("a partition needs members on both sides; asked to separate "
                    + oneSide + " out of " + nodeIds);
        }
        for (String here : side) {
            for (String there : rest) {
                linkTo(there).refuse(here);
                linkTo(here).refuse(there);
            }
        }
    }

    /** Puts every severed pair back, for the half of a case that asks what happens once it heals. */
    void reunite() {
        for (String node : nodeIds) {
            nodeIds.forEach(other -> linkTo(node).admit(other));
        }
    }

    /**
     * Connections no link could attribute to a member, across the whole cluster.
     *
     * <p>Anything but zero invalidates a partition read off this cluster: such a connection is not
     * severed by a cut aimed at its dialler, so the members might be talking through one.
     */
    int unattributedConnections() {
        return links.values().stream().mapToInt(CuttableLink::unattributed).sum();
    }

    /**
     * Waits until the member named answers with a membership of at least {@code atLeast}, and returns
     * what it last read.
     */
    List<String> awaitMembers(String asSeenBy, int atLeast) {
        return await(asSeenBy, "to report " + atLeast + " members", JOIN_BUDGET,
                seen -> seen.size() >= atLeast);
    }

    /**
     * Waits until the member named answers with exactly {@code exactly} members, and returns that.
     *
     * <p>Exactly, not at least: after a partition the interesting failure is a side that still reports
     * more than it should, and "at least" would read that as success.
     */
    List<String> awaitExactly(String asSeenBy, int exactly, Duration budget) {
        return await(asSeenBy, "to report exactly " + exactly + " members", budget,
                seen -> seen.size() == exactly);
    }

    /** One read of what a member currently reports, with no waiting at all. */
    /**
     * Stops one member's process and brings the same member back: same node id, same member port,
     * same link in front of it, same ports to dial from. Only the control API port is new, because
     * that one is taken from whatever is free.
     *
     * <p>For the cases that ask what happens on the way back. A member that took itself out of the
     * cluster -- because its node session lapsed while the coordination store was away -- does not
     * come back on its own, so a case that wants to see it rejoin has to restart it.
     */
    void restart(String nodeId) {
        String known = requireKnown(nodeId);
        servers.get(known).close();
        RealProcessServer replacement =
                RealProcessServer.start(storeUri, "0.0.0.0", launchArguments.get(known));
        servers.put(known, replacement);
        remember(known, replacement);
        ControlPlane plane = new ControlPlane(replacement.baseUrl());
        plane.login(ADMIN, PASSWORD);
        planes.put(known, plane);
    }

    /** Whether one member's process is still running -- what tells failing closed from falling over. */
    boolean isAlive(String nodeId) {
        return servers.get(requireKnown(nodeId)).isAlive();
    }

    List<String> membership(String asSeenBy) {
        return member(asSeenBy).clusterMemberNodeIds();
    }

    @Override
    public void close() {
        List<RuntimeException> failures = new ArrayList<>();
        for (RealProcessServer server : servers.values()) {
            try {
                server.close();
            } catch (RuntimeException failure) {
                failures.add(failure);
            }
        }
        links.values().forEach(CuttableLink::close);
        // After the processes are down, so what a member said on its way out is in what is kept.
        FailureScene.writeMemberLogs(clusterId, logs);
        if (!failures.isEmpty()) {
            throw failures.getFirst();
        }
    }

    private List<String> await(String asSeenBy, String what, Duration budget,
            java.util.function.Predicate<List<String>> settled) {
        ControlPlane plane = member(asSeenBy);
        AtomicReference<List<String>> seen = new AtomicReference<>(List.of());
        Await.until(asSeenBy + " " + what, budget,
                () -> {
                    seen.set(plane.clusterMemberNodeIds());
                    return settled.test(seen.get());
                },
                () -> "the membership it reported was still " + seen.get());
        return seen.get();
    }

    /**
     * A block of local ports for each member to dial from, none overlapping another's.
     *
     * <p>The blocks are drawn from shuffled slots rather than counted off from a fixed base: a second
     * run on this machine would otherwise take the same ports, and while that costs nothing in itself
     * -- a port already held is skipped -- shuffling keeps two runs from contending for every one of
     * them in the same order.
     */
    private static Map<String, int[]> outboundRanges(List<String> nodeIds) {
        if (nodeIds.size() > OUTBOUND_SLOTS) {
            throw new AssertionError("this fixture has " + OUTBOUND_SLOTS + " port blocks and was asked "
                    + "for " + nodeIds.size());
        }
        List<Integer> slots = new ArrayList<>();
        for (int slot = 0; slot < OUTBOUND_SLOTS; slot++) {
            slots.add(slot);
        }
        Collections.shuffle(slots);
        Map<String, int[]> ranges = new LinkedHashMap<>();
        for (int index = 0; index < nodeIds.size(); index++) {
            int low = OUTBOUND_FLOOR + slots.get(index) * OUTBOUND_RANGE_SIZE;
            ranges.put(nodeIds.get(index), new int[] {low, low + OUTBOUND_RANGE_SIZE - 1});
        }
        return ranges;
    }

    private String requireKnown(String nodeId) {
        if (!nodeIds.contains(nodeId)) {
            throw new AssertionError(
                    "this cluster carries " + nodeIds + ", and was asked about '" + nodeId + "'");
        }
        return nodeId;
    }

    private static List<String> arguments(String clusterId, String nodeId, int memberPort,
            String advertised, String seeds, int httpPort, String bindAddress, int members, int[] outbound) {
        return List.of(
                "--tapstate.cluster.id=" + clusterId,
                "--tapstate.cluster.node-id=" + nodeId,
                "--tapstate.cluster.profile=production-ha",
                "--tapstate.cluster.bootstrap-min-members=" + members,
                "--tapstate.control.advertise-url=http://" + bindAddress + ":" + httpPort,
                // Bound where a cluster member is allowed to bind, reported where the others can reach
                // it through its link, and dialling from ports that say which member is dialling. The
                // three differ on purpose: that difference is the fixture.
                "--tapstate.hz.bind-address=" + bindAddress,
                "--tapstate.hz.member-port=" + memberPort,
                "--tapstate.hz.advertised-member-address=" + advertised,
                "--tapstate.hz.outbound-member-ports=" + outbound[0] + "-" + outbound[1],
                "--tapstate.hz.discovery.mode=tcp-ip",
                "--tapstate.hz.discovery.tcp-ip.seeds=" + seeds);
    }
}
