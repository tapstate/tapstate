package io.tapstate.e2e;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Server processes of one cluster whose member traffic runs through links a case can cut.
 *
 * <p>Each member reports a {@link CuttableLink} address instead of its own and every seed names a link,
 * so member traffic reaches a member only through the link in front of it. That is the whole of how a
 * partition is made on one machine here: no packet filter, and no address anybody needs root to create.
 *
 * <p><strong>A link per member does not express a partition, and this was measured rather than
 * reasoned.</strong> A connection lives in the link of whichever member was dialled, and cutting one
 * member's link therefore severs only what was dialled into it — everything that member dialled outward
 * lives in the other links and survives. Cutting the first member's link on a cluster of three left the
 * three holding three different answers: the member whose link was cut still reported all three, one
 * reported two, and the third reported itself alone. Which link a pair lands in is also not decided by
 * bring-up order the way it looks: the links counted 2, 6 and 10 connections in bring-up order, so the
 * member that joined last was dialled the most.
 *
 * <p>So this fixture brings a cluster up through links and lets a case read what they carried. Severing
 * one side from another needs a link per ordered pair, and needs it established that a re-dial cannot
 * reach a member around it — neither of which is here.
 */
final class PartitionableCluster implements AutoCloseable {

    private static final String ADMIN = "e2e";
    private static final String PASSWORD = "e2e-password";

    /** Membership is committed through the coordination store, so it converges rather than being instant. */
    private static final Duration JOIN_BUDGET = Duration.ofSeconds(90);

    private final List<String> nodeIds;
    private final Map<String, RealProcessServer> servers;
    private final Map<String, CuttableLink> links;
    private final Map<String, ControlPlane> planes;
    private final String clusterId;

    private PartitionableCluster(List<String> nodeIds, Map<String, RealProcessServer> servers,
            Map<String, CuttableLink> links, String clusterId) {
        this.nodeIds = List.copyOf(nodeIds);
        this.servers = servers;
        this.links = links;
        this.clusterId = clusterId;
        Map<String, ControlPlane> built = new LinkedHashMap<>();
        servers.forEach((nodeId, server) -> built.put(nodeId, new ControlPlane(server.baseUrl())));
        this.planes = built;
    }

    /**
     * Launches one member per node id, in that order, each reporting its own link.
     *
     * <p>Every port — the member ports and the link ports — is reserved before any process starts,
     * because a member is told what to report and what to dial on its command line and cannot be told
     * either afterwards.
     *
     * @param nodeIds the members to bring up, in bring-up order; the first is the one a cut can isolate
     */
    static PartitionableCluster start(String storeUri, String name, List<String> nodeIds) {
        String clusterId = name + "-" + UUID.randomUUID();
        String bindAddress = RoutableAddress.ofThisMachine();
        Map<String, Integer> memberPorts = new LinkedHashMap<>();
        Map<String, CuttableLink> links = new LinkedHashMap<>();
        for (String nodeId : nodeIds) {
            memberPorts.put(nodeId, RealProcessServer.reservePort());
        }
        for (String nodeId : nodeIds) {
            links.put(nodeId, CuttableLink.open(
                    RealProcessServer.reservePort(), bindAddress, memberPorts.get(nodeId)));
        }
        String seeds = String.join(",", links.values().stream().map(CuttableLink::address).toList());

        Map<String, RealProcessServer> servers = new LinkedHashMap<>();
        try {
            for (String nodeId : nodeIds) {
                int memberPort = memberPorts.get(nodeId);
                String advertised = links.get(nodeId).address();
                servers.put(nodeId, RealProcessServer.start(storeUri, "0.0.0.0",
                        httpPort -> arguments(clusterId, nodeId, memberPort, advertised, seeds,
                                httpPort, bindAddress, nodeIds.size())));
            }
        } catch (RuntimeException | Error failure) {
            servers.values().forEach(RealProcessServer::close);
            links.values().forEach(CuttableLink::close);
            throw failure;
        }
        PartitionableCluster cluster = new PartitionableCluster(nodeIds, servers, links, clusterId);
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
        ControlPlane plane = planes.get(requireKnown(nodeId));
        return plane;
    }

    /** The link in front of one member — what it carried, and what a cut severs. */
    CuttableLink linkTo(String nodeId) {
        return links.get(requireKnown(nodeId));
    }

    /** The cluster id this bring-up runs under; its own, so two runs on one network never merge. */
    String clusterId() {
        return clusterId;
    }

    /**
     * Waits until the member named answers with a membership of at least {@code atLeast}, and returns
     * what it last read.
     */
    List<String> awaitMembers(String asSeenBy, int atLeast) {
        ControlPlane plane = member(asSeenBy);
        AtomicReference<List<String>> seen = new AtomicReference<>(List.of());
        Await.until(asSeenBy + " to report " + atLeast + " members", JOIN_BUDGET,
                () -> {
                    seen.set(plane.clusterMemberNodeIds());
                    return seen.get().size() >= atLeast;
                },
                () -> "the membership it reported was still " + seen.get());
        return seen.get();
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
        if (!failures.isEmpty()) {
            throw failures.getFirst();
        }
    }

    private String requireKnown(String nodeId) {
        if (!nodeIds.contains(nodeId)) {
            throw new AssertionError(
                    "this cluster carries " + nodeIds + ", and was asked about '" + nodeId + "'");
        }
        return nodeId;
    }

    private static List<String> arguments(String clusterId, String nodeId, int memberPort,
            String advertised, String seeds, int httpPort, String bindAddress, int members) {
        return List.of(
                "--tapstate.cluster.id=" + clusterId,
                "--tapstate.cluster.node-id=" + nodeId,
                "--tapstate.cluster.profile=production-ha",
                "--tapstate.cluster.bootstrap-min-members=" + members,
                "--tapstate.control.advertise-url=http://" + bindAddress + ":" + httpPort,
                // Bound where a cluster member is allowed to bind, and reported where the others can
                // reach it through its link. The two differ on purpose: that difference is the fixture.
                "--tapstate.hz.bind-address=" + bindAddress,
                "--tapstate.hz.member-port=" + memberPort,
                "--tapstate.hz.advertised-member-address=" + advertised,
                "--tapstate.hz.discovery.mode=tcp-ip",
                "--tapstate.hz.discovery.tcp-ip.seeds=" + seeds);
    }
}
