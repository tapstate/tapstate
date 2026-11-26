package io.tapstate.testsupport;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Startup arguments for two real members that renew their independent node leases in Mongo. */
public final class MongoRenewedTopology {
    public static final String ORIGINAL = "lease-original";
    public static final String PEER = "lease-peer";
    public static final String UNAVAILABLE = "lease-unavailable";

    private record Ports(int member, int control) {}

    private final String clusterId;
    private final String address;
    private final Map<String, Ports> nodes;

    public MongoRenewedTopology(String clusterId) throws IOException {
        this.clusterId = clusterId;
        InetAddress host = hostAddress();
        address = host.getHostAddress();
        try (var original = new ServerSocket(0, 1, host);
             var peer = new ServerSocket(0, 1, host);
             var originalControl = new ServerSocket(0, 1, host);
             var peerControl = new ServerSocket(0, 1, host);
             var unavailable = new ServerSocket(0, 1, host);
             var unavailableControl = new ServerSocket(0, 1, host)) {
            nodes = Map.of(ORIGINAL, new Ports(original.getLocalPort(), originalControl.getLocalPort()),
                    PEER, new Ports(peer.getLocalPort(), peerControl.getLocalPort()),
                    UNAVAILABLE, new Ports(unavailable.getLocalPort(), unavailableControl.getLocalPort()));
        }
    }

    private static InetAddress hostAddress() throws IOException {
        for (var network : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!network.isUp() || network.isLoopback() || network.isPointToPoint()) continue;
            for (var address : Collections.list(network.getInetAddresses())) {
                if (address instanceof Inet4Address && !address.isLoopbackAddress() && !address.isLinkLocalAddress()) {
                    return address;
                }
            }
        }
        throw new IOException("A non-loopback IPv4 address is required for member discovery");
    }

    public List<String> arguments(String nodeId) {
        Ports ports = ports(nodeId);
        return List.of("--server.address=" + address,
                "--server.port=" + ports.control(),
                "--tapstate.cluster.id=" + clusterId,
                "--tapstate.cluster.node-id=" + nodeId,
                "--tapstate.cluster.profile=process-failure-only",
                "--tapstate.cluster.bootstrap-min-members=2",
                "--tapstate.hz.bind-address=" + address,
                "--tapstate.hz.member-port=" + ports.member(),
                "--tapstate.hz.advertised-member-address=" + address + ":" + ports.member(),
                "--tapstate.hz.outbound-member-ports=20000-29999",
                "--tapstate.hz.discovery.mode=tcp-ip",
                "--tapstate.hz.discovery.tcp-ip.seeds=" + address + ":" + ports(ORIGINAL).member()
                        + "," + address + ":" + ports(PEER).member(),
                "--tapstate.control.advertise-url=" + controlUrl(nodeId));
    }

    public URI controlUrl(String nodeId) {
        return URI.create("http://" + address + ":" + ports(nodeId).control());
    }

    private Ports ports(String nodeId) {
        Ports ports = nodes.get(nodeId);
        if (ports == null) throw new IllegalArgumentException("Unknown test member " + nodeId);
        return ports;
    }
}
