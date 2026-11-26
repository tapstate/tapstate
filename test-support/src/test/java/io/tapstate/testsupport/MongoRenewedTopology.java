package io.tapstate.testsupport;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;

/** Startup arguments for two real members that renew their independent node leases in Mongo. */
public final class MongoRenewedTopology {
    public static final String ORIGINAL = "lease-original";
    public static final String PEER = "lease-peer";

    private final String clusterId;
    private final String address;
    private final int originalPort;
    private final int peerPort;
    private final int originalControlPort;
    private final int peerControlPort;

    public MongoRenewedTopology(String clusterId) throws IOException {
        this.clusterId = clusterId;
        InetAddress host = hostAddress();
        address = host.getHostAddress();
        try (var original = new ServerSocket(0, 1, host);
             var peer = new ServerSocket(0, 1, host);
             var originalControl = new ServerSocket(0, 1, host);
             var peerControl = new ServerSocket(0, 1, host)) {
            originalPort = original.getLocalPort();
            peerPort = peer.getLocalPort();
            originalControlPort = originalControl.getLocalPort();
            peerControlPort = peerControl.getLocalPort();
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
        int memberPort = switch (nodeId) {
            case ORIGINAL -> originalPort;
            case PEER -> peerPort;
            default -> throw new IllegalArgumentException("Unknown test member " + nodeId);
        };
        int controlPort = ORIGINAL.equals(nodeId) ? originalControlPort : peerControlPort;
        return List.of("--tapstate.cluster.id=" + clusterId,
                "--tapstate.cluster.node-id=" + nodeId,
                "--tapstate.cluster.profile=process-failure-only",
                "--tapstate.cluster.bootstrap-min-members=2",
                "--tapstate.hz.bind-address=" + address,
                "--tapstate.hz.member-port=" + memberPort,
                "--tapstate.hz.advertised-member-address=" + address + ":" + memberPort,
                "--tapstate.hz.outbound-member-ports=20000-29999",
                "--tapstate.hz.discovery.mode=tcp-ip",
                "--tapstate.hz.discovery.tcp-ip.seeds=" + address + ":" + originalPort + "," + address + ":" + peerPort,
                "--tapstate.control.advertise-url=http://" + address + ":" + controlPort);
    }
}
