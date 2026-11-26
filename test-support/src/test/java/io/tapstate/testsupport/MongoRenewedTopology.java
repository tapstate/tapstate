package io.tapstate.testsupport;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.Collections;
import java.util.List;

/** Startup arguments for two real members that renew their independent node leases in Mongo. */
public final class MongoRenewedTopology {
    public static final String ORIGINAL = "lease-original";
    public static final String PEER = "lease-peer";
    public static final String UNAVAILABLE = "lease-unavailable";

    public static final String CONTROL_ADVERTISE_URL = "tapstate.control.advertise-url";

    private final String clusterId;
    private final String address;

    public MongoRenewedTopology(String clusterId) throws IOException {
        this.clusterId = clusterId;
        address = hostAddress().getHostAddress();
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

    public List<String> arguments(String nodeId, int seedPort) {
        return List.of("--server.address=" + address,
                "--server.port=0",
                "--tapstate.cluster.id=" + clusterId,
                "--tapstate.cluster.node-id=" + nodeId,
                "--tapstate.cluster.profile=process-failure-only",
                "--tapstate.cluster.bootstrap-min-members=2",
                "--tapstate.hz.bind-address=" + address,
                "--tapstate.hz.member-port=0",
                "--tapstate.hz.outbound-member-ports=20000-29999",
                "--tapstate.hz.discovery.mode=tcp-ip",
                // The bootstrap member has no live seed yet. The joining peer receives its bound port.
                "--tapstate.hz.discovery.tcp-ip.seeds=" + address + ":" + seedPort,
                "--" + CONTROL_ADVERTISE_URL + "=" + controlUrl(address, 0));
    }

    public static URI controlUrl(String address, int port) {
        return URI.create("http://" + address + ":" + port);
    }
}
