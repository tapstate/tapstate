package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;

final class SrsClusterTestSupport {

    private static final String LOOPBACK = "127.0.0.1";

    private SrsClusterTestSupport() {
    }

    static HazelcastInstance startMember(Config config, HazelcastInstance... peers) {
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        var network = config.getNetworkConfig();
        // Let the OS allocate the listening port when Hazelcast binds it.
        network.setPort(0).setPortAutoIncrement(false);
        network.getInterfaces().setEnabled(true).addInterface(LOOPBACK);
        var join = network.getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(Arrays.stream(peers)
                .map(peer -> LOOPBACK + ":" + peer.getCluster().getLocalMember().getAddress().getPort())
                .toList());
        return Hazelcast.newHazelcastInstance(config);
    }
}
