package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Test helper: whether a meta-supplier pins its vertex to a single processor across the whole cluster —
 * the order-preserving property the sink watermark depends on.
 *
 * <p>{@code preferredLocalParallelism() == 1} alone does not prove it: a per-member supplier reports 1 too
 * yet runs one instance on every member. The distinguishing property is that a total-parallelism-one
 * supplier hands the real processor supplier to a single member and a no-op to the rest, so resolving it
 * over several members does not yield the same supplier for all.
 *
 * <p>Working that out needs a live partition table, because the member a vertex is pinned to is the one
 * owning its name's partition — so this starts a member of its own for the duration of the question and
 * shuts it down again. A fabricated address list alone cannot answer it: the pin resolves to whichever
 * address really owns the partition, and an answer against addresses that own nothing is "pinned to
 * nobody", which reads exactly like "not pinned at all".
 */
final class TotalParallelismOne {

    private TotalParallelismOne() {
    }

    /** True when {@code meta} pins to one member when resolved over a {@code members}-member cluster. */
    static boolean pins(ProcessorMetaSupplier meta, int members) throws Exception {
        HazelcastInstance member = Hazelcast.newHazelcastInstance(alone());
        try {
            // The real member goes first: it is the only address that owns any partition, so it is the
            // only one a pin can resolve to. The rest are stand-ins for "somewhere else in the cluster".
            List<Address> addresses = new ArrayList<>();
            addresses.add(member.getCluster().getLocalMember().getAddress());
            for (int i = 1; i < members; i++) {
                addresses.add(Address.createUnresolvedAddress("10.0.0." + i, 5701 + i));
            }
            meta.init(new TestProcessorMetaSupplierContext()
                    .setHazelcastInstance(member)
                    .setTotalParallelism(members).setLocalParallelism(1));
            Function<? super Address, ? extends ProcessorSupplier> assignment = meta.get(addresses);
            return addresses.stream().map(assignment).distinct().count() > 1;
        } finally {
            member.shutdown();
        }
    }

    /** A member that joins nothing, so several test JVMs asking this question never find each other. */
    private static Config alone() {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return config;
    }
}
