package io.tapstate.runtime.engine;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.StateStoreCostReading;
import io.tapstate.runtime.engine.StateStoreCostMetricNames.Kind;
import io.tapstate.runtime.engine.join.JoinMaps;
import io.tapstate.runtime.engine.join.JoinStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.HeapKeyedStateStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** One-time handles retain remote partition writes and reset against each new job's local baseline. */
class StateStoreCostBridgeTest {

    private static final String NAMESPACE = JoinMaps.factMirror("orders", "widen");
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(
            20_000 + 2 * new java.util.Random().nextInt(1_000));

    @Test
    void remoteColdStoreWorkRemainsInJobMetricsAfterBridgeCompletionAndResetsForTheNextJob()
            throws Exception {
        int base = NEXT_PORT.getAndAdd(2);
        String cluster = "state-cost-job-" + UUID.randomUUID();
        HeapKeyedStateStore cold = new HeapKeyedStateStore();
        HazelcastInstance one = null;
        HazelcastInstance two = null;
        try {
            one = member(cluster, base, base, cold);
            two = member(cluster, base + 1, base, cold);
            HazelcastInstance submitter = one;
            await(() -> submitter.getCluster().getMembers().size() == 2
                    && submitter.getPartitionService().isClusterSafe());
            String local = one.getCluster().getLocalMember().getUuid().toString();
            String remote = two.getCluster().getLocalMember().getUuid().toString();
            Engine engine = new Engine(one);

            Job first = JetJobs.submit(one, dag(), "state-cost-job-first");
            try {
                await(() -> values(first, Kind.READY).equals(Map.of(local, 1L, remote, 1L)));
                String firstKey = keyOwnedBy(one, remote, "first-");
                one.<String, Map<String, Object>>getMap(NAMESPACE).set(firstKey, Map.of("id", 1L));
                await(() -> values(first, Kind.SAVE_COMPLETED).equals(Map.of(local, 0L, remote, 1L)));
                await(() -> engine.stateStoreCostReadings("state-cost-job-first")
                        .containsKey(NAMESPACE));
                StateStoreCostReading firstCost = engine.stateStoreCostReadings("state-cost-job-first")
                        .get(NAMESPACE);
                assertThat(firstCost.operations().get("save").completed()).isEqualTo(1);
                assertThat(firstCost.codecs().get("encode").count()).isEqualTo(1);
                assertThat(values(first, Kind.SAVE_PAYLOAD_BYTES).get(remote)).isPositive();
                assertThat(values(first, Kind.ENCODE_COMPLETED).get(remote)).isEqualTo(1);

                one.getMap(NAMESPACE).evict(firstKey);
                assertThat(one.<String, Map<String, Object>>getMap(NAMESPACE).get(firstKey))
                        .containsEntry("id", 1L);
                await(() -> values(first, Kind.LOAD_COMPLETED).equals(Map.of(local, 0L, remote, 1L)));
                assertThat(values(first, Kind.DECODE_COMPLETED).get(remote)).isEqualTo(1);
            } finally {
                first.cancel();
            }
            await(() -> first.getStatus().isTerminal());

            Job second = JetJobs.submit(one, dag(), "state-cost-job-second");
            try {
                await(() -> values(second, Kind.READY).equals(Map.of(local, 1L, remote, 1L)));
                assertThat(values(second, Kind.SAVE_COMPLETED)).containsExactlyInAnyOrderEntriesOf(
                        Map.of(local, 0L, remote, 0L));
                assertThat(engine.stateStoreCostReadings("state-cost-job-second"))
                        .as("a quiet new job has no invented state-cost fact").isEmpty();
                String secondKey = keyOwnedBy(one, remote, "second-");
                one.<String, Map<String, Object>>getMap(NAMESPACE).set(secondKey, Map.of("id", 2L));
                await(() -> values(second, Kind.SAVE_COMPLETED).equals(Map.of(local, 0L, remote, 1L)));
                await(() -> engine.stateStoreCostReadings("state-cost-job-second")
                        .containsKey(NAMESPACE));
                assertThat(engine.stateStoreCostReadings("state-cost-job-second")
                        .get(NAMESPACE).operations().get("save").completed()).isEqualTo(1);

                two.shutdown();
                await(() -> submitter.getCluster().getMembers().size() == 1);
                assertThat(engine.stateStoreCostReadings("state-cost-job-second"))
                        .as("member loss must not publish the surviving member as the cluster total")
                        .isEmpty();
            } finally {
                if (!second.getStatus().isTerminal()) {
                    second.cancel();
                }
            }
        } finally {
            if (one != null && one.getLifecycleService().isRunning()) {
                one.shutdown();
            }
            if (two != null && two.getLifecycleService().isRunning()) {
                two.shutdown();
            }
        }
    }

    private static DAG dag() {
        DAG dag = new DAG();
        dag.newVertex(StateStoreCostMetricNames.VERTEX,
                StateStoreCostBridge.metaSupplier(Set.of(NAMESPACE))).localParallelism(1);
        dag.newVertex("keep-open", ProcessorMetaSupplier.of(
                ProcessorSupplier.of(KeepOpen::new))).localParallelism(1);
        return dag;
    }

    private static HazelcastInstance member(String cluster, int port, int base,
            HeapKeyedStateStore cold) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1:" + base).addMember("127.0.0.1:" + (base + 1));
        config.addMapConfig(JoinMaps.backedStateMaps(JoinMaps.DEFAULT_ENTRIES_HELD_IN_MEMORY));
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        JoinStateMapStoreFactory.bindTo(member, cold);
        return member;
    }

    private static String keyOwnedBy(HazelcastInstance member, String owner, String prefix) {
        for (int index = 0; index < 10_000; index++) {
            String key = prefix + index;
            if (member.getPartitionService().getPartition(key).getOwner().getUuid().toString()
                    .equals(owner)) {
                return key;
            }
        }
        throw new AssertionError("no key belongs to member " + owner);
    }

    private static Map<String, Long> values(Job job, Kind kind) {
        List<Measurement> matching = job.getMetrics().get(
                StateStoreCostMetricNames.nameOf(kind, NAMESPACE)).stream()
                .filter(value -> StateStoreCostMetricNames.VERTEX.equals(value.tag(MetricTags.VERTEX)))
                .toList();
        return matching.stream().collect(Collectors.toMap(
                value -> value.tag(MetricTags.MEMBER), Measurement::value));
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("two-member job metrics did not reach the expected state-store costs");
    }

    private static final class KeepOpen extends AbstractProcessor {
        @Override
        public boolean complete() {
            return false;
        }
    }
}
