package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import io.tapstate.core.lifecycle.QueueReading;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Observes the built-in input queues of a real, single-member Jet job. */
class JetQueueMetricsTest {

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getJetConfig().getDefaultEdgeConfig().setQueueSize(64);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void blockedSinkExposesItsInputQueueSizeAndCapacityWhileAQuietJobDoesNotFillIt() throws Exception {
        Engine engine = new Engine(member);
        Job blocked = JetJobs.submit(member, dag(EmittingSource::new, BlockedSink::new, 64), "blocked");
        await(() -> engine.queueReading("blocked").map(QueueReading::depth).orElse(0L) == 64L);
        QueueReading pressure = engine.queueReading("blocked").orElseThrow();
        assertThat(pressure.depth()).isEqualTo(64);
        assertThat(pressure.capacity()).isEqualTo(64);
        assertThat(pressure.highWater()).isEqualTo(64);
        assertThat(total(blocked, MetricNames.QUEUES_CAPACITY)).isEqualTo(64);
        assertThat(blocked.getMetrics().get(MetricNames.QUEUES_SIZE))
                .extracting(Measurement::value).anyMatch(value -> value > 0);

        Job quiet = JetJobs.submit(member, dag(QuietSource::new, BlockedSink::new, 64), "quiet");
        await(() -> total(quiet, MetricNames.QUEUES_CAPACITY) > 0);
        assertThat(total(quiet, MetricNames.QUEUES_SIZE)).isZero();
        assertThat(engine.queueReading("quiet")).isEmpty();

        engine.cancel("blocked");
        assertThat(engine.queueReading("blocked")).isEmpty();
        await(() -> blocked.getStatus().isTerminal());
        Job restarted = JetJobs.submit(member, dag(EmittingSource::new, BlockedSink::new, 16), "blocked");
        assertThat(member.getJet().getJob("blocked").getId()).isEqualTo(restarted.getId());
        await(() -> engine.queueReading("blocked").map(QueueReading::depth).orElse(0L) == 16L);
        QueueReading fresh = engine.queueReading("blocked").orElseThrow();
        assertThat(restarted.getId()).isNotEqualTo(blocked.getId());
        assertThat(fresh.capacity()).isEqualTo(16);
        assertThat(fresh.highWater()).isEqualTo(16);
        engine.cancel("blocked");
        quiet.cancel();
    }

    private static DAG dag(SupplierEx<Processor> sourceProcessor, SupplierEx<Processor> sinkProcessor,
            int queueSize) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of(sourceProcessor)));
        Vertex sink = dag.newVertex("serve.out", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of(sinkProcessor)));
        dag.edge(Edge.between(source, sink).setConfig(new EdgeConfig().setQueueSize(queueSize)));
        return dag;
    }

    private static long total(Job job, String metric) {
        List<Measurement> measurements = job.getMetrics().get(metric);
        return measurements.stream().mapToLong(Measurement::value).sum();
    }

    private static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Jet did not publish the expected queue measurement");
    }

    private static final class EmittingSource extends AbstractProcessor {
        private int next;

        @Override
        public boolean complete() {
            while (tryEmit(next)) {
                next++;
            }
            return false;
        }
    }

    private static final class QuietSource extends AbstractProcessor {
        @Override
        public boolean complete() {
            return false;
        }
    }

    private static final class BlockedSink extends AbstractProcessor {
        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            return false;
        }
    }
}
