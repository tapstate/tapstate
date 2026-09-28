package io.tapstate.runtime.engine;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.core.metrics.Metrics;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Real outbox refusal preserves native, custom and stage probes and the delivered sequence. */
class OutputPressureOnRealJetTest {

    private static CountDownLatch receiving;
    private static CountDownLatch released;
    private static CountDownLatch refused;
    private static final List<Integer> delivered = java.util.Collections.synchronizedList(new ArrayList<>());

    @Test
    void aHeldReceiverProducesRealRetryIntervalsWithoutLosingOriginalProbes() throws Exception {
        receiving = new CountDownLatch(1);
        released = new CountDownLatch(1);
        refused = new CountDownLatch(1);
        delivered.clear();
        HazelcastInstance member = Hazelcast.newHazelcastInstance(configuration());
        Job job = null;
        try {
            DAG dag = new DAG();
            var producer = dag.newVertex("producer", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(() -> StageOutputPressureProcessor.wrap(new Emitter(64)))));
            var receiver = dag.newVertex("receiver", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(HeldReceiver::new)));
            dag.edge(Edge.between(producer, receiver).setConfig(new com.hazelcast.jet.config.EdgeConfig().setQueueSize(2)));
            job = member.getJet().newJob(dag, new JobConfig().setName("measured-output-" + System.nanoTime()));
            assertThat(receiving.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(refused.await(5, TimeUnit.SECONDS)).isTrue();
            Job running = job;
            await(() -> value(running.getMetrics(), "stage.transform.output.refused") > 0);
            JobMetrics held = job.getMetrics();
            assertThat(value(held, "preservedNative")).isEqualTo(17);
            assertThat(value(held, "preservedCustom")).isEqualTo(31);
            assertThat(value(held, "stage.transform.ready")).isEqualTo(1);
            assertThat(value(held, "stage.transform.expected")).isEqualTo(1);
            assertThat(held.get("stage.transform.active")).hasSize(1);
            assertThat(held.get("queuesCapacity")).hasSize(2);
            assertThat(held.get("stage.transform.output.refused")).singleElement().satisfies(point -> {
                assertThat(point.tag(MetricTags.JOB)).isEqualTo(running.getIdString());
                assertThat(point.tag(MetricTags.EXECUTION)).isNotNull();
                assertThat(point.tag(MetricTags.MEMBER)).isNotNull();
                assertThat(point.tag(MetricTags.VERTEX)).isEqualTo("producer");
                assertThat(point.tag(MetricTags.PROCESSOR)).isEqualTo("0");
                assertThat(point.tag(MetricTags.PROCESSOR_TYPE)).isEqualTo("StageOutputPressureProcessor");
                assertThat(point.tag(MetricTags.USER)).isEqualTo("true");
            });
            long completedBeforeRelease = value(held, "stage.transform.output.retry.count");
            released.countDown();
            await(() -> delivered.size() == 64);
            await(() -> value(running.getMetrics(), "stage.transform.output.retry.count") > completedBeforeRelease);
            await(() -> value(running.getMetrics(), "queuesCapacity") == 2);
            JobMetrics settled = job.getMetrics();
            assertThat(value(settled, "queuesCapacity")).isEqualTo(2);
            assertThat(value(settled, "stage.transform.output.retry.sumNanos")).isPositive();
            long bucketTotal = 0;
            for (int index = 0; index < HistogramBounds.PROCESS_DURATION.buckets(); index++) {
                bucketTotal += value(settled, "stage.transform.output.retry.bucket." + index);
            }
            assertThat(bucketTotal).isEqualTo(value(settled, "stage.transform.output.retry.count"));
            assertThat(delivered).containsExactlyElementsOf(java.util.stream.IntStream.range(0, 64).boxed().toList());
        } finally {
            released.countDown();
            if (job != null) { job.cancel(); }
            member.shutdown();
        }
    }

    @Test
    void aCallbackQuotaCanRefuseOutputEvenWhenThereIsNoDownstreamQueue() throws Exception {
        refused = new CountDownLatch(1);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(configuration());
        Job job = null;
        try {
            DAG dag = new DAG();
            dag.newVertex("producer", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(() -> StageOutputPressureProcessor.wrap(new Emitter(20_000)))));
            job = member.getJet().newJob(dag, new JobConfig().setName("quota-output-" + System.nanoTime()));
            assertThat(refused.await(5, TimeUnit.SECONDS)).isTrue();
            Job running = job;
            await(() -> value(running.getMetrics(), "stage.transform.output.retry.count") > 0);
            assertThat(value(job.getMetrics(), "stage.transform.output.refused")).isPositive();
            assertThat(value(job.getMetrics(), "queuesCapacity")).isZero();
        } finally {
            if (job != null) { job.cancel(); }
            member.shutdown();
        }
    }

    private static long value(JobMetrics metrics, String name) {
        return metrics.get(name).stream().mapToLong(point -> point.value()).sum();
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) { Thread.sleep(30); }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static Config configuration() throws Exception {
        Config config = new Config();
        config.setClusterName("output-pressure-" + System.nanoTime());
        try (ServerSocket socket = new ServerSocket(0)) {
            config.getNetworkConfig().setPort(socket.getLocalPort()).setPortAutoIncrement(false).setReuseAddress(false);
        }
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        var join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return config;
    }

    private static final class Emitter extends AbstractProcessor implements Staged, DynamicMetricsProvider {
        @Probe(name = "preservedNative") private final long nativeValue = 17;
        private final int limit;
        private int sent;
        private volatile StageTimer timer = StageTimer.none(Stage.TRANSFORM);
        private Emitter(int limit) { this.limit = limit; }
        @Override public Stage stage() { return Stage.TRANSFORM; }
        @Override public boolean isCooperative() { return false; }
        @Override protected void init(Context context) { timer = StageTimer.of(stage(), context); }
        @Override public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
            timer.provideDynamicMetrics(descriptor, collection);
        }
        @Override public boolean complete() {
            Metrics.metric("preservedCustom").set(31);
            while (sent < limit) {
                long started = timer.begin();
                try {
                    if (!tryEmit(sent)) {
                        refused.countDown();
                        return false;
                    }
                    sent++;
                } finally {
                    timer.end(started);
                }
            }
            return false;
        }
    }

    private static final class HeldReceiver extends AbstractProcessor {
        @Override public boolean isCooperative() { return false; }
        @Override public void process(int ordinal, Inbox inbox) {
            receiving.countDown();
            try {
                if (!released.await(15, TimeUnit.SECONDS)) { throw new AssertionError("held receiver was not released"); }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
            for (Object item; (item = inbox.poll()) != null;) { delivered.add((Integer) item); }
        }
    }
}
