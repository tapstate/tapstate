package io.tapstate.runtime.engine;

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A live job reports a business unit before that unit returns, rather than only its completed duration. */
class AStageReportsWorkWhileItIsBlockedTest {

    private static CountDownLatch entered;
    private static CountDownLatch released;

    @Test
    void aBlockedUnitIsVisibleBeforeItHasACompletedDuration() throws Exception {
        entered = new CountDownLatch(1);
        released = new CountDownLatch(1);
        String cluster = "stage-work-" + System.nanoTime();
        Config config = configuration(cluster);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance second = null;
        try {
            Config another = configuration(cluster);
            var address = member.getCluster().getLocalMember().getAddress();
            another.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                    .addMember(address.getHost() + ":" + address.getPort());
            second = Hazelcast.newHazelcastInstance(another);
            long joinedBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (member.getCluster().getMembers().size() != 2 && System.nanoTime() < joinedBy) {
                Thread.sleep(50);
            }
            assertThat(member.getCluster().getMembers()).hasSize(2);
            DAG dag = new StageWorkDag();
            StageWorkDag.measured(dag, dag.newVertex("blocked-transform", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(BlockingWork::new))), Stage.TRANSFORM, true);
            Engine engine = new Engine(member);
            engine.submit("blocked-stage-work", dag);
            Job job = member.getJet().getJob("blocked-stage-work");
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (engine.activeWork("blocked-stage-work").activeByStage().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(job.getMetrics().get("stage.transform.active"))
                    .as("the executing unit is measured while it is blocked")
                    .singleElement().satisfies(reading -> assertThat(reading.value()).isEqualTo(1));
            assertThat(job.getMetrics().get("stage.transform.count"))
                    .as("the unit has not completed")
                    .isEmpty();
            assertThat(engine.activeWork("blocked-stage-work").activeByStage())
                    .as("complete current job frame: %s", diagnosticFrame(job))
                    .isEqualTo(Map.of("transform", 1L));
            released.countDown();
            job.join();
            assertThat(engine.activeWork("blocked-stage-work").activeByStage()).isEmpty();
            DAG idle = new StageWorkDag();
            StageWorkDag.measured(idle, idle.newVertex("idle-transform", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(IdleWork::new))), Stage.TRANSFORM, true);
            engine.submit("blocked-stage-work", idle);
            Job replaced = member.getJet().getJob("blocked-stage-work");
            assertThat(replaced.getId()).isNotEqualTo(job.getId());
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (replaced.getMetrics().get("stage.transform.ready").isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(replaced.getMetrics().get("stage.transform.ready")).isNotEmpty();
            assertThat(engine.activeWork("blocked-stage-work").activeByStage()).isEmpty();
            engine.cancel("blocked-stage-work");
        } finally {
            released.countDown();
            if (second != null) {
                second.shutdown();
            }
            member.shutdown();
        }
    }

    private static Map<String, ?> diagnosticFrame(Job job) {
        var metrics = job.getMetrics();
        Map<String, Object> frame = new java.util.LinkedHashMap<>();
        for (String name : List.of("executionStartTime", "executionCompletionTime", "queuesCapacity",
                "stage.transform.active", "stage.transform.ready", "stage.transform.expected", "stage.transform.members")) {
            frame.put(name, metrics.get(name));
        }
        return frame;
    }

    @Test
    void theFirstBlockedTransformIsVisibleBeforeItsSinkReceivesAnyBusinessInput() throws Exception {
        entered = new CountDownLatch(1);
        released = new CountDownLatch(1);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(configuration("first-work-" + System.nanoTime()));
        try {
            DAG dag = new StageWorkDag();
            Vertex source = StageWorkDag.measured(dag, dag.newVertex("source",
                    ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(OneRowSource::new))), Stage.SOURCE, true);
            Vertex transform = StageWorkDag.measured(dag, dag.newVertex("transform",
                    TransformProcessor.metaSupplier("transform", () -> event -> {
                        entered.countDown();
                        try {
                            if (!released.await(20, TimeUnit.SECONDS)) {
                                throw new AssertionError("first transform was not released");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(interrupted);
                        }
                        return List.of(event);
                    })), Stage.TRANSFORM, true);
            Vertex sink = StageWorkDag.measured(dag, dag.newVertex("sink",
                    SinkProcessor.metaSupplier("sink", NoOpWriter::new)), Stage.SINK, true);
            dag.edge(Edge.between(source, transform));
            dag.edge(Edge.between(transform, sink));
            Engine engine = new Engine(member);
            engine.submit("first-row-work", dag);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (engine.activeWork("first-row-work").activeByStage().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(engine.activeWork("first-row-work").activeByStage())
                    .as("an untouched downstream sink does not hide the first executing transform")
                    .isEqualTo(Map.of("transform", 1L));
            released.countDown();
            engine.cancel("first-row-work");
        } finally {
            released.countDown();
            member.shutdown();
        }
    }

    private static final class OneRowSource extends AbstractProcessor implements DynamicMetricsProvider {
        private volatile StageTimer timer;
        @Override public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
            if (timer != null) {
                timer.provideDynamicMetrics(descriptor, collection);
            }
        }
        private boolean emitted;
        @Override protected void init(Context context) { timer = StageTimer.of(Stage.SOURCE, context); }
        @Override public boolean isCooperative() { return false; }
        @Override public boolean complete() {
            if (!emitted) {
                long began = timer.begin();
                try {
                    emitted = tryEmit(Envelope.insert(Instant.parse("2026-09-29T08:00:00Z").toEpochMilli(),
                            "orders", Map.of("id", 1), null));
                } finally {
                    timer.end(began);
                }
            }
            return false;
        }
    }

    private static final class NoOpWriter implements SinkWriter {
        @Override public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }
        @Override public void close() { }
    }

    private static Config configuration(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false).setReuseAddress(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return config;
    }

    private static final class IdleWork extends AbstractProcessor implements DynamicMetricsProvider {
        private volatile StageTimer timer;
        @Override public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
            if (timer != null) {
                timer.provideDynamicMetrics(descriptor, collection);
            }
        }
        @Override protected void init(Context context) { timer = StageTimer.of(Stage.TRANSFORM, context); }
        @Override public boolean complete() {
            timer.discard(timer.beginInactive());
            return false;
        }
    }

    private static final class BlockingWork extends AbstractProcessor implements DynamicMetricsProvider {
        private volatile StageTimer timer;
        @Override public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
            if (timer != null) {
                timer.provideDynamicMetrics(descriptor, collection);
            }
        }

        @Override
        protected void init(Context context) {
            timer = StageTimer.of(Stage.TRANSFORM, context);
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            long began = timer.begin();
            entered.countDown();
            try {
                if (!released.await(15, TimeUnit.SECONDS)) {
                    throw new AssertionError("blocked work was not released");
                }
                return true;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            } finally {
                timer.end(began);
            }
        }
    }
}
