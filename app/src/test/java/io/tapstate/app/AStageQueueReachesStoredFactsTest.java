package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.config.EdgeConfig;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.QueueReading;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.StageTimer;
import io.tapstate.runtime.engine.StageWorkDag;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A backed-up native stage queue remains visible even when no business callback is currently executing. */
class AStageQueueReachesStoredFactsTest {
    private static volatile boolean emitting = true;
    private static volatile boolean refusing = true;

    @Test
    void aFullStageQueueReachesFactsWhileTheActiveWorkReadingIsQuiet() throws Exception {
        emitting = true;
        refusing = true;
        int port;
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Config config = new Config();
        config.setClusterName("stage-queue-facts-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getJetConfig().getDefaultEdgeConfig().setQueueSize(64);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false).setReuseAddress(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            Engine engine = new Engine(member);
            engine.submit("stage-queue", dag(64));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (engine.queueReading("stage-queue").map(QueueReading::depth).orElse(0L) != 64
                    && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(engine.queueReading("stage-queue")).isPresent();
            assertThat(engine.queueReading("stage-queue").orElseThrow().depth()).isEqualTo(64);
            assertThat(engine.activeWork("stage-queue").activeByStage()).isEmpty();
            InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
            store.state().create("stage-queue", StateJson.of(PipelineState.RUNNING), Instant.now());
            ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                    .observationPublisher(store, engine, new NoOpCaptureCoordinator());
            publisher.publish("stage-queue");
            assertThat(store.observations().read("stage-queue").orElseThrow().facts())
                    .filteredOn(fact -> fact.name().equals("tapstate.pipeline.stage.queue.depth"))
                    .as("the measured transform input queue is part of the stored pipeline facts")
                    .singleElement().satisfies(fact -> assertThat(fact.points().getFirst().value()).isEqualTo(64));
            emitting = false;
            refusing = false;
            await(() -> {
                var queue = engine.stageRuntimeReading("stage-queue").queues().byStage().get("transform");
                return queue != null && queue.queue().depth() == 0;
            },
                    "the collected stage queue did not drain");
            assertThat(engine.stageRuntimeReading("stage-queue").queues().byStage().get("transform").queue().highWater())
                    .isEqualTo(64);
            var old = member.getJet().getJob("stage-queue");
            engine.cancel("stage-queue");
            assertThat(engine.stageRuntimeReading("stage-queue").queues().byStage()).isEmpty();
            await(() -> old.getStatus().isTerminal(), "the cancelled job did not end");
            refusing = true;
            engine.submit("stage-queue", dag(16));
            var replacement = member.getJet().getJob("stage-queue");
            assertThat(replacement.getId()).isNotEqualTo(old.getId());
            await(() -> !replacement.getMetrics().get("stage.transform.ready").isEmpty(), "the quiet job did not initialize");
            assertThat(engine.stageRuntimeReading("stage-queue").queues().byStage()).isEmpty();
            emitting = true;
            await(() -> engine.stageRuntimeReading("stage-queue").queues().byStage().containsKey("transform"),
                    "the replacement stage queue did not report occupancy");
            await(() -> {
                var queue = engine.stageRuntimeReading("stage-queue").queues().byStage().get("transform");
                return queue != null && queue.queue().depth() == 16;
            },
                    "the replacement stage queue did not fill");
            var reset = engine.stageRuntimeReading("stage-queue").queues().byStage().get("transform").queue();
            assertThat(reset.depth()).isEqualTo(16);
            assertThat(reset.capacity()).isEqualTo(16);
            assertThat(reset.highWater()).isEqualTo(16);
            engine.cancel("stage-queue");
        } finally {
            emitting = true;
            refusing = true;
            member.shutdown();
        }
    }

    private static DAG dag(int capacity) {
        DAG dag = new StageWorkDag();
        Vertex source = StageWorkDag.measured(dag, dag.newVertex("source", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of(EmittingSource::new))), Stage.SOURCE, true);
        Vertex held = StageWorkDag.measured(dag, dag.newVertex("held-transform", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of(RefusingTransform::new))), Stage.TRANSFORM, true);
        dag.edge(Edge.between(source, held).setConfig(new EdgeConfig().setQueueSize(capacity)));
        return dag;
    }

    private static void await(java.util.function.BooleanSupplier condition, String reason) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError(reason);
    }

    private abstract static class MeasuredProcessor extends AbstractProcessor implements DynamicMetricsProvider {
        private final Stage stage;
        private volatile StageTimer timer;
        private MeasuredProcessor(Stage stage) { this.stage = stage; }
        @Override protected void init(Context context) { timer = StageTimer.of(stage, context); }
        @Override public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
            if (timer != null) {
                timer.provideDynamicMetrics(descriptor, collection);
            }
        }
    }

    private static final class EmittingSource extends MeasuredProcessor {
        private int next;
        private EmittingSource() { super(Stage.SOURCE); }
        @Override public boolean complete() {
            while (emitting && tryEmit(next)) { next++; }
            return false;
        }
    }

    private static final class RefusingTransform extends MeasuredProcessor {
        private RefusingTransform() { super(Stage.TRANSFORM); }
        @Override protected boolean tryProcess(int ordinal, Object item) { return !refusing; }
    }
}
