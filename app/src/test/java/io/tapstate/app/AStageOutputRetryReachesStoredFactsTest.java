package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.StageOutputPressureProcessor;
import io.tapstate.runtime.engine.StageTimer;
import io.tapstate.runtime.engine.StageWorkDag;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A native callback quota can create measurable output retry facts without a downstream-full claim. */
class AStageOutputRetryReachesStoredFactsTest {
    @Test
    void completedOutputRetriesReachTheStoredPipelineFactsWithTheirProducerStart() throws Exception {
        Config config = new Config();
        config.setClusterName("stored-output-retry-" + System.nanoTime());
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            config.getNetworkConfig().setPort(socket.getLocalPort()).setPortAutoIncrement(false).setReuseAddress(false);
        }
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            DAG dag = new StageWorkDag();
            StageWorkDag.measured(dag, dag.newVertex("producer", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(() -> StageOutputPressureProcessor.wrap(new QuotaEmitter())))), Stage.TRANSFORM, true);
            Engine engine = new Engine(member);
            engine.submit("output-retry", dag);
            var job = member.getJet().getJob("output-retry");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (job.getMetrics().get("stage.transform.output.retry.count").stream().mapToLong(p -> p.value()).sum() == 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            var nativeFacts = job.getMetrics();
            long count = nativeFacts.get("stage.transform.output.retry.count").stream().mapToLong(p -> p.value()).sum();
            long since = nativeFacts.get("stage.transform.output.since").getFirst().value();
            assertThat(count).isPositive();
            assertThat(nativeFacts.get("queuesCapacity").stream().mapToLong(p -> p.value()).sum()).isZero();
            InMemoryStorePort store = new InMemoryStorePort(new InMemoryArtifactStore());
            store.state().create("output-retry", StateJson.of(PipelineState.RUNNING), Instant.now());
            var publisher = new RuntimeConvergenceConfiguration().observationPublisher(store, engine, new NoOpCaptureCoordinator());
            publisher.publish("output-retry");
            var stored = store.observations().read("output-retry").orElseThrow().facts();
            assertThat(stored).filteredOn(fact -> fact.name().equals("tapstate.pipeline.stage.output.refused"))
                    .as("completed native output offers are projected onto the stored facts")
                    .singleElement().satisfies(fact -> {
                        assertThat(fact.points().getFirst().value()).isPositive();
                        assertThat(fact.points().getFirst().startTime()).isEqualTo(Instant.ofEpochMilli(since));
                    });
            assertThat(stored).filteredOn(fact -> fact.name().equals("tapstate.pipeline.stage.output.retry.duration"))
                    .singleElement().satisfies(fact -> {
                        assertThat(fact.points().getFirst().histogram().count()).isEqualTo(count);
                        assertThat(fact.points().getFirst().startTime()).isEqualTo(Instant.ofEpochMilli(since));
                    });
            engine.cancel("output-retry");
        } finally {
            member.shutdown();
        }
    }

    private static final class QuotaEmitter extends AbstractProcessor implements Staged, DynamicMetricsProvider {
        private volatile StageTimer timer = StageTimer.none(Stage.TRANSFORM);
        private int sent;
        @Override public Stage stage() { return Stage.TRANSFORM; }
        @Override protected void init(Context context) { timer = StageTimer.of(stage(), context); }
        @Override public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
            timer.provideDynamicMetrics(descriptor, collection);
        }
        @Override public boolean complete() {
            while (sent < 20_000) {
                long began = timer.begin();
                try {
                    if (!tryEmit(sent)) { return false; }
                    sent++;
                } finally {
                    timer.end(began);
                }
            }
            return false;
        }
    }
}
