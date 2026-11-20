package io.tapstate.e2e;

import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.srs.SrsSourceProcessor;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ExecutionFailureRecorder;
import io.tapstate.runtime.engine.Engine;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The source binds its real local processor proof, not a potentially blocking named-job lookup. */
@Timeout(40)
class SrsSourceFailureBindingIT {
    @Test void scopedSourceInitializationDoesNotUseTheLegacyNamedBindingBridge() throws Exception {
        Config config = new Config().setClusterName("source-failure-binding-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", "2");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false)
            .getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        var member = Hazelcast.newHazelcastInstance(config);
        try {
            new Engine(member);
            var legacyCalls = new AtomicInteger();
            member.getUserContext().put(ExecutionFailureRecorder.USER_CONTEXT_KEY,
                (ExecutionFailureRecorder) (pipeline, job, identity) -> {
                    legacyCalls.incrementAndGet();
                    throw new AssertionError("SRS processor initialization must use its local execution proof");
                });
            var buffer = new SnapshotBuffer();
            buffer.declareSnapshot("source-binding", "srs.binding");
            buffer.append("source-binding", "srs.binding",
                Envelope.read(1L, "orders", Map.of("id", 7), Map.of()).withOrder(SourceOrder.snapshotRow(1)));
            buffer.endSnapshot("source-binding", "srs.binding");
            member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);
            var graph = new DAG();
            var source = graph.newVertex("source", SrsSourceProcessor.snapshotOnlyMetaSupplier(
                "source-binding", "srs.binding", "orders", 1L, null, SourcePlacement.anyMember()));
            var project = graph.newVertex("project", Processors.mapP((Envelope row) -> row.after().get("id"))).localParallelism(1);
            var sink = graph.newVertex("sink", SinkProcessors.writeListP("source-binding-output")).localParallelism(1);
            graph.edge(between(source, project)).edge(between(project, sink));
            var job = member.getJet().newJob(graph, new JobConfig().setName("source-binding")
                .setArgument(Engine.RUN_IDENTITY_ARGUMENT, "source-binding-run"));
            job.getFuture().get(20, TimeUnit.SECONDS);
            assertThat(member.getList("source-binding-output")).containsExactly(7);
            assertThat(legacyCalls.get()).isZero();
        } finally {
            member.getLifecycleService().terminate();
        }
    }
}
