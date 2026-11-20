package io.tapstate.e2e;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ExecutionFailureRecorder;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.srs.SrsReadCursorPublisherFactory;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.runtime.srs.StartFrom;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The source binds its real local processor proof through the core lifecycle boundary. */
@Timeout(40)
class SrsSourceFailureBindingIT {
    @Test void scopedSourceInitializationDoesNotUseTheLegacyNamedBindingBridge() throws Exception {
        exercise(true, false);
    }

    @Test void aSourceWithoutAFailureProviderKeepsItsOriginalProcessing() throws Exception {
        exercise(false, false);
    }

    @Test void sourceInitializationRetainsTheOriginalTypedFailure() throws Exception {
        exercise(true, true);
    }

    private static void exercise(boolean withProvider, boolean refused) throws Exception {
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
            Engine engine = withProvider ? new Engine(member) : null;
            var legacyCalls = new AtomicInteger();
            var localCalls = new AtomicInteger();
            if (withProvider) {
                var production = (ExecutionFailureRecorder) member.getUserContext().get(ExecutionFailureRecorder.USER_CONTEXT_KEY);
                member.getUserContext().put(ExecutionFailureRecorder.USER_CONTEXT_KEY, new ExecutionFailureRecorder() {
                    @Override public Consumer<Throwable> forExecution(String pipeline, long job, java.io.Serializable identity) {
                        legacyCalls.incrementAndGet();
                        throw new AssertionError("SRS processor initialization must use its local execution proof");
                    }
                    @Override public Consumer<Throwable> forProcessor(String pipeline, long job, long execution,
                            java.io.Serializable identity) {
                        assertThat(execution).isNotZero();
                        localCalls.incrementAndGet();
                        return production.forProcessor(pipeline, job, execution, identity);
                    }
                });
            }
            var buffer = new SnapshotBuffer();
            buffer.declareSnapshot("source-binding", "srs.binding");
            buffer.append("source-binding", "srs.binding",
                Envelope.read(1L, "orders", Map.of("id", 7), Map.of()).withOrder(SourceOrder.snapshotRow(1)));
            buffer.endSnapshot("source-binding", "srs.binding");
            member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);
            var graph = new DAG();
            var supplier = refused ? SrsSourceProcessor.metaSupplier("source-binding", "srs.binding", "orders",
                    StartFrom.earliest(), 1L, new MissingDurableLog(), SourcePlacement.anyMember())
                : SrsSourceProcessor.snapshotOnlyMetaSupplier("source-binding", "srs.binding", "orders", 1L, null,
                    SourcePlacement.anyMember());
            var source = graph.newVertex("source", supplier);
            var project = graph.newVertex("project", Processors.mapP((Envelope row) -> row.after().get("id"))).localParallelism(1);
            var sink = graph.newVertex("sink", SinkProcessors.writeListP("source-binding-output")).localParallelism(1);
            graph.edge(between(source, project)).edge(between(project, sink));
            var job = member.getJet().newJob(graph, new JobConfig().setName("source-binding")
                .setArgument(Engine.RUN_IDENTITY_ARGUMENT, "source-binding-run"));
            if (refused) {
                assertThatThrownBy(() -> job.getFuture().get(20, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
                var failure = engine.findRun("source-binding", "source-binding-run").orElseThrow().failureFact().orElseThrow();
                assertThat(failure.cause()).isInstanceOfSatisfying(TapstateException.class, typed -> {
                    assertThat(typed.code()).isEqualTo(CaptureError.SRS_NOT_RECOVERABLE);
                    assertThat(typed.args()).containsEntry("chain", "srs.binding");
                    assertThat(typed).hasStackTraceContaining("SrsSourceProcessor.initialize");
                });
                assertThat(member.getList("source-binding-output")).isEmpty();
            } else {
                job.getFuture().get(20, TimeUnit.SECONDS);
                assertThat(member.getList("source-binding-output")).containsExactly(7);
            }
            assertThat(legacyCalls.get()).isZero();
            assertThat(localCalls.get()).isEqualTo(withProvider ? 1 : 0);
        } finally {
            member.getLifecycleService().terminate();
        }
    }

    private static final class MissingDurableLog implements SrsReadCursorPublisherFactory {
        @Override public LongConsumer resolve(com.hazelcast.core.HazelcastInstance member) { return ignored -> { }; }
        @Override public boolean requiresDurableLog() { return true; }
    }
}
