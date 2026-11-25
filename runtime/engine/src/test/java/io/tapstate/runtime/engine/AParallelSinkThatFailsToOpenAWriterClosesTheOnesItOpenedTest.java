package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SyncElement;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink running four writers whose third fails to open fails the run - and closes the two it had opened
 * before, with nothing written through either.
 *
 * <p>Each writer holds a connection of its own. The engine closes only the processors it was handed, and the
 * two made before the failure never are; left open, each run that retries the start opens as many again
 * beside them. Only the happy path closes what it opened by itself, which is why this one is the case to hold.
 */
class AParallelSinkThatFailsToOpenAWriterClosesTheOnesItOpenedTest {

    private static final AtomicInteger OPENED = new AtomicInteger();
    private static final AtomicInteger CLOSED = new AtomicInteger();
    private static final AtomicInteger WRITTEN = new AtomicInteger();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
        OPENED.set(0);
        CLOSED.set(0);
        WRITTEN.set(0);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void theWritersOpenedBeforeTheFailureAreClosedAndNoneWrote() {
        PipelineResource pipeline = new PipelineResource("p", null, List.of(SourceRef.bare("orders")), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders"),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
        ExecutionShape shape = new ExecutionShape(1,
                Map.of("serve.s", new NodeParallelism("serve.s", 4, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 1, 4, 4, List.of())),
                Map.of(),
                Map.of("serve.s", Map.of("orders", new SinkTarget("orders", List.of("id")))));
        DagBindings bindings = new DagBindings(
                sourceId -> ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of((SupplierEx<Processor>) Rows::new), sourceId),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) ThirdFailsToOpen::open,
                ref -> List.of(((FromRef.Literal) ref).ref()));

        Throwable failure = catchThrowable(() ->
                member.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings, null, null, shape)).join());

        assertThat(failure).as("a sink whose writer cannot open does not run").isNotNull();
        assertThat(OPENED).as("writers opened before the one that failed").hasValue(2);
        assertThat(CLOSED).as("and closed again").hasValue(2);
        assertThat(WRITTEN).as("nothing written through them").hasValue(0);
    }

    /** A writer whose third opening fails, counting the openings, closes and rows written. */
    private static final class ThirdFailsToOpen implements SinkWriter {

        static SinkWriter open() {
            if (OPENED.get() == 2) {
                throw new IllegalStateException("the third connection is refused");
            }
            OPENED.incrementAndGet();
            return new ThirdFailsToOpen();
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            WRITTEN.addAndGet(records.size());
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
            CLOSED.incrementAndGet();
        }
    }

    /** A few keyed rows, then done. */
    private static final class Rows extends AbstractProcessor {

        private int next = 1;

        @Override
        public boolean complete() {
            while (next <= 8) {
                if (!tryEmit(Envelope.insert(next, "orders", Map.of("id", next), null))) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }
}
