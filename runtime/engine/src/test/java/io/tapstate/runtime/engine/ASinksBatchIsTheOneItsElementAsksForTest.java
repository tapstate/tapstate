package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SyncElement;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink writes in the batch its {@code serve.sync} element asks for: here at most two rows a batch, waiting up
 * to a minute for more - and a finite input still ends at once, its last short batch written as the input ends
 * rather than a minute later.
 */
class ASinksBatchIsTheOneItsElementAsksForTest {

    private static final List<Integer> BATCH_SIZES = Collections.synchronizedList(new ArrayList<>());

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
        BATCH_SIZES.clear();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void theElementsBatchLimitsTheWritesAndTheEndOfInputSendsTheRest() {
        PipelineResource pipeline = new PipelineResource("p", null, List.of(SourceRef.bare("orders")), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders"),
                        List.of(new SyncElement("s", "dest", null, null, null, null,
                                new ExecutionSpec(null, new BatchSpec(2, "1m")))),
                        null, null),
                null, null);
        DagBindings bindings = new DagBindings(
                sourceId -> ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of((SupplierEx<Processor>) FiveRows::new), sourceId),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) SizeRecordingWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()));

        long started = System.nanoTime();
        member.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings)).join();

        assertThat(BATCH_SIZES).as("no write larger than the two rows the element allows").allMatch(size -> size <= 2);
        assertThat(BATCH_SIZES.stream().mapToInt(Integer::intValue).sum()).isEqualTo(5);
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .as("the input's end wrote the short batch without waiting out the minute")
                .isLessThan(Duration.ofSeconds(30));
    }

    /** Five rows, then done. */
    private static final class FiveRows extends AbstractProcessor {

        private int next = 1;

        @Override
        public boolean complete() {
            while (next <= 5) {
                if (!tryEmit(Envelope.insert(next, "orders", Map.of("id", next), null))) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }

    private static final class SizeRecordingWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            BATCH_SIZES.add(records.size());
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
