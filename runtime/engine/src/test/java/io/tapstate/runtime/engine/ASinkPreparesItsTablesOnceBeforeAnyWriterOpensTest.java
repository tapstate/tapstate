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
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SyncElement;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
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
 * A sink's target tables are prepared once for each execution, before any of its writers opens - however many
 * writers it runs.
 *
 * <p>Preparing is what clears a table for a full load. Done by each of four writers, it would clear the table
 * four times, and a clear arriving after another writer had started writing would take that writer's rows with
 * it; done after a writer opened, that writer could write before the clear. A sink running one writer is held
 * to the same once, which is what makes the two shapes one path.
 */
class ASinkPreparesItsTablesOnceBeforeAnyWriterOpensTest {

    /** Everything the factories were asked to do, in the order it happened. */
    private static final List<String> EVENTS = Collections.synchronizedList(new ArrayList<>());
    private static final String FAILING = "failing";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
        EVENTS.clear();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void eachSinkPreparesOnceForTheExecutionBeforeAnyOfItsWritersOpens() {
        member.getJet().newJob(PipelineDagBuilder.build(pipeline("wide", "narrow"), bindings(), null, null,
                wideRunsFourWriters())).join();

        List<String> events = List.copyOf(EVENTS);
        assertThat(events).filteredOn(event -> event.startsWith("prepare:"))
                .as("each sink prepares once for the execution")
                .containsExactlyInAnyOrder("prepare:wide", "prepare:narrow");
        assertThat(events).filteredOn("open:wide"::equals).as("the wide sink's writers").hasSize(4);
        assertThat(events).filteredOn("open:narrow"::equals).as("the narrow sink's writer").hasSize(1);
        int lastPreparation = Math.max(events.indexOf("prepare:wide"), events.indexOf("prepare:narrow"));
        assertThat(events.subList(0, lastPreparation))
                .as("no writer of either sink opened before both had prepared")
                .noneMatch(event -> event.startsWith("open:"));
    }

    @Test
    void aSinkWhosePreparationFailsOpensNoWriterAnywhere() {
        Throwable failure = catchThrowable(() -> member.getJet().newJob(PipelineDagBuilder.build(
                pipeline(FAILING, "narrow"), bindings(), null, null, wideRunsFourWriters(FAILING))).join());

        assertThat(failure).isNotNull();
        assertThat(List.copyOf(EVENTS)).contains("prepare:" + FAILING)
                .noneMatch(event -> event.startsWith("open:"));
    }

    private static PipelineResource pipeline(String wide, String narrow) {
        return new PipelineResource("p", null, List.of(SourceRef.bare("orders")), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders"),
                        List.of(new SyncElement(wide, "dest", null, null, null),
                                new SyncElement(narrow, "dest", null, null, null)), null, null),
                null, null);
    }

    private static ExecutionShape wideRunsFourWriters() {
        return wideRunsFourWriters("wide");
    }

    /** The first serve element runs four writers on the one member; the second runs one for the cluster. */
    private static ExecutionShape wideRunsFourWriters(String wide) {
        String sink = "serve." + wide;
        return new ExecutionShape(1,
                Map.of(sink, new NodeParallelism(sink, 4, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 1, 4, 4, List.of())),
                Map.of(),
                Map.of(sink, Map.of("orders", new SinkTarget("orders", List.of("id")))));
    }

    private static DagBindings bindings() {
        return new DagBindings(
                sourceId -> ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of((SupplierEx<Processor>) Rows::new), sourceId),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> new PreparingWriters(element.id()),
                ref -> List.of(((FromRef.Literal) ref).ref()));
    }

    /** Writers of tables prepared for them once per execution, recording both the preparing and each open. */
    private static final class PreparingWriters implements SupplierEx<SinkWriter>, PreparesTargets {

        private static final long serialVersionUID = 1L;

        private final String sink;

        PreparingWriters(String sink) {
            this.sink = sink;
        }

        @Override
        public void prepareTargets(HazelcastInstance coordinator) {
            EVENTS.add("prepare:" + sink);
            if (sink.equals(FAILING)) {
                throw new IllegalStateException("the target cannot be prepared");
            }
        }

        @Override
        public SinkWriter getEx() {
            EVENTS.add("open:" + sink);
            return new SettlingWriter();
        }
    }

    private static final class SettlingWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
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
