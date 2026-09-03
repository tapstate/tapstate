package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestClock;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateLedger;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The window is a number in the settings until the graph reads it. This is what stands between the two:
 * the same six changes to one root, run three ways, counted at the sink.
 *
 * <p>It is a whole job rather than a processor built by hand because the failure it is here for lives in
 * the wiring, not in the operator. An assembler told to fold folds, and the tests that drive one directly
 * say so; a builder that never passes the setting on leaves every one of those green while the deployment
 * that configured a window runs without it, and nothing anywhere says so.
 *
 * <p>Two sends is what a folded run costs: the first change goes out on its own leading edge, and what the
 * rest of them add up to goes out when the inputs run out. Six is what no window costs. The append run
 * takes the same long window and still costs six, which is what makes it a test of the root's mode rather
 * than of the number beside it.
 */
class TheSendWindowAPipelineRunsOnIsTheOneItWasGivenTest {

    /** Long enough that nothing in these runs can reach the end of it. */
    private static final long LONGER_THAN_THE_RUN = 10_000L;

    private static final int CHANGES = 6;

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
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
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void withNoWindowEveryChangeToOneRootReachesTheSink() {
        member.getJet().newJob(job(NestSettings.defaults().withSendWindow(documentNamespace(false), 0), false))
                .join();

        assertThat(WRITTEN)
                .describedAs("the control: with the window taken out, the sends are the changes - so the "
                        + "run below is measuring the window rather than how the source happened to batch")
                .hasSize(CHANGES);
    }

    @Test
    void withAWindowTheChangesInsideItReachTheSinkAsOneDocument() {
        member.getJet().newJob(job(NestSettings.defaults()
                        .withSendWindow(documentNamespace(false), LONGER_THAN_THE_RUN), false))
                .join();

        assertThat(WRITTEN)
                .describedAs("the leading edge, and then what the rest added up to when the inputs ran out. "
                        + "A builder that never passed the setting on would write six here")
                .hasSize(2);
        assertThat(WRITTEN.get(WRITTEN.size() - 1).after())
                .describedAs("folding merges versions rather than dropping the later ones")
                .containsEntry("name", "name-" + (CHANGES - 1));
    }

    @Test
    void anAppendRootIsNotFoldedHoweverLongTheWindowIs() {
        member.getJet().newJob(job(NestSettings.defaults()
                        .withSendWindow(documentNamespace(true), LONGER_THAN_THE_RUN), true))
                .join();

        assertThat(WRITTEN)
                .describedAs("same window as the run above and the opposite answer: under append every send "
                        + "is a new record, so a merged version is a row the reader never sees")
                .hasSize(CHANGES);
    }

    // ---- the pipeline under test ------------------------------------------------------

    private static Embed itemEmbed() {
        return new Embed("item", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "items",
                List.of("item_id"), null, null, null);
    }

    private static TransformBody.Nest body(boolean append) {
        return new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), append ? "append" : null, null,
                        List.of(itemEmbed())));
    }

    private static Map<String, NestTable> tables() {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("item", new NestTable("order_items", List.of("item_id")));
        return tables;
    }

    /** The name the compiler gives this step's document level, asked of the compiler rather than spelled. */
    private static String documentNamespace(boolean append) {
        Map<String, NestTable> tables = tables();
        return NestTopology.compile("p", "order_doc", body(append), tables::get).assembler().mapName();
    }

    /** Six changes to one order, each its own drain, with no items ever arriving. */
    private static DAG job(NestSettings settings, boolean append) {
        List<Map<String, Object>> orders = new ArrayList<>();
        for (int i = 0; i < CHANGES; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("order_id", 1);
            row.put("name", "name-" + i);
            orders.add(row);
        }

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("item", FromRef.literal("order_items"));
        Step step = Step.inline("order_doc", FromClause.aliases(aliases), body(append), null, null);

        PipelineResource pipeline = new PipelineResource("p", null,
                List.of(SourceRef.bare("orders"), SourceRef.bare("order_items")),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("order_doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", pacedSource("orders", orders));
        sources.put("order_items", pacedSource("order_items", List.of()));

        Map<String, NestTable> tables = tables();
        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> {
                }, member -> ReplayFloor.NONE, NestStateLedger.NONE, settings, NestClock.SYSTEM));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    /**
     * A source emitting one row per turn with a pause between them, so each row reaches the assembler as a
     * drain of its own. Emitting them together would leave the run with no window measuring the same one
     * send as the run with one, and the comparison would say nothing.
     */
    private static ProcessorMetaSupplier pacedSource(String src, List<Map<String, Object>> rows) {
        List<Map<String, Object>> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                (SupplierEx<Processor>) () -> new PacedRows(src, plan)));
    }

    private static final class PacedRows extends AbstractProcessor {

        private final String src;
        private final List<Map<String, Object>> rows;
        private int next;

        PacedRows(String src, List<Map<String, Object>> rows) {
            this.src = src;
            this.rows = rows;
        }

        /** Not cooperative: it parks between rows, and parking a cooperative thread stops every vertex. */
        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            if (next >= rows.size()) {
                return true;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            Envelope event = Envelope.insert(next + 1L, src, rows.get(next), null)
                    .withOrder(new SourceOrder(1, next));
            if (!tryEmit(event)) {
                return false;
            }
            next++;
            return next >= rows.size();
        }
    }

    private static final class CollectingSinkWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            WRITTEN.addAll(records);
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
