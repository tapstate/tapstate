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
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a document points at a row which has not arrived is a reason to wait, and that the same row having
 * been deleted is not.
 *
 * <p><b>The two look identical in the document and must not be handled alike.</b> Either way the field is
 * simply not there - a row that has not turned up has nothing to render, and one that is gone renders
 * nothing rather than its last value. What separates them is whether the document may go downstream: a
 * document sent early is a document the source never had, showing an order with no customer at all; a
 * document held for a row that was deleted is a document nobody will ever release, because the thing it
 * waits for has already happened.
 *
 * <p>So a namespace that only ever answers "here it is" or nothing cannot serve both, and the second case
 * here is what says so - it passes for free if a deletion merely takes the entry out, and fails the moment
 * the wait in the first case is built on an entry being absent.
 */
class ADocumentWaitsForARowItPointsAtButNotForOneThatIsGoneTest {

    private static final String STEP = "order_doc";

    private static final int SHARED_CUSTOMER = 100;

    private static final int ORDERS = 3;

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-reference-waiting-test-" + System.nanoTime());
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
    @DisplayName("a document whose row has not arrived is not shown without it")
    void nothingGoesOutUntilTheRowTheDocumentPointsAtHasArrived() {
        // The orders are ready at once and the customer they name is a second behind them, which is the
        // ordinary way round for two tables read at once - one of them is simply slower to get going.
        member.getJet().newJob(orders("p-late", Duration.ZERO, List.of(
                new Timed(List.of(row("customer_id", SHARED_CUSTOMER, "name", "Ada")),
                        Duration.ofMillis(1000))))).join();

        assertThat(documentsWritten())
                .describedAs("every document that went out carries the customer it names. A document sent "
                        + "before that row arrived is one the source never had - an order with no customer "
                        + "at all - and the version after it being right does not take it back from a sink "
                        + "that has already passed it on")
                .isNotEmpty()
                .allSatisfy(document -> assertThat(document).containsKey("customer"));
        assertThat(latestPerOrder()).hasSize(ORDERS);
    }

    @Test
    @DisplayName("a document whose row was deleted goes out without it rather than waiting for ever")
    void adocumentWhoseRowWasDeletedIsStillSent() {
        member.getJet().newJob(orders("p-gone", Duration.ofMillis(400), List.of(
                new Timed(List.of(row("customer_id", SHARED_CUSTOMER, "name", "Ada")), Duration.ZERO),
                new Timed(List.of(row("customer_id", SHARED_CUSTOMER, "name", "Ada")),
                        Duration.ofMillis(1500), true)))).join();

        Map<Object, Map<String, Object>> latest = latestPerOrder();
        assertThat(latest)
                .describedAs("each order was assembled while the customer was still there")
                .hasSize(ORDERS);
        assertThat(latest.values())
                .describedAs("and each came out again once it was deleted, with the field simply gone - "
                        + "not frozen at its last value, and not held back for ever waiting on a row whose "
                        + "arrival has already happened and will not happen again")
                .allSatisfy(document -> assertThat(document).doesNotContainKey("customer"));
    }

    /** Every document written, in the order the sink saw them. */
    private static List<Map<String, Object>> documentsWritten() {
        List<Map<String, Object>> documents = new ArrayList<>();
        for (Envelope written : WRITTEN) {
            if (written.after() != null) {
                documents.add(written.after());
            }
        }
        return documents;
    }

    /** The last document written per order - what a sink upserting them would be left holding. */
    private static Map<Object, Map<String, Object>> latestPerOrder() {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Map<String, Object> document : documentsWritten()) {
            latest.put(document.get("order_id"), document);
        }
        return latest;
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** Orders all pointing at one customer, whose own stream is given by {@code customers}. */
    private static DAG orders(String pipelineId, Duration ordersAfter, List<Timed> customers) {
        List<Map<String, Object>> rows = new ArrayList<>(ORDERS);
        for (int i = 1; i <= ORDERS; i++) {
            rows.add(row("order_id", i, "cust_ref", SHARED_CUSTOMER));
        }

        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(customer)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("customer", FromRef.literal("customers"));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(pipelineId, null,
                List.of("orders", "customers"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", rowsSource("orders", List.of(new Timed(rows, ordersAfter))));
        sources.put("customers", rowsSource("customers", customers));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("customer", new NestTable("customers", List.of("customer_id")));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    private static ProcessorMetaSupplier rowsSource(String src, List<Timed> batches) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsSource(src, batches)));
    }

    private static Map<String, Object> row(String a, Object av, String b, Object bv) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(a, av);
        row.put(b, bv);
        return row;
    }

    /** One batch of rows, how long after the source starts it is due, and whether it removes them. */
    private record Timed(List<Map<String, Object>> rows, Duration after, boolean deleting)
            implements Serializable {

        private Timed(List<Map<String, Object>> rows, Duration after) {
            this(rows, after, false);
        }
    }

    /** Emits its batches in order, each held until it is due, yielding rather than sleeping. */
    private static final class RowsSource extends AbstractProcessor {

        private final String src;
        private final List<Timed> batches;
        private long startedAt = -1;
        private int batch;
        private int next;
        private int emitted;

        private RowsSource(String src, List<Timed> batches) {
            this.src = src;
            this.batches = batches;
        }

        @Override
        public boolean complete() {
            if (startedAt < 0) {
                startedAt = System.currentTimeMillis();
            }
            while (batch < batches.size()) {
                Timed due = batches.get(batch);
                if (System.currentTimeMillis() - startedAt < due.after().toMillis()) {
                    return false;
                }
                while (next < due.rows().size()) {
                    Map<String, Object> row = due.rows().get(next);
                    Envelope event = due.deleting()
                            ? Envelope.delete(emitted + 1L, src, row, null)
                            : Envelope.insert(emitted + 1L, src, row, null);
                    if (!tryEmit(event.withOrder(new SourceOrder(1, emitted)))) {
                        return false;
                    }
                    next++;
                    emitted++;
                }
                batch++;
                next = 0;
            }
            return true;
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
