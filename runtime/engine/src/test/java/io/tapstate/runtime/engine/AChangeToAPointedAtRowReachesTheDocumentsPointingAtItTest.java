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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That editing one row every document points at brings every one of those documents out again carrying
 * the new value.
 *
 * <p>This is the direction with no other path back to the document. A row gathered <em>into</em> a
 * document arrives on an edge keyed by the document it belongs to, so a change to it routes itself; a row
 * pointed <em>at</em> belongs to no document at all and arrives keyed by itself, so nothing about it says
 * which documents to redraw. Left alone it is filed and nobody is told - and every document already sent
 * stays right until the moment that row changes, which is why every other case here goes on passing.
 *
 * <p>The discriminating part is the pair, not the end state. Reading the new value out of a document
 * proves nothing on its own: a document assembled after the edit would read it anyway. What says the edit
 * reached back is that the same document went out twice - once with what the row said before, and again
 * with what it says now - which happens only if something woke it.
 */
class AChangeToAPointedAtRowReachesTheDocumentsPointingAtItTest {

    private static final String STEP = "order_doc";

    /** The one customer row every order here points at. */
    private static final int SHARED_CUSTOMER = 100;

    /** How many orders point at it. More than one, so the wake-up is shown to reach all of them. */
    private static final int ORDERS = 3;

    /** How long the orders are held back, so the customer they point at is filed before they arrive. */
    private static final Duration LATE_ORDERS = Duration.ofMillis(400);

    /**
     * How long the edit is held back. Comfortably after the orders, so their documents have been assembled
     * and sent carrying the old value by the time it lands - which is what makes a second send afterwards
     * mean the edit reached them, rather than that they were rendered late enough to see it.
     */
    private static final Duration LATE_EDIT = Duration.ofMillis(1500);

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-reference-propagation-test-" + System.nanoTime());
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
    @DisplayName("editing the row every document points at sends every one of them out again")
    void everyDocumentPointingAtAnEditedRowGoesOutAgainCarryingTheNewValue() {
        member.getJet().newJob(ordersPointingAtOneCustomer("p-edit")).join();

        Set<Object> sawOldValue = ordersWhoseDocumentSaid("Ada");
        Set<Object> sawNewValue = ordersWhoseDocumentSaid("Grace");

        assertThat(sawOldValue)
                .describedAs("every order was assembled and sent while the customer still read Ada, which "
                        + "is what puts those documents downstream before the edit rather than after it")
                .hasSize(ORDERS);
        assertThat(sawNewValue)
                .describedAs("and every one of them came out again once the row they point at was edited. "
                        + "Nothing about those orders changed, so this send exists only if the edited row "
                        + "reached back to them; without that they stay at Ada for the life of the job, "
                        + "with the run reporting nothing wrong")
                .containsExactlyInAnyOrderElementsOf(sawOldValue);
    }

    /** The orders whose document was written with {@code name} in the customer it points at. */
    private static Set<Object> ordersWhoseDocumentSaid(String name) {
        Set<Object> orders = new LinkedHashSet<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document == null) {
                continue;
            }
            if (document.get("customer") instanceof Map<?, ?> row && name.equals(row.get("name"))) {
                orders.add(document.get("order_id"));
            }
        }
        return orders;
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** Orders all pointing at one customer, whose name is edited once they are all downstream. */
    private static DAG ordersPointingAtOneCustomer(String pipelineId) {
        List<Map<String, Object>> orders = new ArrayList<>(ORDERS);
        for (int i = 1; i <= ORDERS; i++) {
            orders.add(row("order_id", i, "cust_ref", SHARED_CUSTOMER));
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
        sources.put("orders", rowsSource("orders", List.of(new Timed(orders, LATE_ORDERS))));
        sources.put("customers", rowsSource("customers", List.of(
                new Timed(List.of(row("customer_id", SHARED_CUSTOMER, "name", "Ada")), Duration.ZERO),
                new Timed(List.of(row("customer_id", SHARED_CUSTOMER, "name", "Grace")), LATE_EDIT))));

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

    /** One batch of rows and how long after the source starts it is due. */
    private record Timed(List<Map<String, Object>> rows, Duration after) implements Serializable {
    }

    /**
     * Emits its batches in order, each held until it is due, and completes. It yields rather than sleeping:
     * the thread is shared with every other cooperative vertex here, and one that sleeps on it stops them
     * too.
     */
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
                    Envelope event = Envelope.insert(emitted + 1L, src, due.rows().get(next), null)
                            .withOrder(new SourceOrder(1, emitted));
                    if (!tryEmit(event)) {
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
