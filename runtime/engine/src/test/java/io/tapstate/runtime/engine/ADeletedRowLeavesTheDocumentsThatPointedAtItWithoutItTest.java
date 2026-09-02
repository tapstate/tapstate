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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That deleting a row documents point at takes it out of them, rather than leaving them holding its last
 * known value for ever.
 *
 * <p><b>Holding the old value is not an error state, which is what makes this worth its own case.</b> An
 * implementation that reads a pointed-at row once and keeps what it read produces documents that are
 * complete, well formed and internally consistent; every count is right, nothing is thrown, and the value
 * they carry was true when it was read. It is wrong only against a source that has since said the row is
 * gone - and no assertion about a steady state can tell the two apart, because in a steady state they are
 * the same document.
 *
 * <p><b>The first assertion is a control and is not optional.</b> It holds the documents to carrying the
 * row <em>before</em> the deletion, so a run in which the pointed-at row never reached them at all -
 * because the fetch never happened, or because the customer stream never started - cannot be read as the
 * deletion having been honoured. Without it, "the field is absent" is satisfied by a document that never
 * had it, which is the outcome a broken fetch produces.
 *
 * <p>The field is expected to be absent rather than null. A null is a value, and a sink handed one writes
 * it over whatever is there; absent is how a document says the row it named is not there to show.
 */
class ADeletedRowLeavesTheDocumentsThatPointedAtItWithoutItTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "deleted-reference";

    private static final int CUSTOMER = 7;
    private static final int ORDERS = 3;
    private static final String NAME = "Ada";

    private static final Duration ORDERS_AFTER_CUSTOMER = Duration.ofMillis(400);
    private static final Duration DELETE_AT = Duration.ofMillis(2_000);

    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicLong SEQ = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        SEQ.set(0);
        Config config = new Config();
        config.setClusterName("nest-deleted-reference-test-" + System.nanoTime());
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
    @DisplayName("a document stops showing the row it points at once that row is deleted")
    void everyDocumentThatCarriedTheRowEndsUpWithoutIt() {
        member.getJet().newJob(ordersPointingAtACustomerThatIsThenDeleted()).join();

        assertThat(ordersThatEverCarriedTheCustomer())
                .describedAs("the control. Every document has to have carried the row at some point, or "
                        + "the assertion below is satisfied by documents that never had it - which is what "
                        + "a fetch that never happened produces, and it looks the same from here")
                .hasSize(ORDERS);

        Map<Object, Map<String, Object>> finalDocuments = latestPerOrder();
        assertThat(finalDocuments)
                .describedAs("all of them were emitted again after the deletion; a document not re-emitted "
                        + "is one the deletion never reached, and it would still be holding the old value")
                .hasSize(ORDERS);
        assertThat(finalDocuments.values())
                .describedAs("none of them still shows the deleted row. Keeping its last value is what an "
                        + "implementation that reads once and holds does, and those documents are complete, "
                        + "consistent and wrong only against a source nobody here asks again")
                .allSatisfy(document -> assertThat(document)
                        .describedAs("a null would be a value a sink writes over what is there; absent is "
                                + "how a document says the row it named is not there to show")
                        .doesNotContainKey("customer"));
    }

    /** Which orders were seen carrying the customer at any point in the run. */
    private static List<Object> ordersThatEverCarriedTheCustomer() {
        List<Object> carried = new ArrayList<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document == null) {
                continue;
            }
            if (document.get("customer") instanceof Map<?, ?> customer
                    && NAME.equals(customer.get("name"))
                    && !carried.contains(document.get("order_id"))) {
                carried.add(document.get("order_id"));
            }
        }
        return carried;
    }

    /** The last document emitted for each order. */
    private static Map<Object, Map<String, Object>> latestPerOrder() {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null && document.get("order_id") != null) {
                latest.put(document.get("order_id"), document);
            }
        }
        return latest;
    }

    // ---- the pipeline under test ------------------------------------------------------

    private static DAG ordersPointingAtACustomerThatIsThenDeleted() {
        List<Map<String, Object>> orders = new ArrayList<>(ORDERS);
        for (int i = 1; i <= ORDERS; i++) {
            orders.add(row("order_id", i, "cust_ref", CUSTOMER));
        }
        Map<String, Object> customer = row("customer_id", CUSTOMER, "name", NAME);

        Embed embed = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(embed)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("customer", FromRef.literal("customers"));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(PIPELINE, null,
                List.of("orders", "customers"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", wavesSource("orders", List.of(new Wave(ORDERS_AFTER_CUSTOMER, false, orders))));
        sources.put("customers", wavesSource("customers", List.of(
                new Wave(Duration.ZERO, false, List.of(customer)),
                new Wave(DELETE_AT, true, List.of(customer)))));

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

    private static Map<String, Object> row(String a, Object av, String b, Object bv) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(a, av);
        row.put(b, bv);
        return row;
    }

    private static ProcessorMetaSupplier wavesSource(String src, List<Wave> waves) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new WavesSource(src, waves)));
    }

    private record Wave(Duration after, boolean deleted, List<Map<String, Object>> rows)
            implements Serializable {
    }

    /** Emits its waves in order, each when it comes due, and completes. */
    private static final class WavesSource extends AbstractProcessor {

        private final String src;
        private final List<Wave> waves;
        private long startedAt = -1;
        private int wave;
        private int next;

        private WavesSource(String src, List<Wave> waves) {
            this.src = src;
            this.waves = waves;
        }

        @Override
        public boolean complete() {
            if (startedAt < 0) {
                startedAt = System.currentTimeMillis();
            }
            while (wave < waves.size()) {
                Wave current = waves.get(wave);
                if (System.currentTimeMillis() - startedAt < current.after().toMillis()) {
                    return false;
                }
                while (next < current.rows().size()) {
                    long seq = SEQ.incrementAndGet();
                    Map<String, Object> row = current.rows().get(next);
                    Envelope event = current.deleted()
                            ? Envelope.delete(seq, src, row, null)
                            : Envelope.insert(seq, src, row, null);
                    if (!tryEmit(event.withOrder(new SourceOrder(1, seq)))) {
                        return false;
                    }
                    next++;
                }
                wave++;
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
