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
 * That one embed is enough to bring in the row a document points at, and that the documents stay one per
 * row of the stream they are rooted in.
 *
 * <p><b>The count is the assertion, and it is the one the shape this replaces gets wrong.</b> Gathering
 * rows under a parent - the direction that already existed - would take these same three orders and two
 * customers and produce two documents, one per customer, each holding its orders. That is a correct
 * document of a different shape, with correct data in it, and it is what an embed pointing the wrong way
 * silently becomes. Three orders have to make three documents; two is not a smaller answer to the same
 * question, it is the answer to the other one.
 *
 * <p>Each document also has to carry <em>its own</em> customer rather than whichever arrived last. Two
 * customers rather than one is what makes that readable: with a single row every document carries the
 * right one no matter how the fetch is keyed.
 */
class OneEmbedBringsInTheRowEachDocumentPointsAtTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "one-embed";

    /** Two customers, so that carrying the right one is a thing a document can get wrong. */
    private static final int ADA = 100;
    private static final int GRACE = 200;

    /** Three orders across those two, which is what makes the document count discriminating. */
    private static final int ORDERS = 3;

    private static final Duration ORDERS_AFTER_CUSTOMERS = Duration.ofMillis(400);

    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicLong SEQ = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        SEQ.set(0);
        Config config = new Config();
        config.setClusterName("nest-one-embed-test-" + System.nanoTime());
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
    @DisplayName("three orders across two customers make three documents, each with its own")
    void thereIsADocumentPerOrderAndEachCarriesTheCustomerItNames() {
        member.getJet().newJob(anAppendRootPointingAtOneCustomer()).join();

        Map<Object, String> byOrder = latestNameByOrder();

        assertThat(byOrder)
                .describedAs("one document per order. Gathering these rows under their customer instead - "
                        + "which is what an embed pointing the other way becomes - produces two documents "
                        + "of entirely correct data, and %d is the answer to a different question",
                        byOrder.size())
                .hasSize(ORDERS);
        assertThat(byOrder)
                .describedAs("and each carries the customer its own column names rather than whichever "
                        + "row arrived last, which a single-customer case could not tell apart")
                .containsEntry(1, "Ada")
                .containsEntry(2, "Ada")
                .containsEntry(3, "Grace");
    }

    /** The customer name on the last document emitted for each order. */
    private static Map<Object, String> latestNameByOrder() {
        Map<Object, String> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null && document.get("customer") instanceof Map<?, ?> customer) {
                latest.put(document.get("order_id"), String.valueOf(customer.get("name")));
            }
        }
        return latest;
    }

    // ---- the pipeline under test ------------------------------------------------------

    private static DAG anAppendRootPointingAtOneCustomer() {
        // Orders 1 and 2 point at one customer, order 3 at the other - so a document carrying the wrong
        // one is visible, and so is a run that folded the three orders into two documents.
        List<Map<String, Object>> orders = List.of(
                row("order_id", 1, "cust_ref", ADA),
                row("order_id", 2, "cust_ref", ADA),
                row("order_id", 3, "cust_ref", GRACE));

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
        sources.put("orders", wavesSource("orders", List.of(new Wave(ORDERS_AFTER_CUSTOMERS, false, orders))));
        sources.put("customers", wavesSource("customers", List.of(new Wave(Duration.ZERO, false, List.of(
                row("customer_id", ADA, "name", "Ada"),
                row("customer_id", GRACE, "name", "Grace"))))));

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
