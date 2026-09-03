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
import io.tapstate.runtime.engine.nest.HeapKeyedStateStore;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
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

    private static final String DEEP_STEP = "order_doc_deep";
    private static final String DEEP_PIPELINE = "deleted-reference-deep";
    private static final int SKU = 90;
    private static final String LABEL = "Blue Mug";

    private static final Duration ORDERS_AFTER_CUSTOMER = Duration.ofMillis(400);
    private static final Duration LINES_AFTER_ORDERS = Duration.ofMillis(700);
    private static final Duration DELETE_AT = Duration.ofMillis(2_000);

    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicLong SEQ = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void forgetTheLastRun() {
        WRITTEN.clear();
        SEQ.set(0);
    }

    /**
     * Starts the member these documents are assembled on, with or without a layer behind its state maps.
     *
     * <p><b>Which of the two is running is the whole point of there being two cases.</b> Without a cold
     * layer the state is a plain distributed map and every read is served from memory; with one, the map
     * is configured to read and write through to a store, and a key can be answered from either side. A
     * deletion has to reach the document through both, and only the second is the shape that is deployed.
     */
    private void startMember(boolean withColdLayer) {
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
        if (withColdLayer) {
            config.addMapConfig(NestSettings.defaults().backedStateMaps());
        }
        member = Hazelcast.newHazelcastInstance(config);
        if (withColdLayer) {
            NestStateMapStoreFactory.bindTo(member, new HeapKeyedStateStore());
        }
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
        startMember(false);
        everyDocumentEndsUpWithoutTheDeletedRow();
    }

    /**
     * The same case on the state shape that is actually deployed: a map with a store behind it.
     *
     * <p><b>It is a second case rather than a second binding of the first because the two are one assertion
     * over two different mechanisms, and passing one says nothing about the other.</b> Without a store, the
     * only place a pointed-at row is ever read from is the map, so "the deletion reached the document"
     * and "the deletion reached the map" are the same sentence. With a store the row has two homes, and a
     * deletion that reaches one of them leaves documents that are complete, consistent, and showing a row
     * the source no longer has - which is the failure the case above was written for and cannot see.
     */
    @Test
    @DisplayName("a document stops showing the deleted row when its state has a cold layer too")
    void everyDocumentEndsUpWithoutTheDeletedRowWithAColdLayer() {
        startMember(true);
        everyDocumentEndsUpWithoutTheDeletedRow();
    }

    /**
     * The same promise on a document that points at rows from two depths at once, which is the shape a tree
     * is actually written in.
     *
     * <p><b>Both halves are asserted in one run because a run that converges only one of them is the whole
     * finding.</b> Told apart, each depth's case passes on a tree that holds only that depth; together, the
     * one that converges is the control for the one that does not - it rules out the source, the deletion
     * reaching the tree at all, and the documents being re-drawn, none of which a single-depth failure can
     * separate.
     */
    @Test
    @DisplayName("a document stops showing deleted rows it points at from the root and from inside an array")
    void bothDepthsEndUpWithoutTheRowsTheyPointedAt() {
        startMember(true);
        member.getJet().newJob(aDocumentPointingAtRowsFromTwoDepthsThatAreThenDeleted()).join();

        assertThat(ordersThatEverCarried("customer"))
                .describedAs("the control for the root-level half: every document carried the customer at "
                        + "some point, so an absence below is the deletion and not a fetch that never was")
                .hasSize(ORDERS);
        assertThat(ordersThatEverCarriedALineWithASku())
                .describedAs("and the control for the nested half, on the same footing")
                .hasSize(ORDERS);

        Map<Object, Map<String, Object>> finalDocuments = latestPerOrder("id");
        assertThat(finalDocuments).hasSize(ORDERS);
        assertThat(finalDocuments.values())
                .describedAs("the row pointed at from inside the array is gone from every line")
                .allSatisfy(document -> assertThat(linesOf(document))
                        .allSatisfy(line -> assertThat(line).doesNotContainKey("sku")));
        assertThat(finalDocuments.values())
                .describedAs("and the row pointed at from the root is gone from every document. This is the "
                        + "half that a tree carrying only one depth cannot tell apart from the other")
                .allSatisfy(document -> assertThat(document).doesNotContainKey("customer"));

        assertTheEmissionSaysTheFieldIsGone(latestEnvelopePerOrder("id"), "customer");
    }

    /** Which orders were seen carrying {@code field} at any point in the run. */
    private static List<Object> ordersThatEverCarried(String field) {
        List<Object> carried = new ArrayList<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null && document.get(field) instanceof Map<?, ?>
                    && !carried.contains(document.get("id"))) {
                carried.add(document.get("id"));
            }
        }
        return carried;
    }

    /** Which orders were seen with a line that carried the product it points at. */
    private static List<Object> ordersThatEverCarriedALineWithASku() {
        List<Object> carried = new ArrayList<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document == null || carried.contains(document.get("id"))) {
                continue;
            }
            for (Map<String, Object> line : linesOf(document)) {
                if (line.get("sku") instanceof Map<?, ?> held && LABEL.equals(held.get("label"))) {
                    carried.add(document.get("id"));
                    break;
                }
            }
        }
        return carried;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> linesOf(Map<String, Object> document) {
        List<Map<String, Object>> lines = new ArrayList<>();
        if (document.get("items") instanceof List<?> held) {
            for (Object line : held) {
                if (line instanceof Map<?, ?> fields) {
                    lines.add((Map<String, Object>) fields);
                }
            }
        }
        return lines;
    }

    private void everyDocumentEndsUpWithoutTheDeletedRow() {
        member.getJet().newJob(ordersPointingAtACustomerThatIsThenDeleted()).join();

        assertThat(ordersThatEverCarriedTheCustomer())
                .describedAs("the control. Every document has to have carried the row at some point, or "
                        + "the assertion below is satisfied by documents that never had it - which is what "
                        + "a fetch that never happened produces, and it looks the same from here")
                .hasSize(ORDERS);

        Map<Object, Map<String, Object>> finalDocuments = latestPerOrder("order_id");
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

        assertTheEmissionSaysTheFieldIsGone(latestEnvelopePerOrder("order_id"), "customer");
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

    /** The last document emitted for each order, told apart by whichever column names the order. */
    private static Map<Object, Map<String, Object>> latestPerOrder(String idField) {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        latestEnvelopePerOrder(idField).forEach((key, written) -> latest.put(key, written.after()));
        return latest;
    }

    /** The last envelope emitted for each order, which is what the sink is actually handed. */
    private static Map<Object, Envelope> latestEnvelopePerOrder(String idField) {
        Map<Object, Envelope> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null && document.get(idField) != null) {
                latest.put(document.get(idField), written);
            }
        }
        return latest;
    }

    /**
     * That the emission says the field is gone, rather than merely not mentioning it.
     *
     * <p><b>A document that has stopped carrying a field and a document that never carried one are the same
     * bytes, and the difference is the whole of what a target needs.</b> A sink that applies an emission by
     * setting the fields in it - which is what an upsert into a document store is - cannot remove what is
     * absent, so a field that vanishes is left standing in the target for ever while every assertion about
     * the emitted document still passes. Measured on a live stack: fifteen minutes after the row was
     * deleted, the document in the target still named it, and the document the engine emitted did not.
     *
     * <p>Only the top level is ever named. A field nested inside another is replaced along with whatever
     * holds it, because that container is itself a field of the emission and is set whole.
     */
    private static void assertTheEmissionSaysTheFieldIsGone(Map<Object, Envelope> latest, String field) {
        assertThat(latest.values())
                .describedAs("every document says '%s' is gone, so a target holding the old value is told "
                        + "to drop it. Leaving it unsaid renders identically and never converges", field)
                .allSatisfy(written -> assertThat(written.removed()).contains(field));
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

    /**
     * The tree as it is actually written: a row pointed at from the root and another pointed at from inside
     * an array, both deleted, in one document.
     *
     * <p><b>Every case above holds one reference at one depth, and a tree with one of them cannot show what
     * a tree with both does.</b> The two are compiled from the same words into two lookups whose entries,
     * registrations and wake-ups all live under names derived from where they hang - so the depths only
     * begin to be able to answer for each other once there are two of them.
     *
     * <p><b>The column names repeat across the tables on purpose.</b> A real schema calls the identity of
     * every table {@code id} and names the reference after the table it points at, so the root row carries
     * an {@code id} of its own beside the {@code customer_id} naming somebody else's, and the row it points
     * at calls its identity {@code id} as well. Every other case here spells the three apart - {@code
     * order_id}, {@code cust_ref}, {@code customer_id} - which makes a column read off the wrong row of the
     * pair land on nothing and show up immediately. Reused names are what let it land on a value instead.
     */
    private static DAG aDocumentPointingAtRowsFromTwoDepthsThatAreThenDeleted() {
        Embed sku = new Embed("sku", Map.of("id", "sku_id"), EmbedAs.OBJECT,
                "sku", null, null, null, null);
        Embed items = new Embed("item", Map.of("order_id", "id"), EmbedAs.ARRAY,
                "items", List.of("id"), null, null, List.of(sku));
        Embed customer = new Embed("customer", Map.of("id", "customer_id"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("id"), null, null, List.of(customer, items)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("customer", FromRef.literal("customers"));
        aliases.put("item", FromRef.literal("items"));
        aliases.put("sku", FromRef.literal("skus"));
        Step step = Step.inline(DEEP_STEP, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(DEEP_PIPELINE, null,
                List.of("orders", "customers", "items", "skus"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(DEEP_STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        List<Map<String, Object>> orders = new ArrayList<>(ORDERS);
        List<Map<String, Object>> lines = new ArrayList<>(ORDERS);
        for (int i = 1; i <= ORDERS; i++) {
            orders.add(row("id", i, "customer_id", CUSTOMER));
            lines.add(row("id", 100 + i, "order_id", i, "sku_id", SKU));
        }
        Map<String, Object> customerRow = row("id", CUSTOMER, "name", NAME);
        Map<String, Object> skuRow = row("id", SKU, "label", LABEL);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", wavesSource("orders", List.of(new Wave(ORDERS_AFTER_CUSTOMER, false, orders))));
        sources.put("items", wavesSource("items", List.of(new Wave(LINES_AFTER_ORDERS, false, lines))));
        sources.put("customers", wavesSource("customers", List.of(
                new Wave(Duration.ZERO, false, List.of(customerRow)),
                new Wave(DELETE_AT, true, List.of(customerRow)))));
        sources.put("skus", wavesSource("skus", List.of(
                new Wave(Duration.ZERO, false, List.of(skuRow)),
                new Wave(DELETE_AT, true, List.of(skuRow)))));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("id")));
        tables.put("customer", new NestTable("customers", List.of("id")));
        tables.put("item", new NestTable("items", List.of("id")));
        tables.put("sku", new NestTable("skus", List.of("id")));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    private static Map<String, Object> row(String a, Object av, String b, Object bv, String c, Object cv) {
        Map<String, Object> row = row(a, av, b, bv);
        row.put(c, cv);
        return row;
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
