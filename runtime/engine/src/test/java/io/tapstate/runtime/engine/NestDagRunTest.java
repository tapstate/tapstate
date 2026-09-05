package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
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
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestClock;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateLedger;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.ReleasedChild;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs a nest node inside a real Jet job on an embedded member. Every other nest test either drives the
 * operators by hand through their inbox or asserts the graph the compiler drew; between "correct when
 * fed by hand" and "correct when Jet feeds it" there is a seam that nothing covered, and the edges a
 * nest draws — partitioned and distributed, one ordinal per stream — are exactly what lives in it.
 *
 * <p>The shape is the smallest one that assembles anything: a root and one leaf embed, so there is no
 * resolver vertex and the assembler is fed straight from both sources. Two roots rather than one,
 * because a single root is satisfied by an implementation that piles every child onto whichever root
 * arrived first.
 */
class NestDagRunTest {

    /** What the sink was handed, in arrival order. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    /** How many times the read side of the durable frontier was bound. Static for the same reason. */
    private static final AtomicInteger FLOORS_BOUND = new AtomicInteger();

    /** What a level handed over rather than placing in a document. Static for the same reason. */
    private static final List<ReleasedChild> RELEASED = Collections.synchronizedList(new ArrayList<>());

    /**
     * How long a change is left waiting before the reading is taken. Long enough for the idle path to run
     * many times over, so that nothing handed over means nothing gives up rather than nothing had time to.
     */
    private static final Duration LEFT_WAITING = Duration.ofSeconds(2);

    /** How long the root row is held back from the source that carries it, well inside {@link #LEFT_WAITING}. */
    private static final Duration LATE_ROOT = Duration.ofMillis(500);

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        FLOORS_BOUND.set(0);
        RELEASED.clear();
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
    void aRootAndItsChildrenLeaveTheJobAsOneDocumentEach() {
        DAG dag = ordersWithItems(
                List.of(row("order_id", 1, "code", "A"), row("order_id", 2, "code", "B")),
                List.of(row("item_id", 10, "order_id", 1, "sku", "s10"),
                        row("item_id", 11, "order_id", 1, "sku", "s11"),
                        row("item_id", 12, "order_id", 1, "sku", "s12"),
                        row("item_id", 20, "order_id", 2, "sku", "s20")));

        member.getJet().newJob(dag).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(documents.keySet())
                .describedAs("every root that arrived has a document, and nothing invented one")
                .containsExactlyInAnyOrder(1, 2);
        assertThat(items(documents.get(1)))
                .describedAs("the three items of order 1 are all in its document")
                .hasSize(3);
        assertThat(items(documents.get(2)))
                .describedAs("order 2 keeps its own single item rather than order 1 taking it")
                .hasSize(1);
        assertThat(items(documents.get(1)).stream().map(item -> item.get("sku")).toList())
                .containsExactlyInAnyOrder("s10", "s11", "s12");
        assertThat(items(documents.get(2)).get(0)).containsEntry("sku", "s20");
    }

    /**
     * The same job with key changes followed, which draws a second edge into the assembler carrying every
     * row again keyed by what it is leaving. Two things could go wrong and neither shows up anywhere else:
     * the extra ordinal is one more edge a level waits on before it promises anything, so a chain never
     * registered against it pins the frontier and the job simply never finishes; and every row arriving
     * twice is every row that could be placed twice.
     *
     * <p>Nothing here moves between documents - that is covered where the operators are driven directly.
     * What is being asked is narrower and only a real job can answer it: with the second edge drawn, does
     * this still run to completion and produce exactly the documents it did without it.
     */
    @Test
    void followingKeyChangesDrawsASecondEdgeAndTheJobStillFinishesWithTheSameDocuments() {
        DAG dag = ordersWithItems(
                List.of(row("order_id", 1, "code", "A"), row("order_id", 2, "code", "B")),
                List.of(row("item_id", 10, "order_id", 1, "sku", "s10"),
                        row("item_id", 11, "order_id", 1, "sku", "s11"),
                        row("item_id", 20, "order_id", 2, "sku", "s20")),
                false, true);

        member.getJet().newJob(dag).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(documents.keySet()).containsExactlyInAnyOrder(1, 2);
        assertThat(items(documents.get(1)))
                .describedAs("arriving on two edges is not arriving twice into the document")
                .hasSize(2);
        assertThat(items(documents.get(2))).hasSize(1);
    }

    @Test
    void theSeamThatReadsBackTheDurableFrontierIsBoundOnTheMemberRunningTheAssembler() {
        DAG dag = ordersWithItems(List.of(row("order_id", 1, "code", "A")),
                List.of(row("item_id", 10, "order_id", 1, "sku", "s10")));

        member.getJet().newJob(dag).join();

        // An assembler that never binds it can still assemble every document correctly and pass every other
        // test here, and simply never forgets a deleted root for as long as the job runs. Nothing about the
        // output says so, which is why the binding itself is what is asserted.
        assertThat(FLOORS_BOUND.get())
                .describedAs("the assembler was supplied without the read side of the durable frontier")
                .isPositive();
    }

    @Test
    void anItemWhoseOrderNeverArrivesIsKeptRatherThanHandedOff() throws Exception {
        // The stray item names an order that never arrives, so it waits in the assembler's state - and goes
        // on waiting. Its order may still arrive tomorrow, and the state it waits in is written through to
        // a store, so nothing here has to choose between keeping it and letting the frontier move: the
        // frontier moves regardless, and the item is still there when the order shows up.
        //
        // Giving up on it instead would mean deciding, from this side, that a row nobody has seen does not
        // exist - and being wrong about that drops a row from a document with every count reading healthy.
        //
        // The sources go on running after their rows, because a job that ends is a job whose vertices are
        // never idle, and anything that gave up on a held change would do it from the idle path.
        DAG dag = ordersWithItems(List.of(row("order_id", 1, "code", "A")),
                List.of(row("item_id", 10, "order_id", 1, "sku", "s10"),
                        row("item_id", 99, "order_id", 999, "sku", "stray")),
                true);

        Job job = member.getJet().newJob(dag);
        try {
            awaitWellPastAnyGivingUp();
        } finally {
            job.cancel();
        }

        assertThat(List.copyOf(RELEASED))
                .describedAs("left waiting long past any window something could have given up in, and the "
                        + "item is still held: nothing gives up on a change for a parent that has not arrived")
                .isEmpty();
    }

    /**
     * Leaves a change waiting long past the window in which anything giving up on it would have done so, so
     * that an empty reading afterwards means nothing gave up rather than that nothing had time to.
     */
    private static void awaitWellPastAnyGivingUp() throws InterruptedException {
        Thread.sleep(LEFT_WAITING.plusSeconds(1).toMillis());
    }

    @Test
    void aRootBringsInTheRowItPointsAtRatherThanTheRowsThatPointAtIt() {
        // The other direction: an order names a customer, so the customer is one row shared by many
        // documents rather than a child grouped under one. Three orders across two customers, and the
        // count alone tells the two readings apart - grouping on customer_id would produce two.
        // The two sides are deliberately named differently. Written as one name on both, a reference read
        // off the wrong side of the join still finds the right value and every assertion here still
        // passes - so the shape that would prove nothing is the one that looks tidiest.
        DAG dag = ordersWithCustomers(
                List.of(row("order_id", 1, "cust_ref", 100, "code", "A"),
                        row("order_id", 2, "cust_ref", 100, "code", "B"),
                        row("order_id", 3, "cust_ref", 200, "code", "C")),
                List.of(row("customer_id", 100, "name", "Ada"),
                        row("customer_id", 200, "name", "Grace")));

        member.getJet().newJob(dag).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(documents.keySet())
                .describedAs("one document per order, not one per customer")
                .containsExactlyInAnyOrder(1, 2, 3);
        assertThat(customer(documents.get(1)).get("name"))
                .describedAs("the customer the order points at, fetched rather than routed here")
                .isEqualTo("Ada");
        assertThat(customer(documents.get(2)).get("name"))
                .describedAs("the same customer row reaches a second document")
                .isEqualTo("Ada");
        assertThat(customer(documents.get(3)).get("name")).isEqualTo("Grace");
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** orders as the root, order_items embedded beneath it as an array at {@code items}. */
    private static DAG ordersWithItems(List<Map<String, Object>> orders, List<Map<String, Object>> items) {
        return ordersWithItems(orders, items, false);
    }

    /**
     * The same, with {@code endless} keeping the sources alive after their rows so the vertices go idle —
     * and, for the same case, holding the root row back so that a child is waiting for it when they do.
     */
    private static DAG ordersWithItems(List<Map<String, Object>> orders, List<Map<String, Object>> items,
            boolean endless) {
        return ordersWithItems(orders, items, endless, false);
    }

    private static DAG ordersWithItems(List<Map<String, Object>> orders, List<Map<String, Object>> items,
            boolean endless, boolean trackKeyChanges) {
        Embed item = new Embed("item", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "items",
                List.of("item_id"), null, trackKeyChanges ? Boolean.TRUE : null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(item)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("item", FromRef.literal("order_items"));
        Step step = Step.inline("order_doc", FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource("p", null,
                List.of("orders", "order_items"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("order_doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", rowsSource("orders", orders, endless, endless ? LATE_ROOT : Duration.ZERO));
        sources.put("order_items", rowsSource("order_items", items, endless, Duration.ZERO));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("item", new NestTable("order_items", List.of("item_id")));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, HeapNestStores.onHeap(),
                        (from, released) -> RELEASED.add(released), new CountingFloors(),
                        NestStateLedger.NONE, NestSettings.defaults(), NestClock.SYSTEM));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    /**
     * orders as the root with the customer each one points at placed at {@code customer}. Nothing here
     * declares a direction: orders.customer_id is not what identifies an order and customers.customer_id
     * is what identifies a customer, which is the whole of what says which way this embed points.
     */
    private static DAG ordersWithCustomers(List<Map<String, Object>> orders,
            List<Map<String, Object>> customers) {
        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(customer)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("customer", FromRef.literal("customers"));
        Step step = Step.inline("order_doc", FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource("p", null,
                List.of("orders", "customers"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("order_doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        // The customers are in place before an order asks for one. What wakes a document whose reference
        // arrives after it is a different mechanism from the one under test here - this case is about the
        // read finding the row, and holding the orders back is what keeps the two apart.
        sources.put("orders", rowsSource("orders", orders, false, LATE_ROOT));
        sources.put("customers", rowsSource("customers", customers, false, Duration.ZERO));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("customer", new NestTable("customers", List.of("customer_id")));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, HeapNestStores.onHeap(),
                        (from, released) -> RELEASED.add(released), new CountingFloors(),
                        NestStateLedger.NONE, NestSettings.defaults(), NestClock.SYSTEM));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    /** A read side that records that it was bound and then knows nothing, so nothing is ever forgotten. */
    private static final class CountingFloors implements ReplayFloorFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public ReplayFloor resolve(HazelcastInstance member) {
            FLOORS_BOUND.incrementAndGet();
            return ReplayFloor.NONE;
        }
    }

    // ---- reading what came out --------------------------------------------------------

    /** The last state each root reached, keyed by its root key: a document is emitted per drain. */
    private static Map<Object, Map<String, Object>> latestPerRoot() {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null) {
                latest.put(document.get("order_id"), document);
            }
        }
        return latest;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> customer(Map<String, Object> document) {
        assertThat(document).describedAs("no document was assembled for this root").isNotNull();
        Object embedded = document.get("customer");
        assertThat(embedded).describedAs("the document carries no customer: %s", document).isNotNull();
        return (Map<String, Object>) embedded;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> items(Map<String, Object> document) {
        assertThat(document).describedAs("no document was assembled for this root").isNotNull();
        Object embedded = document.get("items");
        assertThat(embedded).describedAs("the document carries no items array: %s", document).isNotNull();
        return (List<Map<String, Object>>) embedded;
    }

    // ---- doubles ----------------------------------------------------------------------

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    /**
     * A source that turns rows into inserts on the member, each stamped with the order the engine would
     * have given it. A stateful node crashes bare on an event with no order, so a synthetic source that
     * leaves it null tests nothing.
     */
    private static ProcessorMetaSupplier rowsSource(String src, List<Map<String, Object>> rows,
            boolean endless, Duration startAfter) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                (SupplierEx<Processor>) () -> new RowsSource(src, rows, endless, startAfter)));
    }

    private static final class RowsSource extends AbstractProcessor {

        private final String src;
        private final List<Map<String, Object>> rows;
        private final boolean endless;
        private final long startAfterMillis;
        private long dueAt = -1;
        private int next;

        RowsSource(String src, List<Map<String, Object>> rows, boolean endless, Duration startAfter) {
            this.src = src;
            this.rows = rows;
            this.endless = endless;
            this.startAfterMillis = startAfter.toMillis();
        }

        @Override
        public boolean complete() {
            if (!due()) {
                return false;
            }
            while (next < rows.size()) {
                Envelope event = Envelope.insert(next + 1L, src, rows.get(next), null)
                        .withOrder(new SourceOrder(1, next));
                if (!tryEmit(event)) {
                    return false;
                }
                next++;
            }
            return !endless;
        }

        /**
         * Whether this source's rows are due, counted from the first time it was asked. A source told to
         * start late yields until they are rather than sleeping: the thread it runs on is shared with every
         * other cooperative vertex here, and one that sleeps on it stops them too.
         */
        private boolean due() {
            if (startAfterMillis == 0) {
                return true;
            }
            long now = System.currentTimeMillis();
            if (dueAt < 0) {
                dueAt = now + startAfterMillis;
            }
            return now >= dueAt;
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
