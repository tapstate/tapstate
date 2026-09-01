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
import com.hazelcast.map.IMap;
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
import io.tapstate.runtime.engine.nest.NestLookup;
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
 * What is kept for a row once the last document pointing at it has walked away, and what is let go.
 *
 * <p>Two different things are held per pointed-at row and only one of them is the row. There is the record
 * that it was deleted, which exists to answer a document that still names it - and once nothing names it,
 * that answer is for nobody. And there is the row itself, which is this tree's only copy of it.
 *
 * <p><b>Letting go of the second is what these two cases are here to keep apart.</b> Nothing points at a
 * row is a statement about the documents seen so far, never about the ones still to come: rows are read
 * once and then only when they change, so a row dropped because it happened to be unwanted at that moment
 * cannot be got back - and the document that names it next renders without it, correct-looking and wrong,
 * with no error anywhere. Which is why only the first case lets anything go.
 */
class WhatIsKeptForARowNobodyPointsAtAnyMoreTest {

    private static final String STEP = "order_doc";

    private static final int WALKED_AWAY_FROM = 100;
    private static final int POINTED_AT_NOW = 200;

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-reference-reclaim-test-" + System.nanoTime());
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
    @DisplayName("the record of a deleted row goes once nothing points at it any more")
    void theRecordOfADeletedRowGoesOnceNothingPointsAtItAnyMore() {
        String pipeline = "p-reclaim";
        member.getJet().newJob(orders(pipeline, List.of(
                new Timed(List.of(row("order_id", 1, "cust_ref", WALKED_AWAY_FROM)),
                        Duration.ofMillis(400), null),
                new Timed(List.of(row("order_id", 1, "cust_ref", POINTED_AT_NOW)),
                        Duration.ofMillis(1600), row("order_id", 1, "cust_ref", WALKED_AWAY_FROM))),
                List.of(
                        new Timed(List.of(customer(WALKED_AWAY_FROM, "Ada"), customer(POINTED_AT_NOW,
                                "Grace")), Duration.ZERO, null),
                        new Timed(List.of(customer(WALKED_AWAY_FROM, "Ada")),
                                Duration.ofMillis(1000), null, true)))).join();

        assertThat(rows(pipeline).keySet())
                .describedAs("the deleted row is not kept once nothing names it. The record of a deletion "
                        + "exists to answer a document still pointing there - once none is, it answers "
                        + "nobody, and kept anyway every deletion the source ever makes leaves one behind "
                        + "for the life of the job")
                .doesNotContain(List.of(WALKED_AWAY_FROM));
    }

    @Test
    @DisplayName("a live row nobody points at is still there for the document that names it next")
    void aliveRowIsKeptSoALaterDocumentPointingAtItIsStillRight() {
        String pipeline = "p-kept";
        member.getJet().newJob(orders(pipeline, List.of(
                new Timed(List.of(row("order_id", 1, "cust_ref", WALKED_AWAY_FROM)),
                        Duration.ofMillis(400), null),
                new Timed(List.of(row("order_id", 1, "cust_ref", POINTED_AT_NOW)),
                        Duration.ofMillis(1200), row("order_id", 1, "cust_ref", WALKED_AWAY_FROM)),
                // Arriving after the only document that named that customer walked away from it, which is
                // the moment a reclaim driven by "nobody points at this" would have thrown the row out.
                new Timed(List.of(row("order_id", 2, "cust_ref", WALKED_AWAY_FROM)),
                        Duration.ofMillis(2000), null)),
                List.of(new Timed(List.of(customer(WALKED_AWAY_FROM, "Ada"),
                        customer(POINTED_AT_NOW, "Grace")), Duration.ZERO, null)))).join();

        assertThat(latestPerOrder().get(2))
                .describedAs("the later order carries the customer it names. The source sent that row once "
                        + "and will not send it again until it changes, so a copy dropped for want of "
                        + "anyone pointing at it is a copy nothing can get back - and what goes downstream "
                        + "then is an order with no customer, which reads exactly like an order that has "
                        + "none")
                .containsEntry("customer", Map.of("customer_id", WALKED_AWAY_FROM, "name", "Ada"));
    }

    @Test
    @DisplayName("a deleted document stops pointing at what it named, without needing key tracking on")
    void aDeletedDocumentStopsPointingAtWhatItNamed() {
        String pipeline = "p-deleted";
        List<Map<String, Object>> three = List.of(
                row("order_id", 1, "cust_ref", WALKED_AWAY_FROM),
                row("order_id", 2, "cust_ref", WALKED_AWAY_FROM),
                row("order_id", 3, "cust_ref", WALKED_AWAY_FROM));

        member.getJet().newJob(orders(pipeline, false, List.of(
                new Timed(three, Duration.ofMillis(400), null),
                new Timed(three, Duration.ofMillis(1200), null, true)),
                List.of(new Timed(List.of(customer(WALKED_AWAY_FROM, "Ada")), Duration.ZERO, null))))
                .join();

        assertThat(recordedAgainst(pipeline, WALKED_AWAY_FROM))
                .describedAs("a document that has been deleted points at nothing, and being deleted is how "
                        + "a document stops pointing far more often than being re-pointed is. Left in, the "
                        + "count never comes down at all for a table whose rows are deleted rather than "
                        + "re-pointed - every edit to that customer wakes three documents that no longer "
                        + "exist, and the fanout limit is measured on a number that only ever rises")
                .isEmpty();
    }

    /** Every identity recorded as pointing at {@code customer}, across all of its buckets. */
    private Set<Object> recordedAgainst(String pipeline, int customer) {
        IMap<Object, Set<Object>> index = member.getMap(
                "nest." + pipeline + "." + STEP + ".customer.refs");
        Set<Object> recorded = new LinkedHashSet<>();
        for (int bucket = 0; bucket < NestLookup.BUCKETS; bucket++) {
            Set<Object> held = index.get(NestLookup.bucketKey(List.of(customer), bucket));
            if (held != null) {
                recorded.addAll(held);
            }
        }
        return recorded;
    }

    /** The rows filed for the pointed-at level of {@code pipeline}. */
    private Map<Object, Object> rows(String pipeline) {
        IMap<Object, Object> filed = member.getMap("nest." + pipeline + "." + STEP + ".customer");
        return new LinkedHashMap<>(filed);
    }

    /** The last document written per order - what a sink upserting them would be left holding. */
    private static Map<Object, Map<String, Object>> latestPerOrder() {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            if (written.after() != null) {
                latest.put(written.after().get("order_id"), written.after());
            }
        }
        return latest;
    }

    // ---- the pipeline under test ------------------------------------------------------

    private static DAG orders(String pipelineId, List<Timed> orders, List<Timed> customers) {
        return orders(pipelineId, true, orders, customers);
    }

    private static DAG orders(String pipelineId, boolean trackKeyChanges, List<Timed> orders,
            List<Timed> customers) {
        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, trackKeyChanges, List.of(customer)));

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
        sources.put("orders", rowsSource("orders", orders));
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

    private static Map<String, Object> customer(int id, String name) {
        return row("customer_id", id, "name", name);
    }

    private static Map<String, Object> row(String a, Object av, String b, Object bv) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(a, av);
        row.put(b, bv);
        return row;
    }

    /** One batch: the rows, when they are due, the row each replaces, and whether they are removed. */
    private record Timed(List<Map<String, Object>> rows, Duration after, Map<String, Object> replacing,
            boolean deleting) implements Serializable {

        private Timed(List<Map<String, Object>> rows, Duration after, Map<String, Object> replacing) {
            this(rows, after, replacing, false);
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
                    Envelope event;
                    if (due.deleting()) {
                        event = Envelope.delete(emitted + 1L, src, row, null);
                    } else if (due.replacing() == null) {
                        event = Envelope.insert(emitted + 1L, src, row, null);
                    } else {
                        event = Envelope.update(emitted + 1L, src, due.replacing(), row, null);
                    }
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
