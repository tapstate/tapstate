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
 * That what is held for a pointed-at row is never let go of - the row itself while it is still there, and
 * the record that it was deleted once it is not - however few documents are left pointing at it.
 *
 * <p><b>Both halves are the same sentence and they were arrived at from opposite ends.</b> Keeping every
 * row a document ever pointed at, for as long as the job runs, is a leak nothing reports: the documents are
 * right, the counts are right, and what grows is one entry per row of a table nobody is looking at. Letting
 * go of a row the moment nothing points at it fixes that and introduces something worse - "nothing points
 * at it" is a statement about the documents seen so far and never about the ones to come, and a source
 * sends a row once, so a live row let go of is a row no later document can be given. Such a document
 * renders with the field simply missing, which is what a document with no such row looks like.
 *
 * <p><b>That sentence holds for a row already deleted at least as hard, which is the half this used to get
 * wrong.</b> The record of a deletion is what tells a document naming the row that it is not coming, so it
 * renders without the field and goes. Taken away because nothing happened to point at it in that moment,
 * the next document naming the row finds nothing where that answer was - and an absent entry reads as one
 * that has not arrived yet, so the document waits for an arrival already in the past, for the life of the
 * job. Neither failure moves a count, so each half needs its own case.
 *
 * <p><b>The pair beside this one asks the same two questions through the other door, and that is why both
 * exist.</b> There, the last document stops pointing at the row by being <em>re-pointed</em> somewhere
 * else; here, by being <em>deleted</em>. Those are two separate places in the code that reach the same
 * decision, added at different times for different reasons, and either one can be right while the other
 * does nothing at all - so a case driven by re-pointing cannot see what the deletion path does.
 */
class WhatIsKeptWhenTheLastReferrerIsDeletedTest {

    private static final String STEP = "order_doc";
    private static final int CUSTOMER = 7;
    private static final int ORDERS = 3;

    private static final Duration ORDERS_AFTER_CUSTOMER = Duration.ofMillis(400);
    private static final Duration DELETE_CUSTOMER_AT = Duration.ofMillis(1_600);
    private static final Duration DELETE_ORDERS_AT = Duration.ofMillis(2_600);

    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicLong SEQ = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        SEQ.set(0);
        Config config = new Config();
        config.setClusterName("nest-reclaim-test-" + System.nanoTime());
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
    @DisplayName("the record of a deleted row outlives the last document that pointed at it")
    void theRecordOfADeletedRowOutlivesTheLastDocumentPointingAtIt() {
        String pipeline = "reclaim-gone";
        member.getJet().newJob(ordersPointingAtOneCustomer(pipeline, true)).join();

        assertThat(referrersRecordedFor(pipeline))
                .describedAs("every order that was deleted has to have been taken out of what the "
                        + "customer remembers about who points at it, or what follows is about a "
                        + "reclamation that could not have happened rather than one that did")
                .isZero();
        assertThat(rowsHeldIn(pipeline))
                .describedAs("the deletion is still on record, as an empty row, with nothing pointing at "
                        + "it. It is the only thing that can ever tell the next order naming this customer "
                        + "that the row is not coming: an entry taken away instead is indistinguishable "
                        + "from one that has not arrived, and that order's document then waits for the "
                        + "life of the job with nothing thrown and no count moved")
                .containsExactly(Map.entry(List.of(CUSTOMER), Map.of()));
    }

    @Test
    @DisplayName("a row that is still there is kept even when nothing points at it any more")
    void aLiveRowIsNotLetGoOfJustBecauseNothingPointsAtItRightNow() {
        String pipeline = "reclaim-live";
        member.getJet().newJob(ordersPointingAtOneCustomer(pipeline, false)).join();

        assertThat(referrersRecordedFor(pipeline))
                .describedAs("the orders were deleted here too, so this half differs from the one above "
                        + "in exactly one thing: whether the customer itself was deleted. If they differ "
                        + "in what points at it as well, neither says anything about the other")
                .isZero();
        assertThat(rowsHeldIn(pipeline))
                .describedAs("the customer is still there and still carries its fields. Nothing points at "
                        + "it today, which says nothing about tomorrow: a source sends a row once, so a "
                        + "live row dropped here is one no later document can ever be given, and such a "
                        + "document renders with the field missing - the same document a customer that "
                        + "never existed produces")
                .containsExactly(Map.entry(List.of(CUSTOMER), row("customer_id", CUSTOMER, "name", "Ada")));
    }

    // ---- what the state was left holding ----------------------------------------------

    /** The rows the lookup namespace still holds, by the key each is filed under. */
    private List<Map.Entry<Object, Object>> rowsHeldIn(String pipeline) {
        IMap<Object, Object> lookup = member.getMap("nest." + pipeline + "." + STEP + ".customer");
        List<Map.Entry<Object, Object>> held = new ArrayList<>();
        for (Object key : lookup.keySet()) {
            held.add(Map.entry(key, lookup.get(key)));
        }
        return held;
    }

    /** How many referring rows are still recorded against the customer, across every bucket. */
    private int referrersRecordedFor(String pipeline) {
        IMap<Object, java.util.Collection<Object>> index =
                member.getMap("nest." + pipeline + "." + STEP + ".customer.refs");
        int recorded = 0;
        for (Object key : index.keySet()) {
            java.util.Collection<Object> bucket = index.get(key);
            recorded += bucket == null ? 0 : bucket.size();
        }
        return recorded;
    }

    // ---- the pipeline under test ------------------------------------------------------

    /**
     * One customer and three orders pointing at it, all three orders deleted at the end. Whether the
     * customer is deleted before them is the one thing that differs between the two halves.
     */
    private static DAG ordersPointingAtOneCustomer(String pipelineId, boolean deleteTheCustomer) {
        List<Map<String, Object>> orders = new ArrayList<>();
        for (int i = 1; i <= ORDERS; i++) {
            orders.add(row("order_id", i, "cust_ref", CUSTOMER));
        }
        Map<String, Object> customer = row("customer_id", CUSTOMER, "name", "Ada");

        Embed embed = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(embed)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("customer", FromRef.literal("customers"));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(pipelineId, null,
                List.of("orders", "customers"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        List<Wave> customerWaves = new ArrayList<>();
        customerWaves.add(new Wave(Duration.ZERO, false, List.of(customer)));
        if (deleteTheCustomer) {
            customerWaves.add(new Wave(DELETE_CUSTOMER_AT, true, List.of(customer)));
        }

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", wavesSource("orders", List.of(
                new Wave(ORDERS_AFTER_CUSTOMER, false, orders),
                new Wave(DELETE_ORDERS_AT, true, orders))));
        sources.put("customers", wavesSource("customers", customerWaves));

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

    /** Rows to send, whether they are being deleted, and how long after the start they are due. */
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
