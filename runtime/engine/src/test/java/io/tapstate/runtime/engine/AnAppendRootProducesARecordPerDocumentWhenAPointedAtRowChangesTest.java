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
 * That one edit to a pointed-at row costs an append reader exactly one record per document pointing at
 * it - no fewer, and not folded into one.
 *
 * <p><b>The case beside this one reads the mechanism; this one reads the outcome, and they can disagree.</b>
 * That an append root is not allowed to fold is a property of the compiled tree and is asserted where the
 * tree is compiled. What it is supposed to buy is a record per affected document actually arriving at the
 * sink, and between the two sits every part of the run: the record of who points at the row, the wake-up
 * that reaches them, and the send that is not permitted to merge them. Any of those can be wrong while
 * the compiled property stays right.
 *
 * <p><b>Both directions of wrong are counted, because they are different faults.</b> Fewer records than
 * documents is an append reader losing changes it was owed - the thing folding is forbidden here to
 * prevent. More is the fanout being paid twice, which nothing downstream can undo and no functional
 * assertion notices: every record is correct, there are just more of them than there were documents.
 *
 * <p>The count is taken across the edit rather than over the whole run, because the records assembled on
 * the way in are not what an edit costs. What is being priced is the second event, over documents that
 * already existed.
 */
class AnAppendRootProducesARecordPerDocumentWhenAPointedAtRowChangesTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "append-fanout";

    private static final int CUSTOMER = 100;

    /** How many documents point at that one row, which is what one edit to it is expected to cost. */
    private static final int ORDERS = 5;

    private static final String BEFORE = "Ada";
    private static final String AFTER = "Grace";

    private static final Duration ORDERS_AFTER_CUSTOMER = Duration.ofMillis(400);
    private static final Duration RENAME_AT = Duration.ofMillis(2_400);

    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicLong SEQ = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        SEQ.set(0);
        Config config = new Config();
        config.setClusterName("nest-append-fanout-test-" + System.nanoTime());
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
    @DisplayName("one edit to a row five append documents point at costs exactly five records")
    void theEditCostsOneRecordPerDocumentPointingAtTheRow() {
        member.getJet().newJob(anAppendRootPointingAtOneCustomer()).join();

        List<String> names = namesInOrderOfEmission();
        assertThat(names.stream().filter(BEFORE::equals).count())
                .describedAs("the control: every document has to have been assembled carrying the row "
                        + "before it changed, or what is counted below is the cost of assembling rather "
                        + "than the cost of an edit")
                .isEqualTo(ORDERS);

        assertThat(names.stream().filter(AFTER::equals).count())
                .describedAs("one edit, %d documents pointing at the row, %d records carrying the new "
                        + "value. Fewer means an append reader lost changes it was owed, which is exactly "
                        + "what folding is forbidden here to prevent and what the compiled property alone "
                        + "cannot show; more means the fanout was paid twice, which reads as correct from "
                        + "every angle but this count", ORDERS,
                        names.stream().filter(AFTER::equals).count())
                .isEqualTo(ORDERS);
    }

    /** The customer name each emitted document carried, in the order the documents were emitted. */
    private static List<String> namesInOrderOfEmission() {
        List<String> names = new ArrayList<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null && document.get("customer") instanceof Map<?, ?> customer) {
                names.add(String.valueOf(customer.get("name")));
            }
        }
        return names;
    }

    // ---- the pipeline under test ------------------------------------------------------

    private static DAG anAppendRootPointingAtOneCustomer() {
        List<Map<String, Object>> orders = new ArrayList<>(ORDERS);
        for (int i = 1; i <= ORDERS; i++) {
            orders.add(row("order_id", i, "cust_ref", CUSTOMER));
        }

        Embed embed = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        // append, which is the whole point: this root is not allowed to merge two changes into one record.
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), "append", null, List.of(embed)));

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
                new Wave(Duration.ZERO, false, List.of(row("customer_id", CUSTOMER, "name", BEFORE))),
                new Wave(RENAME_AT, false, List.of(row("customer_id", CUSTOMER, "name", AFTER))))));

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
