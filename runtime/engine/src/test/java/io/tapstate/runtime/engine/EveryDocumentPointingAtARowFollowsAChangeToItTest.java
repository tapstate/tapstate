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
 * That every document pointing at a row carries the row's new value after it changes.
 *
 * <p><b>This is the behaviour the whole outward-pointing direction exists for, and until now nothing on
 * the merge gate read its content.</b> How many documents an edit reaches is counted by the cost gate,
 * which is a different question with the same answer in the failing case that matters least. What is
 * asserted here is what those documents say: an implementation that reads the row once when the document
 * is assembled and never again re-emits nothing at all on the edit, and one that re-emits without
 * re-reading emits the right number of documents carrying the old value. Neither throws, neither loses a
 * document, and only reading the value tells them apart.
 *
 * <p>The root is an ordinary upsert one, deliberately. The two cases beside this one that watch an edit
 * travel are a tree whose pointer sits under an array, and a root that may not fold - each of which is
 * right about its own shape and says nothing about the plainest one, which is the shape almost every
 * tree will actually have.
 *
 * <p>The control comes first: every document has to have carried the old value, or "they all carry the
 * new one" is satisfied by documents that were assembled after the change and never followed anything.
 */
class EveryDocumentPointingAtARowFollowsAChangeToItTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "follows-change";

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
        config.setClusterName("nest-follows-change-test-" + System.nanoTime());
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
    @DisplayName("all five documents end up carrying the row's new value")
    void everyDocumentEndsUpCarryingTheNewValue() {
        member.getJet().newJob(anAppendRootPointingAtOneCustomer()).join();

        assertThat(namesInOrderOfEmission())
                .describedAs("the control: every document has to have carried the old value first, or "
                        + "the assertion below is satisfied by documents assembled after the change that "
                        + "never followed anything")
                .contains(BEFORE);

        Map<Object, String> finalNames = latestNameByOrder();
        assertThat(finalNames)
                .describedAs("all %d documents were assembled, so what follows is about every one of "
                        + "them rather than whichever ones finished", ORDERS)
                .hasSize(ORDERS);
        assertThat(finalNames.values())
                .describedAs("every one of them carries the new value. Reading the row once at assembly "
                        + "and never again re-emits nothing at all here; re-emitting without re-reading "
                        + "produces the right number of documents carrying %s. Neither throws, neither "
                        + "loses a document, and only the value tells them apart", BEFORE)
                .containsOnly(AFTER);
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
        // An ordinary upsert root - the plainest shape, and the one the cases beside this cannot speak for.
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
