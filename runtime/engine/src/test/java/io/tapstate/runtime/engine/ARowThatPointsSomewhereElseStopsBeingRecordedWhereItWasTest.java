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
 * That a row re-pointed at another row stops being recorded against the one it left.
 *
 * <p>Recording is unconditional - every event of the pointing stream says where that row points now - so
 * the entries only ever grow unless something takes the old one back. Nothing about the documents shows
 * that: the re-pointed document renders from the column it now carries and is right either way. What is
 * wrong is behind it. The row it left goes on believing it is pointed at, so editing that row wakes a
 * document that no longer names it, and the entries recording who points where grow with every re-point
 * and never shrink - which is the thing the whole fanout limit is measured against.
 *
 * <p>So this reads the record itself rather than the documents. There is no document that can tell the
 * two apart, which is exactly why it goes unnoticed.
 */
class ARowThatPointsSomewhereElseStopsBeingRecordedWhereItWasTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "p-repoint";

    private static final int LEFT_BEHIND = 100;
    private static final int POINTED_AT_NOW = 200;

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-reference-repoint-test-" + System.nanoTime());
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
    @DisplayName("re-pointing a row takes it out of what it used to point at")
    void anIdentityIsRecordedOnlyAgainstTheRowItNowPointsAt() {
        member.getJet().newJob(anOrderThatChangesWhoItPointsAt()).join();

        assertThat(recordedAgainst(POINTED_AT_NOW))
                .describedAs("recorded against the row it now names, which is what wakes this document "
                        + "when that row is edited")
                .containsExactly(List.of(1));
        assertThat(recordedAgainst(LEFT_BEHIND))
                .describedAs("and no longer against the one it left. Left in, the row it walked away from "
                        + "goes on waking a document that does not name it, and every re-point over the "
                        + "life of the job adds one more entry that nothing ever takes back out - so the "
                        + "count the fanout limit is measured on drifts above the truth and fails a "
                        + "pipeline that is not actually that wide")
                .isEmpty();

        assertThat(latestDocument())
                .describedAs("and the document itself is right either way, which is why nothing above can "
                        + "be shown from a document")
                .containsEntry("customer", Map.of("customer_id", POINTED_AT_NOW, "name", "Grace"));
    }

    /** Every identity recorded as pointing at {@code customer}, across all of its buckets. */
    private Set<Object> recordedAgainst(int customer) {
        IMap<Object, Set<Object>> index = member.getMap(
                "nest." + PIPELINE + "." + STEP + ".customer.refs");
        Set<Object> recorded = new LinkedHashSet<>();
        for (int bucket = 0; bucket < NestLookup.BUCKETS; bucket++) {
            Set<Object> held = index.get(NestLookup.bucketKey(List.of(customer), bucket));
            if (held != null) {
                recorded.addAll(held);
            }
        }
        return recorded;
    }

    /** The last document written - what a sink upserting on the key would be left holding. */
    private static Map<String, Object> latestDocument() {
        Map<String, Object> latest = null;
        for (Envelope written : WRITTEN) {
            if (written.after() != null) {
                latest = written.after();
            }
        }
        return latest;
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** One order, pointed first at one customer and then at another. */
    private static DAG anOrderThatChangesWhoItPointsAt() {
        Map<String, Object> was = row("order_id", 1, "cust_ref", LEFT_BEHIND);
        Map<String, Object> now = row("order_id", 1, "cust_ref", POINTED_AT_NOW);

        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                // Key changes followed, which is what makes the earlier row travel with the update - and
                // the earlier row is the only thing that can say which entry to take this order out of.
                new NestRoot("order", List.of("order_id"), null, true, List.of(customer)));

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
        sources.put("orders", rowsSource("orders", List.of(
                new Timed(List.of(was), Duration.ofMillis(400), null),
                new Timed(List.of(now), Duration.ofMillis(1200), was))));
        sources.put("customers", rowsSource("customers", List.of(new Timed(List.of(
                row("customer_id", LEFT_BEHIND, "name", "Ada"),
                row("customer_id", POINTED_AT_NOW, "name", "Grace")), Duration.ZERO, null))));

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

    /** One batch of rows, when it is due, and the row each replaces where there is one. */
    private record Timed(List<Map<String, Object>> rows, Duration after, Map<String, Object> replacing)
            implements Serializable {
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
                    Envelope event = due.replacing() == null
                            ? Envelope.insert(emitted + 1L, src, row, null)
                            : Envelope.update(emitted + 1L, src, due.replacing(), row, null);
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
