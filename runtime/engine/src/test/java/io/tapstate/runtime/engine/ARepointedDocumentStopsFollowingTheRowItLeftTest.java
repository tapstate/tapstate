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
 * That a document pointed somewhere else stops following the row it left.
 *
 * <p><b>The first half of this passes on an implementation that has done only half the work.</b> Showing
 * the new row after a re-point needs nothing more than reading the document's own column again at render
 * time, which every implementation here does. What it does not say is whether the record of who points at
 * what moved with it - and if that record still names the old row, the document goes on being rebuilt
 * every time the old row changes, carrying a value it no longer has anything to do with.
 *
 * <p><b>So the discriminating half is the one that asserts a non-event.</b> After the re-point, the row
 * the document walked away from is edited, and the document must not follow. That is the only observation
 * that separates "the document was re-rendered" from "the relationship was moved", and it is invisible in
 * any steady state: both implementations show the new row, both keep every count, and neither throws.
 *
 * <p>Waiting is what makes a non-event assertion mean anything. The edit to the abandoned row lands well
 * before the run ends, so a document that was going to follow it has had its chance - an assertion made
 * immediately would be reading a queue rather than an outcome.
 */
class ARepointedDocumentStopsFollowingTheRowItLeftTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "repointed-reference";

    /** The row the document points at first and then walks away from. */
    private static final int LEFT_BEHIND = 100;

    /** The row it points at afterwards. */
    private static final int POINTED_AT_NOW = 200;

    private static final String LEFT_NAME = "Ada";
    private static final String NEW_NAME = "Grace";

    /** What the abandoned row is renamed to. No document should ever be seen carrying this. */
    private static final String AFTER_THE_MOVE = "Ada-renamed-6ba2";

    private static final Duration ORDER_AFTER_CUSTOMERS = Duration.ofMillis(400);
    private static final Duration REPOINT_AT = Duration.ofMillis(1_600);
    private static final Duration RENAME_LEFT_ROW_AT = Duration.ofMillis(2_800);

    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicLong SEQ = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        SEQ.set(0);
        Config config = new Config();
        config.setClusterName("nest-repoint-test-" + System.nanoTime());
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
    @DisplayName("a re-pointed document takes the new row and stops following the old one")
    void theDocumentShowsTheNewRowAndDoesNotFollowTheOneItLeft() {
        member.getJet().newJob(anOrderThatIsRepointed()).join();

        List<String> names = namesInOrderOfEmission();
        assertThat(names)
                .describedAs("the control: the document has to have carried the row it started with, or "
                        + "what follows is about a document that never pointed anywhere")
                .startsWith(LEFT_NAME);
        assertThat(names)
                .describedAs("the first half - after the re-point the document shows the row it now names. "
                        + "This passes on an implementation that has moved nothing but the rendering")
                .contains(NEW_NAME);
        assertThat(names)
                .describedAs("the half that discriminates, and it asserts a non-event: the row the "
                        + "document walked away from was renamed afterwards, and nothing here followed it. "
                        + "An implementation that re-renders without moving the record of who points at "
                        + "what rebuilds this document on that rename and shows %s - with the document "
                        + "complete, the counts right and nothing thrown", AFTER_THE_MOVE)
                .doesNotContain(AFTER_THE_MOVE);
        assertThat(names.get(names.size() - 1))
                .describedAs("and it comes to rest on the row it now points at rather than anywhere else")
                .isEqualTo(NEW_NAME);
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

    private static DAG anOrderThatIsRepointed() {
        Map<String, Object> pointsAtLeft = row("order_id", 1, "cust_ref", LEFT_BEHIND);
        Map<String, Object> pointsAtNew = row("order_id", 1, "cust_ref", POINTED_AT_NOW);

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
        sources.put("orders", wavesSource("orders", List.of(
                new Wave(ORDER_AFTER_CUSTOMERS, false, List.of(pointsAtLeft)),
                new Wave(REPOINT_AT, false, List.of(pointsAtNew)))));
        sources.put("customers", wavesSource("customers", List.of(
                new Wave(Duration.ZERO, false, List.of(
                        row("customer_id", LEFT_BEHIND, "name", LEFT_NAME),
                        row("customer_id", POINTED_AT_NOW, "name", NEW_NAME))),
                new Wave(RENAME_LEFT_ROW_AT, false, List.of(
                        row("customer_id", LEFT_BEHIND, "name", AFTER_THE_MOVE))))));

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
