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
 * That editing a row pointed at from inside an array element reaches every document holding such an
 * element - across documents, not only within one.
 *
 * <p><b>The depth is the whole of it.</b> Doing this at the root can be got right by an implementation that
 * cannot work one level down, and the difference does not show until something is written at depth: a
 * document holds many elements, each pointing somewhere different, so there is no one identity the document
 * could be routed to. What has to happen instead is that the word climbs the way the element's own rows
 * climb - held at the level that owns the element, re-addressed to the document above it - and this is the
 * case that says it does.
 *
 * <p>Two documents share the edited row on purpose. One document would be satisfied by anything that
 * re-drew the document the row was last seen in.
 */
class AnEditToARowPointedAtFromDeepInsideADocumentReachesItTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "p-deep";

    /** The one product both orders have a line for. */
    private static final int SHARED_SKU = 7;

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-deep-reference-test-" + System.nanoTime());
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
    @DisplayName("editing a shared product reaches the lines of every order holding one")
    void bothDocumentsFollowAnEditToTheProductTheirLinesPointAt() {
        member.getJet().newJob(ordersWithLinesPointingAtProducts()).join();

        assertThat(namesSeenForTheSharedProduct())
                .describedAs("the line carrying that product was rendered with its old name and then with "
                        + "the new one. At depth this is the whole mechanism: a document holds several lines "
                        + "each pointing somewhere different, so there is no single identity the document "
                        + "could be routed to - the word has to climb from the level owning the line")
                .containsExactlyInAnyOrder("Widget", "Cog");

        assertThat(ordersWhoseLineSaid("Cog"))
                .describedAs("and it reached both orders, not just whichever one was touched last. One "
                        + "document alone is satisfied by anything that happens to redraw it")
                .containsExactlyInAnyOrder(1, 2);

        assertThat(rowsFiledForProducts())
                .describedAs("with the product still stored once for the whole tree, not copied into each "
                        + "line that names it - the cheapest way to make the above pass at depth is to put "
                        + "the product's fields inside every element, and every assertion here would still "
                        + "hold while the count went from two rows to six")
                .isEqualTo(2);
    }

    /** Every distinct name the shared product was rendered under, anywhere in any document. */
    private static Set<Object> namesSeenForTheSharedProduct() {
        Set<Object> names = new LinkedHashSet<>();
        forEachLine((order, line) -> {
            if (line.get("product") instanceof Map<?, ?> product
                    && SHARED_SKU == (int) product.get("sku_id")) {
                names.add(product.get("name"));
            }
        });
        return names;
    }

    /** The orders in whose document a line was rendered with the product named {@code name}. */
    private static Set<Object> ordersWhoseLineSaid(String name) {
        Set<Object> orders = new LinkedHashSet<>();
        forEachLine((order, line) -> {
            if (line.get("product") instanceof Map<?, ?> product && name.equals(product.get("name"))) {
                orders.add(order);
            }
        });
        return orders;
    }

    private static void forEachLine(java.util.function.BiConsumer<Object, Map<?, ?>> visit) {
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document == null || !(document.get("lines") instanceof List<?> lines)) {
                continue;
            }
            for (Object line : lines) {
                if (line instanceof Map<?, ?> held) {
                    visit.accept(document.get("order_id"), held);
                }
            }
        }
    }

    /** How many product rows the tree is holding - one per product, however many lines name them. */
    private int rowsFiledForProducts() {
        IMap<Object, Object> filed = member.getMap(
                "nest." + PIPELINE + "." + STEP + ".lines.product");
        return filed.size();
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** Two orders, each with lines, some of which point at the same product. */
    private static DAG ordersWithLinesPointingAtProducts() {
        Embed product = new Embed("product", Map.of("sku_id", "sku_ref"), EmbedAs.OBJECT,
                "product", null, null, null, null);
        Embed lines = new Embed("line", Map.of("order_id", "order_id"), EmbedAs.ARRAY,
                "lines", List.of("line_no"), null, null, List.of(product));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(lines)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("line", FromRef.literal("lines"));
        aliases.put("product", FromRef.literal("products"));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(PIPELINE, null,
                List.of("orders", "lines", "products"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", rowsSource("orders", List.of(new Timed(List.of(
                one("order_id", 1), one("order_id", 2)), Duration.ofMillis(400), null))));
        sources.put("lines", rowsSource("lines", List.of(new Timed(List.of(
                line(101, 1, 1, SHARED_SKU), line(102, 1, 2, 8),
                line(201, 2, 1, SHARED_SKU)), Duration.ofMillis(700), null))));
        sources.put("products", rowsSource("products", List.of(
                new Timed(List.of(product(SHARED_SKU, "Widget"), product(8, "Bolt")),
                        Duration.ZERO, null),
                // Long after both documents are downstream carrying the old name.
                new Timed(List.of(product(SHARED_SKU, "Cog")), Duration.ofMillis(1800),
                        product(SHARED_SKU, "Widget")))));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("line", new NestTable("lines", List.of("line_id")));
        tables.put("product", new NestTable("products", List.of("sku_id")));

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

    private static Map<String, Object> one(String field, Object value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(field, value);
        return row;
    }

    /**
     * One order line. It carries an id of its own as well as its number within the order, because the two
     * are different things: the number is only unique inside its order, while what identifies the row - what
     * the record of who points where is written against - has to be unique across the whole table.
     */
    private static Map<String, Object> line(int lineId, int order, int lineNo, int sku) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("line_id", lineId);
        row.put("order_id", order);
        row.put("line_no", lineNo);
        row.put("sku_ref", sku);
        return row;
    }

    private static Map<String, Object> product(int sku, String name) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("sku_id", sku);
        row.put("name", name);
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
