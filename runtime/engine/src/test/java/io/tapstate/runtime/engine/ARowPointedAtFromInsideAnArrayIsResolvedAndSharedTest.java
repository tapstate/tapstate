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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
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
 * That a row pointed at from inside an array is resolved for every element that points at it, that an edit
 * to it reaches all of them wherever they sit, and that it is still held once.
 *
 * <p><b>The level a pointer sits at is a whole failure surface of its own.</b> A design that routes a
 * document to wherever the row it points at lives is green on every case where the pointer is on the root:
 * one document, one row, one place to send it. It cannot work one level down and the reason is structural
 * rather than incidental - a document holds many elements, each pointing at a different row, and a
 * document has one route. So a tree with the pointer under an array is the only thing that separates
 * "resolved by fetching what the document names" from "resolved by sending the document to the row".
 *
 * <p><b>The three assertions fail for three different reasons, which is why none of them stands in for
 * another.</b> Getting the right row into each element says the fetch is keyed off the element's own
 * column. The edit reaching both documents says the pointer is a standing relationship and not a value
 * copied in when the element was mounted - an implementation that reads once at mount time passes the
 * first and fails the second. Counting where the row's fields ended up says it is shared rather than
 * copied into each element, and the cheapest way to make the first two pass is to copy it into all six.
 */
class ARowPointedAtFromInsideAnArrayIsResolvedAndSharedTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "deep-reference";

    /** The sku two elements in two different documents both point at. */
    private static final int SHARED_SKU = 500;

    /** What the shared sku is called to begin with, and what it is renamed to. */
    private static final String BEFORE = "Widget-before-4c1f";
    private static final String AFTER = "Widget-after-9b7e";

    /** When the rows arrive, in waves, so that each phase is over before the next begins. */
    private static final Duration ROWS_AFTER_SKUS = Duration.ofMillis(400);
    private static final Duration EDIT_AFTER_STEADY = Duration.ofMillis(2_500);

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    /** One increasing stamp across every wave, so a later wave is never read as an older row. */
    private static final AtomicLong SEQ = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        SEQ.set(0);
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
    @DisplayName("a sku pointed at from two documents' line items is right, follows edits, and is stored once")
    void everyElementGetsItsOwnSkuAndTheSharedOneIsHeldOnceAndFollowsAnEdit() {
        member.getJet().newJob(ordersWithItemsPointingAtSkus()).join();

        Map<Object, Map<String, Object>> finalDocuments = latestPerOrder();
        assertThat(finalDocuments.keySet())
                .describedAs("both documents have to have been assembled, or the assertions below are "
                        + "about whichever half of the tree happened to run")
                .containsExactlyInAnyOrder(1, 2);

        // (1) every element carries the sku its own column names, not its neighbour's. Asserted per line
        // rather than as an ordered list: where a line sits in the array is not what this case is about,
        // and it is not stable across the partitions the lines of one order arrive on.
        assertThat(skuNameByLine(finalDocuments.get(1)))
                .describedAs("each line of the first document resolves the sku its own column names. "
                        + "A fetch keyed off the document rather than off the line gives every line the "
                        + "same sku, and every line still has one")
                .isEqualTo(Map.of(1, AFTER, 2, "Bolt-2", 3, "Bolt-3"));
        assertThat(skuNameByLine(finalDocuments.get(2)))
                .describedAs("and so does each line of the second, which points at a different set except "
                        + "for the one they share")
                .isEqualTo(Map.of(4, AFTER, 5, "Bolt-5", 6, "Bolt-6"));

        // (2) the edit to the shared sku reached both documents, not just the one that mounted it first.
        assertThat(List.of(skuNameByLine(finalDocuments.get(1)).get(1),
                        skuNameByLine(finalDocuments.get(2)).get(4)))
                .describedAs("one edit to the shared sku, and both documents holding a line that points "
                        + "at it end up carrying the new name. An implementation that reads the row when "
                        + "the line is mounted leaves both at %s while every document stays complete, "
                        + "every count stays right and nothing is thrown", BEFORE)
                .containsExactly(AFTER, AFTER);

        // (3) and it is one row, held once, however many elements across however many documents point at it.
        Map<String, Integer> byNamespace = entriesHoldingTheEditedSku();
        int total = byNamespace.values().stream().mapToInt(Integer::intValue).sum();
        assertThat(total)
                .describedAs("the shared sku's own fields were found in %d state entries across %s. Six "
                        + "elements point at skus here and copying the row into each element is the "
                        + "cheapest way to make the two assertions above pass - it reads as correct from "
                        + "every angle but this count. Zero would mean this looked where the state is not",
                        total, byNamespace)
                .isEqualTo(1);
    }

    // ---- reading what came out and what was left behind ---------------------------------

    /** The last document emitted for each order, which is the one the assertions are about. */
    private static Map<Object, Map<String, Object>> latestPerOrder() {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null && document.get("items") != null) {
                latest.put(document.get("order_id"), document);
            }
        }
        return latest;
    }

    /** The name of the sku under each line of a document, by the line's own id. */
    @SuppressWarnings("unchecked")
    private static Map<Object, String> skuNameByLine(Map<String, Object> document) {
        Map<Object, String> names = new LinkedHashMap<>();
        for (Map<String, Object> item : (List<Map<String, Object>>) document.get("items")) {
            Map<String, Object> sku = (Map<String, Object>) item.get("sku");
            names.put(item.get("item_id"), sku == null ? null : String.valueOf(sku.get("name")));
        }
        return names;
    }

    /**
     * How many entries of each namespace carry the edited sku's name. Searched over the bytes an entry
     * serializes to rather than over its structure: where a copy of a row ends up is the decision under
     * observation, and a scan that knows which field to look in cannot see a copy put anywhere else.
     */
    private Map<String, Integer> entriesHoldingTheEditedSku() {
        Map<String, Integer> found = new LinkedHashMap<>();
        List<String> names = new ArrayList<>();
        member.getDistributedObjects().forEach(object -> {
            if (object instanceof IMap && object.getName().startsWith("nest." + PIPELINE + ".")) {
                names.add(object.getName());
            }
        });
        Collections.sort(names);
        for (String name : names) {
            IMap<Object, Object> map = member.getMap(name);
            int holding = 0;
            for (Object key : map.keySet()) {
                Object state = map.get(key);
                if (state != null && serialized(state).contains(AFTER)) {
                    holding++;
                }
            }
            if (holding > 0) {
                found.put(name, holding);
            }
        }
        return found;
    }

    private static String serialized(Object state) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(state);
            out.flush();
            return new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1);
        } catch (IOException notSerializable) {
            return String.valueOf(state);
        }
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** Two orders of three lines each, two of those lines pointing at one shared sku. */
    private static DAG ordersWithItemsPointingAtSkus() {
        List<Map<String, Object>> orders = List.of(
                row("order_id", 1, "placed", "monday"),
                row("order_id", 2, "placed", "tuesday"));

        // Line 1 of each order points at the shared sku; the rest point at one of their own.
        List<Map<String, Object>> items = List.of(
                item(1, 1, SHARED_SKU), item(2, 1, 502), item(3, 1, 503),
                item(4, 2, SHARED_SKU), item(5, 2, 505), item(6, 2, 506));

        List<Map<String, Object>> skus = List.of(
                row("sku_id", SHARED_SKU, "name", BEFORE),
                row("sku_id", 502, "name", "Bolt-2"),
                row("sku_id", 503, "name", "Bolt-3"),
                row("sku_id", 505, "name", "Bolt-5"),
                row("sku_id", 506, "name", "Bolt-6"));

        Embed sku = new Embed("sku", Map.of("sku_id", "sku_ref"), EmbedAs.OBJECT,
                "sku", null, null, null, null);
        Embed lines = new Embed("item", Map.of("order_id", "order_id"), EmbedAs.ARRAY,
                "items", List.of("item_id"), null, null, List.of(sku));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(lines)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("item", FromRef.literal("items"));
        aliases.put("sku", FromRef.literal("skus"));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(PIPELINE, null,
                List.of("orders", "items", "skus"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", wavesSource("orders", List.of(new Wave(ROWS_AFTER_SKUS, orders))));
        sources.put("items", wavesSource("items", List.of(new Wave(ROWS_AFTER_SKUS, items))));
        // The skus land first so the documents assemble complete, and the rename comes long after the
        // last of them has - an edit racing the assembly would prove nothing about propagation.
        sources.put("skus", wavesSource("skus", List.of(
                new Wave(Duration.ZERO, skus),
                new Wave(EDIT_AFTER_STEADY, List.of(row("sku_id", SHARED_SKU, "name", AFTER))))));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("item", new NestTable("items", List.of("item_id")));
        tables.put("sku", new NestTable("skus", List.of("sku_id")));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    private static Map<String, Object> item(int itemId, int orderId, int skuRef) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("item_id", itemId);
        row.put("order_id", orderId);
        row.put("sku_ref", skuRef);
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

    /** Rows to emit, and how long after the source starts they are due. */
    private record Wave(Duration after, List<Map<String, Object>> rows) implements java.io.Serializable {
    }

    /**
     * Emits its waves in order, each when it comes due, and completes. Waves rather than one list because
     * an edit has to arrive after the documents it edits exist: fed together, "the edit propagated" and
     * "the edit was simply the value at assembly time" are the same output.
     */
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
                    Envelope event = Envelope.insert(seq, src, current.rows().get(next), null)
                            .withOrder(new SourceOrder(1, seq));
                    if (!tryEmit(event)) {
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
