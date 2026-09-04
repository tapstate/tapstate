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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a row a hundred documents point at is held in the state once, and that the hundred hold a way to
 * reach it rather than a copy of it.
 *
 * <p><b>Nothing else here can see the difference.</b> Giving every document its own copy of the row it
 * points at produces documents that are correct, propagation that is correct and a case list that stays
 * green - it is the obvious way to write this and it is what the shape being replaced did. What it costs
 * is invisible until the row is wide or the documents are many, and by then the cost is in every entry
 * already written. Counting where the row's own fields ended up is the only thing that separates the two,
 * which is why this case counts rather than reads.
 *
 * <p><b>The count is asserted as exactly one, and the lower half of that matters as much as the upper.</b>
 * A hundred and one says every document kept a copy. <b>Zero says this case looked in the wrong place</b> -
 * a scan that finds nothing and an implementation that stores nothing are the same reading, and only one
 * of them is worth a green build. So the marker is searched for across every namespace this tree takes,
 * and it has to be found once rather than at most once.
 */
class ARowManyDocumentsPointAtIsStoredOnceTest {

    private static final String STEP = "order_doc";
    private static final String PIPELINE = "shared-customer";

    /** How many documents point at the one row. Enough that a copy per document is unmistakable. */
    private static final int ORDERS = 100;

    /** The row every order points at. */
    private static final int SHARED_CUSTOMER = 100;

    /**
     * The value searched for. Deliberately not a name anything else could hold: the count means nothing
     * if the marker can turn up in a namespace for a reason that is not the row having been copied there.
     */
    private static final String MARKER = "Ada-Lovelace-8f3a2c";

    /**
     * How long the orders are held back, so the row they point at is already filed when they arrive.
     * Waking a document whose reference lands after it is a separate mechanism, and keeping the two apart
     * is what makes a document without its customer here mean the read missed rather than that it was early.
     */
    private static final Duration LATE_ORDERS = Duration.ofMillis(500);

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-one-copy-test-" + System.nanoTime());
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
    @DisplayName("a hundred documents pointing at one row keep one copy of it between them")
    void theRowsOwnFieldsAreHeldInExactlyOneStateEntryHoweverManyPointAtIt() {
        member.getJet().newJob(ordersPointingAtOneCustomer()).join();

        assertThat(documentsCarryingTheCustomer())
                .describedAs("the round that is being counted has to be a round that assembled every "
                        + "document, or what follows counts the leftovers of a job that did not run")
                .isEqualTo(ORDERS);

        Map<String, Integer> byNamespace = entriesHoldingTheMarker();
        int total = byNamespace.values().stream().mapToInt(Integer::intValue).sum();
        assertThat(total)
                .describedAs("the row's own fields were found in %d state entries across %s. One is the "
                        + "whole claim: %d would mean every document kept its own copy, which costs the "
                        + "width of the row times the documents pointing at it and reads correct from "
                        + "every other angle. Zero would mean this case searched somewhere the state is "
                        + "not, which is a green build that checked nothing",
                        total, byNamespace, ORDERS + 1)
                .isEqualTo(1);

        assertThat(byNamespace.keySet())
                .describedAs("the one copy is held somewhere, and where it is held is part of the claim: "
                        + "a copy living in the documents' own namespace rather than in the table's is "
                        + "the same count with the opposite meaning")
                .containsExactly("nest." + PIPELINE + "." + STEP + ".customer");
    }

    // ---- what the state was left holding ----------------------------------------------

    /**
     * How many entries of each namespace this tree takes carry the marker. Namespaces with none are left
     * out, so the map this returns is also the answer to where the row ended up.
     *
     * <p>The search is over the bytes an entry serializes to rather than over its structure. What holds a
     * copy of a row is a decision this case exists to observe, so it must not be reached through the type
     * that would be making it - a scan that knows which field to look in cannot see a copy put anywhere
     * else, and putting it somewhere else is exactly the failure.
     */
    private Map<String, Integer> entriesHoldingTheMarker() {
        Map<String, Integer> found = new LinkedHashMap<>();
        for (String name : stateMapNames()) {
            IMap<Object, Object> map = member.getMap(name);
            int holding = 0;
            for (Object key : map.keySet()) {
                if (holdsMarker(map.get(key))) {
                    holding++;
                }
            }
            if (holding > 0) {
                found.put(name, holding);
            }
        }
        return found;
    }

    /** Every map this pipeline's tree took, whatever it called them. */
    private Collection<String> stateMapNames() {
        List<String> names = new ArrayList<>();
        member.getDistributedObjects().forEach(object -> {
            String name = object.getName();
            if (object instanceof IMap && name.startsWith("nest." + PIPELINE + ".")) {
                names.add(name);
            }
        });
        Collections.sort(names);
        return names;
    }

    private static boolean holdsMarker(Object state) {
        if (state == null) {
            return false;
        }
        return bytesOf(state).contains(MARKER);
    }

    /**
     * One state value as text to search. Serialization first, because that is what the entry actually is
     * and it reaches fields no accessor exposes; where a value cannot be serialized its own rendering is
     * used, which is enough for the identities and collections the other namespaces hold.
     */
    private static String bytesOf(Object state) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(state);
            out.flush();
            return new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1);
        } catch (IOException notSerializable) {
            return String.valueOf(state);
        }
    }

    /** How many distinct roots came out carrying the customer they point at, with its fields intact. */
    private static int documentsCarryingTheCustomer() {
        Map<Object, Object> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document == null) {
                continue;
            }
            Object customer = document.get("customer");
            if (customer instanceof Map<?, ?> fields && MARKER.equals(fields.get("name"))) {
                latest.put(document.get("order_id"), document);
            }
        }
        return latest.size();
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** {@value #ORDERS} orders, every one of them pointing at the same single customer row. */
    private static DAG ordersPointingAtOneCustomer() {
        List<Map<String, Object>> orders = new ArrayList<>(ORDERS);
        for (int i = 1; i <= ORDERS; i++) {
            orders.add(row("order_id", i, "cust_ref", SHARED_CUSTOMER));
        }

        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(customer)));

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
        sources.put("orders", rowsSource("orders", orders, LATE_ORDERS));
        sources.put("customers", rowsSource("customers",
                List.of(row("customer_id", SHARED_CUSTOMER, "name", MARKER)), Duration.ZERO));

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

    private static ProcessorMetaSupplier rowsSource(String src, List<Map<String, Object>> rows,
            Duration startAfter) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsSource(src, rows, startAfter)));
    }

    private static Map<String, Object> row(String a, Object av, String b, Object bv) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(a, av);
        row.put(b, bv);
        return row;
    }

    /** Emits its rows once, each stamped with the order the engine would have given it, and completes. */
    private static final class RowsSource extends AbstractProcessor {

        private final String src;
        private final List<Map<String, Object>> rows;
        private final long startAfterMillis;
        private long dueAt = -1;
        private int next;

        private RowsSource(String src, List<Map<String, Object>> rows, Duration startAfter) {
            this.src = src;
            this.rows = rows;
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
            return true;
        }

        /**
         * Whether this source's rows are due, counted from the first time it was asked. It yields until
         * they are rather than sleeping: the thread is shared with every other cooperative vertex here,
         * and one that sleeps on it stops them too.
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
