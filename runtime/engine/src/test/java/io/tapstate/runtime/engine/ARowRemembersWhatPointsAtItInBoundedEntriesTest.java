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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
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
 * That what a row remembers about the documents pointing at it stays a bounded thing however many of them
 * there are.
 *
 * <p>This is the guard on a failure nothing else here can see. Keeping every identity that points at one
 * row inside a single entry is the obvious way to write this, it is what the system this one replaces
 * did, and while the entry is small it is indistinguishable from the shape that is safe: the documents
 * are right, the propagation is right, and every other case stays green. It only ever fails on the day
 * one entry has grown past what the layer behind it will store, at which point what is lost is the
 * knowledge of who was pointing where. Counting is the only thing that can tell the two apart before then.
 *
 * <p>Two rounds an order of magnitude apart, because a single round cannot separate "bounded" from
 * "happens to be small". The entry count is what stops growing while the number of referring rows keeps
 * going; the largest single entry is what grows by a fraction of it rather than by all of it.
 */
class ARowRemembersWhatPointsAtItInBoundedEntriesTest {

    private static final String STEP = "order_doc";

    /**
     * How long the orders are held back, so the customer they point at is already filed when they arrive.
     * Waking a document whose reference lands after it is a separate mechanism; keeping the two apart is
     * what makes a document without its customer here mean the read missed rather than that it was early.
     */
    private static final Duration LATE_ORDERS = Duration.ofMillis(500);

    /** The one customer every order in this case points at. */
    private static final int SHARED_CUSTOMER = 100;

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-reference-index-test-" + System.nanoTime());
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
    @DisplayName("ten times more rows pointing at one row does not make ten times the entries")
    void whatPointsAtOneRowIsSpreadOverAFixedNumberOfEntriesHoweverManyThereAre() {
        int few = 10;
        int many = 1000;

        Spread whenFew = runWith(few);
        Spread whenMany = runWith(many);

        assertThat(whenFew.identities)
                .describedAs("every order that arrived was recorded as pointing at the customer, so what "
                        + "follows is about how they are spread and not about any of them being missed")
                .isEqualTo(few);
        assertThat(whenMany.identities)
                .describedAs("the same, a hundredfold")
                .isEqualTo(many);

        assertThat(whenFew.entries)
                .describedAs("a bucket nothing lands in is never written, so a row pointed at ten times "
                        + "costs ten entries rather than one per bucket - which is what makes a generous "
                        + "bucket count free at the small end")
                .isLessThanOrEqualTo(few);
        assertThat(whenMany.entries)
                .describedAs("a hundred times the referring rows, and the entries stop at the bucket "
                        + "count instead of following them. An entry per referring row is the shape this "
                        + "separates out, and it would read %d here", many)
                .isLessThanOrEqualTo(NestLookup.BUCKETS);

        assertThat(whenMany.largestEntry)
                .describedAs("the largest single entry holds its share of the identities rather than all "
                        + "of them: one entry for the lot is the shape that works until the day it is too "
                        + "big to store, and at %d rows it would read %d here. The allowance over the even "
                        + "share is for the unevenness any hash has, not for a second order of magnitude",
                        many, many)
                .isLessThanOrEqualTo(3 * many / NestLookup.BUCKETS);

        assertThat(whenMany.documents)
                .describedAs("and the round that proves the bound is a round that actually assembled every "
                        + "document - a spread measured over rows that never arrived would bound nothing")
                .isEqualTo(many);
    }

    /** What one round left behind: the entries of the reverse index, and the documents that came out. */
    private record Spread(int entries, int identities, int largestEntry, int documents) {
    }

    private Spread runWith(int orders) {
        WRITTEN.clear();
        // A namespace of its own per round rather than one cleared between them: the two rounds are only
        // comparable if neither can be reading anything the other left, and every namespace this tree
        // takes is named after the pipeline.
        String pipeline = "p" + orders;

        member.getJet().newJob(ordersPointingAtOneCustomer(pipeline, orders)).join();

        IMap<Object, Set<Object>> index = member.getMap(
                "nest." + pipeline + "." + STEP + ".customer.refs");
        int identities = 0;
        int largest = 0;
        for (Object key : index.keySet()) {
            Collection<Object> bucket = index.get(key);
            identities += bucket.size();
            largest = Math.max(largest, bucket.size());
        }
        return new Spread(index.size(), identities, largest, assembledDocuments());
    }

    /** How many distinct roots came out carrying the customer they point at. */
    private static int assembledDocuments() {
        Map<Object, Object> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null && document.get("customer") != null) {
                latest.put(document.get("order_id"), document);
            }
        }
        return latest.size();
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** {@code orders} orders, every one of them pointing at the same single customer row. */
    private static DAG ordersPointingAtOneCustomer(String pipelineId, int orders) {
        List<Map<String, Object>> rows = new ArrayList<>(orders);
        for (int i = 1; i <= orders; i++) {
            rows.add(row("order_id", i, "cust_ref", SHARED_CUSTOMER));
        }

        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(customer)));

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
        sources.put("orders", rowsSource("orders", rows, LATE_ORDERS));
        sources.put("customers", rowsSource("customers",
                List.of(row("customer_id", SHARED_CUSTOMER, "name", "Ada")), Duration.ZERO));

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
