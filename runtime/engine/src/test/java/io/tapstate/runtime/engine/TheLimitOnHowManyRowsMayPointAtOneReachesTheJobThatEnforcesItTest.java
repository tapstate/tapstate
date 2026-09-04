package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateException;
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
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * How many rows may point at one row is chosen where the job is assembled and enforced on the member that
 * files those rows, and between the two there is a seam nothing else covers. Every other case about this
 * limit hands the number to a vertex directly; this one hands it to the job and asks the job.
 *
 * <p>What fails here if the seam is open is nothing at all: the vertex keeps the default hundred thousand,
 * the run assembles perfectly correct documents, and the configuration meant to bound it reads as set
 * everywhere anyone looks. Which is why what is asserted is a job that stops rather than a number read back.
 *
 * <p><b>And a second seam behind it, which is about what an operator is told rather than what happens.</b>
 * Once a job's terminal result is durable, the live context behind its future is gone and the throwable
 * rebuilt from stored text carries no cause, so a walk looking for a coded one finds nothing and the run
 * reports the generic engine failure. The vertices that assemble and resolve already write the real
 * exception down before rethrowing; the vertex that files the rows being pointed at is newer than that fix
 * and did not. Failing with a code nobody can read is the same news as failing without one, so the
 * registry is asked here too.
 */
class TheLimitOnHowManyRowsMayPointAtOneReachesTheJobThatEnforcesItTest {

    private static final String PIPELINE = "p";
    private static final String NODE = "order_doc";

    /** The one customer every order in this case points at. */
    private static final int SHARED_CUSTOMER = 100;

    /** Three orders may point at the customer; the fourth is what the job must stop on. */
    private static final long LIMIT = 3L;

    /** How long the orders are held back, so the customer they point at is filed before they arrive. */
    private static final Duration LATE_ORDERS = Duration.ofMillis(400);

    /**
     * How long the edit to that customer is held back. After the orders, because the count is weighed on an
     * edit to the row being pointed at - the customer's own first arrival reaches a row nothing points at
     * yet, and a limit that fired there would be measuring an empty index.
     */
    private static final Duration LATE_EDIT = Duration.ofMillis(1400);

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.setClusterName("nest-reference-fanout-limit-test-" + System.nanoTime());
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
    void aRowPointedAtByMoreRowsThanTheJobAllowsStopsTheJobSayingSo() {
        DAG dag = ordersPointingAtOneCustomer(LIMIT + 1);

        assertThatThrownBy(() -> run(dag))
                .hasStackTraceContaining("nest.reference-fanout-limit-exceeded")
                .hasStackTraceContaining("referrers=" + (LIMIT + 1))
                .hasStackTraceContaining("limit=" + LIMIT);
    }

    @Test
    void aRowPointedAtByAsManyAsTheJobAllowsRunsToCompletion() {
        run(ordersPointingAtOneCustomer(LIMIT));

        assertThat(WRITTEN)
                .describedAs("and it ran because it did the work, not because nothing reached the vertex "
                        + "that weighs this - a run that assembled no document bounds nothing")
                .isNotEmpty();
    }

    /**
     * The failure an operator can act on, rather than one they can only see the shape of. What separates
     * the two is whether the vertex wrote the exception down before Jet took the live context away.
     */
    @Test
    void theCodeSurvivesTheJobDyingRatherThanDegradingToTheGenericEngineFailure() {
        DAG dag = ordersPointingAtOneCustomer(LIMIT + 1);

        assertThatThrownBy(() -> run(dag)).isNotNull();

        assertThat(JobFailureRegistry.of(member).get(PIPELINE))
                .describedAs("nothing recorded here is a run that reports engine.job-failed and names "
                        + "neither the row, nor how many point at it, nor the limit")
                .get()
                .isInstanceOf(TapstateException.class)
                .extracting(recorded -> ((TapstateException) recorded).code().code())
                .isEqualTo("nest.reference-fanout-limit-exceeded");
    }

    /** Named, because what a coded failure is filed under is the job's name. */
    private void run(DAG dag) {
        JetJobs.submit(member, dag, PIPELINE).join();
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** {@code orders} orders all pointing at one customer, whose name is edited once they are all filed. */
    private static DAG ordersPointingAtOneCustomer(long orders) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (long i = 1; i <= orders; i++) {
            rows.add(row("order_id", i, "cust_ref", SHARED_CUSTOMER));
        }

        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(customer)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("customer", FromRef.literal("customers"));
        Step step = Step.inline(NODE, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(PIPELINE, null,
                List.of("orders", "customers"), List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(NODE),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", rowsSource("orders", List.of(new Timed(rows, LATE_ORDERS))));
        sources.put("customers", rowsSource("customers", List.of(
                new Timed(List.of(row("customer_id", SHARED_CUSTOMER, "name", "Ada")), Duration.ZERO),
                new Timed(List.of(row("customer_id", SHARED_CUSTOMER, "name", "Grace")), LATE_EDIT))));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables()::get, HeapNestStores.onHeap(), (from, released) -> { },
                        NestSettings.defaults().withReferenceFanoutLimit(lookupNamespace(body), LIMIT)));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    /** The name the compiler gives the pointed-at level, asked of the compiler rather than spelled out. */
    private static String lookupNamespace(TransformBody.Nest body) {
        return NestTopology.compile(PIPELINE, NODE, body, tables()::get).lookups().get(0).mapName();
    }

    private static Map<String, NestTable> tables() {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        return tables;
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

    /** One batch of rows and how long after the source starts it is due. */
    private record Timed(List<Map<String, Object>> rows, Duration after) implements Serializable {
    }

    /**
     * Emits its batches in order, each held until it is due, and completes. It yields rather than sleeping:
     * the thread is shared with every other cooperative vertex here, and one that sleeps on it stops them
     * too.
     */
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
                    Envelope event = Envelope.insert(emitted + 1L, src, due.rows().get(next), null)
                            .withOrder(new SourceOrder(1, emitted));
                    if (!tryEmit(event)) {
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
