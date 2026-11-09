package io.tapstate.runtime.engine;
import static org.assertj.core.api.Assertions.assertThat;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.tapstate.runtime.engine.nest.NestCommitBounds;
import io.tapstate.runtime.engine.nest.NestStateSession;
import io.tapstate.runtime.engine.SettledFloor;

/**
 * A vertex can take in a long run of input that sends nothing on: documents waiting on a row they point at,
 * or a snapshot that has not finished arriving. Its state group must still close on its own bounds while
 * nothing waits to go out. Closed only by output, by a bound or by the end of input, the group grows with
 * the input, and on a large snapshot it outgrows the heap before the first document leaves.
 */
class AStateGroupWithNoOutputBehindItStillCommitsTest {
    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(ORDERS, CUSTOMERS));
    static final List<Map<String, Object>> DOCUMENTS = Collections.synchronizedList(new ArrayList<>());
    static final int N = 3000;
    static final List<String> ACKED = Collections.synchronizedList(new ArrayList<>());
    private String mode;
    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        DOCUMENTS.clear();
        ACKED.clear();
        mode = System.setProperty(NestStateSession.MODE_PROPERTY, "direct");
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stop() {
        if (job != null) job.cancel();
        member.shutdown();
        if (mode == null) System.clearProperty(NestStateSession.MODE_PROPERTY);
        else System.setProperty(NestStateSession.MODE_PROPERTY, mode);
    }

    @Test
    @DisplayName("with nothing going out, the open group still commits at its bounds")
    void aGroupWithNothingGoingOutStillCommitsAtItsBounds() {
        NestStateSession.counters().reset();
        Embed customer = new Embed("c", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT, "customer", null, null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null, new NestRoot("o", List.of("order_id"), null, null, List.of(customer)));
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("o", new NestTable(ORDERS, List.of("order_id")));
        tables.put("c", new NestTable(CUSTOMERS, List.of("customer_id")));
        List<Map<String, Object>> orders = new ArrayList<>(), customers = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            orders.add(new LinkedHashMap<>(Map.of("order_id", "O" + i, "cust_ref", "C" + (i % 50))));
        }
        for (int i = 0; i < 0; i++) customers.add(new LinkedHashMap<>(Map.of("customer_id", "C" + i, "name", "n" + i)));
        DAG dag = new DAG();
        Map<String, Vertex> byAlias = new LinkedHashMap<>();
        byAlias.put("o", dag.newVertex(ORDERS, source(ORDERS, orders)));
        byAlias.put("c", dag.newVertex(CUSTOMERS, source(CUSTOMERS, customers)));
        Map<String, String> chainOfAlias = Map.of("o", ORDERS, "c", CUSTOMERS);
        Map<Vertex, Integer> outbound = new HashMap<>();
        Vertex assembled = NestDag.attach(dag, NestTopology.compile("p-no-output", "doc", body, tables::get), "doc", "o", "doc",
                alias -> List.of(byAlias.get(alias)),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }),
                vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                new NestFrontier(AXES, alias -> List.of(List.of(chainOfAlias.get(alias)))),
                new NodeWidth("doc", 4, 1, null));
        Vertex sink = dag.newVertex("sink", SinkProcessor.metaSupplier("sink",
                (SupplierEx<SinkWriter>) Collecting::new,
                (SinkAckFactory) resolved -> (SinkAck) (chain, position) -> ACKED.add(chain + ":" + position.order().seq()),
                () -> new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN)));
        dag.edge(Edge.from(assembled, outbound.merge(assembled, 1, Integer::sum) - 1).to(sink, 0).distributed());
        job = JetJobs.submit(member, dag, "nest-group-no-output");
        // Every order names a customer that never arrives, and no source sends a bound, so nothing here ever
        // sends anything on: whatever commits, commits because the group reached a bound of its own.
        JobWatch.until(job, Duration.ofSeconds(60), () -> NestStateSession.counters().keysCommitted() >= N,
                () -> "committed " + NestStateSession.counters().keysCommitted() + " keys in "
                        + NestStateSession.counters().commits() + " commits");
        var c = NestStateSession.counters();

        assertThat(DOCUMENTS)
                .describedAs("the control: no document went out, so no output closed any group")
                .isEmpty();
        assertThat(c.keysCommitted() / (double) c.commits())
                .describedAs("keys per commit stay within the group's key bound rather than growing with the "
                        + "input: %d commits for %d keys", c.commits(), c.keysCommitted())
                .isLessThanOrEqualTo(NestCommitBounds.DEFAULT_MAX_KEYS);
    }

    private static ProcessorMetaSupplier source(String stream, List<Map<String, Object>> rows) {
        List<Map<String, Object>> plan = List.copyOf(rows);
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsWithoutBounds(stream, plan)));
    }

    /** Emits its rows and never a bound, the way a snapshot still being read does. */
    private static final class RowsWithoutBounds extends AbstractProcessor {
        private final String stream;
        private final List<Map<String, Object>> rows;
        private int next;
        private boolean rowOut;

        RowsWithoutBounds(String stream, List<Map<String, Object>> rows) {
            this.stream = stream;
            this.rows = rows;
        }

        @Override public boolean isCooperative() { return false; }
        @Override public boolean complete() {
            while (next < rows.size()) {
                SourceOrder at = new SourceOrder(1, next + 1L);
                if (!rowOut) {
                    if (!tryEmit(Envelope.insert(next + 1L, stream, rows.get(next), null).withOrder(at))) return false;
                    rowOut = true;
                }
                rowOut = false; next++;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            return false;
        }
    }

    static final class Collecting implements SinkWriter {
        @Override public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope r : records) if (r.after() != null) DOCUMENTS.add(r.after());
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }
        @Override public void close() { }
    }
}
