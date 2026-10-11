package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SyncElement;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink running four writers applies each row's changes on one writer in the order they were read, writes a
 * table with no key on one writer, spreads a load over more than one writer - and every one of the four
 * reports how far it has landed each table, including a writer that received none of a table's rows.
 *
 * <p>That last part is what the pipeline's durable position rests on: it is the lowest of what every writer
 * has landed, so a writer that has said nothing holds a table back entirely, and a writer never given a row of
 * the table only says anything because the table's bound reaches it too.
 *
 * <p>The sources never finish, as none do in a running pipeline, so nothing here is carried by an edge
 * falling silent by ending.
 */
class ANativeSinkKeepsAKeyOnOneWriterAndEveryWriterReportsTest {

    private static final FrontierBinding FRONTIER =
            new FrontierBinding(Map.of("orders_src", "orders", "logs_src", "logs"));
    private static final int WRITERS = 4;
    private static final int KEYS = 4;
    private static final int UPDATES = 5;

    /** What each writer instance was handed, by the order it was opened in. */
    private static final Map<Integer, List<Envelope>> WRITTEN = new ConcurrentHashMap<>();
    private static final AtomicInteger OPENED = new AtomicInteger();
    /** Every table each named writer reported progress on. */
    private static final Map<String, Set<String>> REPORTED = new ConcurrentHashMap<>();
    private static final List<Map<String, List<String>>> STARTED = Collections.synchronizedList(new ArrayList<>());

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
        WRITTEN.clear();
        OPENED.set(0);
        REPORTED.clear();
        STARTED.clear();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void eachKeyStaysOnOneWriterAndEveryWriterReportsEveryTable() throws InterruptedException {
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of(SourceRef.bare("orders_src"), SourceRef.bare("logs_src")), null, null,
                new ServeBlock.Inline(null,
                        new FromClause.Flow(List.of(FromRef.literal("orders_src"), FromRef.literal("logs_src"))),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
        ExecutionShape shape = new ExecutionShape(1,
                Map.of("serve.s", new NodeParallelism("serve.s", WRITERS, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 1, WRITERS, WRITERS, List.of())),
                Map.of(),
                Map.of("serve.s", Map.of(
                        "orders", new SinkTarget("orders", List.of("id")),
                        "logs", new SinkTarget("logs", List.of()))));
        ChainAxes axes = FRONTIER.axes();
        Map<FromRef, List<String>> upstreams = Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("logs_src"), List.of("logs_src"));
        DagBindings bindings = new DagBindings(
                sourceId -> sourceId.equals("orders_src")
                        ? endless("orders", axes.axisOf("orders"))
                        : endless("logs", axes.axisOf("logs")),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) RecordingWriter::new,
                ref -> upstreams.getOrDefault(ref, List.of()));

        Job job = member.getJet().newJob(
                PipelineDagBuilder.build(pipeline, bindings, new RecordingAcks(), FRONTIER, shape));
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (!everyWriterReportedBothTables() && System.nanoTime() < deadline) {
                Thread.sleep(25);
            }
        } finally {
            job.cancel();
        }

        List<String> writers = List.of("serve.s#0", "serve.s#1", "serve.s#2", "serve.s#3");
        assertThat(STARTED).as("the run the execution started").containsExactly(
                Map.of("orders", writers, "logs", writers));
        for (String writer : writers) {
            assertThat(REPORTED.getOrDefault(writer, Set.of()))
                    .as("the tables writer %s reported progress on - given rows of them or not", writer)
                    .containsExactlyInAnyOrder("orders", "logs");
        }
        for (int key = 1; key <= KEYS; key++) {
            Map<Integer, List<Object>> byWriter = changesOf(key);
            assertThat(byWriter).as("the writers key %d's changes reached", key).hasSize(1);
            assertThat(byWriter.values().iterator().next())
                    .as("and the order they were applied in there")
                    .containsExactly(1, 2, 3, 4, 5);
        }
        assertThat(writersOf("logs", null))
                .as("the writers rows of a table with no key reached: one, so it is written serially")
                .hasSize(1);
        assertThat(writersOf("orders", io.tapstate.core.event.Op.READ))
                .as("the writers the load was spread over")
                .hasSizeGreaterThan(1);
    }

    private static boolean everyWriterReportedBothTables() {
        for (int index = 0; index < WRITERS; index++) {
            Set<String> tables = REPORTED.get("serve.s#" + index);
            if (tables == null || !tables.containsAll(List.of("orders", "logs"))) {
                return false;
            }
        }
        return true;
    }

    /** Per writer instance, the values key {@code key}'s updates carried there, in the order it was handed them. */
    private static Map<Integer, List<Object>> changesOf(int key) {
        Map<Integer, List<Object>> byWriter = new LinkedHashMap<>();
        WRITTEN.forEach((writer, rows) -> {
            for (Envelope row : rows) {
                if (row.src().equals("orders") && row.op() == io.tapstate.core.event.Op.UPDATE
                        && Integer.valueOf(key).equals(row.after().get("id"))) {
                    byWriter.computeIfAbsent(writer, ignored -> new ArrayList<>()).add(row.after().get("v"));
                }
            }
        });
        return byWriter;
    }

    /** The writer instances handed rows of {@code table}, of operation {@code op} or of any where it is null. */
    private static Set<Integer> writersOf(String table, io.tapstate.core.event.Op op) {
        Set<Integer> writers = new HashSet<>();
        WRITTEN.forEach((writer, rows) -> {
            for (Envelope row : rows) {
                if (row.src().equals(table) && (op == null || row.op() == op)) {
                    writers.add(writer);
                }
            }
        });
        return writers;
    }

    /** A load of twelve keyed rows, then five updates to each of four of them, interleaved key by key. */
    private static List<Envelope> orders() {
        List<Envelope> rows = new ArrayList<>();
        for (int id = 1; id <= 12; id++) {
            rows.add(Envelope.read(id, "orders", Map.of("id", id), null)
                    .withPosition(new ChainPosition(SourceOrder.snapshotRow(1), null)));
        }
        long seq = 0;
        for (int value = 1; value <= UPDATES; value++) {
            for (int id = 1; id <= KEYS; id++) {
                rows.add(Envelope.update(seq, "orders", Map.of("id", id), Map.of("id", id, "v", value), null)
                        .withPosition(new ChainPosition(new SourceOrder(1, seq), "o" + seq)));
                seq++;
            }
        }
        return rows;
    }

    /** Six rows of a table with no key. */
    private static List<Envelope> logs() {
        List<Envelope> rows = new ArrayList<>();
        for (long seq = 0; seq < 6; seq++) {
            rows.add(Envelope.insert(seq, "logs", Map.of("line", "l" + seq), null)
                    .withPosition(new ChainPosition(new SourceOrder(1, seq), "l" + seq)));
        }
        return rows;
    }

    /**
     * A source that emits its table's rows, then a bound covering them, then nothing - and never ends. The rows
     * are made where the source runs: a graph carries only what it can serialize.
     */
    private static ProcessorMetaSupplier endless(String table, byte axis) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new EndlessSource(table, axis)));
    }

    private static final class EndlessSource extends AbstractProcessor {

        private final List<Envelope> rows;
        private final Watermark bound;
        private int next;
        private boolean announced;

        EndlessSource(String table, byte axis) {
            this.rows = table.equals("orders") ? orders() : logs();
            SourceOrder last = rows.get(rows.size() - 1).position().order();
            this.bound = new Watermark(FrontierOrders.pack(table, last), axis);
        }

        @Override
        public boolean complete() {
            while (next < rows.size()) {
                if (!tryEmit(rows.get(next))) {
                    return false;
                }
                next++;
            }
            if (!announced && tryEmit(bound)) {
                announced = true;
            }
            return false;
        }
    }

    /** A writer recording what it is handed under the order it was opened in; every write settles at once. */
    private static final class RecordingWriter implements SinkWriter {

        private final int serial = OPENED.getAndIncrement();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            WRITTEN.computeIfAbsent(serial, ignored -> Collections.synchronizedList(new ArrayList<>()))
                    .addAll(records);
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** Acks recording every run started through them and every table each named writer reports on. */
    private static final class RecordingAcks implements SinkAckFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public SinkAck resolve(HazelcastInstance on) {
            return new Unnamed();
        }

        @Override
        public void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain) {
            STARTED.add(Map.copyOf(writersByChain));
        }
    }

    private static final class Unnamed implements SinkAck {

        private static final long serialVersionUID = 1L;

        @Override
        public void advance(String chain, ChainPosition position) {
            throw new AssertionError("a writer of a native sink reported without its name");
        }

        @Override
        public SinkAck forWriter(String writerId) {
            return new Named(writerId);
        }
    }

    private static final class Named implements SinkAck {

        private static final long serialVersionUID = 1L;

        private final String writerId;

        Named(String writerId) {
            this.writerId = writerId;
        }

        @Override
        public void advance(String chain, ChainPosition position) {
            REPORTED.computeIfAbsent(writerId, ignored -> ConcurrentHashMap.newKeySet()).add(chain);
        }

        @Override
        public void bounded(String chain, SourceOrder through) {
            REPORTED.computeIfAbsent(writerId, ignored -> ConcurrentHashMap.newKeySet()).add(chain);
        }
    }
}
