package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
 * A sink running writers on two members, fed by a step running on both of them and by a source running on one,
 * still has every writer on both members report how far it has landed each table - and still keeps each row's
 * changes on one writer, in the order they were read.
 *
 * <p>This is the mix the router in front of such a sink has to take its input from: one table arrives from
 * processors on every member, the other from one processor on one member. A router fed only by the processors
 * on its own member would hear the second table from nobody on the other member, and every writer there would
 * wait on it for as long as the run lasts. On one member nothing is ever on another member, which is why this
 * runs on two.
 *
 * <p>The same holds when what feeds the router is a step running once for the whole cluster over both tables. Such
 * a step runs its one processor on one member and a stand-in on the other, and the router hears from both: every
 * bound goes to every processor of a vertex, and every processor reading it waits for each of them. A stand-in
 * that passed on only what it had combined across both of its edges would say nothing about either table - each
 * arrives on one edge only - and every writer would wait on it for good.
 *
 * <p>The sources never finish, as none do in a running pipeline, so nothing here is carried by an edge falling
 * silent by ending.
 */
class ANativeSinkOnTwoMembersHearsEveryTableAtEveryWriterTest {

    private static final FrontierBinding FRONTIER = new FrontierBinding(Map.of("orders", "orders", "logs", "logs"));
    private static final int PER_MEMBER = 2;
    private static final int WRITERS = 2 * PER_MEMBER;
    private static final int KEYS = 8;
    private static final int UPDATES = 5;

    /** What each writer instance was handed, by the order it was opened in. */
    private static final Map<Integer, List<Envelope>> WRITTEN = new ConcurrentHashMap<>();
    private static final AtomicInteger OPENED = new AtomicInteger();
    /** Every table each named writer reported progress on. */
    private static final Map<String, Set<String>> REPORTED = new ConcurrentHashMap<>();
    private static final List<Map<String, List<String>>> STARTED = Collections.synchronizedList(new ArrayList<>());
    /** How many ports the steps' processors made between them: one per processor that runs a step. */
    private static final AtomicInteger PORTS = new AtomicInteger();

    private HazelcastInstance first;
    private HazelcastInstance second;

    @BeforeEach
    void startTwoMembers() {
        WRITTEN.clear();
        OPENED.set(0);
        REPORTED.clear();
        STARTED.clear();
        PORTS.set(0);
        String cluster = "native-sink-two-members-" + System.nanoTime();
        first = Hazelcast.newHazelcastInstance(clustered(cluster));
        second = Hazelcast.newHazelcastInstance(clustered(cluster));
        assertThat(first.getCluster().getMembers()).hasSize(2);
    }

    @AfterEach
    void stopBoth() {
        if (second != null) {
            second.shutdown();
        }
        if (first != null) {
            first.shutdown();
        }
    }

    @Test
    void everyWriterOnBothMembersReportsBothTablesAndEachKeyStaysOnOneWriter() throws InterruptedException {
        Job job = first.getJet().newJob(
                PipelineDagBuilder.build(pipeline(), bindings(), new RecordingAcks(), FRONTIER, shape()));
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (!(everyWriterReportedBothTables() && everyUpdateWritten()) && System.nanoTime() < deadline) {
                Thread.sleep(25);
            }
        } finally {
            job.cancel();
        }

        List<String> writers = new ArrayList<>();
        for (int index = 0; index < WRITERS; index++) {
            writers.add("serve.s#" + index);
        }
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
        assertThat(WRITTEN.keySet()).as("the writers handed anything, on both members").hasSizeGreaterThan(1);
    }

    @Test
    void aStepRunningOnceForTheClusterOverBothTablesStillLetsEveryWriterHearThem() throws InterruptedException {
        runThroughOneMerge(new TransformBody.Js("row"));

        assertThat(PORTS.get()).as("the step ran one processor for the cluster, the other member a stand-in")
                .isEqualTo(1);
    }

    @Test
    void aUnionRunningOnceForTheClusterOverBothTablesStillLetsEveryWriterHearThem() throws InterruptedException {
        runThroughOneMerge(new TransformBody.Union());
    }

    /** Runs both tables through one {@code body} running once for the cluster, and holds every writer to both. */
    private void runThroughOneMerge(TransformBody body) throws InterruptedException {
        Job job = first.getJet().newJob(PipelineDagBuilder.build(
                pipelineThroughOneMerge(body), bindings(), new RecordingAcks(), FRONTIER, shapeWithTheMergeOnce()));
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (!(everyWriterReportedBothTables() && everyUpdateWritten()) && System.nanoTime() < deadline) {
                Thread.sleep(25);
            }
        } finally {
            job.cancel();
        }

        assertThat(everyUpdateWritten()).as("every change reached a writer through the merge's one processor")
                .isTrue();
        for (int index = 0; index < WRITERS; index++) {
            assertThat(REPORTED.getOrDefault("serve.s#" + index, Set.of()))
                    .as("the tables writer serve.s#%d reported progress on", index)
                    .containsExactlyInAnyOrder("orders", "logs");
        }
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

    private static boolean everyUpdateWritten() {
        long updates = WRITTEN.values().stream()
                .flatMap(rows -> List.copyOf(rows).stream())
                .filter(row -> row.op() == Op.UPDATE)
                .count();
        return updates == (long) KEYS * UPDATES;
    }

    /** Per writer instance, the values key {@code key}'s updates carried there, in the order it was handed them. */
    private static Map<Integer, List<Object>> changesOf(int key) {
        Map<Integer, List<Object>> byWriter = new LinkedHashMap<>();
        WRITTEN.forEach((writer, rows) -> {
            for (Envelope row : List.copyOf(rows)) {
                if (row.src().equals("orders") && row.op() == Op.UPDATE
                        && Integer.valueOf(key).equals(row.after().get("id"))) {
                    byWriter.computeIfAbsent(writer, ignored -> new ArrayList<>()).add(row.after().get("v"));
                }
            }
        });
        return byWriter;
    }

    /** {@code orders} through a step running on both members into the sink; {@code logs} straight into it. */
    private static PipelineResource pipeline() {
        return new PipelineResource("p", null,
                List.of(SourceRef.bare("orders"), SourceRef.bare("logs")),
                List.of(Step.inline("stamp", FromClause.list(FromRef.literal("orders")),
                        new TransformBody.Js("row"), null)),
                null,
                new ServeBlock.Inline(null,
                        new FromClause.Flow(List.of(FromRef.literal("stamp"), FromRef.literal("logs"))),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
    }

    /** Both tables through one step, which runs a single processor for the whole cluster, into the sink. */
    private static PipelineResource pipelineThroughOneMerge(TransformBody body) {
        return new PipelineResource("p", null,
                List.of(SourceRef.bare("orders"), SourceRef.bare("logs")),
                List.of(Step.inline("merge", FromClause.list(FromRef.literal("orders"), FromRef.literal("logs")),
                        body, null)),
                null,
                new ServeBlock.Inline(null, new FromClause.Flow(List.of(FromRef.literal("merge"))),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
    }

    /** The sink on both members as before; the step, given no width, runs once for the cluster. */
    private static ExecutionShape shapeWithTheMergeOnce() {
        return new ExecutionShape(2,
                Map.of("serve.s", new NodeParallelism("serve.s", WRITERS, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 2, PER_MEMBER, WRITERS, List.of())),
                Map.of(),
                Map.of("serve.s", Map.of(
                        "orders", new SinkTarget("orders", List.of("id")),
                        "logs", new SinkTarget("logs", List.of()))));
    }

    private static ExecutionShape shape() {
        return new ExecutionShape(2,
                Map.of("stamp", new NodeParallelism("stamp", WRITERS, NodeParallelism.Origin.EXPLICIT,
                                NodeParallelism.Scope.NATIVE, 2, PER_MEMBER, WRITERS, List.of()),
                        "serve.s", new NodeParallelism("serve.s", WRITERS, NodeParallelism.Origin.EXPLICIT,
                                NodeParallelism.Scope.NATIVE, 2, PER_MEMBER, WRITERS, List.of())),
                Map.of("stamp", Map.of("orders", List.of("id"))),
                Map.of("serve.s", Map.of(
                        "orders", new SinkTarget("orders", List.of("id")),
                        "logs", new SinkTarget("logs", List.of()))));
    }

    private static DagBindings bindings() {
        ChainAxes axes = FRONTIER.axes();
        Map<FromRef, List<String>> upstreams = Map.of(
                FromRef.literal("orders"), List.of("orders"),
                FromRef.literal("logs"), List.of("logs"),
                FromRef.literal("stamp"), List.of("stamp"),
                FromRef.literal("merge"), List.of("merge"));
        return new DagBindings(
                sourceId -> endless(sourceId, axes.axisOf(sourceId)),
                step -> (SupplierEx<TransformPort>) () -> {
                    PORTS.incrementAndGet();
                    return event -> List.of(event);
                },
                element -> (SupplierEx<SinkWriter>) RecordingWriter::new,
                ref -> upstreams.getOrDefault(ref, List.of()));
    }

    private static Config clustered(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(15831).setPortAutoIncrement(true).setPortCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15831", "127.0.0.1:15832"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        return config;
    }

    /** A load of sixteen keyed rows, then five updates to each of eight of them, interleaved key by key. */
    private static List<Envelope> orders() {
        List<Envelope> rows = new ArrayList<>();
        for (int id = 1; id <= 16; id++) {
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
     * A source running once for the cluster, on the member its table's name falls to, that emits its table's
     * rows, then a bound covering them, then nothing - and never ends.
     */
    private static ProcessorMetaSupplier endless(String table, byte axis) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new EndlessSource(table, axis)), table);
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
