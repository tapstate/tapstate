package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SyncElement;
import io.tapstate.runtime.engine.DagBindings;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.FrontierBinding;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.runtime.engine.SinkTarget;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.runtime.srs.SrsReadCursorPublisherFactory;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink writing on several writers hands a keyed target table's load rows to whichever writer has room, and
 * every change to the writer its key belongs to. So a change leaves its source only once every load written into
 * its target table has landed at every one of that sink's writers - and no later: a table with a target of its
 * own moves on as soon as its own load has landed, whatever another table's load is doing.
 *
 * <p>Three tables into one sink running two writers: {@code a} and {@code b} into target {@code x}, {@code c}
 * into {@code y}. The loads of {@code a} and {@code c} land first; then a writer is held part way through writing
 * {@code b}'s load. While it is held, {@code c}'s changes are written and neither {@code b}'s nor {@code a}'s are
 * - a change of {@code a} can carry the key of {@code b}'s load row just as well. Once the writer finishes, every
 * change is written, and no change of a key reached a writer before that key's load row had been written.
 *
 * <p>Each rule has a reading of its own here. A source letting its changes out once its own load was handed over
 * has {@code b}'s written while the load row is held; one waiting only for its own table's load, {@code a}'s; one
 * waiting for every load of the pipeline holds {@code c}'s back behind a table it shares nothing with.
 */
class AChangeIsWrittenOnlyOnceTheLoadsItCouldOvertakeHaveLandedTest {

    private static final String PIPELINE = "p";
    private static final FrontierBinding FRONTIER =
            new FrontierBinding(Map.of("a_src", "a", "b_src", "b", "c_src", "c"));
    private static final Map<String, String> CHAINS = Map.of("a", "mc-a", "b", "mc-b", "c", "mc-c");
    private static final int CHANGES = 16;

    /** What the writers were handed ({@code start}) and finished writing ({@code done}), in that order. */
    private static final List<String> WRITES = new CopyOnWriteArrayList<>();
    /** Finishes the write that carries {@code b}'s load row. */
    private static volatile CompletableFuture<Void> held = new CompletableFuture<>();

    private HazelcastInstance member;
    private InMemorySrsMetaStore store;
    private SnapshotBuffer buffer;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("load-gate-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(64)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        member = Hazelcast.newHazelcastInstance(config);
        store = new InMemorySrsMetaStore();
        buffer = new SnapshotBuffer();
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, store);
        WRITES.clear();
        held = new CompletableFuture<>();
    }

    @AfterEach
    void stopMember() {
        held.complete(null);
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aTargetTablesChangesWaitForEveryLoadOfItWhileAnotherTableMovesOn() throws InterruptedException {
        for (String table : List.of("a", "b", "c")) {
            store.create(CHAINS.get(table), null);
            store.setCdcStart(CHAINS.get(table), PIPELINE, "seam-" + table, 1L);
            buffer.declareSnapshot(PIPELINE, ringOf(table));
            SrsRingbuffer ring = new SrsRingbuffer(member.getRingbuffer(ringOf(table)));
            for (int id = 1; id <= CHANGES; id++) {
                ring.append(new SrsItem(new SourcePosition(table + "-" + id), Op.INSERT, id, null,
                        Map.of("id", id, "v", "new"), 0L));
            }
        }
        for (String table : List.of("a", "c")) {
            for (int id = 1; id <= 4; id++) {
                buffer.append(PIPELINE, ringOf(table), loadRow(table, id));
            }
            buffer.endSnapshot(PIPELINE, ringOf(table));
        }
        StoreBackedSinkAckFactory acks = new StoreBackedSinkAckFactory(CHAINS, PIPELINE, "run-1");
        // Every source placed on this member, the way a server places a source on the member whose capture fills
        // its hand-off: the gate is wrapped around what that placement makes, and the job has to carry the two
        // together to every member.
        SourcePlacement here = SourcePlacement.on(member.getCluster().getLocalMember().getAddress());
        Job job = member.getJet().newJob(PipelineDagBuilder.build(pipeline(), bindings(here), acks, FRONTIER, shape()));
        try {
            awaitLoaded("a");
            awaitLoaded("c");

            buffer.append(PIPELINE, ringOf("b"), loadRow("b", 1));
            buffer.endSnapshot(PIPELINE, ringOf("b"));
            // Handed to a writer, and so out of its source, which lets b's changes go the moment nothing holds
            // them; the writer is now held part way through writing it.
            await(() -> WRITES.contains("start|b|READ|1"), "b's load row handed to a writer");
            await(() -> !changesHandedOver("c").isEmpty(), "a change of c handed to a writer");
            // Three looks at the loads by every source still holding.
            Thread.sleep(1_500);

            assertThat(changesHandedOver("b")).as("b's changes, while b's load row is still being written")
                    .isEmpty();
            assertThat(changesHandedOver("a"))
                    .as("a's changes, which can carry the key of b's load row, while it is still being written")
                    .isEmpty();

            held.complete(null);

            await(() -> changesHandedOver("a").size() == CHANGES && changesHandedOver("b").size() == CHANGES
                    && changesHandedOver("c").size() == CHANGES, "every change handed to a writer");
            int loadRowWritten = WRITES.indexOf("done|b|READ|1");
            assertThat(WRITES.indexOf("start|a|INSERT|1")).as("a's change of x's key 1").isGreaterThan(loadRowWritten);
            assertThat(WRITES.indexOf("start|b|INSERT|1")).as("b's change of x's key 1").isGreaterThan(loadRowWritten);
        } finally {
            job.cancel();
        }
    }

    private static String ringOf(String table) {
        return "srs.ring." + table;
    }

    /** A row of {@code table}'s load: at the reserved position every load row of the generation sits at. */
    private static Envelope loadRow(String table, int id) {
        return Envelope.read(0L, table, Map.of("id", id, "v", "old"), null).withOrder(SourceOrder.snapshotRow(1L));
    }

    /** The changes of {@code table} handed to a writer so far. */
    private static List<String> changesHandedOver(String table) {
        return WRITES.stream().filter(entry -> entry.startsWith("start|" + table + "|INSERT|")).toList();
    }

    private void awaitLoaded(String table) throws InterruptedException {
        await(() -> store.read(CHAINS.get(table)).orElseThrow().snapshotCompletedTables(PIPELINE).contains(table),
                table + "'s load recorded as landed");
    }

    private static void await(BooleanSupplier holds, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!holds.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what + "; the writers saw " + WRITES);
            }
            Thread.sleep(25);
        }
    }

    private static PipelineResource pipeline() {
        return new PipelineResource(PIPELINE, null,
                List.of(SourceRef.bare("a_src"), SourceRef.bare("b_src"), SourceRef.bare("c_src")), null, null,
                new ServeBlock.Inline(null,
                        new FromClause.Flow(List.of(FromRef.literal("a_src"), FromRef.literal("b_src"),
                                FromRef.literal("c_src"))),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
    }

    /** The sink runs two writers on the one member; {@code a} and {@code b} write keyed {@code x}, {@code c} {@code y}. */
    private static ExecutionShape shape() {
        return new ExecutionShape(1,
                Map.of("serve.s", new NodeParallelism("serve.s", 2, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 1, 2, 2, List.of())),
                Map.of(),
                Map.of("serve.s", Map.of(
                        "a", new SinkTarget("x", List.of("id")),
                        "b", new SinkTarget("x", List.of("id")),
                        "c", new SinkTarget("y", List.of("id")))));
    }

    private static DagBindings bindings(SourcePlacement placement) {
        Map<FromRef, List<String>> upstreams = Map.of(
                FromRef.literal("a_src"), List.of("a_src"),
                FromRef.literal("b_src"), List.of("b_src"),
                FromRef.literal("c_src"), List.of("c_src"));
        return new DagBindings(
                sourceKey -> source(FRONTIER.chainOf(sourceKey), placement),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) HoldingWriter::new,
                ref -> upstreams.getOrDefault(ref, List.of()));
    }

    /** {@code table}'s source: its load through the hand-off, then its ring, stamping bounds as it reads. */
    private static ProcessorMetaSupplier source(String table, SourcePlacement placement) {
        byte axis = FRONTIER.axes().axisOf(table);
        return SrsSourceProcessor.metaSupplier(PIPELINE, ringOf(table), table, StartFrom.earliest(), null, 1L,
                SrsReadCursorPublisherFactory.NONE,
                order -> new Watermark(FrontierOrders.pack(table, order), axis), placement);
    }

    /** Writes everything at once, except a write carrying {@code b}'s load row, which waits to be let finish. */
    private static final class HoldingWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            records.forEach(record -> WRITES.add("start|" + describe(record)));
            boolean holds = records.stream().anyMatch(record -> record.src().equals("b") && record.op() == Op.READ);
            CompletableFuture<Void> finished = holds ? held : CompletableFuture.completedFuture(null);
            return finished.thenApply(ignored -> {
                records.forEach(record -> WRITES.add("done|" + describe(record)));
                return new WriteResult(records.size());
            });
        }

        @Override
        public void close() {
        }

        private static String describe(Envelope record) {
            return record.src() + "|" + record.op().name() + "|" + record.after().get("id");
        }
    }
}
