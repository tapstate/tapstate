package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.DagBindings;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The data-plane composition test: the SRS projected source (this module) and the engine's DAG builder plus
 * generic sink adapter (the engine module) wired into one Jet DAG the builder assembled, run on an embedded
 * member over a real per-table change ring. {@link SrsSourceProcessorTest} proves the source vertex alone and
 * the engine's {@code PipelineDagRunTest} proves the builder over synthetic doubles; this proves the two
 * compose - a change appended to the ring flows source -&gt; passthrough transform -&gt; sink, carrying the
 * injected stream name and the source position the projection lifted into the envelope, in read order.
 *
 * <p>It lives here, not in the engine, because the engine's own DAG-run tests are deliberately source-agnostic
 * (they inject synthetic sources so they carry no SRS or connector dependency); the SRS-to-DAG seam is a
 * source-side integration concern, so this module carries the engine as a test-scope dependency rather than
 * the reverse. Every vertex is pinned to total parallelism one, exactly the production topology, so the sink's
 * observed order is the source's read order. The source is a live tail that never completes, so the job is
 * cancelled once the expected changes arrive rather than joined.
 */
class SrsDagRunTest {

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        config.setClusterName("srs-dag-run-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        hz = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void stopMember() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    @Test
    void built_srs_source_passthrough_sink_dag_delivers_projected_changes_to_the_sink()
            throws InterruptedException {
        fill("srs.chain.t0", 4);
        CapturingSinkWriter.reset("t0");

        SupplierEx<TransformPort> passthrough = () -> event -> List.of(event);
        Job job = hz.getJet().newJob(
                PipelineDagBuilder.build(linearPipeline(), bindings("srs.chain.t0", "orders", "t0", passthrough)));
        try {
            awaitSize("t0", 4);
        } finally {
            job.cancel();
        }

        // The sink sees every change the source projected, in read order, each carrying the injected stream
        // name and the source position the projection lifted from the ring item - the whole point of composing
        // the SRS source with the engine's builder, which a structural assertion of the built DAG cannot see.
        assertThat(CapturingSinkWriter.collected("t0"))
                .containsExactly("orders|w0|0", "orders|w1|1", "orders|w2|2", "orders|w3|3");
    }

    @Test
    void built_srs_source_filter_sink_dag_drops_the_changes_the_filter_rejects()
            throws InterruptedException {
        fill("srs.chain.t6filter", 4);
        CapturingSinkWriter.reset("t6filter");

        // A real linear transform in the composed DAG, fed by the SRS source: a filter that keeps even ids.
        // The count drops (two of four survive) and the survivors keep their projected position - proof the
        // transform runs over the real source stream, not just that records flow.
        SupplierEx<TransformPort> keepEvenIds = () -> event ->
                ((Integer) event.after().get("id")) % 2 == 0 ? List.of(event) : List.of();
        Job job = hz.getJet().newJob(PipelineDagBuilder.build(
                linearPipeline(), bindings("srs.chain.t6filter", "orders", "t6filter", keepEvenIds)));
        try {
            awaitSize("t6filter", 2);
        } finally {
            job.cancel();
        }

        assertThat(CapturingSinkWriter.collected("t6filter"))
                .containsExactly("orders|w0|0", "orders|w2|2");
    }

    @Test
    void built_srs_source_map_sink_dag_delivers_the_mapped_content_keeping_the_source_position()
            throws InterruptedException {
        fill("srs.chain.t6map", 3);
        CapturingSinkWriter.reset("t6map");

        // A map that rewrites the row (id -> id * 10) and returns a fresh envelope with no position of its own.
        // The sink must see the mapped content and yet each row must still carry its inbound source position:
        // the framework stamps the inbound position onto the port's outputs, so the port stays a pure function
        // and the sink-ack currency survives a content-changing transform.
        SupplierEx<TransformPort> tenfoldId = () -> event -> List.of(
                Envelope.insert(event.ts(), event.src(),
                        Map.of("id", ((Integer) event.after().get("id")) * 10), null));
        Job job = hz.getJet().newJob(PipelineDagBuilder.build(
                linearPipeline(), bindings("srs.chain.t6map", "orders", "t6map", tenfoldId)));
        try {
            awaitSize("t6map", 3);
        } finally {
            job.cancel();
        }

        assertThat(CapturingSinkWriter.collected("t6map"))
                .containsExactly("orders|w0|0", "orders|w1|10", "orders|w2|20");
    }

    /** One source -&gt; one linear transform step -&gt; one serve sink. */
    private static PipelineResource linearPipeline() {
        return new PipelineResource(
                "p", null,
                List.of("orders_src"),
                List.of(Step.inline("transform",
                        FromClause.list(FromRef.literal("orders_src")),
                        new TransformBody.Filter("true"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("transform"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null, null)),
                        null, null),
                null, null);
    }

    /**
     * Binds the one source id to the SRS projected source vertex, the one step to the given port (the body type
     * routes it as a linear step; the bound port, not the filter expression, is what runs), and the one sync
     * element to a capturing sink writer.
     */
    private static DagBindings bindings(String ringName, String src, String sinkName,
            SupplierEx<TransformPort> transformPort) {
        SupplierEx<SinkWriter> intoSink = () -> new CapturingSinkWriter(sinkName);
        return new DagBindings(
                sourceId -> SrsSourceProcessor.metaSupplier(
                        ringName, src, StartFrom.earliest(), 1L, SrsReadCursorPublisherFactory.NONE),
                step -> transformPort,
                syncElement -> intoSink,
                ref -> Map.of(
                        FromRef.literal("orders_src"), List.of("orders_src"),
                        FromRef.literal("transform"), List.of("transform")).getOrDefault(ref, List.of()));
    }

    /** Pre-fills a ring with {@code count} inserts at sequences {@code 0..count-1}, positions {@code w0..}. */
    private static void fill(String ringName, int count) {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
        for (int i = 0; i < count; i++) {
            ring.append(new SrsItem(new SourcePosition("w" + i), Op.INSERT, 1L, null, Map.of("id", i), 0L));
        }
    }

    private static void awaitSize(String sinkName, int size) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (CapturingSinkWriter.collected(sinkName).size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "timed out waiting for " + size + ", got " + CapturingSinkWriter.collected(sinkName).size());
            }
            Thread.sleep(50);
        }
    }

    /**
     * A sink writer that records each event as a stable {@code src|srcPos|id} string into a named JVM-static
     * queue, so an embedded run can assert what the built DAG delivered. Keyed by name rather than held by the
     * test because the writer is opened member-side behind the sink seam; on the single embedded member the
     * static queue is shared with the test thread. Completes each write synchronously.
     */
    private static final class CapturingSinkWriter implements SinkWriter {

        private static final Map<String, Queue<String>> SINKS = new ConcurrentHashMap<>();

        private final String name;

        CapturingSinkWriter(String name) {
            this.name = name;
        }

        static Queue<String> collected(String name) {
            return SINKS.computeIfAbsent(name, key -> new ConcurrentLinkedQueue<>());
        }

        static void reset(String name) {
            SINKS.remove(name);
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            Queue<String> queue = collected(name);
            for (Envelope record : records) {
                queue.add(record.src() + "|" + token(record) + "|" + record.after().get("id"));
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        /** The connector token of the spot this record sits at on its own stream, or null where it sits nowhere. */
        private static String token(Envelope record) {
            ChainPosition at = record.position();
            return at == null ? null : at.token();
        }

        @Override
        public void close() {
        }
    }
}
