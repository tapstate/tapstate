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
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsMetaStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The data-flow proof for a nest started end to end over two sources: two single-table capture units fill
 * two change rings, the store-backed topology reads both into one job, and what reaches the sink is whole
 * documents with their children attached.
 *
 * <p>This is the connector-free twin of the real two-source witness, and it exists because that one takes
 * minutes per run while this takes seconds. Everything the two share is the production path - two capture
 * units, two chains, the real assembly of the nest binding, the real ack-bearing sink - and what it drops
 * is only the connectors at either end.
 *
 * <p>The shape is the smallest that assembles anything: a root and one leaf embed, so no resolver vertex
 * is drawn and the assembler is fed by both sources directly. Two roots rather than one, because a single
 * root is satisfied by an implementation that piles every child onto whichever root arrived first.
 *
 * <p>Read mode is {@code snapshot_and_cdc} rather than {@code snapshot_only} deliberately: a stateful node
 * needs every row to carry its order, and a source reading no chain of its own supplies none. The seeded
 * rows arrive as snapshot reads; the chain is what puts an order on them.
 */
class NestOverTwoSourcesDataFlowTest {

    private static final String PIPELINE = "nested_orders";
    private static final String PARENT_SOURCE = "src_orders";
    private static final String CHILD_SOURCE = "src_items";
    private static final String DEST_ID = "tgt_mongo";
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String STEP = "order_doc";
    private static final String EMBED_PATH = "items";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("nest-two-source-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        member = Hazelcast.newHazelcastInstance(config);
        CapturingSinkWriter.reset();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("two capture units feed one nest, and whole documents with their children reach the sink")
    void childrenFromTheSecondSourceAreAssembledIntoTheDocumentsOfTheFirst() {
        InMemoryStorePort store = seedStore();
        LifecycleActuator actuator = wireRuntime(store, new SrsCoordinator(store.meta()));

        actuator.start(PIPELINE);
        List<Map<String, Object>> documents;
        try {
            awaitAssembled();
            documents = List.copyOf(CapturingSinkWriter.collected());
        } finally {
            stopQuietly(actuator);
        }

        Map<Object, Map<String, Object>> latest = latestPerRoot(documents);
        assertThat(latest.keySet())
                .describedAs("every root that was read has a document, and nothing invented one")
                .containsExactlyInAnyOrder(1L, 2L);
        assertThat(elementsOf(latest.get(1L)))
                .describedAs("the three items naming order 1 are all in its document")
                .hasSize(3);
        assertThat(elementsOf(latest.get(2L)))
                .describedAs("order 2 keeps its own single item rather than order 1 taking it")
                .hasSize(1);
        assertThat(elementsOf(latest.get(1L)).stream().map(item -> item.get("sku")).toList())
                .containsExactlyInAnyOrder("sku-1", "sku-2", "sku-3");
        assertThat(elementsOf(latest.get(2L))).singleElement()
                .satisfies(item -> assertThat(item).containsEntry("sku", "sku-4"));
    }

    /**
     * Both sources name the same connection, so they are one mining chain with a ring per table - which is
     * what a nest over two tables of one database is, and what the real two-source witness seeds. Stopping
     * such a pipeline must not fall over on the second source, and must still leave the chain closed.
     *
     * <p>It lives beside the data-flow proof because this is the first pipeline in the repo with two
     * sources on one connection: the fixture is the point, and duplicating it elsewhere would let the two
     * drift. Both halves are load-bearing - a stop that skipped the teardown entirely would no longer
     * throw, and would leave the chain open behind it.
     */
    @Test
    @DisplayName("stopping a pipeline whose two sources share one chain tears that chain down exactly once")
    void stoppingTwoSourcesThatShareOneChainDoesNotFallOverOnTheSecond() {
        InMemoryStorePort store = seedStore();
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        LifecycleActuator actuator = wireRuntime(store, srsCoordinator);
        MiningChainId chain = SourceCaptureResolution
                .of(StoredArtifacts.requireSource(store.artifacts(), PARENT_SOURCE)).chainId();

        actuator.start(PIPELINE);
        assertThat(srsCoordinator.isProvisioned(chain))
                .describedAs("the two sources opened one shared chain between them")
                .isTrue();
        assertThat(srsCoordinator.sourcesOf(chain))
                .describedAs("both sources merged onto it rather than opening one each")
                .containsExactlyInAnyOrder(PARENT_SOURCE, CHILD_SOURCE);

        actuator.stop(PIPELINE);

        assertThat(srsCoordinator.isProvisioned(chain))
                .describedAs("the shared chain is closed once, not once per source")
                .isFalse();
    }

    /**
     * Tears the run down without letting the teardown replace what the run itself proved. A failure thrown
     * from a finally block masks the assertion that was on its way out, which is how a data-flow result
     * gets reported as a teardown error; the teardown has a witness of its own above.
     */
    private static void stopQuietly(LifecycleActuator actuator) {
        try {
            actuator.stop(PIPELINE);
        } catch (RuntimeException e) {
            System.out.println("stop failed after the run: " + e);
        }
    }

    // ---- the pipeline under test ------------------------------------------------------

    /**
     * The runtime around the seeded store: one fake connector serving both capture units, the real capture
     * coordinator and store-backed topology, and a sink bound to a capturing writer. Everything between is
     * the production path.
     */
    private LifecycleActuator wireRuntime(InMemoryStorePort store, SrsCoordinator srsCoordinator) {
        SrsMetaStore meta = store.meta();
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY,
                (ConnectorProvisioner) connectorId -> {
                    throw new UnsupportedOperationException("not resolved by this data-flow test");
                });
        SnapshotBuffer snapshotBuffer = new SnapshotBuffer();
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, snapshotBuffer);

        // One fake connector for both capture units, telling them apart by the stream each asks for -
        // which is what a single-table capture unit names.
        Map<String, List<Envelope>> rowsByTable = new LinkedHashMap<>();
        rowsByTable.put(PARENT_TABLE, List.of(order(1), order(2)));
        rowsByTable.put(CHILD_TABLE, List.of(item(1, 1), item(2, 1), item(3, 1), item(4, 2)));

        CaptureRunUnit captureRunUnit =
                new CaptureRunUnit(new FakeSource(rowsByTable), srsCoordinator, meta, member);
        PipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, captureRunUnit::start, srsCoordinator, snapshotBuffer);

        StoreBackedDagSource.SinkWriterBinder capturingSink =
                (connectorId, settings, writeMode, ddl, target) -> (SupplierEx<SinkWriter>) CapturingSinkWriter::new;
        return new EngineLifecycleActuator(
                new Engine(member), new StoreBackedDagSource(store, capturingSink), coordinator,
                new NestStateTeardown(member, store.keyedState()));
    }

    /** The two sources, the sink connection and the nest pipeline, plus a discovered model for each source. */
    private static InMemoryStorePort seedStore() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source(PARENT_SOURCE, PARENT_TABLE));
        artifacts.save(source(CHILD_SOURCE, CHILD_TABLE));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));

        Embed item = new Embed("i", Map.of("order_id", "id"), EmbedAs.ARRAY, EMBED_PATH, List.of("id"),
                null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("id"), null, null, List.of(item)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("o", FromRef.literal(PARENT_TABLE));
        aliases.put("i", FromRef.literal(CHILD_TABLE));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        artifacts.save(new PipelineResource(PIPELINE, null, List.of(PARENT_SOURCE, CHILD_SOURCE),
                List.of(step), null,
                new ServeBlock.Inline(null, FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));

        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        // Both models are discovered: the parent's resolves the target the sink writes, and each supplies
        // the key an embed falls back on when it declares no arrayKey of its own.
        store.schemas().save(discovered(PARENT_SOURCE,
                new SourceTable(PARENT_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("name", "string")),
                        List.of("id"), List.of())));
        store.schemas().save(discovered(CHILD_SOURCE,
                new SourceTable(CHILD_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("order_id", "int"),
                                new SourceField("sku", "string")),
                        List.of("id"), List.of())));
        return store;
    }

    private static SourceResource source(String id, String table) {
        return new SourceResource(id, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null, null);
    }

    private static DiscoveredSourceModel discovered(String connectionId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, "fake", 0L, new SourceModel(List.of(table)));
    }

    private static Envelope order(long id) {
        return Envelope.read(id, PARENT_TABLE, Map.of("id", id, "name", "order-" + id), Map.of());
    }

    private static Envelope item(long id, long orderId) {
        return Envelope.read(id, CHILD_TABLE,
                Map.of("id", id, "order_id", orderId, "sku", "sku-" + id), Map.of());
    }

    // ---- reading what came out --------------------------------------------------------

    /** The last state each root reached, keyed by its root key: a document is emitted per drain. */
    private static Map<Object, Map<String, Object>> latestPerRoot(List<Map<String, Object>> documents) {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Map<String, Object> document : documents) {
            latest.put(document.get("id"), document);
        }
        return latest;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> elementsOf(Map<String, Object> document) {
        assertThat(document).describedAs("no document was assembled for this root").isNotNull();
        Object embedded = document.get(EMBED_PATH);
        assertThat(embedded)
                .describedAs("the root reached the sink with nothing assembled under it: %s", document)
                .isNotNull();
        return (List<Map<String, Object>>) embedded;
    }

    /**
     * Waits until every root is present with every child attached. On timeout it reports what the job is
     * actually doing: zero documents is a symptom shared by a job that failed, a job still starting and a
     * job assembling nothing, and only the job status tells them apart.
     */
    private void awaitAssembled() {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (settled(CapturingSinkWriter.collected())) {
                return;
            }
            sleep();
        }
        Job job = member.getJet().getJob(PIPELINE);
        throw new AssertionError("the nest never assembled what was read: the sink holds "
                + CapturingSinkWriter.collected()
                + System.lineSeparator() + "  job status: " + (job == null ? "no job" : job.getStatus())
                + System.lineSeparator() + "  job failure: " + failureOf(job));
    }

    /** What killed the job, when it is the job that died rather than the data that never came. */
    private static String failureOf(Job job) {
        if (job == null || !job.getStatus().isTerminal()) {
            return "none reported";
        }
        try {
            job.join();
            return "none, the job completed";
        } catch (Exception e) {
            return String.valueOf(e);
        }
    }

    /** Whether every root is present and every child attached somewhere, so waiting can stop. */
    private static boolean settled(Queue<Map<String, Object>> written) {
        Map<Object, Map<String, Object>> latest = latestPerRoot(new ArrayList<>(written));
        if (latest.size() != 2) {
            return false;
        }
        int attached = 0;
        for (Map<String, Object> document : latest.values()) {
            if (!(document.get(EMBED_PATH) instanceof List<?> elements)) {
                return false;
            }
            attached += elements.size();
        }
        return attached == 4;
    }

    private static void sleep() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the assembled documents", e);
        }
    }

    // ---- doubles ----------------------------------------------------------------------

    /** One fake connector for both capture units, serving the rows of whichever stream is asked for. */
    private static final class FakeSource implements CapturePort {

        private final Map<String, List<Envelope>> rowsByTable;

        FakeSource(Map<String, List<Envelope>> rowsByTable) {
            this.rowsByTable = rowsByTable;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch(rowsFor(config));
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureListener listener) {
            // Nothing changes after the snapshot: what is under test is that the rows already there
            // assemble, which is exactly what the real two-source witness seeds.
            return () -> { };
        }

        private List<Envelope> rowsFor(CaptureConfig config) {
            List<String> streams = config.streams();
            return streams.isEmpty() ? List.of() : rowsByTable.getOrDefault(streams.get(0), List.of());
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FakeBatch implements CaptureBatch {

        private final Iterator<Envelope> rows;

        FakeBatch(List<Envelope> rows) {
            this.rows = new ArrayList<>(rows).iterator();
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public Envelope next() {
            return rows.next();
        }

        @Override
        public void close() {
        }
    }

    /** Records each assembled document into a JVM-static queue, shared with the test thread on one member. */
    private static final class CapturingSinkWriter implements SinkWriter {

        private static final Queue<Map<String, Object>> COLLECTED = new ConcurrentLinkedQueue<>();

        static Queue<Map<String, Object>> collected() {
            return COLLECTED;
        }

        static void reset() {
            COLLECTED.clear();
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope record : records) {
                if (record.after() != null) {
                    COLLECTED.add(record.after());
                }
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
