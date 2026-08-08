package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
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
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsMetaStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The overlap-window idempotency proof for a {@code snapshot_and_cdc} pipeline: a sink keyed on the resolved
 * target model's primary key absorbs the window where cdc replays a change already carried by the snapshot,
 * so the same key lands once rather than twice. The snapshot and cdc rows share a key (id 3); with an upsert
 * keyed on that key the sink holds five distinct rows, not six, and the newer cdc value wins because all
 * snapshot rows reach the sink before any cdc change.
 *
 * <p>Connector-free like the sibling data-flow proofs: a fake {@link CapturePort} drives a bounded snapshot
 * and a cdc stream, the target model is resolved from a seeded discovered schema, and the sink is a keyed
 * upsert bound through the builder seam. The resolution, the buffer-then-tail ordering, the transform chain
 * and the verb composition are production code driven through the real actuator over one embedded member.
 */
class UpsertOverlapIdempotencyTest {

    private static final String PIPELINE = "p";
    private static final String SOURCE_ID = "orders_src";
    private static final String DEST_ID = "orders_dest";
    private static final String TABLE = "orders";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("upsert-overlap-test-" + System.nanoTime());
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
        UpsertSink.reset();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("an upsert keyed on the resolved primary key absorbs the snapshot/cdc overlap window")
    void upsertOnTheResolvedKeyAbsorbsTheOverlapWindow() {
        InMemoryStorePort store = seedPipelineAndSchema();
        makeMemberCapable(store);
        // Three snapshot rows (ids 1..3) and three cdc changes (ids 3..5): id 3 overlaps. The snapshot value is
        // "snap" and the cdc value is "cdc", so a correct run keys on id, holds five rows and lets cdc win on 3.
        FakeSource source = new FakeSource(
                List.of(read(1, "snap"), read(2, "snap"), read(3, "snap")),
                List.of(insert(3, "cdc"), insert(4, "cdc"), insert(5, "cdc")));
        LifecycleActuator actuator = actuator(store, source);

        actuator.start(PIPELINE);
        try {
            awaitKeyPresent("5");
        } finally {
            actuator.stop(PIPELINE);
        }

        assertThat(UpsertSink.keys()).containsExactlyInAnyOrder("1", "2", "3", "4", "5");
        assertThat(UpsertSink.amountOf("3")).as("the newer cdc value wins the overlap").isEqualTo("cdc");
    }

    @Test
    @DisplayName("re-delivering the same keys does not duplicate under the keyed upsert (replay safety)")
    void reDeliveredKeysDoNotDuplicate() {
        // A replay re-reads the source from earliest, so the sink sees every change again. This models that by
        // having cdc deliver ids 3..5 twice: the second pass carries a newer value. With an upsert keyed on the
        // resolved primary key the sink still holds five distinct rows, and the last delivery wins each key.
        InMemoryStorePort store = seedPipelineAndSchema();
        makeMemberCapable(store);
        FakeSource source = new FakeSource(
                List.of(read(1, "snap"), read(2, "snap"), read(3, "snap")),
                List.of(insert(3, "cdc"), insert(4, "cdc"), insert(5, "cdc"),
                        insert(3, "replay"), insert(4, "replay"), insert(5, "replay")));
        LifecycleActuator actuator = actuator(store, source);

        actuator.start(PIPELINE);
        try {
            awaitTotalAtLeast(9);
        } finally {
            actuator.stop(PIPELINE);
        }

        // Nine rows delivered (three snapshot, six cdc including the three re-delivered), yet the keyed upsert
        // holds exactly five distinct rows and the re-delivered value wins -- re-delivery does not duplicate.
        assertThat(UpsertSink.keys()).containsExactlyInAnyOrder("1", "2", "3", "4", "5");
        assertThat(UpsertSink.amountOf("3")).as("the re-delivered value wins").isEqualTo("replay");
    }

    // ---- wiring ------------------------------------------------------------------------

    private InMemoryStorePort seedPipelineAndSchema() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE_ID, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(TABLE)), null, null, null));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SOURCE_ID),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.schemas().save(new DiscoveredSourceModel(SOURCE_ID, "fake", 0L, new SourceModel(List.of(
                new SourceTable(TABLE,
                        List.of(new SourceField("id", "INT"), new SourceField("amount", "STRING")),
                        List.of("id"),
                        List.of())))));
        return store;
    }

    private void makeMemberCapable(InMemoryStorePort store) {
        SrsMetaStore meta = store.meta();
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        ConnectorProvisioner provisioner = connectorId -> {
            throw new UnsupportedOperationException("not resolved by this idempotency test");
        };
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, provisioner);
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, new SnapshotBuffer());
    }

    private LifecycleActuator actuator(InMemoryStorePort store, FakeSource source) {
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        CaptureRunUnit captureRunUnit = new CaptureRunUnit(source, srsCoordinator, store.meta(), member);
        PipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, captureRunUnit::start, srsCoordinator,
                (SnapshotBuffer) member.getUserContext().get(SnapshotBuffer.USER_CONTEXT_KEY));
        StoreBackedDagSource.SinkWriterBinder upsertSink =
                (connectorId, settings, writeMode, ddl, target) -> (SupplierEx<SinkWriter>) () -> new UpsertSink(target);
        DagSource dagSource = new StoreBackedDagSource(store, upsertSink);
        return new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, new NestStateTeardown(member, store.keyedState()));
    }

    private void awaitKeyPresent(String key) {
        awaitCondition(() -> UpsertSink.keys().contains(key),
                () -> "timed out waiting for key " + key + " at the sink, have " + UpsertSink.keys());
    }

    private void awaitTotalAtLeast(int total) {
        awaitCondition(() -> UpsertSink.total() >= total,
                () -> "timed out waiting for " + total + " total writes, have " + UpsertSink.total());
    }

    private void awaitCondition(java.util.function.BooleanSupplier done, java.util.function.Supplier<String> onTimeout) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(onTimeout.get());
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting sink", e);
            }
        }
    }

    private static Envelope read(int id, String amount) {
        return Envelope.read(id, TABLE, Map.of("id", (long) id, "amount", amount), Map.of());
    }

    private static Envelope insert(int id, String amount) {
        return Envelope.insert(id, TABLE, Map.of("id", (long) id, "amount", amount), Map.of());
    }

    /** A fake connector: a bounded snapshot batch and a fixed cdc stream driven synchronously when cdc starts. */
    private static final class FakeSource implements CapturePort {

        private final List<Envelope> snapshotRows;
        private final List<Envelope> changes;

        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes) {
            this.snapshotRows = snapshotRows;
            this.changes = changes;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch(snapshotRows);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureListener listener) {
            for (Envelope change : changes) {
                listener.onEvent(change);
            }
            return () -> { };
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

    /** A bounded snapshot batch over a fixed list of rows. */
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

    /**
     * A sink that upserts into a JVM-static store keyed on the resolved target's primary-key columns, so the
     * embedded run can assert what the built DAG delivered. A repeated key overwrites rather than accumulates,
     * which is what makes the overlap window idempotent. Shared with the test thread on one member.
     */
    private static final class UpsertSink implements SinkWriter {

        private static final Map<String, Map<String, Object>> STORE = new ConcurrentHashMap<>();
        private static final AtomicInteger TOTAL = new AtomicInteger();

        private final List<String> keyColumns;

        UpsertSink(TargetTable target) {
            this.keyColumns = target.fields().stream()
                    .filter(TargetField::primaryKey)
                    .map(TargetField::name)
                    .toList();
        }

        static void reset() {
            STORE.clear();
            TOTAL.set(0);
        }

        static java.util.Set<String> keys() {
            return STORE.keySet();
        }

        static Object amountOf(String key) {
            return STORE.get(key).get("amount");
        }

        static int total() {
            return TOTAL.get();
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope record : records) {
                Map<String, Object> row = record.after() != null ? record.after() : record.before();
                String key = keyColumns.stream().map(column -> String.valueOf(row.get(column)))
                        .collect(java.util.stream.Collectors.joining("|"));
                STORE.put(key, row);
                TOTAL.incrementAndGet();
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
