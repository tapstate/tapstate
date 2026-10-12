package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.Resource;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsConsumerId;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

class AJoinRecordsItsTargetProgressTest {

    private static final String PIPELINE = "joined_orders";
    private static final Map<Long, Map<String, Object>> VIEW = new ConcurrentHashMap<>();
    private static final Map<String, List<Envelope>> ROWS = Map.of(
            "orders", List.of(
                    Envelope.read(1L, "orders", Map.of("id", 10L, "customer_id", 1L), null),
                    Envelope.read(2L, "orders", Map.of("id", 11L, "customer_id", 1L), null)),
            "customers", List.of(
                    Envelope.read(1L, "customers", Map.of("id", 1L, "tier", "gold"), null)));

    @Test
    void aRestartKeepsTheLoadsThatTheJoinAlreadyLanded() throws InterruptedException {
        VIEW.clear();
        InMemoryStorePort store = store();
        Config config = new Config();
        config.setClusterName("join-progress-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.addRingBufferConfig(new RingbufferConfig("srs.*").setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT).setTimeToLiveSeconds(0).setBackupCount(0)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setEnabled(true)
                        .setFactoryImplementation(new SrsLogRingbufferStoreFactory(store.srsLog()))));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        SnapshotBuffer snapshots = new SnapshotBuffer();
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, snapshots);
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, store.meta());
        member.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, store.srsLog());
        SnapshotSource source = new SnapshotSource();
        SrsCoordinator srs = new SrsCoordinator(store.meta());
        CaptureRunUnit capture = new CaptureRunUnit(source, srs, store.meta(), member);
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, capture::start, srs, snapshots);
        try {
            StoreBackedDagSource.SinkWriterBinder sink = (connector, settings, mode, ddl, target, node) ->
                    (SupplierEx<SinkWriter>) UpsertingView::new;
            EngineLifecycleActuator actuator = new EngineLifecycleActuator(
                    new Engine(member), new StoreBackedDagSource(store, sink), coordinator,
                    new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()));
            actuator.start(PIPELINE);
            try {
                await(() -> VIEW.size() == 2 && VIEW.values().stream()
                        .allMatch(row -> "gold".equals(row.get("tier"))), member, "both joined rows in the view");
                assertThat(VIEW).containsOnlyKeys(10L, 11L);
                assertThat(source.loads).containsExactlyInAnyOrderEntriesOf(Map.of("orders", 1, "customers", 1));

                // Give the source's idle bounds time to confirm the last writes before keeping the run's state.
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (System.nanoTime() < deadline && !loadsConfirmed(store)) {
                    requireRunning(member);
                    Thread.sleep(25);
                }
                Map<String, ConsumerOffset> beforeRestart = Map.of(
                        "orders", progress(store, "orders"), "customers", progress(store, "customers"));
                actuator.stop(PIPELINE, false);
                actuator.start(PIPELINE);
                await(() -> source.tails.getOrDefault("orders", 0) == 2
                        && source.tails.getOrDefault("customers", 0) == 2,
                        member, "both source tails reattached after the restart");

                SoftAssertions.assertSoftly(soft -> {
                    beforeRestart.forEach((table, offset) -> {
                        soft.assertThat(offset.snapshotCompletedTables())
                                .as("%s was written through the join and must be recorded as loaded", table)
                                .contains(table);
                        soft.assertThat(offset.sinkAcked())
                                .as("%s must have a target-confirmed position after its joined rows landed", table)
                                .isNotNull();
                    });
                    soft.assertThat(source.loads)
                            .as("a restart keeping state must not read either fully landed joined table again")
                            .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 1, "customers", 1));
                });
            } finally {
                actuator.stop(PIPELINE, false);
            }
        } finally {
            coordinator.close();
            member.shutdown();
            VIEW.clear();
        }
    }

    private static boolean loadsConfirmed(InMemoryStorePort store) {
        return ROWS.keySet().stream().allMatch(table -> progress(store, table)
                .snapshotCompletedTables().contains(table));
    }

    private static ConsumerOffset progress(InMemoryStorePort store, String table) {
        String source = table + "_src";
        String chain = SourceCaptureResolution.of(StoredArtifacts.requireSource(store.artifacts(), source))
                .chainId().value();
        String consumer = SrsConsumerId.of(PIPELINE, source).value();
        return store.meta().consumerOffsets(chain).stream()
                .filter(offset -> offset.pipelineId().equals(consumer)).findFirst().orElseThrow();
    }

    private static void await(BooleanSupplier ready, HazelcastInstance member, String what)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!ready.getAsBoolean()) {
            requireRunning(member);
            assertThat(System.nanoTime()).as("waiting for %s; view=%s", what, VIEW).isLessThan(deadline);
            Thread.sleep(25);
        }
    }

    private static void requireRunning(HazelcastInstance member) {
        Job job = member.getJet().getJob(PIPELINE);
        assertThat(job).as("the pipeline job was submitted").isNotNull();
        if (job.getStatus().isTerminal()) {
            job.join();
            throw new AssertionError("the pipeline ended before the probe completed");
        }
    }

    private static InMemoryStorePort store() {
        DslParser parser = new DslParser();
        List<Resource> resources = List.of(parser.parse("""
                version: tapstate/v1
                kind: source
                id: orders_src
                connector: fake
                config: { host: h }
                mode: cdc
                tables: [ orders ]
                """), parser.parse("""
                version: tapstate/v1
                kind: source
                id: customers_src
                connector: fake
                config: { host: h }
                mode: cdc
                tables: [ customers ]
                """), parser.parse("""
                version: tapstate/v1
                kind: source
                id: views
                connector: fake
                config: { host: view-store }
                """), parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: joined_orders
                source: [ { id: orders_src, srs: true }, { id: customers_src, srs: true } ]
                transforms:
                  - id: joined
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      SELECT o.id AS order_id, c.tier AS tier
                      FROM o LEFT JOIN c ON o.customer_id = c.id
                view:
                  id: order_view
                  from: joined
                  primary_key: order_id
                  storage: { warm: { collection: order_view } }
                settings: { read_mode: snapshot_and_cdc, start_from: earliest }
                """));
        Workspace.of(resources);
        InMemoryStorePort store = new InMemoryStorePort();
        resources.forEach(store.artifacts()::save);
        store.schemas().save(new DiscoveredSourceModel("orders_src", "fake", 0L,
                new SourceModel(List.of(new SourceTable("orders", List.of(
                        new SourceField("id", "bigint"), new SourceField("customer_id", "bigint")),
                        List.of("id"), List.of())))));
        store.schemas().save(new DiscoveredSourceModel("customers_src", "fake", 0L,
                new SourceModel(List.of(new SourceTable("customers", List.of(
                        new SourceField("id", "bigint"), new SourceField("tier", "varchar")),
                        List.of("id"), List.of())))));
        return store;
    }

    private static final class SnapshotSource implements CapturePort {
        private final Map<String, Integer> loads = new ConcurrentHashMap<>();
        private final Map<String, Integer> tails = new ConcurrentHashMap<>();

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            String table = config.streams().getFirst();
            loads.merge(table, 1, Integer::sum);
            Iterator<Envelope> rows = ROWS.get(table).iterator();
            return new CaptureBatch() {
                @Override public boolean hasNext() { return rows.hasNext(); }
                @Override public Envelope next() { return rows.next(); }
                @Override public Optional<SourcePosition> seam() {
                    return Optional.of(new SourcePosition("seam-0"));
                }
                @Override public void close() { }
            };
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            config.streams().forEach(table -> tails.merge(table, 1, Integer::sum));
            return () -> { };
        }

        @Override public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
        @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class UpsertingView implements SinkWriter {
        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            records.forEach(row -> VIEW.put(((Number) row.after().get("order_id")).longValue(), row.after()));
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override public void close() { }
    }
}
