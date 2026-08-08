package io.tapstate.app;

import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
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
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The four lifecycle verbs (start / pause / resume / stop) driving a real assembled pipeline through the
 * production converge loop, not a stand-in idle topology. It wires the same chain the {@code --role=all}
 * process runs -- desired intent -> converge loop -> real {@link EngineLifecycleActuator} over a store-backed
 * DAG and a real capture run -> Jet job, and converge loop -> observation -> read faces -- over one embedded
 * member, and drives a data-carrying pipeline through all four verbs.
 *
 * <p>This closes the gap the idle-topology lifecycle proof leaves: there the verbs drive a job that moves no
 * data; here they drive the real capture -> transform -> sink job. At each verb it witnesses the mapped Jet
 * operation on the live job (RUNNING / SUSPENDED / RUNNING / cancelled), the store-backed read face reporting
 * the converged state with a strictly increasing fencing epoch, and -- once running -- the seeded rows
 * actually reaching the sink, so the verbs are shown acting on a real running pipeline and not an empty one.
 */
class LifecycleVerbsOnRealChainE2ETest {

    private static final String PIPELINE = "orders-pipe";
    private static final String SOURCE_ID = "orders_src";
    private static final String DEST_ID = "orders_dest";
    private static final String TABLE = "orders";
    private static final String REV = "rev-1";
    private static final Instant T0 = Instant.parse("2026-07-15T00:00:00Z");

    private HazelcastInstance member;
    private InMemoryStorePort store;
    private ConvergenceDriver driver;
    private PipelineObservationQueryService readFaces;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("lifecycle-real-chain-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        // Collect job metrics once a second so recordCount is observable well within a test budget (default 5s).
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        member = Hazelcast.newHazelcastInstance(config);
        RecordingSink.reset();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("start -> pause -> resume -> stop drives the real capture->transform->sink Jet job and the read faces reflect every transition")
    void theFourVerbsDriveTheRealAssembledPipeline() {
        store = seedPipelineAndSchema();
        makeMemberCapable(store);
        FakeSource source = new FakeSource(
                List.of(read(1), read(2), read(3)),
                List.of(insert(4), insert(5), insert(6)));
        wireConvergeChain(source);

        // start: the converger submits the real job; the seeded snapshot + cdc rows reach the sink.
        desire(RUNNING);
        Job job = member.getJet().getJob(PIPELINE);
        assertThat(job).as("start submits the pipeline's Jet job").isNotNull();
        awaitStatus(job, JobStatus.RUNNING);
        awaitKeys("1", "2", "3", "4", "5", "6");
        assertActualState(RUNNING, 1);
        assertThat(readFaces.status(PIPELINE)).isEqualTo(new PipelineStatus(PIPELINE, RUNNING));

        // pause: the live job suspends and the read face reports PAUSED.
        desire(PAUSED);
        awaitStatus(job, JobStatus.SUSPENDED);
        assertActualState(PAUSED, 2);
        assertThat(readFaces.status(PIPELINE)).isEqualTo(new PipelineStatus(PIPELINE, PAUSED));

        // resume: the suspended job runs again and the read face reports RUNNING.
        desire(RUNNING);
        awaitStatus(job, JobStatus.RUNNING);
        assertActualState(RUNNING, 3);
        assertThat(readFaces.status(PIPELINE)).isEqualTo(new PipelineStatus(PIPELINE, RUNNING));

        // stop: the job is cancelled (Jet reports a cancelled job as FAILED) and the read face reports STOPPED.
        desire(STOPPED);
        awaitStatus(job, JobStatus.FAILED);
        assertActualState(STOPPED, 4);
        assertThat(readFaces.status(PIPELINE)).isEqualTo(new PipelineStatus(PIPELINE, STOPPED));
    }

    @Test
    @DisplayName("the assembly-built publisher surfaces recordCount from the real live job")
    void theWiredPublisherSurfacesRecordCountFromTheLiveJob() {
        store = seedPipelineAndSchema();
        makeMemberCapable(store);
        FakeSource source = new FakeSource(
                List.of(read(1), read(2), read(3)),
                List.of(insert(4), insert(5), insert(6)));
        wireConvergeChain(source);

        desire(RUNNING);
        Job job = member.getJet().getJob(PIPELINE);
        awaitStatus(job, JobStatus.RUNNING);
        awaitKeys("1", "2", "3", "4", "5", "6");

        // Re-publish until the live job's collected metrics carry the six delivered records, so recordCount is
        // witnessed riding the real engine through the assembly-built publisher rather than a default. The
        // position port is witnessed off the store elsewhere (the real sink-ack advance in
        // CaptureToSinkAckFrontierTest, the publisher's projection in AssemblyObservationPublisherTest); racing a
        // value into the chain this live sink also writes would be a fragile witness, so it is not done here.
        Observation observed = awaitObservation(obs -> obs.metrics().getOrDefault("recordCount", -1L) == 6L);

        assertThat(observed.metrics())
                .as("recordCount is the number of records the live job drove to its serve sink; errorCount stays 0")
                .containsEntry("recordCount", 6L)
                .containsEntry("errorCount", 0L);
    }

    @Test
    @DisplayName("the assembly-built publisher surfaces snapshot progress from the real capture coordinator")
    void theWiredPublisherSurfacesSnapshotProgressFromTheRealCaptureCoordinator() {
        // This is the seam the two capture-coordinator snapshot bugs shipped through untested: the only
        // other place snapshotProgress is exercised uses a stub, so a coordinator built here but never
        // actually wired into the publisher (or a publisher wired to a no-op stand-in) would still leave
        // the whole reactor green. Wiring the real StoreBackedPipelineCaptureCoordinator all the way
        // through -- the same one wireConvergeChain hands to the publisher -- closes that gap.
        store = seedPipelineAndSchema();
        makeMemberCapable(store);
        FakeSource source = new FakeSource(List.of(read(1), read(2), read(3)), List.of());
        wireConvergeChain(source);

        desire(RUNNING);
        Job job = member.getJet().getJob(PIPELINE);
        awaitStatus(job, JobStatus.RUNNING);
        awaitKeys("1", "2", "3");

        Observation observed = awaitObservation(obs -> !obs.snapshot().isEmpty());

        assertThat(observed.snapshot()).containsKey(TABLE);
        assertThat(observed.snapshot().get(TABLE).rowsDone())
                .as("the three rows the fake source's bounded snapshot batch drained")
                .isEqualTo(3L);
    }

    // ---- wiring ------------------------------------------------------------------------

    private void wireConvergeChain(FakeSource source) {
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        CaptureRunUnit captureRunUnit = new CaptureRunUnit(source, srsCoordinator, store.meta(), member);
        PipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, captureRunUnit::start, srsCoordinator,
                (SnapshotBuffer) member.getUserContext().get(SnapshotBuffer.USER_CONTEXT_KEY));
        StoreBackedDagSource.SinkWriterBinder recordingSink =
                (connectorId, settings, writeMode, ddl, target) -> (SupplierEx<SinkWriter>) () -> new RecordingSink(target);
        DagSource dagSource = new StoreBackedDagSource(store, recordingSink);
        Engine engine = new Engine(member);
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(
                engine, dagSource, coordinator, new NestStateTeardown(member, store.keyedState()));

        Clock clock = Clock.fixed(T0, ZoneOffset.UTC);
        PipelineConverger converger = new PipelineConverger(store.desired(), store.state(), actuator, clock);
        // Build the publisher through the production assembly factory, so the metric ports it binds
        // (recordCount from the live job, per-table positions from the store, per-table snapshot progress
        // from the capture coordinator) are the ones under test. The real coordinator built above -- the
        // same one actuator.start() drives -- is passed here too, not a stub: that is what actually joins
        // the capture seam to the publisher, the join a stub silently skips.
        ObservationPublisher publisher = new RuntimeConvergenceConfiguration()
                .observationPublisher(store, engine, coordinator);
        driver = new ConvergenceDriver(converger, store.desired(), publisher);
        readFaces = new PipelineObservationQueryService(new ArtifactQueryService(store.artifacts()), store.observations());
    }

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
        InMemoryStorePort seeded = new InMemoryStorePort(artifacts);
        seeded.schemas().save(new DiscoveredSourceModel(SOURCE_ID, "fake", 0L, new SourceModel(List.of(
                new SourceTable(TABLE,
                        List.of(new SourceField("id", "INT"), new SourceField("amount", "STRING")),
                        List.of("id"),
                        List.of())))));
        return seeded;
    }

    private void makeMemberCapable(InMemoryStorePort seeded) {
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, seeded.meta());
        ConnectorProvisioner provisioner = connectorId -> {
            throw new UnsupportedOperationException("not resolved by this lifecycle test");
        };
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, provisioner);
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, new SnapshotBuffer());
    }

    /** Saves the pipeline's desired target and runs one reconcile pass, the tick the scheduled driver makes. */
    private void desire(PipelineState target) {
        store.desired().save(new DesiredState(PIPELINE, target, REV));
        driver.reconcile();
    }

    private void assertActualState(PipelineState expected, long epoch) {
        CheckpointDoc doc = store.state().read(PIPELINE).orElseThrow();
        assertThat(doc.stateJson()).isEqualTo(StateJson.of(expected));
        assertThat(doc.epoch()).as("the fencing epoch advances once per converged transition").isEqualTo(epoch);
    }

    private void awaitKeys(String... keys) {
        Set<String> expected = Set.of(keys);
        awaitCondition(() -> RecordingSink.keys().containsAll(expected),
                () -> "timed out waiting for keys " + expected + " at the sink, have " + RecordingSink.keys());
    }

    /**
     * Reconciles (republishing the pipeline's latest observation from the live job and the store) until the
     * saved observation satisfies {@code done}, then returns it. A reconcile is what refreshes the observation
     * on the driver's tick, so this drives that same tick until the awaited metric values land.
     */
    private Observation awaitObservation(Predicate<Observation> done) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        Observation last = null;
        while (System.nanoTime() < deadline) {
            driver.reconcile();
            last = store.observations().read(PIPELINE).orElse(null);
            if (last != null && done.test(last)) {
                return last;
            }
            sleep(50);
        }
        throw new AssertionError("observation did not satisfy the condition within budget; last was "
                + (last == null ? "absent" : last.metrics() + " / positions " + last.positions()));
    }

    private static void awaitStatus(Job job, JobStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = job.getStatus();
            if (last == expected) {
                return;
            }
            sleep(25);
        }
        throw new AssertionError("job did not reach " + expected + " within budget; last status was " + last);
    }

    private static void awaitCondition(java.util.function.BooleanSupplier done, java.util.function.Supplier<String> onTimeout) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(onTimeout.get());
            }
            sleep(50);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted awaiting a lifecycle condition", e);
        }
    }

    private static Envelope read(int id) {
        return Envelope.read(id, TABLE, Map.of("id", (long) id, "amount", "v" + id), Map.of());
    }

    private static Envelope insert(int id) {
        return Envelope.insert(id, TABLE, Map.of("id", (long) id, "amount", "v" + id), Map.of());
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
     * A sink that records into a JVM-static store keyed on the resolved target's primary-key columns, so the
     * embedded run can assert which rows the built DAG delivered. Shared with the test thread on one member.
     */
    private static final class RecordingSink implements SinkWriter {

        private static final Set<String> KEYS = ConcurrentHashMap.newKeySet();
        private static final AtomicInteger TOTAL = new AtomicInteger();

        private final List<String> keyColumns;

        RecordingSink(TargetTable target) {
            this.keyColumns = target.fields().stream()
                    .filter(TargetField::primaryKey)
                    .map(TargetField::name)
                    .toList();
        }

        static void reset() {
            KEYS.clear();
            TOTAL.set(0);
        }

        static Set<String> keys() {
            return Set.copyOf(KEYS);
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope record : records) {
                Map<String, Object> row = record.after() != null ? record.after() : record.before();
                String key = keyColumns.stream().map(column -> String.valueOf(row.get(column)))
                        .collect(java.util.stream.Collectors.joining("|"));
                KEYS.add(key);
                TOTAL.incrementAndGet();
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
