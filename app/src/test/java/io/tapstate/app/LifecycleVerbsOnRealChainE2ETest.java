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
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.SourceRef;
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
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    @Test
    void slowSnapshotLeavesAnotherCdcPipelinesConvergenceAndObservationCadenceLive() throws Exception {
        store = seedPipelineAndSchema();
        addFastCdcPipeline();
        makeMemberCapable(store);
        CountDownLatch slowRead = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("streaming start must not materialize a batch");
            }
            @Override public void streamSnapshot(CaptureConfig config, SnapshotListener listener) {
                listener.seam(Optional.of(new SourcePosition("seam-0")));
                listener.row(read(1));
                slowRead.countDown();
                try {
                    if (!releaseSlow.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("slow snapshot was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                listener.row(read(2));
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                if (config.streams().contains("fast_orders")) {
                    listener.onBatch(List.of(Envelope.insert(100L, "fast_orders",
                                    Map.of("id", 100L, "amount", "fast"), Map.of())),
                            Optional.of(new SourcePosition("fast-100")));
                }
                return () -> { };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        };
        wireConvergeChain(source, true);
        try {
            store.desired().save(new DesiredState(PIPELINE, RUNNING, REV));
            store.desired().save(new DesiredState("fast-pipe", RUNNING, REV));
            driver.reconcile();
            assertThat(slowRead.await(10, TimeUnit.SECONDS)).isTrue();
            awaitKeys("100");
            Observation first = store.observations().read("fast-pipe").orElseThrow();
            assertThat(first.state()).isEqualTo(RUNNING);
            assertThat(releaseSlow.getCount()).isEqualTo(1);

            Thread.sleep(10);
            driver.reconcile();
            Observation next = store.observations().read("fast-pipe").orElseThrow();
            assertThat(next.observedAt()).isAfter(first.observedAt());
            assertThat(releaseSlow.getCount()).isEqualTo(1);
        } finally {
            releaseSlow.countDown();
        }
    }

    @Test
    void streamingSnapshotAdvancesRowsDoneBeforeCompletionAndKeepsCdcBehindIt() throws Exception {
        store = seedPipelineAndSchema();
        makeMemberCapable(store);
        CountDownLatch firstRead = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("streaming start must not materialize a batch");
            }
            @Override public void streamSnapshot(CaptureConfig config, SnapshotListener listener) {
                listener.seam(Optional.of(new SourcePosition("seam-0")));
                listener.row(read(1));
                firstRead.countDown();
                try {
                    if (!releaseRead.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("snapshot read was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                listener.row(read(2));
                listener.row(read(3));
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                listener.onBatch(List.of(insert(4)), Optional.of(new SourcePosition("src-4")));
                return () -> { };
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        };
        wireConvergeChain(source, true);
        try {
            desire(RUNNING);
            assertThat(firstRead.await(10, TimeUnit.SECONDS)).isTrue();
            awaitKeys("1");
            awaitCondition(() -> captureCoordinator.snapshotProgress(PIPELINE)
                            .byTable().get(TABLE).rowsDone() == 1L,
                    () -> "live snapshot rowsDone did not reach one: "
                            + captureCoordinator.snapshotProgress(PIPELINE));
            driver.reconcile();
            Observation inFlight = store.observations().read(PIPELINE).orElseThrow();
            assertThat(inFlight.snapshot().get(TABLE).rowsDone()).isEqualTo(1L);
            assertThat(inFlight.observedAt()).isNotNull();
            Thread.sleep(100);
            assertThat(RecordingSink.keys()).contains("1").doesNotContain("2", "3", "4");

            releaseRead.countDown();
            awaitKeys("1", "2", "3", "4");
            awaitCondition(() -> captureCoordinator.snapshotProgress(PIPELINE)
                            .byTable().get(TABLE).rowsDone() == 3L,
                    () -> "live snapshot rowsDone did not reach three: "
                            + captureCoordinator.snapshotProgress(PIPELINE));
        } finally {
            releaseRead.countDown();
        }
    }

    @Test
    void stoppingAMidReadSnapshotCancelsItsWorkerAndReleasesItsSession() throws Exception {
        store = seedPipelineAndSchema();
        makeMemberCapable(store);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("streaming start must not materialize a batch");
            }
            @Override public void streamSnapshot(CaptureConfig config, SnapshotListener listener) {
                try {
                    listener.seam(Optional.of(new SourcePosition("seam-0")));
                    listener.row(read(1));
                    reading.countDown();
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException stopped) {
                    Thread.currentThread().interrupt();
                } finally {
                    exited.countDown();
                }
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                throw new AssertionError("cancelled snapshot must not start CDC");
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        };
        wireConvergeChain(source, true);
        SnapshotBuffer buffer = (SnapshotBuffer) member.getUserContext().get(SnapshotBuffer.USER_CONTEXT_KEY);
        SourceResource artifact = (SourceResource) store.artifacts().get(SOURCE_ID).orElseThrow();
        String ringName = SourceCaptureResolution.of(artifact).ringName(TABLE);
        try {
            desire(RUNNING);
            assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(buffer.hasSnapshot(PIPELINE, ringName)).isTrue();

            desire(STOPPED);

            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(buffer.hasSnapshot(PIPELINE, ringName)).isFalse();
            assertThat(captureCoordinator.snapshotProgress(PIPELINE)).isEqualTo(SnapshotReading.NONE);
            assertActualState(STOPPED, 2L);
        } finally {
            release.countDown();
        }
    }

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
    private PipelineCaptureCoordinator captureCoordinator;
    private io.tapstate.runtime.srs.SnapshotWorkers snapshotWorkers;

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
        if (snapshotWorkers != null) {
            snapshotWorkers.close();
        }
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
        assertReadFaceReports(PIPELINE, RUNNING);
        // The assembled path stamps when the reading was taken. Without it a run whose publisher stopped
        // and a run whose state has not changed are the same bytes -- and nothing else in this case would
        // notice, because every assertion here holds just as well over a reading from four minutes ago.
        assertThat(readFaces.status(PIPELINE).observedAt())
                .as("the converge pass records when it observed the pipeline")
                .isNotNull();

        // pause: the live job suspends and the read face reports PAUSED.
        desire(PAUSED);
        awaitStatus(job, JobStatus.SUSPENDED);
        assertActualState(PAUSED, 2);
        assertReadFaceReports(PIPELINE, PAUSED);

        // resume: the suspended job runs again and the read face reports RUNNING.
        desire(RUNNING);
        awaitStatus(job, JobStatus.RUNNING);
        assertActualState(RUNNING, 3);
        assertReadFaceReports(PIPELINE, RUNNING);

        // stop: the job is cancelled (Jet reports a cancelled job as FAILED) and the read face reports STOPPED.
        desire(STOPPED);
        awaitStatus(job, JobStatus.FAILED);
        assertActualState(STOPPED, 4);
        assertReadFaceReports(PIPELINE, STOPPED);
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
                .as("recordCount is the number of records the live job drove to its serve sink; a run that"
                        + " failed nothing publishes no failure metric")
                .containsEntry("recordCount", 6L)
                .doesNotContainKey("errorCount");
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

    private void wireConvergeChain(CapturePort source) {
        wireConvergeChain(source, false);
    }

    private void wireConvergeChain(CapturePort source, boolean streamingSnapshot) {
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        SnapshotBuffer buffer = (SnapshotBuffer) member.getUserContext().get(SnapshotBuffer.USER_CONTEXT_KEY);
        snapshotWorkers = streamingSnapshot ? new io.tapstate.runtime.srs.SnapshotWorkers(1, 1) : null;
        CaptureRunUnit captureRunUnit = streamingSnapshot
                ? new CaptureRunUnit(source, srsCoordinator, store.meta(), member, buffer, snapshotWorkers)
                : new CaptureRunUnit(source, srsCoordinator, store.meta(), member);
        PipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, captureRunUnit::start, srsCoordinator, buffer);
        captureCoordinator = coordinator;
        StoreBackedDagSource.SinkWriterBinder recordingSink =
                (connectorId, settings, writeMode, ddl, target, node) -> (SupplierEx<SinkWriter>) () -> new RecordingSink(target);
        DagSource dagSource = streamingSnapshot
                ? new StoreBackedDagSource(store, recordingSink, buffer)
                : new StoreBackedDagSource(store, recordingSink);
        Engine engine = new Engine(member);
        EngineLifecycleActuator actuator = TestEngineLifecycleActuators.create(
                engine, dagSource, coordinator, new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()));

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
                List.of(TableRef.literal(TABLE)), null, null));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null));
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(SOURCE_ID, true)),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        InMemoryStorePort seeded = new InMemoryStorePort(artifacts);
        seeded.schemas().save(new DiscoveredSourceModel(SOURCE_ID, "fake", 0L, new SourceModel(List.of(
                new SourceTable(TABLE,
                        List.of(new SourceField("id", "INT"), new SourceField("amount", "STRING")),
                        List.of("id"),
                        List.of())))));
        return seeded;
    }

    private void addFastCdcPipeline() {
        store.artifacts().save(new SourceResource("fast_src", null, "fake", Map.of("host", "fast"),
                SourceMode.CDC, List.of(TableRef.literal("fast_orders")), null, null));
        store.artifacts().save(new PipelineResource("fast-pipe", null,
                List.of(SourceRef.spec("fast_src", true)), List.of(), null,
                new ServeBlock.Inline(null, FromRef.literal("fast_src"),
                        List.of(new SyncElement("fast_sync", DEST_ID, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        store.schemas().save(new DiscoveredSourceModel("fast_src", "fake", 0L, new SourceModel(List.of(
                new SourceTable("fast_orders",
                        List.of(new SourceField("id", "INT"), new SourceField("amount", "STRING")),
                        List.of("id"), List.of())))));
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
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            for (Envelope change : changes) {
                listener.onBatch(java.util.List.of(change), java.util.Optional.of(new SourcePosition("src-" + change.ts())));
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
        public Optional<SourcePosition> seam() {
            // Sampled by the source before its first row. The run refuses to start a tail without one,
            // because a tail that begins wherever it likes loses every change made while the snapshot ran.
            return Optional.of(new SourcePosition("seam-0"));
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

    /**
     * The state the read face reports, compared on the fields this case is about.
     *
     * <p>Not whole-record equality: the observation now carries when it was taken, so a record built here
     * to compare against would have to invent a time, and would fail on whatever time it invented. What
     * this case is about is the state the converger reached and the absence of a failure over it -- and
     * that the reading is stamped at all is asserted once, separately, where it means something.
     */
    private void assertReadFaceReports(String pipelineId, PipelineState expected) {
        assertThat(readFaces.status(pipelineId))
                .returns(pipelineId, PipelineStatus::pipelineId)
                .returns(expected, PipelineStatus::state)
                .returns(null, PipelineStatus::failure);
    }

}
