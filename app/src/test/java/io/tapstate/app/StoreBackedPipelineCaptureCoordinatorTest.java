package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.core.model.FromRef;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.StorePort;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The app-side capture coordinator: how it derives a source run spec from a stored source and the pipeline
 * settings (including the L1 mock collaborators that stand in for real connector machinery), and how it holds
 * the live capture handles it starts so a stop can tear them down. Spec derivation is a pure function tested
 * directly; the handle lifecycle is driven over an in-memory store and a fake capture starter, so it needs no
 * running Jet member.
 */
class StoreBackedPipelineCaptureCoordinatorTest {

    // ---- spec derivation -------------------------------------------------------------------------

    @Test
    void derivesTheRunSpecFromTheSourceAndPipelineSettings() {
        SourceResource source = cdcSource("orders_src", "orders", null);
        Settings settings = new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest");

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", settings, source, SourceCaptureResolution.of(source));

        assertThat(spec.pipelineId()).isEqualTo("pipe-1");
        assertThat(spec.sourceId()).isEqualTo("orders_src");
        assertThat(spec.config().connectorId()).isEqualTo("mysql");
        assertThat(spec.config().streams()).containsExactly("orders");
        assertThat(spec.readMode()).isEqualTo(ReadMode.CDC_ONLY);
        assertThat(spec.srsKey()).isNull();
        assertThat(spec.srsEnabled()).as("srs defaults on when the source declares no srs block").isTrue();
        assertThat(spec.startFrom()).isEqualTo(io.tapstate.runtime.srs.StartFrom.earliest());
        assertThat(spec.schemaVer()).isZero();
    }

    @Test
    void theL1MockWatermarkIsMonotonicAndItsOrderIsNumericNotLexical() {
        SourceResource source = cdcSource("orders_src", "orders", null);
        Settings settings = new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest");

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", settings, source, SourceCaptureResolution.of(source));

        // The mock watermark is a monotonic source-position generator. What ranks the tokens it hands out
        // is not carried here at all: the order is the ring's own, assigned on append, so the spec has
        // nothing to say about it - and nothing that could disagree with what the sink ranks by.
        assertThat(spec.watermark().get()).isEqualTo(new SourcePosition("w1"));
        assertThat(spec.watermark().get()).isEqualTo(new SourcePosition("w2"));
        assertThat(spec.cdcStart()).isNotNull();
    }

    @Test
    void aSourceWithSrsDisabledDerivesTheDirectTailAndAnExplicitKeyIsCarried() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null,
                new Srs("shared-key", null, null, null, false), null);

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", null, source, SourceCaptureResolution.of(source));

        assertThat(spec.srsEnabled()).as("srs.enabled:false is honoured").isFalse();
        assertThat(spec.srsKey()).isEqualTo("shared-key");
        assertThat(spec.readMode()).as("null settings default the read mode").isEqualTo(ReadMode.SNAPSHOT_AND_CDC);
        assertThat(spec.startFrom()).as("null settings default the start position to earliest")
                .isEqualTo(io.tapstate.runtime.srs.StartFrom.earliest());
    }

    // ---- handle lifecycle ------------------------------------------------------------------------

    @Test
    void startRetainsALiveHandleAndStopClosesItThenTearsTheChainDown() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src"));
        StorePort store = artifactsOnly(artifacts);

        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        AtomicBoolean subscriptionClosed = new AtomicBoolean(false);
        AtomicReference<CaptureRunSpec> startedSpec = new AtomicReference<>();
        // A fake capture starter mirrors what the real run unit does to the coordinator -- provision the chain
        // and attach the consumer -- and hands back a run whose subscription records that it was closed.
        CaptureStarter starter = (spec, passthrough) -> {
            startedSpec.set(spec);
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            Subscription subscription = () -> subscriptionClosed.set(true);
            return new CaptureRun(
                    Optional.of(chainId), false, 0L, Optional.empty(), Optional.of(subscription), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, new SnapshotBuffer());

        coordinator.startCapture("p");

        MiningChainId chainId = MiningChainId.resolve(startedSpec.get().config(), startedSpec.get().srsKey());
        assertThat(coordinator.isActive("p")).as("start retains a live handle for the pipeline").isTrue();
        assertThat(srsCoordinator.isProvisioned(chainId)).isTrue();

        coordinator.stopCapture("p");

        assertThat(subscriptionClosed).as("stop closes the capture subscription, stopping the daemon").isTrue();
        assertThat(srsCoordinator.isProvisioned(chainId)).as("stop tears the source chain down").isFalse();
        assertThat(coordinator.isActive("p")).as("stop drops the handle").isFalse();
    }

    @Test
    void startRoutesSnapshotRowsToTheBufferUnderTheSourcesRingName() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipeline("p", "orders_src"));
        StorePort store = artifactsOnly(artifacts);

        SnapshotBuffer buffer = new SnapshotBuffer();
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        // A fake starter drains two snapshot rows to the pass-through, exactly as the real snapshot phase does,
        // so the routing under test -- pass-through to the buffer keyed by the source's ring name -- is exercised
        // without a Jet member.
        CaptureStarter starter = (spec, passthrough) -> {
            passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 1L), Map.of()));
            passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 2L), Map.of()));
            return new CaptureRun(Optional.empty(), false, 2L, Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, buffer);

        coordinator.startCapture("p");

        // The snapshot rows land in the buffer under the ring the source resolves to -- the same ring the source
        // vertex drains member-side, which is what routes the snapshot through the transform chain ahead of cdc.
        String ringName = SourceCaptureResolution.of(source).ringName();
        assertThat(buffer.drain(ringName)).extracting(e -> e.after().get("id")).containsExactly(1L, 2L);
    }

    @Test
    void snapshotProgressReportsTheRowsEachTableLoaded() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        // A read mode that actually runs the snapshot phase -- cdc_only (the pipeline() fixture's default)
        // never does, so a table it names must not report a snapshot at all (see the cdc_only test below).
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        CaptureStarter starter = (spec, passthrough) -> new CaptureRun(
                Optional.empty(), false, 500L, Optional.empty(), Optional.empty(), new CaptureHealth());
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        coordinator.startCapture("p");

        // The rows a table loaded are known only once its bounded read has drained, so this is the finished
        // load rather than a live position in it. The total is not reported by any source, so it stays null
        // and the percentage with it -- progress with no total is honest partial data, never a faked 100%.
        assertThat(coordinator.snapshotProgress("p"))
                .containsOnly(entry("orders", new TableSnapshot(500L, null, null)));
    }

    @Test
    void snapshotProgressOfACdcOnlyPipelineReportsNothingRatherThanAFabricatedZero() {
        // cdc_only never runs the bounded snapshot phase (CapturePlan.forReadMode), so its table has no
        // snapshot to report at all -- not a real "0 rows" entry, which would tell a reader the table was
        // read and found empty when in fact its snapshot phase never ran.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src")); // pipeline() defaults to ReadMode.CDC_ONLY
        CaptureStarter starter = (spec, passthrough) -> new CaptureRun(
                Optional.empty(), false, 0L, Optional.empty(), Optional.empty(), new CaptureHealth());
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p")).isEmpty();
    }

    @Test
    void snapshotProgressOfTwoSourcesReadingASameNamedTableQualifiesBothByTheirSourceId() {
        // Two different databases can both name a table "orders": a normal multi-source shape, not an
        // error. A bare table-name key would have the second source's count silently overwrite the
        // first's and attribute it to the wrong source; each entry must stay individually addressable.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("src_a", "orders", null));
        artifacts.save(cdcSource("src_b", "orders", null));
        artifacts.save(new PipelineResource("p", null, List.of("src_a", "src_b"), null, null,
                new ServeBlock.Inline(null, FromRef.literal("src_a"),
                        List.of(new SyncElement("sync_1", "src_a", null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        java.util.Map<String, Long> countsBySource = Map.of("src_a", 100L, "src_b", 200L);
        CaptureStarter starter = (spec, passthrough) -> new CaptureRun(Optional.empty(), false,
                countsBySource.get(spec.sourceId()), Optional.empty(), Optional.empty(), new CaptureHealth());
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p")).containsOnly(
                entry("src_a.orders", new TableSnapshot(100L, null, null)),
                entry("src_b.orders", new TableSnapshot(200L, null, null)));
    }

    @Test
    void snapshotProgressIsEmptyForAPipelineWhoseCaptureIsNotRunning() {
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(new InMemoryArtifactStore()),
                (spec, passthrough) -> {
                    throw new AssertionError("no capture should be started");
                },
                new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        // Empty is a reading: nothing has been loaded because nothing is running, which the read face
        // publishes as an unavailable snapshot rather than a table that loaded zero rows.
        assertThat(coordinator.snapshotProgress("never-started")).isEmpty();
    }

    @Test
    void stopIsANoOpForAPipelineThatWasNeverStarted() {
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(new InMemoryArtifactStore()),
                (spec, passthrough) -> {
                    throw new AssertionError("no capture should be started");
                },
                new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        coordinator.stopCapture("never-started");

        assertThat(coordinator.isActive("never-started")).isFalse();
    }

    @Test
    void captureFailureSurfacesADeadTailOfAStartedPipeline() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src"));
        StorePort store = artifactsOnly(artifacts);
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());

        // The run the starter hands back carries the health its cdc tail reports failures on; the tail dies after
        // the run started, exactly as a real stream failing on its daemon thread would.
        CaptureHealth health = new CaptureHealth();
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            return new CaptureRun(
                    Optional.of(chainId), false, 0L, Optional.empty(), Optional.of(() -> {
            }), health);
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, new SnapshotBuffer());
        coordinator.startCapture("p");

        assertThat(coordinator.captureFailure("p")).as("healthy while the tail is alive").isEmpty();
        RuntimeException boom = new RuntimeException("cdc tail died");
        health.fail(boom);

        assertThat(coordinator.captureFailure("p")).as("surfaces the tail's death").contains(boom);
    }

    @Test
    void captureFailureIsEmptyForAPipelineThatWasNeverStarted() {
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(new InMemoryArtifactStore()),
                (spec, passthrough) -> {
                    throw new AssertionError("no capture should be started");
                },
                new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        assertThat(coordinator.captureFailure("never-started")).isEmpty();
    }

    @Test
    void captureFailureSurfacesAFailedRunPastAHealthyEarlierRun() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("src_a", "orders", null));
        artifacts.save(cdcSource("src_b", "customers", null));
        artifacts.save(twoSourcePipeline("p", "src_a", "src_b"));
        StorePort store = artifactsOnly(artifacts);
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());

        // One run per source, in source order; the second source's tail dies while the first stays healthy.
        CaptureHealth healthA = new CaptureHealth();
        CaptureHealth healthB = new CaptureHealth();
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            CaptureHealth health = spec.sourceId().equals("src_b") ? healthB : healthA;
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(), Optional.of(() -> {
            }), health);
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, new SnapshotBuffer());
        coordinator.startCapture("p");

        RuntimeException boom = new RuntimeException("the second source's tail died");
        healthB.fail(boom);

        // captureFailure must return the first PRESENT failure, skipping the healthy earlier run -- not merely
        // the first run's failure, which would mask a later run's dead tail while an earlier one is still alive.
        assertThat(coordinator.captureFailure("p"))
                .as("surfaces the failed run past the healthy earlier one")
                .contains(boom);
    }

    // ---- fixtures --------------------------------------------------------------------------------

    private static SourceResource cdcSource(String id, String table, String srsKey) {
        Srs srs = srsKey == null ? null : new Srs(srsKey, null, null, null, null);
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, srs, null);
    }

    private static PipelineResource pipeline(String id, String sourceId) {
        return pipelineWithReadMode(id, sourceId, ReadMode.CDC_ONLY);
    }

    private static PipelineResource pipelineWithReadMode(String id, String sourceId, ReadMode readMode) {
        return new PipelineResource(id, null, List.of(sourceId), null, null,
                new ServeBlock.Inline(null, FromRef.literal(sourceId),
                        List.of(new SyncElement("sync_1", sourceId, null, null, null, null)), null, null),
                new Settings(null, null, null, null, readMode, "earliest"), null);
    }

    private static PipelineResource twoSourcePipeline(String id, String sourceA, String sourceB) {
        return new PipelineResource(id, null, List.of(sourceA, sourceB), null, null,
                new ServeBlock.Inline(null, FromRef.literal(sourceA),
                        List.of(new SyncElement("sync_1", sourceA, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
    }

    private static StorePort artifactsOnly(InMemoryArtifactStore artifacts) {
        return new InMemoryStorePort(artifacts);
    }
}
