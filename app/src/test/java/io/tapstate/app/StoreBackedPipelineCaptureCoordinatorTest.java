package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.common.TapstateException;
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
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.core.model.FromRef;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.StorePort;
import java.time.Instant;
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
                "pipe-1", settings, source, SourceCaptureResolution.of(source), true);

        assertThat(spec.pipelineId()).isEqualTo("pipe-1");
        assertThat(spec.sourceId()).isEqualTo("orders_src");
        assertThat(spec.config().connectorId()).isEqualTo("mysql");
        assertThat(spec.config().streams()).containsExactly("orders");
        assertThat(spec.readMode()).isEqualTo(ReadMode.CDC_ONLY);
        assertThat(spec.srsKey()).isNull();
        assertThat(spec.srsEnabled()).as("the switch this pipeline recorded is carried through").isTrue();
        assertThat(spec.startFrom()).isEqualTo(io.tapstate.runtime.srs.StartFrom.earliest());
        assertThat(spec.schemaVer()).isZero();
    }

    /**
     * The config a run is driven with names the node it is driven for. This is the only place both halves
     * exist: a source resolves without knowing any pipeline, and a pipeline is what starts each of its
     * sources — so a spec that carried the pair beside the config while the config itself named nothing
     * would leave the connector, which is handed only the config, with nowhere to keep what it records
     * for itself. The two ids on the spec are not the witness for this; the one on the config is.
     */
    @Test
    void theConfigTheRunIsDrivenWithNamesTheNodeItIsDrivenFor() {
        SourceResource source = cdcSource("orders_src", "orders", null);

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", null, source, SourceCaptureResolution.of(source), true);

        assertThat(spec.config().node()).isEqualTo(new PipelineNode("pipe-1", "orders_src"));
    }

    /**
     * Two pipelines reading one database still mine it once. The node is what separates their notes, and
     * the chain is what merges their reading — deriving one from the other in either direction breaks the
     * half that was not being thought about, and neither break shows up in the rows.
     */
    @Test
    void twoPipelinesReadingOneSourceKeepSeparateNodesAndStillShareOneChain() {
        SourceResource source = cdcSource("orders_src", "orders", null);

        CaptureRunSpec one = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", null, source, SourceCaptureResolution.of(source), true);
        CaptureRunSpec other = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-2", null, source, SourceCaptureResolution.of(source), true);

        assertThat(one.config().node()).isNotEqualTo(other.config().node());
        assertThat(MiningChainId.of(one.config()))
                .as("the chain both pipelines mine is the source's, not either pipeline's")
                .isEqualTo(MiningChainId.of(other.config()));
    }

    /**
     * The run spec carries no position of any kind, and that absence is the point.
     *
     * <p>A seam and a per-change position are the source's own, learned from it as the read happens. When
     * this layer supplied them instead, both were invented here: a fixed seam token, and a generator that
     * began again at its first value on every run. The second is what silently rewound the durable offset
     * — a restarted run's invented positions start over while its ring generation rises, so every ordering
     * check reads the rewind as an advance.
     *
     * <p>Asserted structurally, over the record's components, so that putting either back is a red test
     * rather than a thing a reader has to notice.
     */
    @Test
    void carriesNoPositionOfItsOwnBecausePositionsAreTheSourcesToState() {
        SourceResource source = cdcSource("orders_src", "orders", null);
        Settings settings = new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest");

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", settings, source, SourceCaptureResolution.of(source), true);

        assertThat(spec.getClass().getRecordComponents())
                .extracting(java.lang.reflect.RecordComponent::getType)
                .as("no component of the run spec is a source position, nor a supplier of one")
                .doesNotContain(SourcePosition.class, java.util.function.Supplier.class);
    }

    @Test
    void aPipelineWhoseSwitchIsOffDerivesTheDirectTailAndAnExplicitKeyIsStillCarried() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")),
                new Srs("shared-key", null, null, null, false), null);

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", null, source, SourceCaptureResolution.of(source), false);

        assertThat(spec.srsEnabled()).as("the off switch is honoured").isFalse();
        assertThat(spec.srsKey()).as("the key is the chain's identity, not the switch, so it is carried either way")
                .isEqualTo("shared-key");
        assertThat(spec.readMode()).as("null settings default the read mode").isEqualTo(ReadMode.SNAPSHOT_AND_CDC);
        assertThat(spec.startFrom()).as("null settings default the start position to latest")
                .isEqualTo(io.tapstate.runtime.srs.StartFrom.latest());
    }

    /**
     * A pipeline that names no start position begins where its published contract says it does.
     *
     * <p>The setting documents its own default as {@code latest} and the canonical form encodes that same
     * reading by dropping an explicit {@code latest}, so a run that filled in {@code earliest} instead told
     * an author one thing and did the opposite: "only what is written from now on" against "replay every
     * change still retained". Nothing read the filled-in value until a tail that reads its source directly
     * began resolving it, at which point the disagreement became a first run that re-reads the whole
     * retention window.
     *
     * <p>The third case is what makes the other two mean anything: an implementation that answers
     * {@code latest} to everything satisfies both defaults and still throws away what an author wrote.
     */
    @Test
    void aPipelineThatNamesNoStartPositionBeginsWhereItsContractSaysItDoes() {
        SourceResource source = cdcSource("orders_src", "orders", null);

        CaptureRunSpec noSettings = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", null, source, SourceCaptureResolution.of(source), true);
        CaptureRunSpec settingsWithoutOne = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", new Settings(null, null, null, null, ReadMode.CDC_ONLY, null),
                source, SourceCaptureResolution.of(source), true);
        CaptureRunSpec authorAskedForEarliest = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"),
                source, SourceCaptureResolution.of(source), true);

        assertThat(noSettings.startFrom()).as("no settings at all")
                .isEqualTo(io.tapstate.runtime.srs.StartFrom.latest());
        assertThat(settingsWithoutOne.startFrom()).as("settings that name every other field but this one")
                .isEqualTo(io.tapstate.runtime.srs.StartFrom.latest());
        assertThat(authorAskedForEarliest.startFrom()).as("what the author wrote still wins")
                .isEqualTo(io.tapstate.runtime.srs.StartFrom.earliest());
    }

    /**
     * A hand-written source position is refused whether or not this pipeline buffers, rather than passed
     * through to the connector.
     *
     * <p>Handing one through was the drafted behaviour for the unbuffered path, and there is no channel for
     * it: a recorded position is the connector own offset object serialized, not text a person writes, and
     * the plugin interface offers no way to build an offset from a string. It could not be documented
     * either -- the offset type differs per connector and per connector configuration, so no shape could be
     * named for an author to write. Asking for an exact position is served instead by reading one back and
     * writing it again, which refuses out loud when it cannot be honoured; a start setting yields silently
     * to an already-recorded position, so the same ask would have gone unanswered with nothing said.
     *
     * <p>Both paths are asserted because only their conjunction discriminates: an implementation that
     * forwards the value once buffering is off stays green on the buffered case, which is the half a
     * single-case test would have picked.
     */
    @Test
    void aHandWrittenSourcePositionIsRefusedOnBothPathsRatherThanPassedThrough() {
        String binlogCoordinate = """
                {"file":"mysql-bin.000003","pos":154}""";
        Settings settings = new Settings(null, null, null, null, ReadMode.CDC_ONLY, binlogCoordinate);
        SourceResource buffered = cdcSource("orders_src", "orders", null);
        SourceResource direct = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")),
                new Srs(null, null, null, null, false), null);

        assertThat(buffered.srsEnabled()).as("the two sources really do take different paths").isTrue();
        assertThat(direct.srsEnabled()).isFalse();

        for (boolean srsEnabled : List.of(true, false)) {
            SourceResource source = srsEnabled ? buffered : direct;
            // The description goes on the call, not after it: a .as() chained onto the returned assertion
            // never reaches the "no throwable was raised" failure, which is precisely the failure this
            // case exists to produce -- and without it the report cannot say which of the two paths broke.
            assertThatThrownBy(() -> StoreBackedPipelineCaptureCoordinator.deriveSpec(
                            "pipe-1", settings, source, SourceCaptureResolution.of(source), srsEnabled),
                    "srs enabled: %s", srsEnabled)
                    .isInstanceOf(TapstateException.class)
                    .satisfies(e -> {
                        TapstateException refused = (TapstateException) e;
                        assertThat(refused.code().code()).isEqualTo("capture.start-from-unparsable");
                        assertThat(refused.args()).containsEntry("value", binlogCoordinate);
                    });
        }
    }


    /**
     * The whole point of moving the switch: the source says buffered, this pipeline recorded direct, and
     * the run it derives is direct. Reading the source here -- even as a fallback -- puts back the
     * coupling where editing one source re-routed every pipeline reading it.
     */
    @Test
    void thePipelinesOwnSwitchDecidesEvenWhereTheSourceDisagrees() {
        SourceResource buffered = cdcSource("orders_src", "orders", null);
        assertThat(buffered.srsEnabled()).as("the source itself says buffered").isTrue();

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", null, buffered, SourceCaptureResolution.of(buffered), false);

        assertThat(spec.srsEnabled())
                .as("the pipeline recorded off, so it reads direct however the source is configured")
                .isFalse();
    }

    /** And the mirror, so neither direction is satisfied by an implementation that answers a constant. */
    @Test
    void thePipelinesOwnSwitchDecidesInTheOtherDirectionToo() {
        SourceResource direct = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")),
                new Srs(null, null, null, null, false), null);
        assertThat(direct.srsEnabled()).as("the source itself says direct").isFalse();

        CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", null, direct, SourceCaptureResolution.of(direct), true);

        assertThat(spec.srsEnabled()).isTrue();
    }

    /**
     * A stored pipeline carries a recorded switch on every reference -- apply puts one there. One that
     * does not has never been through apply, so starting it is refused loudly rather than run on a guess.
     *
     * <p>Guessing is the failure worth preventing: the pipeline would come up, report healthy, and read
     * through the other path, which is the same silent shape this line exists to close. The refusal names
     * both halves, so a reader is told which reference is missing it rather than that something is.
     */
    @Test
    void aReferenceWithNoRecordedSwitchIsRefusedRatherThanRunOnAGuess() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(new PipelineResource("p", null, List.of(SourceRef.bare("orders_src")), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(new SyncElement("sync_1", "orders_src", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        CaptureStarter starter = (spec, passthrough) -> {
            throw new AssertionError("capture must not start: no switch was ever recorded");
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        assertThatThrownBy(() -> coordinator.startCapture("p"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'p'")
                .hasMessageContaining("'orders_src'");
    }

    // ---- what the sources handed over ------------------------------------------------------------

    /**
     * Drives rows into a run's account the way a connector does — through the one seam every capture path
     * is started with — so what is asserted below is the coordinator's arithmetic and not a second way of
     * counting invented for the test.
     */
    private static void handOver(CaptureHealth health, Envelope... events) {
        health.recording((batch, position) -> { }).onBatch(List.of(events), Optional.empty());
    }

    private static CaptureHealth healthStrictlyAfter(CaptureHealth earlier) {
        Instant open = earlier.countingSince();
        CaptureHealth later = new CaptureHealth();
        while (!later.countingSince().isAfter(open)) {
            later = new CaptureHealth();
        }
        return later;
    }

    @Test
    void everySourcesArrivalsAreAddedUpForTheOnePipeline() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(cdcSource("items_src", "items", null));
        artifacts.save(twoSourcePipeline("p", "orders_src", "items_src"));
        CaptureHealth orders = new CaptureHealth();
        CaptureHealth items = new CaptureHealth();
        handOver(orders, Envelope.insert(1L, "orders", Map.of("id", 1), Map.of()),
                Envelope.update(2L, "orders", Map.of("id", 1), Map.of("id", 1), Map.of()));
        handOver(items, Envelope.delete(3L, "items", Map.of("id", 9), Map.of()));
        StoreBackedPipelineCaptureCoordinator coordinator =
                coordinatorOver(artifacts, List.of(orders, items));

        coordinator.startCapture("p");

        // A pipeline reads through one run per source and each counts its own tables, so what a reader
        // asked "how much is this pipeline taking in" wants is the runs added together.
        assertThat(coordinator.capturedRows("p").rowsByTableAndOp()).containsOnly(
                entry("orders", Map.of("i", 1L, "u", 1L)), entry("items", Map.of("d", 1L)));
    }

    @Test
    void twoSourcesNamingOneTableAreAddedRatherThanOneWinning() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(cdcSource("archive_src", "orders", null));
        artifacts.save(twoSourcePipeline("p", "orders_src", "archive_src"));
        CaptureHealth live = new CaptureHealth();
        CaptureHealth archive = new CaptureHealth();
        handOver(live, Envelope.insert(1L, "orders", Map.of("id", 1), Map.of()));
        handOver(archive, Envelope.insert(2L, "orders", Map.of("id", 2), Map.of()));
        StoreBackedPipelineCaptureCoordinator coordinator =
                coordinatorOver(artifacts, List.of(live, archive));

        coordinator.startCapture("p");

        // Both arrivals are rows this pipeline read. Keyed by table alone the two runs collide, and the
        // shape that loses one of them reads as a source that has gone half quiet.
        assertThat(coordinator.capturedRows("p").rowsByTableAndOp())
                .containsExactly(entry("orders", Map.of("i", 2L)));
        // And what they weighed is added the same way, which is a separate decision in a separate line:
        // the two runs' payloads collide on this table exactly as their counts do, and overwriting
        // instead of adding leaves a figure that is neither run's and looks like either.
        assertThat(coordinator.capturedRows("p").bytesByTable())
                .containsExactly(entry("orders", 20L));
    }

    @Test
    void theLatestStartAmongTheRunsIsTheOneReported() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(cdcSource("items_src", "items", null));
        artifacts.save(twoSourcePipeline("p", "orders_src", "items_src"));
        CaptureHealth first = new CaptureHealth();
        CaptureHealth second = healthStrictlyAfter(first);
        StoreBackedPipelineCaptureCoordinator coordinator =
                coordinatorOver(artifacts, List.of(first, second));

        coordinator.startCapture("p");

        // The witness the assertion below needs: two accounts opened at genuinely different moments, or
        // earliest and latest would be the same value and this would hold either way.
        assertThat(second.countingSince()).isAfter(first.countingSince());
        // A run that is replaced resets its own count, so the sum falls; moving this instant forward with
        // it is what makes the fall read as the restart it is rather than as a counter going backwards.
        assertThat(coordinator.capturedRows("p").countingSince()).isEqualTo(second.countingSince());
    }

    @Test
    void aPipelineWithNoCaptureRunningReportsNothingRatherThanZero() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src"));
        StoreBackedPipelineCaptureCoordinator coordinator =
                coordinatorOver(artifacts, List.of(new CaptureHealth()));

        CaptureReading before = coordinator.capturedRows("p");

        // Absent, not zero, and it has to be absent before the start as well as after a stop: a pipeline
        // nobody is capturing and one capturing nothing want opposite responses.
        assertThat(before).isEqualTo(CaptureReading.NONE);
        assertThat(before.start()).isEmpty();

        coordinator.startCapture("p");
        assertThat(coordinator.capturedRows("p").start()).isPresent();

        coordinator.stopCapture("p", true);
        assertThat(coordinator.capturedRows("p")).isEqualTo(CaptureReading.NONE);
    }

    /** A coordinator whose runs hand back the given accounts, one per source, in declaration order. */
    private static StoreBackedPipelineCaptureCoordinator coordinatorOver(
            InMemoryArtifactStore artifacts, List<CaptureHealth> healths) {
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        java.util.Iterator<CaptureHealth> next = healths.iterator();
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(
                    spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                    Optional.of(() -> { }), next.next());
        };
        return new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, srsCoordinator, new SnapshotBuffer());
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

        coordinator.stopCapture("p", true);

        assertThat(subscriptionClosed).as("stop closes the capture subscription, stopping the daemon").isTrue();
        assertThat(srsCoordinator.isProvisioned(chainId)).as("stop tears the source chain down").isFalse();
        assertThat(coordinator.isActive("p")).as("stop drops the handle").isFalse();
    }

    @Test
    void aStopAskedToKeepLeavesTheCursorExactlyWhereItIs() {
        CursorFixture fixture = new CursorFixture();
        fixture.coordinator.startCapture("p");
        fixture.leaveACursorFor("p");

        fixture.coordinator.stopCapture("p", false);

        // Its pair above is what makes this an assertion. Both stops close the run and give the chain
        // back; only the record says which one was asked to throw the position away, and that position
        // is the entire thing a later resume reads.
        assertThat(fixture.consumersOnTheChain()).containsExactly("p");
    }

    /** One pipeline over one source, with a durable cursor on the chain it reads. */
    private static final class CursorFixture {

        private final InMemoryStorePort store;
        private final SrsCoordinator srsCoordinator;
        private final StoreBackedPipelineCaptureCoordinator coordinator;
        private final SourceResource source = cdcSource("orders_src", "orders", null);

        CursorFixture() {
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            artifacts.save(source);
            artifacts.save(pipeline("p", "orders_src"));
            store = new InMemoryStorePort(artifacts);
            srsCoordinator = new SrsCoordinator(store.meta());
            coordinator = new StoreBackedPipelineCaptureCoordinator(
                    store, this::start, srsCoordinator, new SnapshotBuffer());
        }

        private CaptureRun start(CaptureRunSpec spec, java.util.function.Consumer<Envelope> passthrough) {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(), Optional.of(() -> {
            }), new CaptureHealth());
        }

        /** Writes the durable cursor a run leaves behind, which is what a purge has to take away. */
        void leaveACursorFor(String pipelineId) {
            store.meta().upsertConsumerOffset(chainId(), new ConsumerOffset(pipelineId, Map.of(), null));
        }

        List<String> consumersOnTheChain() {
            return store.meta().read(chainId()).orElseThrow().consumerOffsets().stream()
                    .map(ConsumerOffset::pipelineId)
                    .toList();
        }

        private String chainId() {
            return MiningChainId.resolve(SourceCaptureResolution.of(source).config(), null).value();
        }
    }

    @Test
    void startRoutesSnapshotRowsToTheBufferUnderThePipelineAndSourcesRingName() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipeline("p", "orders_src"));
        StorePort store = artifactsOnly(artifacts);

        SnapshotBuffer buffer = new SnapshotBuffer();
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        // A fake starter drains two snapshot rows to the pass-through, exactly as the real snapshot phase does,
        // so the routing under test -- pass-through to the buffer keyed by the consumer pipeline and source's
        // ring name -- is exercised without a Jet member.
        CaptureStarter starter = (spec, passthrough) -> {
            passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 1L), Map.of()));
            passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 2L), Map.of()));
            return new CaptureRun(Optional.empty(), false, 2L, Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, buffer);

        coordinator.startCapture("p");

        // The snapshot rows land in the buffer under the pipeline and ring the source resolves to -- the same
        // coordinates the source vertex drains member-side, which routes its own snapshot through the transform
        // chain ahead of cdc.
        String ringName = SourceCaptureResolution.of(source).ringName();
        assertThat(buffer.drain("p", ringName)).extracting(e -> e.after().get("id")).containsExactly(1L, 2L);
    }

    @Test
    void stopReleasesRowsTheCancelledJobDidNotDrainFromThePipelineBuffer() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipeline("p", "orders_src"));
        SnapshotBuffer buffer = new SnapshotBuffer();
        AtomicBoolean captureClosed = new AtomicBoolean();
        CaptureStarter starter = (spec, passthrough) -> {
            passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 1L), Map.of()));
            return new CaptureRun(Optional.empty(), false, 1L, Optional.empty(),
                    Optional.of(() -> captureClosed.set(true)), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()), buffer);
        coordinator.startCapture("p");

        coordinator.stopCapture("p", false);

        assertThat(captureClosed).as("the producer stopped before its hand-off was released").isTrue();
        assertThat(buffer.drain("p", SourceCaptureResolution.of(source).ringName())).isEmpty();
    }

    @Test
    void stopKeepsTheBufferReachableWhenCaptureRefusesToStop() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipeline("p", "orders_src"));
        SnapshotBuffer buffer = new SnapshotBuffer();
        Envelope row = Envelope.read(1L, "orders", Map.of("id", 1L), Map.of());
        CaptureStarter starter = (spec, passthrough) -> {
            passthrough.accept(row);
            return new CaptureRun(Optional.empty(), false, 1L, Optional.empty(),
                    Optional.of(() -> { throw new IllegalStateException("capture did not stop"); }),
                    new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()), buffer);
        coordinator.startCapture("p");

        assertThatThrownBy(() -> coordinator.stopCapture("p", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("capture did not stop");

        assertThat(buffer.drain("p", SourceCaptureResolution.of(source).ringName()))
                .as("a producer that may still append keeps its hand-off attached")
                .containsExactly(row);
    }

    @Test
    void multiTableSnapshotProgressAndBufferRoutingStayPerTable() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = new SourceResource("multi_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders"), TableRef.literal("customers")), null, null);
        artifacts.save(source);
        artifacts.save(pipelineWithReadMode("p", "multi_src", ReadMode.SNAPSHOT_AND_CDC));
        SnapshotBuffer buffer = new SnapshotBuffer();
        CaptureStarter starter = (spec, passthrough) -> {
            passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 1L), Map.of()));
            passthrough.accept(Envelope.read(2L, "customers", Map.of("id", 2L), Map.of()));
            return new CaptureRun(Optional.empty(), false, 2L, Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()), buffer);

        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p").byTable()).containsOnly(
                entry("orders", new TableSnapshot(1L, null, null)),
                entry("customers", new TableSnapshot(1L, null, null)));
        SourceCaptureResolution resolution = SourceCaptureResolution.of(source);
        assertThat(buffer.drain("p", resolution.ringName("orders")))
                .extracting(e -> e.src()).containsExactly("orders");
        assertThat(buffer.drain("p", resolution.ringName("customers")))
                .extracting(e -> e.src()).containsExactly("customers");
    }

    @Test
    void rejects_a_snapshot_row_from_a_table_outside_the_source_selection() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = new SourceResource("selected_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null);
        artifacts.save(source);
        artifacts.save(pipelineWithReadMode("p", "selected_src", ReadMode.SNAPSHOT_ONLY));
        CaptureStarter starter = (spec, passthrough) -> {
            passthrough.accept(Envelope.read(1L, "customers", Map.of("id", 1L), Map.of()));
            return new CaptureRun(Optional.empty(), false, 1L, Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        assertThatThrownBy(() -> coordinator.startCapture("p"))
                .isInstanceOfSatisfying(TapstateException.class, exception -> {
                    assertThat(exception.code().code()).isEqualTo("capture.event-table-not-selected");
                    assertThat(exception.args()).containsEntry("table", "customers");
                });
        assertThat(coordinator.isActive("p")).isFalse();
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
        // load rather than a live position in it. This source was never discovered, so nothing counted its
        // table and the total stays null with the percentage -- progress with no total is honest partial
        // data, never a faked 100%.
        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(500L, null, null)));
        // A total without what it accumulates from cannot be read, so the load says when it opened.
        assertThat(coordinator.snapshotProgress("p").start()).isPresent();
    }

    @Test
    void theLoadCarriesTheRowCountTheLastDiscoveryTookOfTheTable() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        InMemoryStorePort store = discoveredAs(artifacts, "orders", 120_000L);
        StoreBackedPipelineCaptureCoordinator coordinator = coordinatorLoading(store, 90_000L);

        coordinator.startCapture("p");

        // About how many rows there are comes from the source's own count, taken at discovery -- the one
        // place anybody counted. It is an estimate and stays one: nothing maintains it afterwards, and the
        // percentage below is derived from it rather than measured.
        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(90_000L, 120_000L, 75)));
    }

    @Test
    void aTableNobodyCountedLeavesBothTheTotalAndThePercentageAbsent() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        // The same shape as the case above, discovered by a connector that cannot count: the control group
        // for it. Without this pair, a total wired to a constant would pass the case above unnoticed.
        InMemoryStorePort store = discoveredAs(artifacts, "orders", null);
        StoreBackedPipelineCaptureCoordinator coordinator = coordinatorLoading(store, 90_000L);

        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(90_000L, null, null)));
    }

    @Test
    void aLoadThatReadPastAStaleEstimateReportsCompleteRatherThanOverFull() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        // The table grew between the discovery that counted it and the load that read it -- ordinary, since
        // nothing maintains that count. The load is finished by the time this is asked, so it is complete;
        // a progress figure above full is not a state anything can be in.
        InMemoryStorePort store = discoveredAs(artifacts, "orders", 1_000L);
        StoreBackedPipelineCaptureCoordinator coordinator = coordinatorLoading(store, 1_500L);

        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(1_500L, 1_000L, 100)));
    }

    @Test
    void aTableCountedAsEmptyKeepsItsTotalAndHasNoPercentage() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        // Counted as empty and then read for rows: the count is stale, and no share of nothing describes
        // that. The zero is kept, because it is what discovery actually counted -- dropping it would say
        // nobody counted, which is a different and also real state.
        InMemoryStorePort store = discoveredAs(artifacts, "orders", 0L);
        StoreBackedPipelineCaptureCoordinator coordinator = coordinatorLoading(store, 7L);

        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(7L, 0L, null)));
    }

    @Test
    void theLoadIsCountedFromBeforeItsFirstSourceRanRatherThanFromWhenItFinished() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        AtomicReference<Instant> whenTheSourceRan = new AtomicReference<>();
        CaptureStarter starter = (spec, passthrough) -> {
            whenTheSourceRan.set(Instant.now());
            // Held until the clock has actually moved on. Both candidate moments are a few microseconds
            // apart otherwise, and an assertion between them would pass or fail by whether the clock
            // happened to tick -- which is a flake, not a witness.
            Instant moved = whenTheSourceRan.get().plusMillis(5);
            while (Instant.now().isBefore(moved)) {
                Thread.onSpinWait();
            }
            return new CaptureRun(
                    Optional.empty(), false, 3L, Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        coordinator.startCapture("p");

        // The rows are counted from when the load opened, not from when it returned. Stamped afterwards,
        // the whole load would be dated to the moment it ended -- and a rate taken across the first two
        // scrapes of the counter would divide by a window that had already closed.
        assertThat(coordinator.snapshotProgress("p").start()).isPresent();
        assertThat(coordinator.snapshotProgress("p").start().orElseThrow())
                .isBeforeOrEqualTo(whenTheSourceRan.get());
    }

    /** A store whose schema layer holds one discovered table, counted at {@code rows} or not at all. */
    private static InMemoryStorePort discoveredAs(InMemoryArtifactStore artifacts, String table, Long rows) {
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.schemaStore().save(new DiscoveredSourceModel("orders_src", "mysql", 1L,
                new SourceModel(List.of(new SourceTable(table, List.of(), List.of(), List.of(), rows)))));
        return store;
    }

    /** A coordinator whose one source run reports {@code loaded} rows from its bounded read. */
    private static StoreBackedPipelineCaptureCoordinator coordinatorLoading(StorePort store, long loaded) {
        CaptureStarter starter = (spec, passthrough) -> new CaptureRun(
                Optional.empty(), false, loaded, Optional.empty(), Optional.empty(), new CaptureHealth());
        return new StoreBackedPipelineCaptureCoordinator(
                store, starter, new SrsCoordinator(new InMemorySrsMetaStore()), new SnapshotBuffer());
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

        assertThat(coordinator.snapshotProgress("p").byTable()).isEmpty();
        // No load ran, so there is no moment to have counted from either. A start with no rows would say a
        // load ran and read nothing, which is a different and real state.
        assertThat(coordinator.snapshotProgress("p").start()).isEmpty();
    }

    @Test
    void snapshotProgressOfTwoSourcesReadingASameNamedTableQualifiesBothByTheirSourceId() {
        // Two different databases can both name a table "orders": a normal multi-source shape, not an
        // error. A bare table-name key would have the second source's count silently overwrite the
        // first's and attribute it to the wrong source; each entry must stay individually addressable.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("src_a", "orders", null));
        artifacts.save(cdcSource("src_b", "orders", null));
        artifacts.save(new PipelineResource("p", null, List.of(SourceRef.spec("src_a", true), SourceRef.spec("src_b", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("src_a"),
                        List.of(new SyncElement("sync_1", "src_a", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        java.util.Map<String, Long> countsBySource = Map.of("src_a", 100L, "src_b", 200L);
        CaptureStarter starter = (spec, passthrough) -> new CaptureRun(Optional.empty(), false,
                countsBySource.get(spec.sourceId()), Optional.empty(), Optional.empty(), new CaptureHealth());
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p").byTable()).containsOnly(
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
        assertThat(coordinator.snapshotProgress("never-started").byTable()).isEmpty();
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

        coordinator.stopCapture("never-started", true);

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

    @Test
    void startFailureClosesRunsStartedForEarlierSources() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource first = cdcSource("src_a", "orders", null);
        SourceResource second = new SourceResource("src_b", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null);
        artifacts.save(first);
        artifacts.save(second);
        artifacts.save(twoSourcePipeline("p", "src_a", "src_b"));
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        AtomicBoolean firstSubscriptionClosed = new AtomicBoolean(false);
        AtomicReference<CaptureRunSpec> firstSpec = new AtomicReference<>();
        CaptureStarter starter = (spec, passthrough) -> {
            firstSpec.set(spec);
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                    Optional.of(() -> firstSubscriptionClosed.set(true)), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, srsCoordinator, new SnapshotBuffer());

        assertThatThrownBy(() -> coordinator.startCapture("p"))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);

        MiningChainId firstChain = MiningChainId.resolve(firstSpec.get().config(), firstSpec.get().srsKey());
        assertThat(firstSubscriptionClosed).isTrue();
        assertThat(srsCoordinator.isProvisioned(firstChain)).isFalse();
        assertThat(coordinator.isActive("p")).isFalse();
    }

    @Test
    void startFailurePreservesTheOriginalErrorWhenRunCleanupFails() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("src_a", "orders", null));
        artifacts.save(cdcSource("src_b", "customers", null));
        artifacts.save(new SourceResource("src_c", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null));
        artifacts.save(new PipelineResource("p", null, List.of(SourceRef.spec("src_a", true), SourceRef.spec("src_b", true), SourceRef.spec("src_c", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("src_a"),
                        List.of(new SyncElement("sync_1", "src_a", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        AtomicBoolean secondClosed = new AtomicBoolean(false);
        AtomicReference<CaptureRunSpec> firstSpec = new AtomicReference<>();
        int[] starts = {0};
        CaptureStarter starter = (spec, passthrough) -> {
            starts[0]++;
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            if (starts[0] == 1) {
                firstSpec.set(spec);
                return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                        Optional.of(() -> { throw new IllegalStateException("close failed"); }), new CaptureHealth());
            }
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                    Optional.of(() -> secondClosed.set(true)), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, srsCoordinator, new SnapshotBuffer());

        assertThatThrownBy(() -> coordinator.startCapture("p"))
                .isInstanceOfSatisfying(TapstateException.class, exception -> {
                    assertThat(exception.code().code()).isEqualTo("actuation.source-schema-not-discovered");
                    assertThat(exception.getSuppressed()).hasSize(1);
                });
        assertThat(secondClosed).isTrue();
        assertThat(srsCoordinator.isProvisioned(MiningChainId.resolve(
                firstSpec.get().config(), firstSpec.get().srsKey()))).isFalse();
    }

    /**
     * What a resume has to know is whether the load reached the target, and this is the face that answers it.
     *
     * <p>The read side cannot: a bounded read drains in one blocking pass before the job carrying its rows is
     * submitted, so every table's read has returned before anyone can hold the pipeline, while almost none of
     * what it read has been written. This case puts the two in exactly that state -- every table present on
     * the read face, not one of them confirmed on the record -- because that is the state a hold lands in, and
     * an implementation reading the wrong one of the two answers "delivered" throughout it.
     */
    @Test
    void aLoadIsDeliveredOnlyOnceTheRecordShowsEveryTableWrittenNotOnceItsReadReturned() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders"), TableRef.literal("customers")), null, null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        AtomicReference<MiningChainId> chain = new AtomicReference<>();
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            chain.set(chainId);
            return new CaptureRun(Optional.of(chainId), false, 2L, Map.of("orders", 1L, "customers", 1L),
                    Optional.empty(), Optional.of(() -> {
                    }), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, new SnapshotBuffer());

        coordinator.startCapture("p");

        // Both reads have returned -- the read face has an entry for every table ...
        assertThat(coordinator.snapshotProgress("p").byTable()).containsOnlyKeys("orders", "customers");
        // ... and the target has confirmed none of it, which is where a hold part way through a load lands.
        assertThat(coordinator.loadDelivered("p")).as("read but not written is not delivered").isFalse();

        store.meta().markSnapshotComplete(chain.get().value(), "p", "orders");
        assertThat(coordinator.loadDelivered("p")).as("one table of two is not the load").isFalse();

        store.meta().markSnapshotComplete(chain.get().value(), "p", "customers");
        assertThat(coordinator.loadDelivered("p")).as("every table confirmed").isTrue();
    }

    /**
     * Another pipeline finishing the same table says nothing about this one: the mark is written against the
     * pipeline because each writes to a target of its own. Read at the chain level, a pipeline new to the
     * chain would inherit the first one's answer and be resumed over a load it never did.
     */
    @Test
    void aTableAnotherPipelineFinishedIsNotThisPipelinesLoad() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        AtomicReference<MiningChainId> chain = new AtomicReference<>();
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            chain.set(chainId);
            return new CaptureRun(Optional.of(chainId), false, 1L, Map.of("orders", 1L),
                    Optional.empty(), Optional.of(() -> {
                    }), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, new SnapshotBuffer());

        coordinator.startCapture("p");
        store.meta().markSnapshotComplete(chain.get().value(), "another-pipeline", "orders");

        assertThat(coordinator.loadDelivered("p")).isFalse();
    }

    /**
     * A read mode with no load has nothing that could be undelivered, and neither has a pipeline this
     * coordinator is running nothing for. Both answer true, which is what keeps a resume on the engine-only
     * path everywhere the question does not arise -- the overwhelming majority of them.
     */
    @Test
    void aPipelineWithNoLoadAndOneThatIsNotRunningBothReportDelivered() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src")); // pipeline() defaults to ReadMode.CDC_ONLY
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(), Optional.of(() -> {
            }), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, new SnapshotBuffer());

        coordinator.startCapture("p");

        assertThat(coordinator.loadDelivered("p")).as("cdc_only runs no load").isTrue();
        assertThat(coordinator.loadDelivered("never-started")).as("nothing running has no load").isTrue();
    }

    /**
     * A stop takes the question away with the run. What is kept here is which tables to ask about and on
     * which chain -- both belong to the run being torn down, and a stopped pipeline that kept them would
     * answer for a run that is over.
     */
    @Test
    void aStoppedPipelineReportsDeliveredRatherThanAnsweringForTheRunItNoLongerHas() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator srsCoordinator = new SrsCoordinator(store.meta());
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            return new CaptureRun(Optional.of(chainId), false, 1L, Map.of("orders", 1L),
                    Optional.empty(), Optional.of(() -> {
                    }), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, starter, srsCoordinator, new SnapshotBuffer());

        coordinator.startCapture("p");
        assertThat(coordinator.loadDelivered("p")).isFalse();

        coordinator.stopCapture("p", false);

        assertThat(coordinator.loadDelivered("p")).isTrue();
    }

    // ---- fixtures --------------------------------------------------------------------------------

    private static SourceResource cdcSource(String id, String table, String srsKey) {
        Srs srs = srsKey == null ? null : new Srs(srsKey, null, null, null, null);
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), srs, null);
    }

    private static PipelineResource pipeline(String id, String sourceId) {
        return pipelineWithReadMode(id, sourceId, ReadMode.CDC_ONLY);
    }

    private static PipelineResource pipelineWithReadMode(String id, String sourceId, ReadMode readMode) {
        return new PipelineResource(id, null, List.of(SourceRef.spec(sourceId, true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal(sourceId),
                        List.of(new SyncElement("sync_1", sourceId, null, null, null)), null, null),
                new Settings(null, null, null, null, readMode, "earliest"), null);
    }

    private static PipelineResource twoSourcePipeline(String id, String sourceA, String sourceB) {
        return new PipelineResource(id, null, List.of(SourceRef.spec(sourceA, true), SourceRef.spec(sourceB, true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal(sourceA),
                        List.of(new SyncElement("sync_1", sourceA, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
    }

    private static StorePort artifactsOnly(InMemoryArtifactStore artifacts) {
        return new InMemoryStorePort(artifacts);
    }
}
