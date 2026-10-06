package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SnapshotPhase;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.core.model.FromRef;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.StorePort;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

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
     * The run spec carries no connector position, and that absence is the point.
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
    void everySourceOnOneChainCarriesThePipelinesCompleteTableSelectionAndCursorToken() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(cdcSource("items_src", "items", null));
        artifacts.save(twoSourcePipeline("p", "orders_src", "items_src"));
        List<CaptureRunSpec> started = new ArrayList<>();
        CaptureStarter starter = (spec, passthrough) -> {
            started.add(spec);
            return new CaptureRun(Optional.empty(), false, 0L,
                    Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                new InMemoryStorePort(artifacts), starter,
                new SrsCoordinator(new InMemorySrsMetaStore()), new SnapshotBuffer());

        coordinator.startCapture("p");

        assertThat(started).hasSize(2);
        assertThat(started).extracting(CaptureRunSpec::consumerId).doesNotHaveDuplicates();
        assertThat(started.getFirst().snapshotWriterToken()).isNotBlank()
                .isEqualTo(started.getLast().snapshotWriterToken());
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(
                    spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                    Optional.of(() -> { }), next.next());
        };
        return new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, srsCoordinator, new SnapshotBuffer());
    }

    // ---- handle lifecycle ------------------------------------------------------------------------

    @Test
    void fifteenPipelinesResumingOnePostgresBacklogOpenOneSharedCdcTail() {
        List<String> tableNames = java.util.stream.IntStream.range(0, 27)
                .mapToObj(index -> "table_" + index).toList();
        SourceResource source = new SourceResource("shared_postgres", null, "postgres",
                Map.of("host", "postgres"), SourceMode.CDC,
                tableNames.stream().<TableRef>map(TableRef::literal).toList(), null, null);
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source);
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        String chainId = SourceCaptureResolution.of(source).chainId().value();
        store.meta().create(chainId, null);
        long snapshotEpoch = store.meta().openEpoch(chainId);
        ChainPosition baseline = new ChainPosition(SourceOrder.snapshotRow(snapshotEpoch), "before-backlog");
        store.meta().advanceCaptureCheckpoint(chainId, baseline);
        Map<String, Long> initialReads = new java.util.LinkedHashMap<>();
        Map<String, ChainPosition> confirmedSnapshots = new java.util.LinkedHashMap<>();
        tableNames.forEach(table -> {
            initialReads.put(table, -1L);
            confirmedSnapshots.put(table, baseline);
        });
        for (int index = 0; index < 15; index++) {
            String pipelineId = "pipeline_" + index;
            artifacts.save(pipelineWithReadMode(pipelineId, source.id(), ReadMode.SNAPSHOT_AND_CDC));
            String consumerId = SrsConsumerId.of(pipelineId, source.id()).value();
            store.meta().upsertConsumerOffset(chainId, new ConsumerOffset(consumerId, initialReads, baseline,
                    tableNames, "before-backlog", snapshotEpoch, confirmedSnapshots, ConsumerProgressKind.SRS));
            for (String table : tableNames) {
                store.meta().startRingAfter(chainId, consumerId, table, -1L);
            }
        }

        AtomicInteger liveTails = new AtomicInteger();
        List<CaptureStart> starts = new ArrayList<>();
        CapturePort port = new CapturePort() {
            @Override
            public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("completed snapshots must not run again");
            }

            @Override
            public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                assertThat(config.streams()).containsExactlyElementsOf(tableNames);
                starts.add(start);
                liveTails.incrementAndGet();
                return liveTails::decrementAndGet;
            }

            @Override
            public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        };
        HazelcastInstance member = mock(HazelcastInstance.class);
        var context = new java.util.concurrent.ConcurrentHashMap<String, Object>();
        context.put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, store.srsLog());
        when(member.getUserContext()).thenReturn(context);
        var config = new com.hazelcast.config.Config();
        config.addRingBufferConfig(new com.hazelcast.config.RingbufferConfig("srs.*")
                .setInMemoryFormat(com.hazelcast.config.InMemoryFormat.OBJECT)
                .setRingbufferStoreConfig(new com.hazelcast.config.RingbufferStoreConfig()
                        .setEnabled(true)
                        .setFactoryImplementation(new io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory(
                                store.srsLog()))));
        when(member.getConfig()).thenReturn(config);
        var ring = mock(com.hazelcast.ringbuffer.Ringbuffer.class);
        when(ring.tailSequence()).thenReturn(-1L);
        when(member.getRingbuffer(any())).thenReturn(ring);
        SrsCoordinator chains = new SrsCoordinator(store.meta());
        CaptureRunUnit runs = new CaptureRunUnit(port, chains, store.meta(), member);
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, runs::begin, chains, new SnapshotBuffer());

        for (int index = 0; index < 15; index++) {
            coordinator.startCapture("pipeline_" + index);
        }

        assertThat(starts).as("at least one tail resumes the recorded backlog").isNotEmpty()
                .allSatisfy(start -> assertThat(start)
                        .isEqualTo(CaptureStart.resume(new SourcePosition("before-backlog"))));
        assertThat(liveTails.get())
                .as("15 pipelines over one source must not decode the same WAL backlog 15 times at once")
                .isEqualTo(1);
        var retained = store.meta().read(chainId).orElseThrow();
        coordinator.close();
        assertThat(liveTails.get()).as("all local attachments end the single actual shared reader").isZero();
        assertThat(store.meta().read(chainId)).contains(retained);
        coordinator.close();
        assertThat(liveTails.get()).as("shared-reader shutdown is idempotent").isZero();
    }

    @Test
    void aFailedOrdinaryCloseRetriesTheSameActualHandleAndKeepsTheSourceState() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator chains = new SrsCoordinator(store.meta());
        AtomicInteger closes = new AtomicInteger();
        AtomicBoolean actuallyEnded = new AtomicBoolean();
        AtomicReference<CaptureRunSpec> captured = new AtomicReference<>();
        TapstateException original = new TapstateException(io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled first close refusal"), null);
        CaptureStarter starter = (spec, handoff) -> {
            captured.set(spec);
            chains.provisionSource(spec.sourceId(), spec.miningChainId(), spec.config().streams(), spec.retention());
            chains.attachConsumer(spec.miningChainId(), spec.consumerId());
            Subscription sameHandle = () -> {
                if (closes.incrementAndGet() == 1) { throw original; }
                actuallyEnded.set(true);
            };
            return new CaptureRun(Optional.of(spec.miningChainId()), false, 0L, Optional.empty(),
                    Optional.of(sameHandle), new CaptureHealth());
        };
        var coordinator = new StoreBackedPipelineCaptureCoordinator(store, starter, chains, new SnapshotBuffer());
        coordinator.startCapture("p");
        String chain = captured.get().miningChainId().value();
        store.meta().upsertConsumerOffset(chain, new ConsumerOffset(captured.get().consumerId(), Map.of(), null));
        var retained = store.meta().read(chain).orElseThrow();

        assertThatThrownBy(() -> coordinator.stopCapture("p", false)).isSameAs(original);
        assertThat(actuallyEnded).isFalse();
        coordinator.stopCapture("p", false);

        assertThat(closes).as("the retry must reach the same actual retained subscription").hasValue(2);
        assertThat(actuallyEnded).isTrue();
        assertThat(coordinator.isActive("p")).isFalse();
        assertThat(store.meta().read(chain)).contains(retained);
    }

    @Test
    void coordinatorShutdownClosesItsOwnedSubscriptionAndKeepsDurableState() {
        CursorFixture fixture = new CursorFixture();
        fixture.coordinator.startCapture("p");
        fixture.leaveACursorFor("p");
        var desired = new io.tapstate.core.lifecycle.DesiredState("p",
                io.tapstate.core.lifecycle.PipelineState.RUNNING, "controlled-revision");
        fixture.store.desired().save(desired);
        var retained = fixture.store.meta().read(fixture.chainId()).orElseThrow();
        assertThat(fixture.coordinator.isActive("p")).isTrue();
        assertThat(fixture.subscriptionsClosed).hasValue(0);

        fixture.coordinator.close();

        assertThat(fixture.subscriptionsClosed).as("shutdown closes the actual retained CaptureRun subscription").hasValue(1);
        assertThat(fixture.coordinator.isActive("p")).isFalse();
        assertThat(fixture.coordinator.snapshotProgress("p").byTable()).isEmpty();
        assertThat(fixture.store.meta().read(fixture.chainId())).contains(retained);
        assertThat(fixture.store.desired().read("p")).contains(desired);
        fixture.coordinator.close();
        assertThat(fixture.subscriptionsClosed).as("the same captured handle is closed once").hasValue(1);
    }

    @Test
    void coordinatorShutdownRefusesANewStartWithoutOpeningAnotherSubscription() {
        CursorFixture fixture = new CursorFixture();
        fixture.coordinator.startCapture("p");
        fixture.coordinator.close();
        assertThatThrownBy(() -> fixture.coordinator.startCapture("p"))
                .isInstanceOf(CancellationException.class);
        assertThat(fixture.subscriptionsClosed).hasValue(1);
        assertThat(fixture.coordinator.isActive("p")).isFalse();
    }

    @Test
    void coordinatorShutdownCancelsAPendingStartAndClosesTheHandleItReturnsLate() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicInteger closed = new AtomicInteger();
        CaptureStarter starter = (spec, passthrough) -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("the controlled source open was not released"); }
            } catch (InterruptedException cancelled) { interrupted.set(true); Thread.currentThread().interrupt(); }
            return new CaptureRun(Optional.empty(), false, 1L, Optional.empty(),
                    Optional.of(closed::incrementAndGet), new CaptureHealth());
        };
        var coordinator = new StoreBackedPipelineCaptureCoordinator(artifactsOnly(artifacts), starter,
                new SrsCoordinator(new InMemorySrsMetaStore()), new SnapshotBuffer());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> start = workers.submit(() -> coordinator.startCapture("p"));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<?> close = workers.submit(coordinator::close);
            close.get(5, TimeUnit.SECONDS);
            assertThat(interrupted).as("shutdown cancels the already-entered source open").isTrue();
            assertThatThrownBy(() -> start.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(CancellationException.class);
            assertThat(closed).as("a late returned handle cannot be published live").hasValue(1);
            assertThat(coordinator.isActive("p")).isFalse();
        } finally {
            release.countDown(); workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void coordinatorShutdownRetainsTheOriginalFailureAndStillClosesAnotherLocalCapture() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("failed_src", "orders", "failed-chain"));
        artifacts.save(cdcSource("healthy_src", "customers", "healthy-chain"));
        artifacts.save(pipeline("a_failed", "failed_src"));
        artifacts.save(pipeline("b_healthy", "healthy_src"));
        SrsCoordinator chains = new SrsCoordinator(new InMemorySrsMetaStore());
        AtomicInteger failedCloses = new AtomicInteger(), healthyCloses = new AtomicInteger();
        TapstateException original = new TapstateException(io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled subscription shutdown refusal"), null);
        CaptureStarter starter = (spec, passthrough) -> {
            chains.provisionSource(spec.sourceId(), spec.miningChainId(), spec.config().streams(), spec.retention());
            chains.attachConsumer(spec.miningChainId(), spec.consumerId());
            Subscription subscription = () -> {
                if (spec.pipelineId().equals("a_failed")) { failedCloses.incrementAndGet(); throw original; }
                healthyCloses.incrementAndGet();
            };
            return new CaptureRun(Optional.of(spec.miningChainId()), false, 0L, Optional.empty(),
                    Optional.of(subscription), new CaptureHealth());
        };
        var coordinator = new StoreBackedPipelineCaptureCoordinator(artifactsOnly(artifacts), starter, chains, new SnapshotBuffer());
        coordinator.startCapture("a_failed"); coordinator.startCapture("b_healthy");

        assertThatThrownBy(coordinator::close).isSameAs(original);

        assertThat(failedCloses).hasValue(1);
        assertThat(healthyCloses).as("one refusal cannot strand another owned local handle").hasValue(1);
        assertThat(coordinator.isActive("b_healthy")).isFalse();
    }

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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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
        private final AtomicInteger subscriptionsClosed = new AtomicInteger();
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                    Optional.of(subscriptionsClosed::incrementAndGet), new CaptureHealth());
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
    void aPipelineCapturesOnlyTheTableItsGraphReads() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource("crm", null, "postgres", Map.of("host", "postgres"),
                SourceMode.CDC, List.of(TableRef.literal("account"), TableRef.literal("contact"),
                        TableRef.literal("pricebook2")), null, null));
        artifacts.save(new PipelineResource("pricebook_state", null, List.of(SourceRef.spec("crm", true)),
                List.of(Step.inline("t_pricebook2", FromClause.list(FromRef.literal("pricebook2")),
                        new TransformBody.Js("function process(record, ctx) { return record; }"), null)),
                new ViewBlock.Inline("pricebook", FromRef.literal("t_pricebook2"), "id", null), null,
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        AtomicReference<CaptureRunSpec> startedSpec = new AtomicReference<>();
        CaptureStarter starter = (spec, passthrough) -> {
            startedSpec.set(spec);
            return new CaptureRun(Optional.empty(), false, 2L, Optional.empty(), Optional.empty(),
                    new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        coordinator.startCapture("pricebook_state");

        assertThat(startedSpec.get().config().streams()).containsExactly("pricebook2");
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
    void eachSnapshotOnlyRebuildCarriesANewerOrderWithoutOpeningAChain() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_ONLY));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        List<Long> generations = new ArrayList<>();
        CaptureStarter starter = (spec, passthrough) -> {
            generations.add(spec.snapshotEpoch());
            return new CaptureRun(
                    Optional.empty(), false, 1L, Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, starter, new SrsCoordinator(store.meta()), new SnapshotBuffer());

        coordinator.startCapture("p");
        coordinator.stopCapture("p", false);
        coordinator.startCapture("p");

        assertThat(generations).containsExactly(1L, 2L);
    }

    @Test
    void aSnapshotOnlyPlanBindsTheSuppliedWriterTokenToItsRealSession() throws Exception {
        requireSuppliedSnapshotSession(ReadMode.SNAPSHOT_ONLY, true);
    }

    @Test
    void aDirectSnapshotPlanBindsTheSuppliedWriterTokenToItsRealSession() throws Exception {
        requireSuppliedSnapshotSession(ReadMode.SNAPSHOT_AND_CDC, false);
    }

    private void requireSuppliedSnapshotSession(ReadMode mode, boolean srsEnabled) throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(new PipelineResource("p", null, List.of(SourceRef.spec("orders_src", srsEnabled)),
                null, null, new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(new SyncElement("sync_1", "orders_src", null, null, null)), null, null),
                new Settings(null, null, null, null, mode, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator srs = new SrsCoordinator(store.meta());
        SnapshotBuffer buffer = new SnapshotBuffer();
        AtomicReference<CaptureRunSpec> admitted = new AtomicReference<>();
        AtomicReference<CaptureRun> opened = new AtomicReference<>();
        String suppliedToken = java.util.UUID.randomUUID().toString();
        try (var workers = new io.tapstate.runtime.srs.SnapshotWorkers(1, 1)) {
            HeldSource port = new HeldSource(1, new CountDownLatch(0), mode == ReadMode.SNAPSHOT_AND_CDC);
            CaptureRunUnit unit = new CaptureRunUnit(port, srs, store.meta(),
                    mock(HazelcastInstance.class), buffer, workers);
            CaptureStarter starter = (spec, handoff) -> {
                admitted.set(spec);
                CaptureRun run = unit.begin(spec, handoff, true);
                opened.set(run);
                return run;
            };
            StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                    store, starter, srs, buffer);
            String ring = SourceCaptureResolution.of(source).scopedTo("p", srsEnabled).ringName();
            try {
                coordinator.startCapture("p", artifacts, suppliedToken);
                assertThat(buffer.hasSnapshot("p", ring, suppliedToken))
                        .as("the actual deferred buffer session owns the caller-supplied token").isTrue();
                assertThat(admitted.get().snapshotWriterToken()).isEqualTo(suppliedToken);
                assertThat(admitted.get().readMode()).isEqualTo(mode);
                assertThat(admitted.get().srsEnabled()).isEqualTo(srsEnabled);
                coordinator.activateSnapshot("p");
                assertThat(opened.get().awaitLoaded(Duration.ofSeconds(5))).isTrue();
                assertThat(coordinator.captureFailure("p")).as("the real row and completion handoffs remain current").isEmpty();
                var rows = buffer.drainSnapshot("p", ring, suppliedToken, 8);
                assertThat(rows.rows()).hasSize(1);
                assertThat(rows.rows().getFirst().after()).containsEntry("id", 1L);
                assertThat(rows.state()).isEqualTo(SnapshotBuffer.SessionState.DONE);
                assertThat(coordinator.snapshotProgress("p").byTable().get("orders").rowsDone()).isEqualTo(1L);
            } finally {
                coordinator.stopCapture("p", false);
            }
        }
    }

    // ---- a load read while the pipeline runs ---------------------------------------------------------

    /**
     * The start comes back with the load still being read and reports its actual prefix. The hand-off is
     * ended only once every row of the table has arrived. Here
     * the source hands over one row and then holds the rest, as a source does while the pipeline behind it
     * catches up: the start still returns, and it is the only way this pipeline's job could ever be submitted.
     */
    @Test
    void aStreamingLoadReportsLiveRowsAndEndsOnlyAfterItsTableIsThrough() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_ONLY));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        CountDownLatch rest = new CountDownLatch(1);
        HeldSource port = new HeldSource(3, rest);
        CaptureRunUnit unit = new CaptureRunUnit(
                port, new SrsCoordinator(store.meta()), store.meta(), mock(HazelcastInstance.class));
        SnapshotBuffer buffer = new SnapshotBuffer();
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, unit::begin, new SrsCoordinator(store.meta()), buffer);
        String ring = SourceCaptureResolution.of(source).ringName();

        coordinator.startCapture("p");

        try {
            assertThat(buffer.snapshotState("p", ring).declared()).as("declared before the job is assembled").isTrue();
            assertThat(buffer.snapshotState("p", ring).handedOver()).isFalse();
            long prefixDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (coordinator.snapshotProgress("p").byTable().get("orders").rowsDone() < 1) {
                if (System.nanoTime() > prefixDeadline) { throw new AssertionError("the live first row was never reported"); }
                Thread.sleep(20);
            }
            assertThat(coordinator.snapshotProgress("p").byTable())
                    .containsOnly(entry("orders", new TableSnapshot(1L, null, null)));
            assertThat(buffer.snapshotState("p", ring).handedOver()).as("one live row does not complete the load").isFalse();

            rest.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (coordinator.snapshotProgress("p").byTable().get("orders").rowsDone() < 3) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the table was never published once its read was through");
                }
                Thread.sleep(20);
            }

            assertThat(coordinator.snapshotProgress("p").byTable())
                    .containsOnly(entry("orders", new TableSnapshot(3L, null, null)));
            assertThat(buffer.snapshotState("p", ring).handedOver())
                    .as("all rows have arrived but the source vertex has taken none").isFalse();
            assertThat(buffer.drain("p", ring)).hasSize(3);
            while (!buffer.snapshotState("p", ring).handedOver()) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the completed load was not handed over after its rows drained");
                }
                Thread.sleep(20);
            }
            assertThat(buffer.snapshotState("p", ring).handedOver()).isTrue();
        } finally {
            rest.countDown();
            coordinator.stopCapture("p", false);
        }
    }

    /** A stop reaches a load still being read: the read is abandoned and the source let go of. */
    @Test
    void aStopAbandonsALoadStillBeingRead() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_ONLY));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        HeldSource port = new HeldSource(3, new CountDownLatch(1));
        CaptureRunUnit unit = new CaptureRunUnit(
                port, new SrsCoordinator(store.meta()), store.meta(), mock(HazelcastInstance.class));
        SnapshotBuffer buffer = new SnapshotBuffer();
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, unit::begin, new SrsCoordinator(store.meta()), buffer);
        coordinator.startCapture("p");

        coordinator.stopCapture("p", false);

        assertThat(port.closed).as("the read under way was closed").isTrue();
        assertThat(coordinator.snapshotProgress("p")).isEqualTo(SnapshotReading.NONE);
        assertThat(coordinator.captureFailure("p")).as("a stop is not a failure").isEmpty();
        assertThat(buffer.snapshotState("p", SourceCaptureResolution.of(source).ringName()))
                .isEqualTo(SnapshotBuffer.SnapshotState.UNDECLARED);
    }

    /**
     * A starter that reads its load on the calling thread hands its run back with the load over; each of its
     * tables is ended and published all the same, whether or not the starter said so itself.
     */
    @Test
    void aRunHandedBackWithItsLoadOverHasEveryTableEndedAndPublished() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        SnapshotBuffer buffer = new SnapshotBuffer();
        String ring = SourceCaptureResolution.of(source).ringName();
        List<Boolean> declaredAtStart = new ArrayList<>();
        CaptureStarter starter = (spec, handoff) -> {
            declaredAtStart.add(buffer.snapshotState("p", ring).declared());
            handoff.accept(Envelope.read(1L, "orders", Map.of("id", 1L), Map.of()));
            return new CaptureRun(Optional.empty(), false, 1L, Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()), buffer);

        coordinator.startCapture("p");

        assertThat(declaredAtStart).containsExactly(true);
        assertThat(buffer.drain("p", ring)).hasSize(1);
        assertThat(buffer.snapshotState("p", ring).handedOver()).isTrue();
        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(1L, null, null)));
    }

    /**
     * A load that has already failed by the time the start looks at its run is neither ended nor published.
     * What arrived of it is a prefix of the table, and ending its declaration is what lets the source vertex
     * take that prefix for the whole load: it moves on to the ring and promises the load's bound, and the sink
     * records a short table as written. The start is overtaken here on purpose -- it waits for the load to be
     * over before handing the run back -- which is an order a table too small ever to wait for room, followed
     * by a read that fails at once, takes on its own.
     */
    @Test
    void aFailedLoadReportsItsReadPrefixWithoutEndingOrConfirmingTheTable() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = cdcSource("orders_src", "orders", null);
        artifacts.save(source);
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_ONLY));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        IllegalStateException broken = new IllegalStateException("the source broke off after its first row");
        CaptureRunUnit unit = new CaptureRunUnit(new BrokenOffSource(broken), new SrsCoordinator(store.meta()),
                store.meta(), mock(HazelcastInstance.class));
        CaptureStarter overtaken = (spec, handoff) -> {
            CaptureRun run = unit.begin(spec, handoff);
            try {
                assertThat(run.awaitLoaded(Duration.ofSeconds(10))).isTrue();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
            return run;
        };
        SnapshotBuffer buffer = new SnapshotBuffer();
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, overtaken, new SrsCoordinator(store.meta()), buffer);
        String ring = SourceCaptureResolution.of(source).ringName();

        coordinator.startCapture("p");

        assertThat(buffer.drain("p", ring)).as("the row that arrived before the read broke off").hasSize(1);
        assertThat(buffer.snapshotState("p", ring).handedOver())
                .as("a prefix of the table is not the table's load").isFalse();
        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(1L, null, null)));
        assertThat(coordinator.runSnapshotProgress("p").byTable().get("orders").rowsDone()).isEqualTo(1);
        assertThat(coordinator.loadDelivered("p")).as("the failed prefix has no target completion proof").isFalse();
        assertThat(store.keyedState().count(SnapshotLoadCounts.namespaceOf("p")))
                .as("a failed prefix does not install a completed load count").isZero();
        assertThat(coordinator.captureFailure("p")).containsSame(broken);
        coordinator.stopCapture("p", false);
    }

    /** A source whose read hands over one row and then fails with {@code failure}. */
    private static final class BrokenOffSource implements CapturePort {
        private final RuntimeException failure;

        BrokenOffSource(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new CaptureBatch() {
                private boolean handed;

                @Override
                public boolean hasNext() {
                    if (handed) {
                        throw failure;
                    }
                    return true;
                }

                @Override
                public Envelope next() {
                    handed = true;
                    return Envelope.read(1L, "orders", Map.of("id", 1L), Map.of());
                }

                @Override
                public Optional<SourcePosition> seam() {
                    return Optional.of(new SourcePosition("seam-0"));
                }

                @Override
                public void close() {
                    // Nothing is held open.
                }
            };
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            throw new UnsupportedOperationException("a snapshot-only read opens no tail");
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

    /**
     * A source whose read hands over its first row and then holds the rest until {@code rest} opens -- or
     * until it is closed, which is what a stop does to it.
     */
    private static final class HeldSource implements CapturePort {
        private final int rows;
        private final CountDownLatch rest;
        private final boolean permitsTail;
        volatile boolean closed;

        HeldSource(int rows, CountDownLatch rest) {
            this(rows, rest, false);
        }

        HeldSource(int rows, CountDownLatch rest, boolean permitsTail) {
            this.rows = rows;
            this.rest = rest;
            this.permitsTail = permitsTail;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new CaptureBatch() {
                private int next = 1;

                @Override
                public boolean hasNext() {
                    if (next == 2) {
                        awaitRest();
                    }
                    return next <= rows;
                }

                @Override
                public Envelope next() {
                    return Envelope.read(1L, "orders", Map.of("id", (long) next++), Map.of());
                }

                @Override
                public Optional<SourcePosition> seam() {
                    return Optional.of(new SourcePosition("seam-0"));
                }

                @Override
                public void close() {
                    closed = true;
                }
            };
        }

        private void awaitRest() {
            try {
                while (!rest.await(20, TimeUnit.MILLISECONDS)) {
                    if (closed) {
                        throw new CancellationException("closed while holding the rest");
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CancellationException("interrupted while holding the rest");
            }
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            if (permitsTail) {
                assertThat(start).isEqualTo(CaptureStart.resume(new SourcePosition("seam-0")));
                if (listener instanceof io.tapstate.spi.capture.CaptureStartedListener started) {
                    started.onStart(new SourcePosition("seam-0"));
                }
                return () -> { };
            }
            throw new UnsupportedOperationException("a snapshot-only read opens no tail");
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
    void aSlowSnapshotDoesNotBlockAnotherPipelinesCaptureStart() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("slow_src", "orders", null));
        // The fast source has a different physical chain; one shared chain must serialize its owner.
        artifacts.save(cdcSource("fast_src", "customers", "fast-chain"));
        artifacts.save(pipelineWithReadMode("slow", "slow_src", ReadMode.SNAPSHOT_AND_CDC));
        artifacts.save(pipelineWithReadMode("fast", "fast_src", ReadMode.CDC_ONLY));
        CountDownLatch slowSnapshotEntered = new CountDownLatch(1);
        CountDownLatch releaseSlowSnapshot = new CountDownLatch(1);
        CountDownLatch fastStartAttempted = new CountDownLatch(1);
        CountDownLatch fastCaptureEntered = new CountDownLatch(1);
        CaptureStarter starter = (spec, passthrough) -> {
            if (spec.pipelineId().equals("slow")) {
                passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 1L), Map.of()));
                slowSnapshotEntered.countDown();
                try {
                    if (!releaseSlowSnapshot.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("slow snapshot was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("slow snapshot was interrupted", interrupted);
                }
            } else {
                fastCaptureEntered.countDown();
            }
            return new CaptureRun(Optional.empty(), false, 1L, Optional.empty(), Optional.empty(),
                    new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> slowStart = workers.submit(() -> coordinator.startCapture("slow"));
            assertThat(slowSnapshotEntered.await(5, TimeUnit.SECONDS))
                    .as("the slow snapshot must enter the capture starter").isTrue();
            Future<?> fastStart = workers.submit(() -> {
                fastStartAttempted.countDown();
                coordinator.startCapture("fast");
            });
            assertThat(fastStartAttempted.await(5, TimeUnit.SECONDS))
                    .as("the fast pipeline must attempt its independent start").isTrue();
            assertThat(releaseSlowSnapshot.getCount())
                    .as("the slow snapshot must not have been released").isEqualTo(1);
            assertThat(fastCaptureEntered.await(5, TimeUnit.SECONDS))
                    .as("the fast capture must start while the slow snapshot is blocked").isTrue();
            fastStart.get(5, TimeUnit.SECONDS);
            assertThat(releaseSlowSnapshot.getCount())
                    .as("the fast capture must finish before releasing the slow snapshot").isEqualTo(1);
            releaseSlowSnapshot.countDown();
            slowStart.get(5, TimeUnit.SECONDS);
        } finally {
            releaseSlowSnapshot.countDown();
            workers.shutdown();
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        }
    }

    @Test
    void stopCancelsAnInFlightSnapshotAndClosesTheRunItReturned() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        CountDownLatch snapshotEntered = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicInteger closed = new AtomicInteger();
        CaptureStarter starter = (spec, passthrough) -> {
            passthrough.accept(Envelope.read(1L, "orders", Map.of("id", 1L), Map.of()));
            snapshotEntered.countDown();
            try {
                new CountDownLatch(1).await(30, TimeUnit.SECONDS);
            } catch (InterruptedException cancelled) {
                interrupted.set(true);
            }
            // A connector may finish opening just as cancellation arrives. Its returned handle is still
            // this abandoned start's responsibility and must never be published as the active capture.
            return new CaptureRun(Optional.empty(), false, 1L, Optional.empty(),
                    Optional.of(closed::incrementAndGet), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> start = workers.submit(() -> coordinator.startCapture("p"));
            assertThat(snapshotEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<?> stop = workers.submit(() -> coordinator.stopCapture("p", false));

            stop.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> start.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(CancellationException.class);
            assertThat(interrupted).isTrue();
            assertThat(closed).hasValue(1);
            assertThat(coordinator.isActive("p")).isFalse();
            assertThat(coordinator.snapshotProgress("p").byTable()).isEmpty();
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentStartsOfOnePipelineOpenOneCapture() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src"));
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger opened = new AtomicInteger();
        CaptureStarter starter = (spec, passthrough) -> {
            if (opened.incrementAndGet() == 1) {
                firstEntered.countDown();
                try {
                    if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("first capture was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("first capture was interrupted", interrupted);
                }
            }
            return new CaptureRun(Optional.empty(), false, 0L,
                    Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = workers.submit(() -> coordinator.startCapture("p"));
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            CountDownLatch secondAttempted = new CountDownLatch(1);
            Future<?> second = workers.submit(() -> {
                secondAttempted.countDown();
                coordinator.startCapture("p");
            });
            assertThat(secondAttempted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> second.get(200, TimeUnit.MILLISECONDS))
                    .as("a duplicate start waits for the first source open")
                    .isInstanceOf(TimeoutException.class);
            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertThat(opened).hasValue(1);
        } finally {
            releaseFirst.countDown();
            workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentPipelinesSharingACaptureOpenOnlyOneTail() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipeline("p", "orders_src"));
        artifacts.save(pipeline("q", "orders_src"));
        CountDownLatch tailEntered = new CountDownLatch(1);
        CountDownLatch releaseTail = new CountDownLatch(1);
        AtomicInteger tails = new AtomicInteger();
        AtomicInteger attachments = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        CaptureAttacher attacher = (spec, passthrough, startTail) -> {
            if (startTail) {
                tails.incrementAndGet();
                tailEntered.countDown();
                try {
                    if (!releaseTail.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("shared tail was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("shared tail was interrupted", interrupted);
                }
            } else {
                attachments.incrementAndGet();
            }
            return new CaptureRun(Optional.empty(), false, 0L, Optional.empty(),
                    startTail ? Optional.of(closed::incrementAndGet) : Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), attacher, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer(), CaptureOwnership.single(), Duration.ZERO);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = workers.submit(() -> coordinator.startCapture("p"));
            assertThat(tailEntered.await(5, TimeUnit.SECONDS)).isTrue();
            CountDownLatch secondAttempted = new CountDownLatch(1);
            Future<?> second = workers.submit(() -> {
                secondAttempted.countDown();
                coordinator.startCapture("q");
            });
            assertThat(secondAttempted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> second.get(200, TimeUnit.MILLISECONDS))
                    .as("an attachment waits for its shared tail to finish opening")
                    .isInstanceOf(TimeoutException.class);
            assertThat(tails).hasValue(1);
            releaseTail.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertThat(tails).hasValue(1);
            assertThat(attachments).hasValue(1);
            coordinator.stopCapture("p", false);
            assertThat(closed).hasValue(0);
            coordinator.stopCapture("q", false);
            assertThat(closed).hasValue(1);
        } finally {
            releaseTail.countDown();
            workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
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
    void aCompletedLoadStillReadsAsCompleteAfterTheRunIsReplaced() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        InMemoryStorePort store = discoveredAs(artifacts, "orders", 5L);
        CapturePort source = mock(CapturePort.class);
        CaptureBatch batch = mock(CaptureBatch.class);
        AtomicInteger row = new AtomicInteger();
        when(source.snapshot(any())).thenReturn(batch);
        when(batch.seam()).thenReturn(Optional.of(new SourcePosition("after-load")));
        when(batch.hasNext()).thenReturn(true, true, true, true, true, false);
        when(batch.next()).thenAnswer(ignored -> Envelope.read(
                row.incrementAndGet(), "orders", Map.of("id", row.get()), Map.of()));

        AtomicReference<MiningChainId> chain = new AtomicReference<>();
        for (int member = 0; member < 2; member++) {
            SrsCoordinator srs = new SrsCoordinator(store.meta());
            CaptureStarter starter = (spec, passthrough) -> {
                MiningChainId chainId = spec.miningChainId();
                chain.set(chainId);
                long epoch = srs.provisionSource(
                        spec.sourceId(), chainId, spec.config().streams(), spec.retention()).epoch();
                srs.attachConsumer(chainId, spec.consumerId());
                long rows = SnapshotPhase.run(source, spec.config(), chainId.value(), spec.consumerId(),
                        spec.config().streams(), epoch, store.meta(), passthrough).rows();
                return new CaptureRun(Optional.of(chainId), false, rows, Map.of("orders", rows),
                        Optional.empty(), Optional.empty(), new CaptureHealth());
            };
            StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                    store, starter, srs, new SnapshotBuffer());
            coordinator.startCapture("p");

            if (member == 0) {
                assertThat(coordinator.snapshotProgress("p").byTable())
                        .containsOnly(entry("orders", new TableSnapshot(5L, 5L, 100)));
                store.meta().markSnapshotComplete(chain.get().value(), SrsConsumerId.of("p", "orders_src").value(), "orders");
                assertThat(coordinator.loadDelivered("p")).isTrue();
            } else {
                verify(source, times(1)).snapshot(any());
                assertThat(coordinator.loadDelivered("p")).isTrue();
                assertThat(coordinator.snapshotProgress("p").byTable())
                        .as("a replacement that skips the confirmed load still reports its five rows")
                        .containsOnly(entry("orders", new TableSnapshot(5L, 5L, 100)));
            }
        }
    }

    @Test
    void aReplacedRunReportsConfirmedRowsWhileAnotherTableIsStillLoading() throws Exception {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders"), TableRef.literal("customers")), null, null));
        artifacts.save(new PipelineResource("p", null, List.of(SourceRef.spec("orders_src", false)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(new SyncElement("sync_1", "orders_src", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator srs = new SrsCoordinator(store.meta());
        CountDownLatch secondCustomerRead = new CountDownLatch(1);
        CountDownLatch releaseSecondRead = new CountDownLatch(1);
        AtomicInteger customerReads = new AtomicInteger();
        CapturePort source = mock(CapturePort.class);
        when(source.snapshot(any())).thenAnswer(invocation -> {
            String table = ((CaptureConfig) invocation.getArgument(0)).streams().getFirst();
            if (table.equals("orders")) {
                return snapshotBatch(table, 5, null, null);
            }
            int read = customerReads.incrementAndGet();
            return snapshotBatch(table, read == 1 ? 1 : 2,
                    read == 1 ? null : releaseSecondRead,
                    read == 1 ? null : secondCustomerRead);
        });
        when(source.cdc(any(), any(), any())).thenReturn(mock(Subscription.class));
        CaptureRunUnit unit = new CaptureRunUnit(source, srs, store.meta(), mock(HazelcastInstance.class));
        AtomicReference<CaptureRun> latest = new AtomicReference<>();
        CaptureStarter starter = (spec, handoff) -> {
            CaptureRun run = unit.begin(spec, handoff);
            latest.set(run);
            return run;
        };

        StoreBackedPipelineCaptureCoordinator first = new StoreBackedPipelineCaptureCoordinator(
                store, starter, srs, new SnapshotBuffer());
        first.startCapture("p");
        assertThat(latest.get().awaitLoaded(Duration.ofSeconds(10))).isTrue();
        assertThat(first.captureFailure("p")).isEmpty();
        assertThat(first.snapshotProgress("p").byTable().get("orders").rowsDone()).isEqualTo(5L);
        MiningChainId chain = latest.get().chainId().orElseThrow();
        store.meta().markSnapshotComplete(chain.value(), SrsConsumerId.of("p", "orders_src").value(), "orders");
        first.stopCapture("p", false);

        StoreBackedPipelineCaptureCoordinator replacement = new StoreBackedPipelineCaptureCoordinator(
                store, starter, srs, new SnapshotBuffer());
        try {
            replacement.startCapture("p");
            assertThat(secondCustomerRead.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(replacement.runSnapshotProgress("p").byTable()).containsOnly(
                    entry("orders", new TableSnapshot(0L, null, null)),
                    entry("customers", new TableSnapshot(0L, null, null)));
            assertThat(replacement.snapshotProgress("p").byTable())
                    .containsOnly(entry("orders", new TableSnapshot(5L, 5L, 100)),
                            entry("customers", new TableSnapshot(0L, null, null)));
            assertThat(replacement.loadDelivered("p")).as("the held customer table is still target-unconfirmed").isFalse();
            verify(source, times(1)).snapshot(org.mockito.ArgumentMatchers.argThat(
                    config -> config.streams().equals(List.of("orders"))));
        } finally {
            releaseSecondRead.countDown();
            replacement.stopCapture("p", false);
        }
    }

    private static CaptureBatch snapshotBatch(
            String table, int rows, CountDownLatch release, CountDownLatch entered) {
        return new CaptureBatch() {
            private int next = 1;

            @Override
            public boolean hasNext() {
                if (release != null) {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("the held snapshot was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new CancellationException("the held snapshot was interrupted");
                    }
                }
                return next <= rows;
            }

            @Override
            public Envelope next() {
                return Envelope.read(next, table, Map.of("id", next++), Map.of());
            }

            @Override
            public Optional<SourcePosition> seam() {
                return Optional.of(new SourcePosition("before-load"));
            }

            @Override
            public void close() {
                if (release != null) {
                    release.countDown();
                }
            }
        };
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
    void aConfirmedLoadRetainsItsMeasuredRowsAndReplacesTheStaleTotalOnResume() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("orders_src", "orders", null));
        artifacts.save(pipelineWithReadMode("p", "orders_src", ReadMode.SNAPSHOT_AND_CDC));
        InMemoryStorePort store = discoveredAs(artifacts, "orders", 5L);
        SrsCoordinator srs = new SrsCoordinator(store.meta());
        AtomicReference<MiningChainId> chain = new AtomicReference<>();
        CaptureStarter starter = (spec, passthrough) -> {
            MiningChainId chainId = spec.miningChainId();
            chain.set(chainId);
            srs.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srs.attachConsumer(chainId, spec.consumerId());
            long rows = SnapshotPhase.stillOwed(
                    store.meta().read(chainId.value()), spec.consumerId(), spec.config().streams())
                    .isEmpty() ? 0L : 8L;
            return new CaptureRun(Optional.of(chainId), false, rows, Map.of("orders", rows),
                    Optional.empty(), Optional.empty(), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, starter, srs, new SnapshotBuffer());

        coordinator.startCapture("p");
        Map<String, TableSnapshot> first = coordinator.snapshotProgress("p").byTable();
        assertThat(first).containsOnly(entry("orders", new TableSnapshot(8L, 5L, 100)));
        store.meta().markSnapshotComplete(chain.get().value(), SrsConsumerId.of("p", "orders_src").value(), "orders");
        coordinator.stopCapture("p", false);
        coordinator.startCapture("p");

        assertThat(coordinator.snapshotProgress("p").byTable())
                .containsOnly(entry("orders", new TableSnapshot(8L, 8L, 100)));

        coordinator.stopCapture("p", true);
        assertThat(store.keyedState().count(SnapshotLoadCounts.namespaceOf(SrsConsumerId.of("p", "orders_src").value()))).isZero();
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
                new ServeBlock.Inline(null, FromClause.list(FromRef.literal("src_a"), FromRef.literal("src_b")),
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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
        artifacts.save(cdcSource("src_a", "orders", null));
        artifacts.save(cdcSource("src_b", "items", null));
        artifacts.save(twoSourcePipeline("p", "src_a", "src_b"));
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        AtomicBoolean firstSubscriptionClosed = new AtomicBoolean(false);
        AtomicReference<CaptureRunSpec> firstSpec = new AtomicReference<>();
        RuntimeException wouldNotOpen = new IllegalStateException("the second source would not open");
        CaptureStarter starter = (spec, passthrough) -> {
            if (firstSpec.get() != null) {
                throw wouldNotOpen;
            }
            firstSpec.set(spec);
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                    Optional.of(() -> firstSubscriptionClosed.set(true)), new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, srsCoordinator, new SnapshotBuffer());

        assertThatThrownBy(() -> coordinator.startCapture("p")).isSameAs(wouldNotOpen);

        MiningChainId firstChain = MiningChainId.resolve(firstSpec.get().config(), firstSpec.get().srsKey());
        assertThat(firstSubscriptionClosed).isTrue();
        assertThat(srsCoordinator.isProvisioned(firstChain)).isFalse();
        assertThat(coordinator.isActive("p")).isFalse();
    }

    /**
     * A source that cannot be worked out fails the start before any source is opened. Every source of a
     * start is settled first, so an unusable later source costs the earlier ones nothing -- no load read
     * only to be thrown away with the start.
     */
    @Test
    void aSourceThatCannotBeWorkedOutFailsTheStartBeforeAnySourceIsOpened() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("src_a", "orders", null));
        artifacts.save(new SourceResource("src_b", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null));
        artifacts.save(twoSourcePipeline("p", "src_a", "src_b"));
        List<String> opened = new ArrayList<>();
        CaptureStarter starter = (spec, passthrough) -> {
            opened.add(spec.sourceId());
            return new CaptureRun(Optional.empty(), false, 0L, Optional.empty(), Optional.empty(),
                    new CaptureHealth());
        };
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                artifactsOnly(artifacts), starter, new SrsCoordinator(new InMemorySrsMetaStore()),
                new SnapshotBuffer());

        assertThatThrownBy(() -> coordinator.startCapture("p"))
                .isInstanceOfSatisfying(TapstateException.class, refused -> assertThat(refused.code().code())
                        .isEqualTo("actuation.source-schema-not-discovered"));
        assertThat(opened).isEmpty();
        assertThat(coordinator.isActive("p")).isFalse();
    }

    @Test
    void aFailedStartupKeepsItsUnclosedExactHandleForContextCleanupWithoutPublishingIt() {
        AbortedStartFixture fixture = new AbortedStartFixture();
        assertThatThrownBy(() -> fixture.coordinator.startCapture("p")).isSameAs(fixture.startFailure)
                .satisfies(failure -> assertThat(failure.getSuppressed()).contains(fixture.closeFailure));
        assertThat(fixture.coordinator.isActive("p")).as("a half-open failed assembly is never an active reader").isFalse();
        var retained = fixture.store.meta().read(fixture.chain.get()).orElseThrow();
        fixture.allowClose.set(true);

        fixture.coordinator.close();

        assertThat(fixture.closeAttempts).as("context teardown must reach the original unclosed subscription").hasValue(2);
        assertThat(fixture.ended).isTrue();
        assertThat(fixture.store.meta().read(fixture.chain.get())).contains(retained);
    }

    @Test
    void anotherStartCannotReuseTheFailedHalfAssemblyOrOpenSourcesBeforeItsHandleEnds() {
        AbortedStartFixture fixture = new AbortedStartFixture();
        assertThatThrownBy(() -> fixture.coordinator.startCapture("p")).isSameAs(fixture.startFailure);
        assertThat(fixture.sourceOpens).hasValue(2);

        assertThatThrownBy(() -> fixture.coordinator.startCapture("p")).isSameAs(fixture.closeFailure);

        assertThat(fixture.sourceOpens).as("a fresh assembly cannot begin over an unclosed aborted source").hasValue(2);
        assertThat(fixture.coordinator.isActive("p")).isFalse();
        fixture.allowClose.set(true);
        fixture.coordinator.close();
        assertThat(fixture.closeAttempts).hasValue(3);
        assertThat(fixture.ended).isTrue();
    }

    private static final class AbortedStartFixture {
        private final AtomicBoolean allowClose = new AtomicBoolean(), ended = new AtomicBoolean();
        private final AtomicInteger sourceOpens = new AtomicInteger(), closeAttempts = new AtomicInteger();
        private final AtomicReference<String> chain = new AtomicReference<>();
        private final TapstateException startFailure = new TapstateException(io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled second source admission refusal"), null);
        private final TapstateException closeFailure = new TapstateException(io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled first source close refusal"), null);
        private final InMemoryStorePort store;
        private final StoreBackedPipelineCaptureCoordinator coordinator;
        private AbortedStartFixture() {
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            artifacts.save(cdcSource("src_a", "orders", "abort-a"));
            artifacts.save(cdcSource("src_b", "customers", "abort-b"));
            artifacts.save(twoSourcePipeline("p", "src_a", "src_b"));
            store = new InMemoryStorePort(artifacts);
            SrsCoordinator chains = new SrsCoordinator(store.meta());
            CaptureStarter starter = (spec, handoff) -> {
                sourceOpens.incrementAndGet();
                if (spec.sourceId().equals("src_b")) { throw startFailure; }
                chain.set(spec.miningChainId().value());
                chains.provisionSource(spec.sourceId(), spec.miningChainId(), spec.config().streams(), spec.retention());
                chains.attachConsumer(spec.miningChainId(), spec.consumerId());
                Subscription exact = () -> {
                    closeAttempts.incrementAndGet();
                    if (!allowClose.get()) { throw closeFailure; }
                    ended.set(true);
                };
                return new CaptureRun(Optional.of(spec.miningChainId()), false, 0L, Optional.empty(), Optional.of(exact), new CaptureHealth());
            };
            coordinator = new StoreBackedPipelineCaptureCoordinator(store, starter, chains, new SnapshotBuffer());
        }
    }

    @Test
    void aManagedHandleRejectedAfterOpeningIsRetainedWhenItsImmediateCloseFails() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("managed_src", "orders", "managed-late-refusal"));
        artifacts.save(pipeline("p", "managed_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        SrsCoordinator chains = new SrsCoordinator(store.meta());
        AtomicInteger closes = new AtomicInteger();
        AtomicBoolean allowClose = new AtomicBoolean(), ended = new AtomicBoolean();
        AtomicReference<CaptureRunSpec> admitted = new AtomicReference<>();
        TapstateException closeFailure = new TapstateException(io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled just-opened managed handle close refusal"), null);
        CaptureAttacher attacher = (spec, handoff, startsTail) -> {
            admitted.set(spec);
            chains.provisionSource(spec.sourceId(), spec.miningChainId(), spec.config().streams(), spec.retention());
            chains.attachConsumer(spec.miningChainId(), spec.consumerId());
            CaptureRun actual = new CaptureRun(Optional.of(spec.miningChainId()), false, 0L, Optional.empty(),
                    Optional.of(() -> {
                        closes.incrementAndGet();
                        if (!allowClose.get()) { throw closeFailure; }
                        ended.set(true);
                    }), new CaptureHealth());
            // The actual source callback cancels its caller after returning a real handle. The
            // managed opening check must refuse it before publication under the original permit.
            Thread.currentThread().interrupt();
            return actual;
        };
        var coordinator = new StoreBackedPipelineCaptureCoordinator(store, attacher, chains, new SnapshotBuffer());
        try {
            assertThatThrownBy(() -> coordinator.startCapture("p"))
                    .isInstanceOf(CancellationException.class)
                    .satisfies(refused -> assertThat(refused.getSuppressed()).containsExactly(closeFailure));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
        assertThat(admitted.get()).isNotNull();
        assertThat(coordinator.isActive("p")).isFalse();
        assertThat(closes).hasValue(1);
        var retained = store.meta().read(admitted.get().miningChainId().value()).orElseThrow();
        allowClose.set(true);

        coordinator.close();

        assertThat(closes).as("context cleanup must return to the actual pre-publication handle").hasValue(2);
        assertThat(ended).isTrue();
        assertThat(coordinator.hasActiveCapture("p")).isFalse();
        assertThat(store.meta().read(admitted.get().miningChainId().value())).contains(retained);
    }

    @Test
    void startFailurePreservesTheOriginalErrorWhenRunCleanupFails() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(cdcSource("src_a", "orders", null));
        artifacts.save(cdcSource("src_b", "customers", null));
        artifacts.save(cdcSource("src_c", "items", null));
        artifacts.save(new PipelineResource("p", null, List.of(SourceRef.spec("src_a", true), SourceRef.spec("src_b", true), SourceRef.spec("src_c", true)), null, null,
                new ServeBlock.Inline(null,
                        FromClause.list(FromRef.literal("src_a"), FromRef.literal("src_b"), FromRef.literal("src_c")),
                        List.of(new SyncElement("sync_1", "src_a", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        AtomicBoolean secondClosed = new AtomicBoolean(false);
        AtomicReference<CaptureRunSpec> firstSpec = new AtomicReference<>();
        RuntimeException wouldNotOpen = new IllegalStateException("the third source would not open");
        int[] starts = {0};
        CaptureStarter starter = (spec, passthrough) -> {
            starts[0]++;
            if (starts[0] == 3) {
                throw wouldNotOpen;
            }
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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
                .isSameAs(wouldNotOpen)
                .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(1));
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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

        store.meta().markSnapshotComplete(chain.get().value(), SrsConsumerId.of("p", "orders_src").value(), "orders");
        assertThat(coordinator.loadDelivered("p")).as("one table of two is not the load").isFalse();

        store.meta().markSnapshotComplete(chain.get().value(), SrsConsumerId.of("p", "orders_src").value(), "customers");
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
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

    @Test
    void aJoinedDirectConsumerCarriesItsExplicitOwnerIntoTheReaderThatTakesOver() {
        try (JoinedOwnerFixture fixture = new JoinedOwnerFixture(false, false)) {
            fixture.coordinator.startCapture("p", fixture.store.artifacts(), "writer-p");
            io.tapstate.core.logging.LogSink.Scope scope = new io.tapstate.core.logging.LogSink.Scope("resource-p", 7);
            fixture.coordinator.activateSnapshot("p", scope);
            assertThat(fixture.logicalOwners).containsExactly(scope);
            assertThat(fixture.port.opened).isEmpty();

            fixture.allowCapture.set(true);
            fixture.coordinator.tailWhatNobodyTails();

            assertThat(fixture.port.opened).singleElement().satisfies(opened -> {
                assertThat(opened.config().node()).isEqualTo(new PipelineNode("p", "src_p"));
                assertThat(opened.scope()).isEqualTo(scope);
            });
            assertThat(fixture.tailSpecs).singleElement().satisfies(spec -> {
                assertThat(spec.pipelineId()).isEqualTo("p");
                assertThat(spec.sourceId()).isEqualTo("src_p");
                assertThat(spec.snapshotWriterToken()).isEqualTo("writer-p");
                assertThat(spec.readMode()).isEqualTo(ReadMode.CDC_ONLY);
            });
        }
    }

    @Test
    void aJoinedSharedReaderUsesTheSurvivingConsumersNodeAndOwnerWhenTheFirstLeaves() {
        try (JoinedOwnerFixture fixture = new JoinedOwnerFixture(true, false)) {
            fixture.coordinator.startCapture("p", fixture.store.artifacts(), "writer-p");
            fixture.coordinator.startCapture("q", fixture.store.artifacts(), "writer-q");
            io.tapstate.core.logging.LogSink.Scope first = new io.tapstate.core.logging.LogSink.Scope("resource-p", 7);
            io.tapstate.core.logging.LogSink.Scope survivor = new io.tapstate.core.logging.LogSink.Scope("resource-q", 19);
            fixture.coordinator.activateSnapshot("p", first);
            fixture.coordinator.activateSnapshot("q", survivor);
            fixture.coordinator.stopCapture("p", false);
            assertThat(fixture.port.opened).isEmpty();

            fixture.allowCapture.set(true);
            fixture.coordinator.tailWhatNobodyTails();

            assertThat(fixture.tailSpecs).singleElement().satisfies(spec -> {
                assertThat(spec.config().node()).isEqualTo(new PipelineNode("q", "src_q"));
                assertThat(spec.consumerId()).isEqualTo(SrsConsumerId.of("q", "src_q").value());
                assertThat(spec.snapshotWriterToken()).isEqualTo("writer-q");
            });
            assertThat(fixture.port.opened).singleElement().satisfies(opened -> {
                assertThat(opened.config().node()).isEqualTo(new PipelineNode("q", "src_q"));
                assertThat(opened.scope()).isEqualTo(survivor).isNotEqualTo(first);
            });
        }
    }

    @Test
    void aSixArgumentCaptureOnlyTakeoverResumesWithoutInventingAnObservationOwner() throws InterruptedException {
        try (JoinedOwnerFixture fixture = new JoinedOwnerFixture(false, true)) {
            fixture.coordinator.startCapture("p", fixture.store.artifacts(), "writer-p");
            fixture.coordinator.activateSnapshot("p");
            assertThat(fixture.port.opened).isEmpty();

            fixture.allowCapture.set(true);
            fixture.coordinator.tailWhatNobodyTails();

            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (fixture.port.opened.isEmpty() && System.nanoTime() - deadline < 0) { Thread.sleep(10); }
            assertThat(fixture.port.opened).singleElement().satisfies(opened -> {
                assertThat(opened.config().node()).isEqualTo(new PipelineNode("p", "src_p"));
                assertThat(opened.scope()).isNull();
                assertThat(opened.start()).isEqualTo(CaptureStart.resume(new SourcePosition("processed-source-token")));
            });
        }
    }

    @Test
    void anUnpublishedTakeoverRetriesItsExactTailCloseBeforeOpeningAnotherTail() {
        AtomicInteger logicalCloses = new AtomicInteger(), tailOpens = new AtomicInteger();
        AtomicInteger firstCloses = new AtomicInteger(), replacementCloses = new AtomicInteger(), widened = new AtomicInteger();
        AtomicBoolean firstEnded = new AtomicBoolean(), replacementEnded = new AtomicBoolean();
        List<String> order = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<CaptureRun> returnedTails = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicReference<MiningChainId> logicalChain = new AtomicReference<>();
        AtomicReference<String> logicalConsumer = new AtomicReference<>();
        AtomicReference<SrsCoordinator> joinedChains = new AtomicReference<>();
        AtomicBoolean stillAttachedWhenCloseRetried = new AtomicBoolean();
        TapstateException activationFailure = new TapstateException(io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled takeover activation refusal"), null);
        TapstateException closeFailure = new TapstateException(io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled first takeover tail close refusal"), null);
        CaptureAttacher actualAttachments = (spec, handoff, startTail) -> {
            if (!startTail) {
                logicalChain.set(spec.miningChainId());
                logicalConsumer.set(spec.consumerId());
                joinedChains.get().joinSource(spec.sourceId(), spec.miningChainId(), spec.config().streams());
                joinedChains.get().attachConsumer(spec.miningChainId(), spec.consumerId());
                io.tapstate.runtime.srs.SnapshotActivation logical = new io.tapstate.runtime.srs.SnapshotActivation() {
                    private boolean closed;
                    @Override public void activateSnapshot() { order.add("logical-activated"); }
                    @Override public void close() {
                        if (closed) { return; }
                        closed = true;
                        logicalCloses.incrementAndGet();
                        order.add("logical-closed");
                    }
                };
                return new CaptureRun(Optional.of(spec.miningChainId()), false, 0L, Optional.empty(),
                        Optional.of(logical), new CaptureHealth());
            }
            int opened = tailOpens.incrementAndGet();
            order.add("tail-open-" + opened);
            io.tapstate.runtime.srs.SnapshotActivation tail = new io.tapstate.runtime.srs.SnapshotActivation() {
                private boolean closed;
                @Override public void activateSnapshot() {
                    order.add("tail-activate-" + opened);
                    if (opened == 1) { throw activationFailure; }
                }
                @Override public void close() {
                    if (closed) { return; }
                    if (opened == 1) {
                        int attempt = firstCloses.incrementAndGet();
                        order.add("first-close-" + attempt);
                        if (attempt == 1) { throw closeFailure; }
                        stillAttachedWhenCloseRetried.set(joinedChains.get().isProvisioned(logicalChain.get())
                                && joinedChains.get().affectedConsumers(logicalChain.get()).contains(logicalConsumer.get())
                                && logicalCloses.get() == 0 && tailOpens.get() == 1);
                        firstEnded.set(true);
                        order.add("first-ended");
                    } else {
                        replacementCloses.incrementAndGet();
                        replacementEnded.set(true);
                        order.add("replacement-ended");
                    }
                    closed = true;
                }
            };
            CaptureRun actual = new CaptureRun(Optional.of(spec.miningChainId()), false, 0L, Optional.empty(),
                    Optional.of(tail), new CaptureHealth()).withWidening(widened::incrementAndGet);
            returnedTails.add(actual);
            return actual;
        };
        try (JoinedOwnerFixture fixture = new JoinedOwnerFixture(false, false, actualAttachments)) {
            joinedChains.set(fixture.chains);
            fixture.coordinator.startCapture("p", fixture.store.artifacts(), "writer-p");
            fixture.coordinator.activateSnapshot("p");
            var logicalMetadata = fixture.store.meta().read(logicalChain.get().value()).orElseThrow();
            assertThat(fixture.coordinator.isActive("p")).isTrue();
            assertThat(tailOpens).hasValue(0);
            fixture.allowCapture.set(true);

            fixture.coordinator.tailWhatNobodyTails();

            assertThat(returnedTails).hasSize(1);
            assertThat(activationFailure.getSuppressed()).containsExactly(closeFailure);
            assertThat(firstCloses).hasValue(1);
            assertThat(firstEnded).isFalse();
            assertThat(logicalCloses).hasValue(0);
            assertThat(fixture.store.meta().read(logicalChain.get().value())).contains(logicalMetadata);
            fixture.coordinator.widenTheReadersHere();
            assertThat(widened).as("an unpublished refused tail is not a serving owner").hasValue(0);

            fixture.coordinator.tailWhatNobodyTails();

            assertThat(firstCloses).as("the next maintenance must reach the same returned tail handle").hasValue(2);
            assertThat(firstEnded).isTrue();
            assertThat(logicalCloses).as("the joined consumer outlives cleanup of the refused tail").hasValue(0);
            assertThat(stillAttachedWhenCloseRetried).as("close confirmation precedes reopen or logical release").isTrue();
            assertThat(fixture.store.meta().read(logicalChain.get().value())).contains(logicalMetadata);
            if (tailOpens.get() == 1) { fixture.coordinator.tailWhatNobodyTails(); }
            assertThat(tailOpens).hasValue(2);
            assertThat(returnedTails).hasSize(2);
            assertThat(order.indexOf("first-ended")).isLessThan(order.indexOf("tail-open-2"));
            assertThat(replacementEnded).isFalse();
            assertThat(fixture.coordinator.isActive("p")).isTrue();

            fixture.coordinator.stopCapture("p", false);

            assertThat(logicalCloses).hasValue(1);
            assertThat(replacementCloses).hasValue(1);
            assertThat(replacementEnded).isTrue();
            assertThat(firstCloses).hasValue(2);
            assertThat(fixture.coordinator.hasActiveCapture("p")).isFalse();
            fixture.coordinator.tailWhatNobodyTails();
            assertThat(tailOpens).hasValue(2);
        } finally {
            // Fixture-owned fallback cleanup cannot turn a failed maintenance assertion into evidence.
            returnedTails.forEach(CaptureRun::close);
        }
    }

    @Test
    void aNormalStopRetriesAnUnpublishedTailAndRetainsItsConsumerUntilCloseSucceeds() {
        try (UnpublishedTailCloseFixture fixture = new UnpublishedTailCloseFixture(2)) {
            fixture.openAndRejectTakeover();

            assertThatThrownBy(() -> fixture.joined.coordinator.stopCapture("p", false))
                    .isSameAs(fixture.closeFailure);

            fixture.assertUnconfirmed(2);
            fixture.joined.coordinator.stopCapture("p", false);

            fixture.assertClosed(3);
            fixture.joined.coordinator.tailWhatNobodyTails();
            assertThat(fixture.tailOpens).hasValue(1);
        }
    }

    @Test
    void contextShutdownRetriesAnUnpublishedTailBeforeReleasingItsLogicalConsumer() {
        try (UnpublishedTailCloseFixture fixture = new UnpublishedTailCloseFixture(1)) {
            fixture.openAndRejectTakeover();

            fixture.joined.coordinator.close();

            fixture.assertClosed(2);
            assertThat(fixture.joined.coordinator.shutdownComplete()).isTrue();
            fixture.joined.coordinator.tailWhatNobodyTails();
            assertThat(fixture.tailOpens).hasValue(1);
        }
    }

    private static final class UnpublishedTailCloseFixture implements AutoCloseable {
        private record CloseEvidence(int logicalCloses, int tailOpens, boolean consumerAttached,
                Optional<io.tapstate.spi.store.SrsMeta> metadata) { }

        private final AtomicInteger refusals;
        private final AtomicInteger tailOpens = new AtomicInteger(), tailCloses = new AtomicInteger();
        private final AtomicInteger logicalCloses = new AtomicInteger();
        private final AtomicBoolean tailEnded = new AtomicBoolean();
        private final List<CaptureRun> returnedTails = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final List<CloseEvidence> closeEvidence = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final List<String> order = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final TapstateException activationFailure = new TapstateException(
                io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled unpublished takeover activation refusal"), null);
        private final TapstateException closeFailure = new TapstateException(
                io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled unpublished takeover close refusal"), null);
        private final JoinedOwnerFixture joined;
        private MiningChainId chain;
        private String consumer;
        private io.tapstate.spi.store.SrsMeta retainedMetadata;

        private UnpublishedTailCloseFixture(int refusedCloses) {
            refusals = new AtomicInteger(refusedCloses);
            joined = new JoinedOwnerFixture(false, false, this::attach);
        }

        private CaptureRun attach(CaptureRunSpec spec, io.tapstate.runtime.srs.CaptureHandoff handoff,
                boolean startTail) {
            if (!startTail) {
                chain = spec.miningChainId();
                consumer = spec.consumerId();
                joined.chains.joinSource(spec.sourceId(), chain, spec.config().streams());
                joined.chains.attachConsumer(chain, consumer);
                io.tapstate.runtime.srs.SnapshotActivation logical = new io.tapstate.runtime.srs.SnapshotActivation() {
                    private boolean closed;
                    @Override public void activateSnapshot() { }
                    @Override public void close() {
                        if (closed) { return; }
                        closed = true;
                        logicalCloses.incrementAndGet();
                        order.add("logical-ended");
                    }
                };
                return new CaptureRun(Optional.of(chain), false, 0L, Optional.empty(), Optional.of(logical),
                        new CaptureHealth());
            }
            tailOpens.incrementAndGet();
            io.tapstate.runtime.srs.SnapshotActivation exact = new io.tapstate.runtime.srs.SnapshotActivation() {
                private boolean closed;
                @Override public void activateSnapshot() { throw activationFailure; }
                @Override public void close() {
                    if (closed) { return; }
                    tailCloses.incrementAndGet();
                    closeEvidence.add(new CloseEvidence(logicalCloses.get(), tailOpens.get(),
                            joined.chains.isProvisioned(chain) && joined.chains.affectedConsumers(chain).contains(consumer),
                            joined.store.meta().read(chain.value())));
                    if (refusals.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) { throw closeFailure; }
                    closed = true;
                    tailEnded.set(true);
                    order.add("tail-ended");
                }
            };
            CaptureRun actual = new CaptureRun(Optional.of(spec.miningChainId()), false, 0L, Optional.empty(),
                    Optional.of(exact), new CaptureHealth());
            returnedTails.add(actual);
            return actual;
        }

        private void openAndRejectTakeover() {
            joined.coordinator.startCapture("p", joined.store.artifacts(), "writer-p");
            joined.coordinator.activateSnapshot("p");
            retainedMetadata = joined.store.meta().read(chain.value()).orElseThrow();
            assertThat(joined.chains.affectedConsumers(chain)).containsExactly(consumer);
            joined.allowCapture.set(true);

            joined.coordinator.tailWhatNobodyTails();

            assertThat(activationFailure.getSuppressed()).containsExactly(closeFailure);
            assertThat(returnedTails).hasSize(1);
            assertUnconfirmed(1);
        }

        private void assertUnconfirmed(int attempts) {
            assertThat(tailCloses).hasValue(attempts);
            assertThat(tailEnded).isFalse();
            assertThat(logicalCloses).hasValue(0);
            assertThat(tailOpens).hasValue(1);
            assertThat(joined.coordinator.isActive("p")).isTrue();
            assertThat(joined.coordinator.hasActiveCapture("p")).isTrue();
            assertThat(joined.chains.affectedConsumers(chain)).containsExactly(consumer);
            assertThat(joined.store.meta().read(chain.value())).contains(retainedMetadata);
            assertBeforeConsumerRelease(attempts);
        }

        private void assertClosed(int attempts) {
            assertThat(tailCloses).as("cleanup reaches only the original returned tail").hasValue(attempts);
            assertThat(tailEnded).isTrue();
            assertThat(logicalCloses).hasValue(1);
            assertThat(tailOpens).hasValue(1);
            assertThat(returnedTails).hasSize(1);
            assertBeforeConsumerRelease(attempts);
            assertThat(order).containsExactly("tail-ended", "logical-ended");
            assertThat(joined.coordinator.hasActiveCapture("p")).isFalse();
            assertThat(joined.chains.isProvisioned(chain)).isFalse();
            assertThat(joined.store.meta().read(chain.value())).isPresent();
        }

        private void assertBeforeConsumerRelease(int attempts) {
            assertThat(closeEvidence).hasSize(attempts).allSatisfy(evidence -> {
                assertThat(evidence.logicalCloses()).isZero();
                assertThat(evidence.tailOpens()).isEqualTo(1);
                assertThat(evidence.consumerAttached()).isTrue();
                assertThat(evidence.metadata()).contains(retainedMetadata);
            });
        }

        @Override public void close() {
            refusals.set(0);
            try { joined.close(); }
            finally {
                // Only fixture-owned fallback cleanup runs after the evidence assertions.
                returnedTails.forEach(CaptureRun::close);
            }
        }
    }

    /** Controlled scope/permit inputs isolate the coordinator; they are not native claim or Job receipts. */
    private static final class JoinedOwnerFixture implements AutoCloseable {
        private final InMemoryStorePort store;
        private final SrsCoordinator chains;
        private final AtomicBoolean allowCapture = new AtomicBoolean();
        private final List<io.tapstate.core.logging.LogSink.Scope> logicalOwners = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final List<CaptureRunSpec> tailSpecs = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final OwnerPort port = new OwnerPort(null);
        private final io.tapstate.runtime.srs.SnapshotWorkers workers;
        private final CaptureRunUnit unit;
        private final StoreBackedPipelineCaptureCoordinator coordinator;

        private JoinedOwnerFixture(boolean shared, boolean actualUnit) {
            this(shared, actualUnit, null);
        }

        private JoinedOwnerFixture(boolean shared, boolean actualUnit, CaptureAttacher attachmentOverride) {
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            for (String pipeline : shared ? List.of("p", "q") : List.of("p")) {
                String source = "src_" + pipeline;
                artifacts.save(cdcSource(source, "orders", null));
                artifacts.save(new PipelineResource(pipeline, null, List.of(SourceRef.spec(source, shared)), null, null,
                        new ServeBlock.Inline(null, FromRef.literal(source),
                                List.of(new SyncElement("sync_1", source, null, null, null)), null, null),
                        new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
            }
            store = new InMemoryStorePort(artifacts);
            chains = new SrsCoordinator(store.meta());
            for (String pipeline : shared ? List.of("p", "q") : List.of("p")) {
                String source = "src_" + pipeline;
                store.schemas().save(new DiscoveredSourceModel(source, "mysql", 1L, new SourceModel(List.of(
                        new SourceTable("orders", List.of(new io.tapstate.spi.store.SourceField(
                                "id", "bigint", io.tapstate.core.common.TapstateType.INT64)), List.of("id"), List.of())))));
                CaptureRunSpec spec = StoreBackedPipelineCaptureCoordinator.deriveSpec(pipeline,
                        ((PipelineResource) artifacts.get(pipeline).orElseThrow()).settings(),
                        (SourceResource) artifacts.get(source).orElseThrow(),
                        SourceCaptureResolution.of((SourceResource) artifacts.get(source).orElseThrow()), shared);
                String chain = spec.miningChainId().value();
                if (store.meta().read(chain).isEmpty()) {
                    store.meta().create(chain, null);
                    long epoch = store.meta().openEpoch(chain);
                    if (shared) {
                        store.meta().requestCaptureTables(chain, List.of("orders"));
                        assertThat(store.meta().publishCaptureTables(chain, epoch, List.of("orders"))).isTrue();
                    } else {
                        store.meta().beginDirectCapture(chain, spec.consumerId(), epoch, "processed-source-token");
                    }
                }
            }
            CaptureOwnership ownership = mock(CaptureOwnership.class);
            when(ownership.acquire(any())).thenAnswer(ignored -> allowCapture.get()
                    ? CaptureOwnership.Permit.unfenced() : CaptureOwnership.Permit.denied());
            when(ownership.ttl()).thenReturn(Duration.ofSeconds(30));
            SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
            if (actualUnit) {
                HazelcastInstance member = mock(HazelcastInstance.class);
                when(member.getUserContext()).thenReturn(new java.util.concurrent.ConcurrentHashMap<>());
                workers = new io.tapstate.runtime.srs.SnapshotWorkers(1, 1);
                unit = new CaptureRunUnit(port, chains, store.meta(), member, buffer, workers);
            } else { workers = null; unit = null; }
            CaptureAttacher attacher = (spec, handoff, startTail) -> {
                if (startTail) { tailSpecs.add(spec); }
                if (attachmentOverride != null) { return attachmentOverride.start(spec, handoff, startTail); }
                if (unit != null) { return unit.begin(spec, handoff, startTail); }
                CaptureHealth health = new CaptureHealth();
                io.tapstate.runtime.srs.SnapshotActivation deferred = new io.tapstate.runtime.srs.SnapshotActivation() {
                    private Subscription reader;
                    private boolean activated;
                    @Override public void activateSnapshot() { activate(null); }
                    @Override public void activateSnapshot(io.tapstate.core.logging.LogSink.Scope scope) { activate(scope); }
                    private void activate(io.tapstate.core.logging.LogSink.Scope scope) {
                        if (activated) { return; }
                        activated = true;
                        if (!startTail) { logicalOwners.add(scope); return; }
                        CapturePort view = scope == null ? port : port.forLogOwner(spec.config().node(), scope);
                        reader = view.cdc(spec.config(), CaptureStart.resume(new SourcePosition("processed-source-token")),
                                new CaptureListener() {
                                    @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) { }
                                    @Override public void onError(Throwable failure) { health.fail(failure); }
                                });
                    }
                    @Override public void close() { if (reader != null) { reader.close(); } }
                };
                return new CaptureRun(Optional.empty(), false, 0L, Optional.empty(), Optional.of(deferred), health);
            };
            coordinator = new StoreBackedPipelineCaptureCoordinator(store, attacher, chains, buffer, ownership,
                    Duration.ofHours(1));
        }

        @Override public void close() {
            try { coordinator.close(); }
            finally {
                try { if (unit != null) { unit.close(); } }
                finally { if (workers != null) { workers.close(); } }
            }
        }
    }

    private static final class OwnerPort implements CapturePort, io.tapstate.spi.capture.LogScopedCapturePort {
        private record Opening(CaptureConfig config, CaptureStart start, io.tapstate.core.logging.LogSink.Scope scope) { }
        private final io.tapstate.core.logging.LogSink.Scope scope;
        private final PipelineNode node;
        private final List<Opening> opened;
        private OwnerPort(io.tapstate.core.logging.LogSink.Scope scope) {
            this(scope, null, new java.util.concurrent.CopyOnWriteArrayList<>());
        }
        private OwnerPort(io.tapstate.core.logging.LogSink.Scope scope, PipelineNode node, List<Opening> opened) {
            this.scope = scope; this.node = node; this.opened = opened;
        }
        @Override public CapturePort forLogOwner(PipelineNode node, io.tapstate.core.logging.LogSink.Scope owner) {
            return new OwnerPort(owner, node, opened);
        }
        @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            if (node != null) { assertThat(config.node()).as("the immutable view opens its own admitted node").isEqualTo(node); }
            opened.add(new Opening(config, start, scope));
            return () -> { };
        }
        @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError("no snapshot is owed"); }
        @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
        @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
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
                new ServeBlock.Inline(null, FromClause.list(FromRef.literal(sourceA), FromRef.literal(sourceB)),
                        List.of(new SyncElement("sync_1", sourceA, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
    }

    private static StorePort artifactsOnly(InMemoryArtifactStore artifacts) {
        return new InMemoryStorePort(artifacts);
    }
}
