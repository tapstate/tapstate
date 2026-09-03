package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

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
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.core.model.FromRef;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ConsumerOffset;
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
                "pipe-1", settings, source, SourceCaptureResolution.of(source));

        assertThat(spec.getClass().getRecordComponents())
                .extracting(java.lang.reflect.RecordComponent::getType)
                .as("no component of the run spec is a source position, nor a supplier of one")
                .doesNotContain(SourcePosition.class, java.util.function.Supplier.class);
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
                "pipe-1", null, source, SourceCaptureResolution.of(source));
        CaptureRunSpec settingsWithoutOne = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", new Settings(null, null, null, null, ReadMode.CDC_ONLY, null),
                source, SourceCaptureResolution.of(source));
        CaptureRunSpec authorAskedForEarliest = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                "pipe-1", new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"),
                source, SourceCaptureResolution.of(source));

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
                SourceMode.CDC, List.of(TableRef.literal("orders")), null,
                new Srs(null, null, null, null, false), null);

        assertThat(buffered.srsEnabled()).as("the two sources really do take different paths").isTrue();
        assertThat(direct.srsEnabled()).isFalse();

        for (SourceResource source : List.of(buffered, direct)) {
            // The description goes on the call, not after it: a .as() chained onto the returned assertion
            // never reaches the "no throwable was raised" failure, which is precisely the failure this
            // case exists to produce -- and without it the report cannot say which of the two paths broke.
            assertThatThrownBy(() -> StoreBackedPipelineCaptureCoordinator.deriveSpec(
                            "pipe-1", settings, source, SourceCaptureResolution.of(source)),
                    "srs enabled: %s", source.srsEnabled())
                    .isInstanceOf(TapstateException.class)
                    .satisfies(e -> {
                        TapstateException refused = (TapstateException) e;
                        assertThat(refused.code().code()).isEqualTo("capture.start-from-unparsable");
                        assertThat(refused.args()).containsEntry("value", binlogCoordinate);
                    });
        }
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
    void multiTableSnapshotProgressAndBufferRoutingStayPerTable() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = new SourceResource("multi_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders"), TableRef.literal("customers")), null, null, null);
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

        assertThat(coordinator.snapshotProgress("p")).containsOnly(
                entry("orders", new TableSnapshot(1L, null, null)),
                entry("customers", new TableSnapshot(1L, null, null)));
        SourceCaptureResolution resolution = SourceCaptureResolution.of(source);
        assertThat(buffer.drain(resolution.ringName("orders"))).extracting(e -> e.src()).containsExactly("orders");
        assertThat(buffer.drain(resolution.ringName("customers"))).extracting(e -> e.src()).containsExactly("customers");
    }

    @Test
    void rejects_a_snapshot_row_from_a_table_outside_the_source_selection() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = new SourceResource("selected_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null);
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
                SourceMode.CDC, null, null, null, null);
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
                SourceMode.CDC, null, null, null, null));
        artifacts.save(new PipelineResource("p", null, List.of("src_a", "src_b", "src_c"), null, null,
                new ServeBlock.Inline(null, FromRef.literal("src_a"),
                        List.of(new SyncElement("sync_1", "src_a", null, null, null, null)), null, null),
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
