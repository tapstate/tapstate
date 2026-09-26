package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The capture run unit assembles the snapshot phase, cdc phase, the self-built Jet ring source and the
 * mining-chain coordinator into one source run, dispatched by the pipeline's consumption plan (its read
 * mode and its {@code srs.enabled} flag). It runs over a single embedded Hazelcast member sized to the L1
 * hot-buffer shape (capacity 8): a real per-table change ring for the shared-ring cdc paths, and a mock
 * connector (a fixed snapshot batch and a fixed change stream) standing in for a real PDK source.
 */
class CaptureRunUnitTest {

    @Test
    void reservedSnapshotReturnsBeforeReadingAndStartsOnlyAfterActivation() throws Exception {
        CountDownLatch firstRow = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("whole snapshot batch must not be used");
            }
            @Override public void streamSnapshot(CaptureConfig config, SnapshotListener listener) {
                listener.seam(Optional.empty());
                listener.row(row(1));
                firstRow.countDown();
                try {
                    if (!releaseRead.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("snapshot read was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                listener.row(row(2));
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                throw new AssertionError("snapshot-only capture has no CDC tail");
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        };
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        InMemoryMeta meta = new InMemoryMeta();
        String ringName = SrsRingbuffer.ringName(
                MiningChainId.resolve(config(), "deferred-snapshot-test").value(), "orders");
        List<Envelope> observed = new java.util.concurrent.CopyOnWriteArrayList<>();
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1);
                var caller = Executors.newSingleThreadExecutor()) {
            CaptureRunUnit unit = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, hz,
                    buffer, workers);
            CaptureRunSpec request = spec(ReadMode.SNAPSHOT_ONLY, false, "deferred-snapshot-test")
                    .withChainSelection(List.of("orders"), "run-a");
            CaptureRun run = caller.submit(() -> unit.start(request, observed::add)).get(5, TimeUnit.SECONDS);
            try {
                assertThat(firstRow.getCount()).isEqualTo(1);
                assertThat(buffer.hasSnapshot("pipe-1", ringName, "run-a")).isTrue();
                assertThat(buffer.drainSnapshot("pipe-1", ringName,
                        request.cursorWriterToken(), 1).state())
                        .isEqualTo(SnapshotBuffer.SessionState.ACTIVE);
                run.activateSnapshot();
                assertThat(firstRow.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(observed).hasSize(1);
                List<Envelope> buffered = new ArrayList<>(
                        buffer.drainSnapshot("pipe-1", ringName, "run-a", 1).rows());
                releaseRead.countDown();
                SnapshotBuffer.SessionState terminal = SnapshotBuffer.SessionState.ACTIVE;
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (terminal != SnapshotBuffer.SessionState.DONE && System.nanoTime() < until) {
                    SnapshotBuffer.SessionDrain next = buffer.drainSnapshot("pipe-1", ringName, "run-a", 1);
                    buffered.addAll(next.rows());
                    terminal = next.state();
                    if (terminal == SnapshotBuffer.SessionState.ACTIVE) {
                        Thread.sleep(5);
                    }
                }
                assertThat(terminal).isEqualTo(SnapshotBuffer.SessionState.DONE);
                assertThat(buffered).extracting(event -> event.after().get("id")).containsExactly(1, 2);
                assertThat(observed).hasSize(2);
            } finally {
                releaseRead.countDown();
                run.close();
            }
        }
    }

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("srs-rununit-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
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

    private static CaptureConfig config() {
        return new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders"));
    }

    private static Envelope row(int id) {
        return Envelope.read(id, "orders", Map.of("id", id), Map.of());
    }

    private static Envelope change(int id) {
        return Envelope.insert(id, "orders", Map.of("id", id), Map.of());
    }


    /**
     * Turning the buffer off between two runs does not lose where the first one got to.
     *
     * <p>What the switch decides is whether changes are staged for replay. Where a run begins is decided
     * by the record, and the record belongs to the chain rather than to the buffer -- so a run that reads
     * its source directly picks up exactly where a buffered one stopped. When the accounting was kept with
     * the buffer instead, flipping the switch threw the position away with it and the next run began at the
     * source's present moment: every change made in between simply never arrived.
     *
     * <p>The position is read back out of the record rather than written into the assertion. What it is
     * exactly is the chain's business; what this case is about is that the second run is handed the same
     * one, and spelling a token here would make the case fail the day that spelling changes for a reason
     * nobody cares about.
     */
    @Test
    void aRunThatTurnsTheBufferOffBeginsWhereTheBufferedOneStopped() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource buffered = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun first = runUnit(buffered, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-flip-off"), e -> { });
        String chain = first.chainId().orElseThrow().value();
        meta.markSnapshotComplete(chain, "pipe-1", "orders");
        String recorded = recordedStart(meta, chain);

        FakeSource direct = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        runUnit(direct, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-flip-off"), e -> { });

        assertThat(direct.cdcStart)
                .as("where the run reads from after the buffer was turned off: the record is the chain's "
                        + "and not the buffer's, so it is still there to be picked up. Kept with the "
                        + "buffer, this is the source's present moment and the changes in between are gone")
                .isEqualTo(CaptureStart.resume(new SourcePosition(recorded)));
    }

    /**
     * And the other way, which is a different question rather than the same one mirrored.
     *
     * <p>A directly-read run writes its position through a path of its own -- it has no buffer to ride
     * along with -- so that the record it leaves is one a buffered run can pick up is a second thing to
     * be true, not a restatement of the first. Both directions are here because an accounting that worked
     * one way and not the other would look correct from whichever side happened to be tested.
     */
    @Test
    void aRunThatTurnsTheBufferOnBeginsWhereTheDirectOneStopped() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource direct = new FakeSource(List.of(row(1), row(2)), List.of(change(20)));
        CaptureRun first = runUnit(direct, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, false, "chain-flip-on"), e -> { });
        String chain = first.chainId().orElseThrow().value();
        meta.markSnapshotComplete(chain, "pipe-1", "orders");
        String recorded = recordedStart(meta, chain);

        FakeSource buffered = new FakeSource(List.of(row(1), row(2)), List.of(change(21)));
        runUnit(buffered, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-flip-on"), e -> { });

        assertThat(buffered.cdcStart)
                .as("where the run reads from after the buffer was turned back on: what the direct run "
                        + "wrote is on the chain, so the buffered one that follows it begins there")
                .isEqualTo(CaptureStart.resume(new SourcePosition(recorded)));
    }

    /** What the chain says a next run should begin at: the read offset if there is one, else the seam. */
    private static String recordedStart(InMemoryMeta meta, String chain) {
        SrsMeta record = meta.read(chain).orElseThrow();
        String start = record.sourceReadOffset() != null
                ? record.sourceReadOffset()
                : record.consumerOffset("pipe-1").map(ConsumerOffset::cdcStartPosition).orElse(null);
        return Objects.requireNonNull(start, "the first run recorded nothing for the second to pick up");
    }
    /** A run spec for a config-derived chain (no srs.key). */
    private static CaptureRunSpec spec(ReadMode mode, boolean srsEnabled) {
        return spec(mode, srsEnabled, null);
    }

    /**
     * A run spec keyed by an explicit {@code srsKey} so each shared-ring test gets its own mining chain —
     * and so its own per-table ring on the member shared across this class, keeping the tests isolated.
     */
    private static CaptureRunSpec spec(ReadMode mode, boolean srsEnabled, String srsKey) {
        return spec(mode, srsEnabled, srsKey, StartFrom.earliest());
    }

    /** A run spec that pins {@code start_from} rather than taking this fixture's default. */
    private static CaptureRunSpec spec(
            ReadMode mode, boolean srsEnabled, String srsKey, StartFrom startFrom) {
        return new CaptureRunSpec(
                config(), mode, srsKey, srsEnabled, "src-1", "pipe-1", startFrom, null, 0L);
    }

    /** A run spec for a named consumer pipeline on an explicitly keyed chain. */
    private static CaptureRunSpec specFor(String pipelineId, ReadMode mode, String srsKey) {
        return new CaptureRunSpec(
                config(), mode, srsKey, true, "src-1", pipelineId, StartFrom.earliest(), null, 0L);
    }

    private CaptureRunUnit runUnit(CapturePort port, SrsMetaStore meta) {
        return new CaptureRunUnit(port, new SrsCoordinator(meta), meta, hz);
    }

    @Test
    void snapshotOnlyDrainsToThePassthroughWithNoChainNoCdcStartAndNoTail() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of());
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_ONLY, true), passthrough::add);

        // snapshot_only is a bounded pass straight to the sink: no shared chain a cdc tail resumes against,
        // so nothing is provisioned, no cdc-start is recorded, and no tail is attached.
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(1, 2, 3);
        assertThat(passthrough).extracting(event -> event.position().order())
                .containsOnly(SourceOrder.snapshotRow(1L));
        assertThat(run.snapshotCount()).isEqualTo(3);
        assertThat(run.snapshotCounts()).containsEntry("orders", 3L);
        assertThat(run.chainId()).isEmpty();
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isEmpty();
        assertThat(meta.created).isEmpty();
        assertThat(port.cdcStarted).isFalse();
    }

    @Test
    void directSnapshotOnlyRunsWithoutAnAssignedGenerationStillAdvanceLocally() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunUnit unit = runUnit(new FakeSource(List.of(row(1)), List.of()), meta);
        List<SourceOrder> accepted = new ArrayList<>();

        unit.start(spec(ReadMode.SNAPSHOT_ONLY, true), event -> accepted.add(event.position().order()));
        unit.start(spec(ReadMode.SNAPSHOT_ONLY, true), event -> accepted.add(event.position().order()));

        assertThat(accepted).containsExactly(
                SourceOrder.snapshotRow(1L), SourceOrder.snapshotRow(2L));
    }

    @Test
    @DisplayName("what the load read is on the run's own account, under the operation the source performed")
    void theLoadsRowsAreCountedOnTheRunsAccountAsReads() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of());

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_ONLY, true), event -> { });

        // The account is opened before the load, not with the tail that follows it. Opened after, a run
        // would report having read nothing until its first change arrived - and for a snapshot_only run,
        // which never opens a tail at all, for ever.
        assertThat(run.health().receivedRows()).containsExactly(Map.entry("orders", Map.of("r", 3L)));
    }

    /**
     * A second run over a chain that has already been read picks up where the first left off: it does not
     * re-read the full load, and its tail begins at the recorded position rather than at the source's
     * present moment.
     *
     * <p>Both halves are the same failure seen from two sides. Starting the tail at the present drops
     * every change made since the last run stopped; re-reading the full load re-sends rows the sink has
     * already taken. The first is silent and the second is merely slow, which is why only the first has
     * ever been noticed.
     *
     * <p>The two runs share a meta store and get separate coordinators, which is what a restart is: the
     * durable record survives, the in-memory chain state does not.
     *
     * <p>What makes a table done is the sink confirming it, and these runs have no sink: the confirmation
     * is stood in for here. Reading a table is not writing it, so the read side records nothing -- a run
     * that skipped a table on the strength of having read it would drop every row of it that has not
     * changed since.
     */
    @Test
    void aSecondRunResumesFromTheRecordedPositionInsteadOfReReadingFromThePresent() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun firstRun =
                runUnit(first, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-resume"), e -> { });
        meta.markSnapshotComplete(firstRun.chainId().orElseThrow().value(), "pipe-1", "orders");

        FakeSource restarted = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        CaptureRun second = runUnit(restarted, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-resume"), e -> { });

        assertThat(second.snapshotCount())
                .as("the full load already finished for every selected table, so it is not read again")
                .isZero();
        assertThat(restarted.cdcStart)
                .as("the tail resumes at the recorded seam rather than at the source's present moment")
                .isEqualTo(CaptureStart.resume(new SourcePosition("seam-0")));
    }

    /**
     * The case above with its one stand-in removed: nothing confirms the write, so the table is read again.
     *
     * <p>These are the two halves of one rule, and only together do they discriminate. A table is owed
     * until a sink has confirmed it, which is a different question from whether it was read -- and only the
     * confirmed one is safe to skip on. A run that took the read for the answer would skip this table on
     * the way back, and every row of it that has not changed since would be absent from the target for
     * good: the tail only replays what changed after the seam, so nothing would ever fetch them again.
     * Nothing thrown, nothing logged.
     *
     * <p>Asserted on the assembled run rather than on the snapshot phase alone, which is the whole of why
     * it is here. The phase's own tests cover the phase; they stay green if the mark is made by whatever
     * calls it, once the read returns. That is a read-side mark by another name, and this is the reading
     * that sees it.
     */
    @Test
    void aTableNoSinkConfirmedIsReadAgainByTheRunThatFollows() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun firstRun =
                runUnit(first, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-unacked"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();

        // The read drained every row of the table, and no sink confirmed any of them.
        assertThat(firstRun.snapshotCount()).isEqualTo(2);
        assertThat(meta.read(chainId)).get()
                .extracting(record -> record.snapshotCompletedTables("pipe-1"))
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.list(String.class))
                .isEmpty();

        FakeSource restarted = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        CaptureRun second = runUnit(restarted, meta)
                .start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-unacked"), e -> { });

        assertThat(second.snapshotCount())
                .as("no sink confirmed the table, so the full load is owed and read again")
                .isEqualTo(2);
    }

    /**
     * A pipeline new to a chain reads its own full load, whatever the pipelines already on that chain
     * have finished.
     *
     * <p>Completion answers "are this pipeline's rows in this pipeline's target", and every pipeline on a
     * chain has a target of its own. A chain is keyed by the source connection alone -- the table subset
     * is deliberately not part of it -- so two pipelines reading one database share a chain by
     * construction, and a mark left by the first answers the second's question with the first's answer.
     *
     * <p>What a chain shares is the mining: the source's change log is read once for everyone on it. The
     * initial load is not part of that. A new pipeline's target starts empty, so its rows can only come
     * from a read of its own, and a second read of the source is what that costs.
     *
     * <p>Asserted on the per-table counts rather than the total, because the two failures differ: a run
     * that never entered the snapshot phase reports an empty map, and a run that read an empty table
     * reports a zero. Only the first is this defect, and a total of zero cannot tell them apart.
     */
    @Test
    void aPipelineNewToAChainReadsItsOwnFullLoad() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun firstRun = runUnit(first, meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-shared"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();
        // Stands in for pipe-a's sink confirming the table -- the only thing that ever marks one done.
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");

        FakeSource second = new FakeSource(List.of(row(1), row(2)), List.of(change(11)));
        CaptureRun secondRun = runUnit(second, meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-shared"), e -> { });

        assertThat(secondRun.snapshotCounts())
                .as("pipe-b's target is empty, so it owes itself every row of the table pipe-a finished")
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 2L));
    }

    /**
     * A pipeline new to a chain begins its tail at the seam its own load sampled, not at the one the
     * chain was created at.
     *
     * <p>The same reckoning as the load above, one field over. A recorded seam says where the snapshot
     * that recorded it began, and that snapshot belongs to one pipeline. A pipeline new to the chain has
     * a seam of its own, sampled by its own bounded read moments ago, while the recorded one may be days
     * old -- and a source keeps its change log for a window and refuses a start from before it. Handed
     * the chain's, every join to a chain older than that window fails; and where a source answers such a
     * start with an empty stream instead of a refusal, the pipeline comes up healthy, reports running and
     * delivers nothing, which is the same shape as a source that has no changes for it.
     *
     * <p>Reaching back to the chain's birth buys a joiner nothing either. What a chain shares is the
     * mining; the initial load is not part of that, so the joiner reads the source in full for itself and
     * every change before its own seam is already covered by that read.
     */
    @Test
    void aPipelineNewToAChainKeepsItsOwnSeamWithoutOpeningAnotherPhysicalTail() {
        InMemoryMeta meta = new InMemoryMeta();
        SrsCoordinator coordinator = new SrsCoordinator(meta);
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-the-chain-began-at");
        CaptureRun firstRun = new CaptureRunUnit(first, coordinator, meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-joined"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();
        // Stands in for pipe-a's sink confirming the table -- the only thing that ever marks one done.
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");

        FakeSource joiner = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-the-joiner-began-at");
        new CaptureRunUnit(joiner, coordinator, meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-joined"), e -> { }, false);

        assertThat(joiner.cdcStarted).as("the joiner reads the existing physical tail").isFalse();
        assertThat(meta.read(chainId).orElseThrow().consumerOffset("pipe-b").orElseThrow().cdcStartPosition())
                .as("the joiner's load retains its own seam for recovery")
                .isEqualTo("seam-the-joiner-began-at");
    }

    @Test
    void aPipelineWhoseCompletedLoadSamplesNoSeamRestartsAtItsOwnSeam() {
        InMemoryMeta meta = new InMemoryMeta();
        SrsCoordinator coordinator = new SrsCoordinator(meta);
        FakeSource first = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-chain-birth");
        CaptureRun firstRun = new CaptureRunUnit(first, coordinator, meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-completed-join"), e -> { });
        String chainId = firstRun.chainId().orElseThrow().value();
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");

        FakeSource joiner = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-join");
        new CaptureRunUnit(joiner, coordinator, meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-completed-join"), e -> { }, false);
        meta.markSnapshotComplete(chainId, "pipe-b", "orders");

        FakeSource restarted = new FakeSource(List.of(row(1), row(2)), List.of(), "seam-not-sampled");
        CaptureRun resumed = new CaptureRunUnit(restarted, coordinator, meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-completed-join"), e -> { }, false);

        assertThat(resumed.snapshotCount()).as("pipe-b owes no second load").isZero();
        assertThat(restarted.cdcStarted).as("the existing physical tail stays unique").isFalse();
        assertThat(meta.read(chainId).orElseThrow().consumerOffset("pipe-b").orElseThrow().cdcStartPosition())
                .as("pipe-b keeps its own seam instead of adopting pipe-a's")
                .isEqualTo("seam-join");
    }

    @Test
    void aCdcOnlyPipelineDoesNotAdoptAnotherPipelinesSnapshotSeam() {
        InMemoryMeta meta = new InMemoryMeta();
        SrsCoordinator coordinator = new SrsCoordinator(meta);
        FakeSource loader = new FakeSource(List.of(row(1)), List.of(), "seam-loader");
        CaptureRun first = new CaptureRunUnit(loader, coordinator, meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-cdc-only-join"), e -> { });

        FakeSource cdcOnly = new FakeSource(List.of(), List.of(), "seam-never-sampled");
        CaptureRun run = new CaptureRunUnit(cdcOnly, coordinator, meta, hz)
                .start(specFor("pipe-b", ReadMode.CDC_ONLY, "chain-cdc-only-join"), e -> { }, false);

        assertThat(run.snapshotCount()).as("cdc_only has no load from which to sample a seam").isZero();
        assertThat(cdcOnly.cdcStarted).as("joining does not open another physical reader").isFalse();
        assertThat(meta.read(first.chainId().orElseThrow().value()).orElseThrow()
                .consumerOffset("pipe-b").orElseThrow().cdcStartPosition())
                .as("cdc_only does not inherit another pipeline's load seam")
                .isNull();
    }

    /**
     * A pipeline told to re-read everything does, on a chain another pipeline is still using -- and the
     * other one is not made to re-read anything.
     *
     * <p>Giving back this pipeline's own record is the whole of what a clearing stop does while somebody
     * else is on the chain, and the tables it had finished are part of that record. They used to be the
     * chain's, so there was nowhere to clear them from without deciding it on every other consumer's
     * behalf -- they were left alone, and the next run skipped the load the operator had just asked for.
     * The command reported success and the target kept whatever it had.
     *
     * <p>Both halves are asserted because each fails on its own, and each failure is silent. A rerun that
     * still reads nothing is the defect this closes; a rerun that made its neighbour re-read its whole
     * source is the one the chain-level record was avoiding.
     */
    @Test
    void aPipelineToldToRereadEverythingDoesSoWithoutDisturbingItsChainNeighbour() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(10)));
        CaptureRun aRun = runUnit(port, meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        String chainId = aRun.chainId().orElseThrow().value();
        runUnit(new FakeSource(List.of(row(1), row(2)), List.of(change(11))), meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        // Both sinks confirmed the table, which is what makes a plain restart read nothing.
        meta.markSnapshotComplete(chainId, "pipe-a", "orders");
        meta.markSnapshotComplete(chainId, "pipe-b", "orders");

        // What a clearing stop leaves behind on a chain somebody else is still reading: this pipeline's
        // own record, gone; everything shared, untouched.
        meta.detachConsumer(chainId, "pipe-a");

        CaptureRun reran = runUnit(new FakeSource(List.of(row(1), row(2)), List.of(change(12))), meta)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        assertThat(reran.snapshotCounts())
                .as("the pipeline that asked to re-read everything reads its whole table again")
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 2L));

        CaptureRun neighbour = runUnit(new FakeSource(List.of(row(1), row(2)), List.of(change(13))), meta)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-rerun"), e -> { });
        assertThat(neighbour.snapshotCounts())
                .as("and the pipeline that asked for nothing still owes nothing")
                .isEmpty();
    }

    @Test
    void snapshotAndCdcOverASharedRingProvisionsSnapshotsAttachesAndWritesTheChangeRing() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-snap-cdc"), passthrough::add);

        // The full source run: the chain is provisioned and seeded, the snapshot drains straight to the sink
        // (recording the cdc-start position at the seam), the consumer attaches, and the cdc tail writes the
        // shared change ring the exposed Jet source reads.
        assertThat(run.chainId()).isPresent();
        assertThat(run.merged()).isFalse();
        assertThat(run.snapshotCount()).isEqualTo(2);
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(1, 2);
        assertThat(run.ringSource()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(port.cdcStarted).isTrue();

        String chainId = run.chainId().get().value();
        assertThat(meta.created).containsExactly(chainId);
        // The seam the source itself sampled, not a constant this layer supplied: the recorded value is
        // the batch's own, which is what makes the tail's join to the snapshot a real one.
        assertThat(meta.read(chainId).orElseThrow().consumerOffset("pipe-1")).get()
                .extracting(ConsumerOffset::cdcStartPosition)
                .isEqualTo("seam-0");

        Ringbuffer<SrsItem> ring = hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders"));
        assertThat(ring.tailSequence()).isEqualTo(1L);
        assertThat(ring.readOne(0).after()).containsEntry("id", 10);
        assertThat(ring.readOne(1).after()).containsEntry("id", 11);
    }

    @Test
    void cdcOnlyOverASharedRingSkipsTheSnapshotButStillProvisionsAndWritesTheRing() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1)), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, true, "chain-cdc-only"), passthrough::add);

        // cdc_only skips the initial snapshot: nothing drains to the sink and no cdc-start position is
        // recorded (there is no snapshot seam), but the chain is still provisioned and the tail writes the ring.
        assertThat(run.snapshotCount()).isEqualTo(0);
        assertThat(passthrough).isEmpty();
        assertThat(run.chainId()).isPresent();
        assertThat(run.ringSource()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();

        String chainId = run.chainId().get().value();
        assertThat(meta.read(chainId).orElseThrow().consumerOffset("pipe-1"))
                .get().extracting(ConsumerOffset::selectedTables)
                .isEqualTo(List.of("orders"));
        Ringbuffer<SrsItem> ring = hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders"));
        assertThat(ring.tailSequence()).isEqualTo(1L);
    }

    @Test
    void srsDisabledStreamsTheTailStraightToThePassthroughWithNoRingButKeepsTheRecord() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false), passthrough::add);

        // srs.enabled:false is the direct path: the cdc tail streams straight to the single consumer with
        // no shared ring. What the flag does not turn off is the account -- the chain is opened and its
        // durable record seeded, because that record is what the run after this one starts from.
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(10, 11);
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(port.cdcStarted).isTrue();
        assertThat(run.chainId()).isPresent();
        assertThat(meta.created).containsExactly(run.chainId().orElseThrow().value());
    }

    /**
     * A direct tail -- {@code srs.enabled:false} -- begins where the durable record says, exactly as a
     * shared-ring tail does.
     *
     * <p>{@code srs.enabled} chooses whether the tail is buffered through the shared replay ring. It does
     * not choose whether the position is written down: the position never lived in the ring, so a pipeline
     * that turns the buffering off keeps the position it had, and one that turns it back on finds it still
     * there. That symmetry is the whole reason nothing has to be migrated when the flag changes -- there is
     * no second account to move a position into, and a move is the step that loses one.
     *
     * <p>Taking the present here instead is the silent loss this exists to prevent: the tail comes up
     * healthy, reports healthy, and every change between where it had reached and now is simply gone.
     */
    @Test
    void aDirectTailBeginsWhereTheRecordSaysRatherThanAtThePresent() {
        InMemoryMeta meta = new InMemoryMeta();
        MiningChainId chainId = MiningChainId.resolve(config(), "chain-direct-resume");
        meta.create(chainId.value(), null);
        meta.advanceSourceReadOffset(chainId.value(), new ChainPosition(new SourceOrder(1L, 7L), "src-11"));

        FakeSource port = new FakeSource(List.of(), List.of(change(12)));
        CaptureRun run = runUnit(port, meta)
                .start(spec(ReadMode.CDC_ONLY, false, "chain-direct-resume"), e -> { });

        assertThat(port.cdcStart)
                .as("the direct tail picks up at the recorded position, not at the source's present moment")
                .isEqualTo(CaptureStart.resume(new SourcePosition("src-11")));
        assertThat(run.chainId())
                .as("the chain is there either way -- srs.enabled only decides the buffering")
                .contains(chainId);
        assertThat(run.ringSource())
                .as("no ring: that half of it does follow the flag")
                .isEmpty();
    }

    /**
     * A direct tail writes down how far the source has been read, into the same account a buffered tail
     * keeps. That account is the whole point of keeping the chain when the ring is off: without it the run
     * after this one has nothing to start from and takes the present, losing everything in between.
     *
     * <p>The offset only ever moves to a position a consumer has durably landed. Reading is not writing,
     * and an offset that ran ahead of the sink would skip, on the way back, changes no sink ever took. A
     * sink confirmation is therefore stood in for here, high enough that the clamp is not what this case
     * measures; the case below measures the clamp itself.
     */
    @Test
    void aDirectTailRecordsHowFarTheSourceHasBeenReadOnceASinkHasLandedIt() {
        InMemoryMeta meta = new InMemoryMeta();
        MiningChainId chainId = MiningChainId.resolve(config(), "chain-direct-offset");
        meta.create(chainId.value(), null);
        meta.advanceSinkAcked(chainId.value(), "pipe-1",
                new ChainPosition(new SourceOrder(Long.MAX_VALUE, Long.MAX_VALUE), "landed"));

        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false, "chain-direct-offset"), e -> { });

        assertThat(meta.read(chainId.value()).orElseThrow().sourceReadOffset())
                .as("the direct tail wrote down where it read to, in the account a buffered tail also keeps")
                .isEqualTo("src-11");
    }

    /**
     * The case above with its stand-in removed: no consumer has landed a change, so only the connector's
     * safe start boundary is written down.
     *
     * <p>The two are one rule seen from both sides, and only together do they discriminate. An offset is a
     * claim that everything below it is safely out of the source's reach -- true only once a sink has taken
     * it, because the direct tail buffers nothing and a change it forwarded but nobody wrote is gone the
     * moment the process is. An implementation that recorded the read unconditionally passes the case above
     * and fails here, which is the only place that difference is visible.
     */
    @Test
    void aDirectTailKeepsOnlyItsSafeStartUntilASinkLandsAnything() {
        InMemoryMeta meta = new InMemoryMeta();
        MiningChainId chainId = MiningChainId.resolve(config(), "chain-direct-unacked");

        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false, "chain-direct-unacked"), e -> { });

        assertThat(meta.read(chainId.value()).orElseThrow().sourceReadOffset())
                .as("the start boundary precedes every forwarded change and remains safe for replay")
                .isEqualTo("src-start");
    }

    /**
     * A direct tail stamps each change with an order, so a sink downstream can rank and ack it.
     *
     * <p>A direct tail has no ring, and the ring's sequence is where a buffered change's order comes from.
     * Leaving the order off is not the neutral choice it looks like: every downstream that ranks positions
     * drops one that carries none, so an unstamped direct tail is one nothing can ever confirm, and an
     * account nothing confirms never advances. The count of changes this run has forwarded is the sequence
     * instead -- monotonic within the generation, exactly like the ring's, and taken afresh with each new
     * generation the chain opens.
     */
    @Test
    void aDirectTailStampsEachChangeWithAnOrderSoASinkCanRankIt() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta)
                .start(spec(ReadMode.CDC_ONLY, false, "chain-direct-order"), passthrough::add);

        long epoch = meta.read(run.chainId().orElseThrow().value()).orElseThrow().epoch();
        assertThat(passthrough).extracting(e -> e.position().order())
                .as("each change is ordered within the generation the chain opened for this run")
                .containsExactly(new SourceOrder(epoch, 0L), new SourceOrder(epoch, 1L));
        assertThat(passthrough).extracting(e -> e.position().token())
                .as("the token the source named for a run rides with the change that closes it")
                .containsExactly("src-10", "src-11");
    }

    @Test
    void srsDisabledSurfacesADeadTailAsAFailureOnTheRun() {
        InMemoryMeta meta = new InMemoryMeta();
        RuntimeException boom = new RuntimeException("tail boom");
        FakeSource port = new FakeSource(List.of(), List.of()).failing(boom);
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false), passthrough::add);

        // The direct tail reported a failure; the run surfaces it so a coordinator polling the run can see a
        // dead tail rather than a run that merely stopped emitting.
        assertThat(run.failure()).contains(boom);
    }

    @Test
    void surfacesTheForceMergeWhenASecondSourceResolvesToTheSameChain() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunUnit unit = new CaptureRunUnit(
                new FakeSource(List.of(), List.of()), new SrsCoordinator(meta), meta, hz);

        CaptureRun first = unit.start(spec(ReadMode.CDC_ONLY, true, "chain-merge"), e -> { });
        CaptureRun second = unit.start(spec(ReadMode.CDC_ONLY, true, "chain-merge"), e -> { });

        // The first source opens the chain; a second source resolving to the same chain force-merges onto it
        // rather than mining the source twice -- the signal a caller surfaces as a shared capture.
        assertThat(first.merged()).isFalse();
        assertThat(second.merged()).isTrue();
    }

    @Test
    void anAttachedPipelineDoesNotOpenASecondTailForTheCaptureOwner() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(), List.of(change(10)));
        CaptureRunUnit unit = new CaptureRunUnit(port, new SrsCoordinator(meta), meta, hz);

        CaptureRun owner = unit.start(specFor("pipe-a", ReadMode.CDC_ONLY, "chain-owned"), e -> { }, true);
        CaptureRun attached = unit.start(specFor("pipe-b", ReadMode.CDC_ONLY, "chain-owned"), e -> { }, false);

        assertThat(port.cdcStarts).isEqualTo(1);
        assertThat(owner.cdcSubscription()).isPresent();
        assertThat(attached.cdcSubscription()).isEmpty();
        assertThat(attached.chainId()).isEqualTo(owner.chainId());
    }

    /**
     * A pipeline attaching on a member that does not hold the capture reads under the generation the
     * member holding it opened.
     *
     * <p>The member holding a capture opens the chain's ring generation as it starts the tail, and every
     * change that tail writes is ordered under it. A pipeline driven by another member attaches to the same
     * ring with no tail of its own, and the rows of its own load are ordered by the generation stamped on
     * them: beneath every change of that generation, above every change of an older one. An attaching
     * member that opened a generation of its own would put its load above the changes the holder goes on
     * writing, so a row the source changed after the load read it would keep the value the load saw; and
     * every run assembled afterwards would read a generation no tail writes under.
     */
    @Test
    void aPipelineAttachingOnAMemberThatDidNotOpenTheChainReadsUnderTheGenerationAlreadyRunning() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-held-elsewhere").value();
        CaptureRun held = new CaptureRunUnit(
                new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-held-elsewhere"), e -> { }, true);
        long running = meta.read(chain).orElseThrow().epoch();

        // Another member: a coordinator of its own, over the same durable record and the same ring.
        List<Envelope> loaded = new ArrayList<>();
        CaptureRun attached = new CaptureRunUnit(
                new FakeSource(List.of(row(1), row(2)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-held-elsewhere"), loaded::add, false);

        assertThat(meta.read(chain).orElseThrow().epoch())
                .as("attaching opened no generation of its own")
                .isEqualTo(running);
        assertThat(loaded)
                .as("it ran a load of its own")
                .hasSize(2);
        assertThat(loaded).extracting(e -> e.position().order())
                .as("and that load is ordered beneath every change of the generation the holder writes under")
                .containsOnly(SourceOrder.snapshotRow(running));
        assertThat(attached.cdcSubscription()).as("it opened no tail").isEmpty();
        assertThat(held.cdcSubscription()).as("the holder's tail is the one tail").isPresent();
    }

    /**
     * Where a pipeline arriving on a chain starts reading the ring is marked as it arrives, before its own
     * load reads anything: just past what the ring already holds. Everything under the mark is history from
     * before the pipeline existed, or a change its own load covers; left unmarked, the pipeline's run reads
     * the ring from its head and hands its target every change the ring has ever kept.
     */
    @Test
    void aPipelineArrivingOnARingThatAlreadyHoldsChangesStartsPastThem() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-arrival").value();
        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-arrival"), e -> { }, true);
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chain, "orders")));
        ring.append(buffered(1));
        long heldAtArrival = ring.append(buffered(2));

        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-arrival"), e -> { }, false);

        assertThat(meta.ringDoneThrough(chain, "pipe-b"))
                .as("the pipeline starts just past the two changes the ring held when it arrived")
                .containsExactly(Map.entry("orders", heldAtArrival));
    }

    @Test
    void aPipelineComingBackKeepsThePlaceItHadInTheRingRatherThanTheOneTheRingHasReached() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-return").value();
        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-return"), e -> { }, true);
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chain, "orders")));
        long epoch = meta.read(chain).orElseThrow().epoch();
        long hadReached = ring.append(buffered(1));
        meta.selectConsumerTables(chain, "pipe-b", List.of("orders"), epoch, "previous-job");
        meta.startRingAfter(chain, "pipe-b", "orders", epoch, hadReached);
        meta.markSnapshotComplete(chain, "pipe-b", "orders");
        ring.append(buffered(2));
        ring.append(buffered(3));
        meta.advanceConsumerReadSeq(chain, "pipe-b", "orders", epoch, "previous-job", 1L);

        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-b", ReadMode.SNAPSHOT_AND_CDC, "chain-return"), e -> { }, false);

        assertThat(meta.ringDoneThrough(chain, "pipe-b"))
                .as("the two changes written while it was away are still owed to it")
                .containsExactly(Map.entry("orders", hadReached));
        assertThat(meta.consumerOffsets(chain).stream()
                .filter(offset -> offset.pipelineId().equals("pipe-b"))
                .findFirst().orElseThrow().perTableSeq())
                .as("a new ring generation cannot trust an earlier generation's read cursor")
                .isEmpty();
    }

    @Test
    void aCdcOnlyReadFromThePresentIsMarkedAndOneFromTheEarliestChangeIsLeftToItsStart() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = MiningChainId.resolve(config(), "chain-cdc-start").value();
        new CaptureRunUnit(new FakeSource(List.of(row(1)), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(specFor("pipe-a", ReadMode.SNAPSHOT_AND_CDC, "chain-cdc-start"), e -> { }, true);
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chain, "orders")));
        long held = ring.append(buffered(1));

        new CaptureRunUnit(new FakeSource(List.of(), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(new CaptureRunSpec(config(), ReadMode.CDC_ONLY, "chain-cdc-start", true, "src-1",
                        "pipe-present", StartFrom.latest(), null, 0L), e -> { }, false);
        new CaptureRunUnit(new FakeSource(List.of(), List.of()), new SrsCoordinator(meta), meta, hz)
                .start(new CaptureRunSpec(config(), ReadMode.CDC_ONLY, "chain-cdc-start", true, "src-1",
                        "pipe-earliest", StartFrom.earliest(), null, 0L), e -> { }, false);

        assertThat(meta.ringDoneThrough(chain, "pipe-present"))
                .as("a read from the present owes nothing the ring held before it arrived")
                .containsExactly(Map.entry("orders", held));
        assertThat(meta.ringDoneThrough(chain, "pipe-earliest"))
                .as("a read from the earliest change is owed everything the ring holds, so nothing is marked")
                .isEmpty();
        assertThat(meta.consumerOffsets(chain)).filteredOn(offset ->
                        offset.pipelineId().equals("pipe-present") || offset.pipelineId().equals("pipe-earliest"))
                .allSatisfy(offset -> assertThat(offset.selectedTables())
                        .as("both selected readers are subscribed before their Jet readers open")
                        .containsExactly("orders"));
    }

    private static SrsItem buffered(int id) {
        return new SrsItem(new SourcePosition("b" + id), Op.INSERT, 1L, null, Map.of("id", id), 0L);
    }

    @Test
    void routesAMultiTableSharedRingRunToOneSubscriptionAndTwoRings() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureConfig multi = new CaptureConfig("mysql", Map.of(), List.of("orders", "customers"));
        CaptureRunSpec spec = new CaptureRunSpec(multi, ReadMode.SNAPSHOT_AND_CDC, "k-multi", true, "src-1", "pipe-1",
                StartFrom.earliest(), null, 0L);
        List<Envelope> snapshots = List.of(
                Envelope.read(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.read(2, "customers", Map.of("id", 2), Map.of()));
        List<Envelope> changes = List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.insert(2, "customers", Map.of("id", 2), Map.of()));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(new FakeSource(snapshots, changes), meta).start(spec, passthrough::add);

        assertThat(run.chainId()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(run.snapshotCounts()).containsExactlyInAnyOrderEntriesOf(Map.of("orders", 1L, "customers", 1L));
        assertThat(meta.read(run.chainId().orElseThrow().value()).orElseThrow()
                .consumerOffset("pipe-1").orElseThrow().selectedTables())
                .containsExactlyInAnyOrder("orders", "customers");
        assertThat(passthrough).extracting(Envelope::src).containsExactly("orders", "customers");
        String chainId = run.chainId().orElseThrow().value();
        assertThat(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders")).tailSequence()).isEqualTo(0L);
        assertThat(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "customers")).tailSequence()).isEqualTo(0L);
    }

    @Test
    void physicalTailSubscribesToTheUnionOfPreviouslyAttachedConsumers() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureConfig orders = new CaptureConfig("mysql", Map.of(), List.of("orders"));
        String chain = MiningChainId.resolve(orders, "shared-union").value();
        meta.create(chain, null);
        long earlier = meta.openEpoch(chain);
        meta.selectConsumerTables(chain, "customer-reader", List.of("customers"), earlier, "old-reader");
        assertThat(meta.establishPhysicalAnchor(chain,
                new ChainPosition(new SourceOrder(earlier, -1L), "previous-safe-start"))).isTrue();
        FakeSource source = new FakeSource(List.of(), List.of());
        CaptureRunSpec spec = new CaptureRunSpec(orders, ReadMode.CDC_ONLY, "shared-union", true,
                "orders-source", "orders-reader", StartFrom.latest(), null, 0L);

        try (CaptureRun ignored = runUnit(source, meta).start(spec, event -> { })) {
            assertThat(source.cdcStreams).containsExactly("customers", "orders");
            assertThat(meta.physicalSelection(chain)).contains(new SrsMetaStore.PhysicalSelection(
                    meta.read(chain).orElseThrow().epoch(), List.of("customers", "orders")));
        }
    }

    @Test
    void aLateNewTableCannotAttachToAnOldPhysicalSubscription() {
        InMemoryMeta meta = new InMemoryMeta();
        SrsCoordinator coordinator = new SrsCoordinator(meta);
        FakeSource source = new FakeSource(List.of(), List.of());
        CaptureRunUnit unit = new CaptureRunUnit(source, coordinator, meta, hz);
        CaptureConfig orders = new CaptureConfig("mysql", Map.of(), List.of("orders"));
        CaptureConfig customers = new CaptureConfig("mysql", Map.of(), List.of("customers"));
        CaptureRunSpec first = new CaptureRunSpec(orders, ReadMode.CDC_ONLY, "shared-expand", true,
                "orders-source", "orders-reader", StartFrom.latest(), null, 0L);
        CaptureRunSpec late = new CaptureRunSpec(customers, ReadMode.CDC_ONLY, "shared-expand", true,
                "customers-source", "customers-reader", StartFrom.latest(), null, 0L);

        try (CaptureRun ignored = unit.start(first, event -> { })) {
            assertThatThrownBy(() -> unit.start(late, event -> { }, false))
                    .isInstanceOfSatisfying(TapstateException.class,
                            coded -> assertThat(coded.code())
                                    .isEqualTo(CaptureError.SHARED_SELECTION_RESTART_REQUIRED));
            assertThat(source.cdcStarts).isEqualTo(1);
        }
    }

    @Test
    void aReopenedChainWithoutAProvenStartCannotResumeAtThePresent() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureConfig orders = new CaptureConfig("mysql", Map.of(), List.of("orders"));
        String chain = MiningChainId.resolve(orders, "legacy-no-anchor").value();
        meta.create(chain, null);
        meta.openEpoch(chain);
        FakeSource source = new FakeSource(List.of(), List.of(change(10)));
        CaptureRunSpec spec = new CaptureRunSpec(orders, ReadMode.CDC_ONLY, "legacy-no-anchor", true,
                "orders-source", "orders-reader", StartFrom.latest(), null, 0L);

        assertThatThrownBy(() -> runUnit(source, meta).start(spec, event -> { }))
                .isInstanceOfSatisfying(TapstateException.class,
                        refused -> assertThat(refused.code()).isEqualTo(CaptureError.SHARED_POSITION_UNVERIFIED));
        assertThat(source.cdcStarted).as("a present start would skip previously forwarded work").isFalse();
    }

    @Test
    void aPhysicalTableExpansionClosesTheOldSubscriptionBeforeOpeningTheUnion() {
        InMemoryMeta meta = new InMemoryMeta();
        java.util.concurrent.atomic.AtomicInteger active = new java.util.concurrent.atomic.AtomicInteger();
        List<List<String>> opened = new ArrayList<>();
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                throw new AssertionError("this case opens only CDC subscriptions");
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                assertThat(active.incrementAndGet()).as("only one physical subscription may read this chain")
                        .isEqualTo(1);
                opened.add(config.streams());
                listener.onStart(Optional.of(start instanceof CaptureStart.Resume resume
                        ? resume.position() : new SourcePosition("safe-start")));
                return active::decrementAndGet;
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) {
                throw new UnsupportedOperationException();
            }
        };
        CaptureConfig orders = new CaptureConfig("mysql", Map.of(), List.of("orders"));
        CaptureRunSpec spec = new CaptureRunSpec(orders, ReadMode.CDC_ONLY, "expand-live", true,
                "orders-source", "orders-reader", StartFrom.latest(), null, 0L);
        CaptureRunUnit unit = new CaptureRunUnit(source, new SrsCoordinator(meta), meta, hz);

        CaptureRun first = unit.start(spec, event -> { });
        String chain = first.chainId().orElseThrow().value();
        long epoch = meta.read(chain).orElseThrow().epoch();
        assertThat(meta.requestPhysicalTables(chain, epoch, List.of("customers"))).isTrue();
        CaptureRun expanded = unit.reopenPhysicalTail(spec, first);
        try {
            assertThat(opened).containsExactly(List.of("orders"), List.of("customers", "orders"));
            assertThat(active).hasValue(1);
            assertThat(meta.read(chain).orElseThrow().epoch()).isEqualTo(epoch);
            assertThat(meta.physicalSelection(chain)).contains(
                    new SrsMetaStore.PhysicalSelection(epoch, 2L, List.of("customers", "orders")));
            assertThat(meta.requestedPhysicalTables(chain)).isEmpty();
            assertThat(meta.read(chain).orElseThrow().sourceReadOffset()).isEqualTo("safe-start");
        } finally {
            expanded.close();
        }
        assertThat(active).hasValue(0);
    }

    @Test
    void aReturningTableReadsChangesReminedBeforeItsNewJobAttaches() {
        InMemoryMeta meta = new InMemoryMeta();
        SrsCoordinator coordinator = new SrsCoordinator(meta);
        CaptureConfig orders = new CaptureConfig("mysql", Map.of(), List.of("orders"));
        CaptureConfig customers = new CaptureConfig("mysql", Map.of(), List.of("customers"));
        String chain = MiningChainId.resolve(orders, "restart-backlog").value();
        meta.create(chain, null);
        long oldEpoch = meta.openEpoch(chain);
        assertThat(meta.establishPhysicalAnchor(chain,
                new ChainPosition(new SourceOrder(oldEpoch, -1L), "safe-before-downtime"))).isTrue();
        meta.selectConsumerTables(chain, "customer-reader", List.of("customers"), oldEpoch, "old-job");
        meta.startRingAfter(chain, "customer-reader", "customers", oldEpoch, -1L);

        Envelope downtimeChange = Envelope.insert(10L, "customers", Map.of("id", 10), Map.of());
        CaptureRunUnit owner = new CaptureRunUnit(
                new FakeSource(List.of(), List.of(downtimeChange)), coordinator, meta, hz);
        CaptureRunSpec ownerSpec = new CaptureRunSpec(orders, ReadMode.CDC_ONLY, "restart-backlog", true,
                "orders-source", "orders-reader", StartFrom.latest(), null, 0L);
        CaptureRunSpec returningSpec = new CaptureRunSpec(customers, ReadMode.CDC_ONLY, "restart-backlog", true,
                "customers-source", "customer-reader", StartFrom.latest(), null, 0L);
        try (CaptureRun ignored = owner.start(ownerSpec, event -> { })) {
            long currentEpoch = meta.read(chain).orElseThrow().epoch();
            assertThat(currentEpoch).isGreaterThan(oldEpoch);
            assertThat(hz.getRingbuffer(SrsRingbuffer.ringName(chain, "customers")).tailSequence())
                    .as("the new physical owner already re-mined a customer change")
                    .isEqualTo(0L);

            new CaptureRunUnit(new FakeSource(List.of(), List.of()), coordinator, meta, hz)
                    .start(returningSpec, event -> { }, false);
            assertThat(meta.ringDoneThrough(chain, "customer-reader"))
                    .as("a returning reader must start at the new generation's beginning, before the backlog")
                    .containsEntry("customers", -1L);
        }
    }

    @Test
    void aQuietLaterTableCannotReleaseThePhysicalPrefixBeforeAnEarlierTableSettles() {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = "physical-prefix-inverse-ack";
        meta.create(chain, null);
        long epoch = meta.openEpoch(chain);
        assertThat(meta.establishPhysicalAnchor(chain,
                new ChainPosition(new SourceOrder(epoch, -1L), "t0"))).isTrue();
        meta.selectConsumerTables(chain, "reader", List.of("orders", "customers"), epoch, "reader-run");

        try (PhysicalSourcePrefix prefix = new PhysicalSourcePrefix(meta, chain, epoch, new CaptureHealth())) {
            prefix.anchor(Optional.of(new SourcePosition("t0")));
            prefix.admitted(Map.of("orders", 0L), "t1");
            prefix.admitted(Map.of("customers", 0L), "t2");

            ConsumerOffset selected = meta.read(chain).orElseThrow().consumerOffset("reader").orElseThrow();
            meta.upsertConsumerOffset(chain, new ConsumerOffset(
                    selected.pipelineId(), selected.perTableSeq(), selected.sinkAcked(),
                    selected.snapshotCompletedTables(), selected.cdcStartPosition(), selected.snapshotEpoch(),
                    selected.selectedTables(), selected.selectedTablesEpoch(), selected.cursorWriterToken(),
                    Map.of("customers", new ChainPosition(new SourceOrder(epoch, 0L), "t2"))));
            prefix.tick();
            assertThat(meta.read(chain).orElseThrow().sourceReadOffset())
                    .as("the later table cannot skip pending orders work")
                    .isEqualTo("t0");

            meta.upsertConsumerOffset(chain, new ConsumerOffset(
                    selected.pipelineId(), selected.perTableSeq(), selected.sinkAcked(),
                    selected.snapshotCompletedTables(), selected.cdcStartPosition(), selected.snapshotEpoch(),
                    selected.selectedTables(), selected.selectedTablesEpoch(), selected.cursorWriterToken(),
                    Map.of("orders", new ChainPosition(new SourceOrder(epoch, 0L), "t1"),
                            "customers", new ChainPosition(new SourceOrder(epoch, 0L), "t2"))));
            prefix.tick();
            assertThat(meta.read(chain).orElseThrow().sourceReadOffset())
                    .as("an idle source releases both batches only after the earlier one lands")
                    .isEqualTo("t2");
        }
    }

    @Test
    void aFullPhysicalBarrierWaitsForDurableSinkProgressInsteadOfDroppingABatch() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        String chain = "physical-prefix-backpressure";
        meta.create(chain, null);
        long epoch = meta.openEpoch(chain);
        assertThat(meta.establishPhysicalAnchor(chain,
                new ChainPosition(new SourceOrder(epoch, -1L), "t0"))).isTrue();
        meta.selectConsumerTables(chain, "reader", List.of("orders"), epoch, "reader-run");
        var caller = Executors.newSingleThreadExecutor();
        try (PhysicalSourcePrefix prefix = new PhysicalSourcePrefix(meta, chain, epoch, new CaptureHealth())) {
            prefix.anchor(Optional.of(new SourcePosition("t0")));
            for (long seq = 0; seq < PhysicalSourcePrefix.MAX_PENDING_BATCHES; seq++) {
                prefix.admitted(Map.of("orders", seq), "t" + (seq + 1L));
            }
            var waiting = caller.submit(prefix::awaitRoom);
            assertThatThrownBy(() -> waiting.get(100, TimeUnit.MILLISECONDS))
                    .as("the full queue retains its recovery barriers while the sink is behind")
                    .isInstanceOf(java.util.concurrent.TimeoutException.class);

            ConsumerOffset selected = meta.read(chain).orElseThrow().consumerOffset("reader").orElseThrow();
            meta.upsertConsumerOffset(chain, new ConsumerOffset(
                    selected.pipelineId(), selected.perTableSeq(), selected.sinkAcked(),
                    selected.snapshotCompletedTables(), selected.cdcStartPosition(), selected.snapshotEpoch(),
                    selected.selectedTables(), selected.selectedTablesEpoch(), selected.cursorWriterToken(),
                    Map.of("orders", new ChainPosition(new SourceOrder(epoch, 0L), "t1"))));
            prefix.tick();
            waiting.get(5, TimeUnit.SECONDS);
            assertThat(meta.read(chain).orElseThrow().sourceReadOffset()).isEqualTo("t1");
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void theReadCursorPublisherResolvesTheStoreMemberSideAndAdvancesTheConsumerCursor() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-pub", null);
        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        try {
            meta.openEpoch("chain-pub");
            meta.selectConsumerTables("chain-pub", "pipe-7", List.of("orders"), 1L, "run-1");
            SrsReadCursorPublisherFactory factory =
                    CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "orders", 1L, "run-1");

            factory.resolve(hz).accept(7L);

            // The factory holds only coordinates; resolved on the member it binds the store from the user
            // context and advances exactly this consumer's per-table cursor.
            ConsumerOffset offset = meta.read("chain-pub").orElseThrow().consumerOffsets().stream()
                    .filter(c -> c.pipelineId().equals("pipe-7")).findFirst().orElseThrow();
            assertThat(offset.perTableSeq()).containsEntry("orders", 7L);
        } finally {
            hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        }
    }

    @Test
    void readCursorPublishersAdvanceIndependentTableCursors() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-pub", null);
        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        try {
            meta.openEpoch("chain-pub");
            meta.selectConsumerTables("chain-pub", "pipe-7", List.of("orders", "customers"), 1L, "run-1");
            CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "orders", 1L, "run-1")
                    .resolve(hz).accept(7L);
            CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "customers", 1L, "run-1")
                    .resolve(hz).accept(11L);

            ConsumerOffset offset = meta.read("chain-pub").orElseThrow().consumerOffsets().stream()
                    .filter(c -> c.pipelineId().equals("pipe-7")).findFirst().orElseThrow();
            assertThat(offset.perTableSeq()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "orders", 7L, "customers", 11L));
        } finally {
            hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        }
    }

    @Test
    void rejects_an_empty_stream_selection_before_provisioning() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunSpec spec = new CaptureRunSpec(
                new CaptureConfig("mysql", Map.of("host", "h"), List.of()),
                ReadMode.CDC_ONLY, "chain-empty", true, "src-1", "pipe-1", StartFrom.earliest(), null, 0L);

        assertThatThrownBy(() -> runUnit(new FakeSource(List.of(), List.of()), meta)
                .start(spec, ignored -> { }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one stream");
        assertThat(meta.created).isEmpty();
    }

    @Test
    void snapshot_passthrough_failure_rolls_back_a_chain_created_by_this_start() {
        InMemoryMeta meta = new InMemoryMeta();
        SrsCoordinator coordinator = new SrsCoordinator(meta);
        FakeSource source = new FakeSource(List.of(row(1)), List.of());
        CaptureRunSpec spec = spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-snapshot-failure");
        MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
        RuntimeException failure = new IllegalStateException("snapshot sink failed");
        CaptureRunUnit unit = new CaptureRunUnit(source, coordinator, meta, hz);

        assertThatThrownBy(() -> unit.start(spec, ignored -> { throw failure; }))
                .isSameAs(failure);

        assertThat(coordinator.isProvisioned(chainId)).isFalse();
        assertThat(source.cdcStarted).isFalse();
    }

    @Test
    void theReadCursorPublisherResolvesToANoOpWhenNoStoreIsBoundOnTheMember() {
        hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);

        // No store bound on the member: the factory resolves to a no-op sink, so a source still runs before
        // the assembly layer makes the member SRS-capable. Resolving and calling it does not throw.
        SrsReadCursorPublisherFactory factory = CaptureRunUnit.readCursorPublisher(
                "chain-x", "pipe-x", "orders", 1L, "run-1");
        factory.resolve(hz).accept(3L);
    }

    /**
     * What a run of changes costs the coordination record must not grow with what the record has
     * accumulated. Both bounds a run applies -- how far ahead of its readers the ring may be written, and
     * how far the durable read offset may advance -- are functions of the consumer cursors alone, so a run
     * asks for those and nothing else.
     *
     * <p>The record also carries a schema history that grows by one entry per DDL, up to the bound the
     * store keeps it under. Fetching the whole record per run therefore carries that history back on every
     * change, and the cost of doing so climbs with what the record holds: measured against a real endpoint,
     * a chain with 500 DDLs behind it reads at 6.4 ms where the cursors alone read at 0.5 ms.
     *
     * <p>So this pins two things at once, and the second is the one that would rot silently: a run reads
     * the cursors once rather than once per bound, and the number of whole-record fetches does not move
     * when the number of runs does.
     */
    @Test
    void aRunOfChangesReadsTheCursorsOnceAndNeverFetchesTheWholeRecord() {
        InMemoryMeta few = new InMemoryMeta();
        runUnit(new FakeSource(List.of(), List.of(change(10), change(11))), few)
                .start(spec(ReadMode.CDC_ONLY, true, "chain-reads-few"), e -> { });
        InMemoryMeta many = new InMemoryMeta();
        runUnit(new FakeSource(List.of(), List.of(
                        change(10), change(11), change(12), change(13), change(14),
                        change(15), change(16), change(17))),
                many)
                .start(spec(ReadMode.CDC_ONLY, true, "chain-reads-many"), e -> { });

        // One cursor read per run of changes -- not two, which is what asking for each bound separately
        // costs when both come from the same record.
        assertThat(many.cursorReads - few.cursorReads)
                .as("cursor reads scale one-for-one with runs of changes")
                .isEqualTo(6);
        // And the whole record is fetched only by the start path, the same number of times either way:
        // six more runs of changes fetch it not once more.
        assertThat(many.wholeRecordReads)
                .as("whole-record fetches do not scale with the number of change runs")
                .isEqualTo(few.wholeRecordReads);
    }

    @Test
    void theHeadroomBoundIsTheSlowestCursorAcrossTheChainsConsumers() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-min", null);
        meta.openEpoch("chain-min");
        // Two consumers on the chain: one has read orders up to 5, the other only to 2 -- the slowest bounds it.
        meta.selectConsumerTables("chain-min", "p1", List.of("orders"), 1L, "run-p1");
        meta.selectConsumerTables("chain-min", "p2", List.of("orders"), 1L, "run-p2");
        meta.advanceConsumerReadSeq("chain-min", "p1", "orders", 5L);
        meta.advanceConsumerReadSeq("chain-min", "p2", "orders", 2L);

        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-min"), "orders", 1L)).isEqualTo(2L);
    }

    @Test
    void theHeadroomBoundIsUnconstrainedWhenNoConsumerHasACursorYet() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-none", null);

        // No consumer has published a cursor: nothing constrains the ring, so the write gate sees no bound.
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-none"), "orders", 1L))
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void theHeadroomBoundTreatsAnExplicitUnreadSubscriptionAsHavingReadNothing() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-other", null);
        meta.openEpoch("chain-other");
        // The consumer has a cursor on another selected table but none on orders. Its orders selection
        // still protects the ring's first change.
        meta.selectConsumerTables("chain-other", "p1", List.of("customers", "orders"), 1L, "run-1");
        meta.advanceConsumerReadSeq("chain-other", "p1", "customers", 9L);

        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-other"), "orders", 1L)).isEqualTo(-1L);
    }

    @Test
    void aSelectionChangeClearsRemovedTablesBeforeTheyCanBeReadded() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-selection", null);
        meta.openEpoch("chain-selection");
        meta.selectConsumerTables("chain-selection", "p1", List.of("orders", "items"), 1L, "run-1");
        meta.advanceConsumerReadSeq("chain-selection", "p1", "orders", 7L);
        meta.advanceConsumerReadSeq("chain-selection", "p1", "items", 99L);
        meta.selectConsumerTables("chain-selection", "p1", List.of("orders", "items"), 1L, "run-1");
        assertThat(meta.consumerOffsets("chain-selection").getFirst().perTableSeq())
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 7L, "items", 99L));

        meta.selectConsumerTables("chain-selection", "p1", List.of("orders"), 1L, "run-2");
        assertThat(meta.consumerOffsets("chain-selection").getFirst().perTableSeq())
                .isEmpty();
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-selection"), "items", 1L))
                .isEqualTo(Long.MAX_VALUE);

        meta.selectConsumerTables("chain-selection", "p1", List.of("orders", "items"), 1L, "run-3");
        assertThat(meta.consumerOffsets("chain-selection").getFirst().perTableSeq())
                .isEmpty();
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-selection"), "items", 1L))
                .as("the earlier items cursor is not valid for the renewed subscription")
                .isEqualTo(-1L);
        meta.advanceConsumerReadSeq("chain-selection", "p1", "items", 1L, "run-1", 100L);
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-selection"), "items", 1L))
                .as("a reader from the previous run cannot raise the new run's headroom")
                .isEqualTo(-1L);
    }

    @Test
    void anOldReaderCannotPublishIntoTheNewRingGeneration() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-generation", null);
        meta.openEpoch("chain-generation");
        meta.selectConsumerTables("chain-generation", "p1", List.of("orders"), 1L, "run-1");
        meta.advanceConsumerReadSeq("chain-generation", "p1", "orders", 99L);

        meta.openEpoch("chain-generation");
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-generation"), "orders", 2L))
                .isEqualTo(-1L);
        meta.selectConsumerTables("chain-generation", "p1", List.of("orders"), 2L, "run-2");
        meta.advanceConsumerReadSeq("chain-generation", "p1", "orders", 1L, "run-1", 100L);
        assertThat(meta.consumerOffsets("chain-generation").getFirst().perTableSeq()).isEmpty();
        meta.advanceConsumerReadSeq("chain-generation", "p1", "orders", 2L, "run-2", 0L);
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-generation"), "orders", 2L))
                .isEqualTo(0L);
    }

    @Test
    void aSameGenerationReassemblyStartsUnreadBelowTheOldReadersProgress() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-reassembly", null);
        meta.openEpoch("chain-reassembly");
        meta.selectConsumerTables("chain-reassembly", "p1", List.of("orders"), 1L, "old-reader");
        meta.advanceConsumerReadSeq("chain-reassembly", "p1", "orders", 1L, "old-reader", 99L);
        ChainPosition acked = new ChainPosition(new SourceOrder(1, 7), "w7");
        meta.advanceSinkAcked("chain-reassembly", "p1", acked);

        meta.selectConsumerTables("chain-reassembly", "p1", List.of("orders"), 1L, "new-reader");

        ConsumerOffset current = meta.consumerOffsets("chain-reassembly").getFirst();
        assertThat(current.sinkAcked()).isEqualTo(acked);
        assertThat(current.perTableSeq()).isEmpty();
        assertThat(CdcPhase.headroomBound(List.of(current), "orders", 1L)).isEqualTo(-1L);
        meta.advanceConsumerReadSeq("chain-reassembly", "p1", "orders", 1L, "old-reader", 100L);
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-reassembly"), "orders", 1L))
                .isEqualTo(-1L);
    }

    /** A mock connector: a fixed snapshot batch and a fixed change stream driven into the listener when cdc starts. */
    private static final class FakeSource implements CapturePort {
        private final List<Envelope> snapshotRows;
        private final List<Envelope> changes;
        /** The position this source samples before its bounded read -- where a tail of it must join. */
        private final String seam;
        private Throwable cdcError;
        boolean cdcStarted;
        int cdcStarts;
        List<String> cdcStreams;
        /** Where the run asked this source to begin -- the whole of what a resume is observable as. */
        CaptureStart cdcStart;
        boolean cdcClosed;

        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes) {
            this(snapshotRows, changes, "seam-0");
        }

        /** A source whose bounded read samples a named seam, so two runs of one chain can differ in it. */
        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes, String seam) {
            this.snapshotRows = snapshotRows;
            this.changes = changes;
            this.seam = seam;
        }

        /** Makes this source's cdc stream report a failure through the listener rather than deliver changes. */
        FakeSource failing(Throwable error) {
            this.cdcError = error;
            return this;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            // A bounded read yields the rows of the streams it selected and no others; an empty selection
            // is every stream the source exposes. The snapshot phase reads one table at a time, so a double
            // that ignored the selection would answer each of those reads with the whole source.
            List<String> selected = config.streams();
            return new FakeBatch(selected.isEmpty() ? snapshotRows
                    : snapshotRows.stream().filter(row -> selected.contains(row.src())).toList(), seam);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            cdcStarted = true;
            cdcStarts++;
            cdcStart = start;
            cdcStreams = config.streams();
            if (cdcError != null) {
                listener.onError(cdcError);
                return () -> cdcClosed = true;
            }
            listener.onStart(Optional.of(start instanceof CaptureStart.Resume resume
                    ? resume.position() : new SourcePosition("src-start")));
            for (Envelope e : changes) {
                listener.onBatch(java.util.List.of(e), Optional.of(new SourcePosition("src-" + e.ts())));
            }
            return () -> cdcClosed = true;
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

    /** A bounded snapshot batch over a fixed list of events. */
    private static final class FakeBatch implements CaptureBatch {
        private final Iterator<Envelope> events;
        private final String seam;

        FakeBatch(List<Envelope> events, String seam) {
            this.events = events.iterator();
            this.seam = seam;
        }

        @Override
        public boolean hasNext() {
            return events.hasNext();
        }

        @Override
        public Envelope next() {
            return events.next();
        }

        @Override
        public Optional<SourcePosition> seam() {
            // The source sampled this before reading its first row; the run under test refuses to start a
            // tail without one, because a tail that begins wherever it likes loses every change made while
            // the snapshot ran.
            return Optional.of(new SourcePosition(seam));
        }

        @Override
        public void close() {
        }
    }

    /**
     * A faithful in-memory {@link SrsMetaStore}: insert-only create, per-facet mutators that reject an
     * unseeded chain, and a read-cursor advance that upserts one consumer's {@code perTableSeq} without
     * clobbering its sink-ack — enough to exercise the run unit's provision, cdc-start, offset and cursor
     * wiring without a store backend.
     */
    private static final class InMemoryMeta implements SrsMetaStore {
        /** Per chain and pipeline, how far each table's ring is done with -- kept once, never raised here. */
        final Map<String, Map<String, Long>> ringDone = new LinkedHashMap<>();

        @Override
        public void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
            ringDone.computeIfAbsent(miningChainId + "/" + pipelineId, key -> new LinkedHashMap<>())
                    .putIfAbsent(table, seq);
        }

        @Override
        public void startRingAfter(String miningChainId, String pipelineId, String table, long epoch, long seq) {
            SrsMeta current = require(miningChainId);
            ConsumerOffset consumer = current.consumerOffset(pipelineId).orElse(null);
            if (current.epoch() == epoch && consumer != null
                    && Objects.equals(consumer.selectedTablesEpoch(), epoch)
                    && consumer.selectedTables().contains(table)) {
                startRingAfter(miningChainId, pipelineId, table, seq);
            }
        }

        @Override
        public Map<String, Long> ringDoneThrough(String miningChainId, String pipelineId) {
            return Map.copyOf(ringDone.getOrDefault(miningChainId + "/" + pipelineId, Map.of()));
        }

        @Override
        public java.util.List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override
        public void dropChain(String miningChainId) {
            throw new UnsupportedOperationException(
                    "chain removal is not exercised by this double");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            SrsMeta m = records.get(miningChainId);
            if (m == null) {
                return;
            }
            List<ConsumerOffset> kept = m.consumerOffsets().stream()
                    .filter(c -> !c.pipelineId().equals(pipelineId))
                    .toList();
            records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), kept,
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        final List<String> created = new ArrayList<>();
        /** How often the whole record was fetched, and how often the cursors alone were. */
        int wholeRecordReads;
        int cursorReads;
        private final Map<String, SrsMeta> records = new LinkedHashMap<>();
        private final java.util.Set<String> trustedPhysicalPrefixes = new java.util.HashSet<>();
        private final Map<String, PhysicalSelection> physicalSelections = new LinkedHashMap<>();
        private final Map<String, java.util.Set<String>> physicalRequests = new LinkedHashMap<>();
        private final Map<String, Map<String, Long>> ringStarts = new LinkedHashMap<>();

        @Override
        public OptionalLong ringGenerationStartAfter(String miningChainId, String table, long epoch) {
            SrsMeta current = records.get(miningChainId);
            Long start = current == null || current.epoch() != epoch ? null
                    : ringStarts.getOrDefault(miningChainId, Map.of()).get(table);
            return start == null ? OptionalLong.empty() : OptionalLong.of(start);
        }

        @Override
        public OptionalLong establishRingGenerationStartAfter(
                String miningChainId, String table, long epoch, long proposedSeq) {
            if (require(miningChainId).epoch() != epoch) {
                return OptionalLong.empty();
            }
            long start = ringStarts.computeIfAbsent(miningChainId, ignored -> new LinkedHashMap<>())
                    .computeIfAbsent(table, ignored -> proposedSeq);
            return OptionalLong.of(start);
        }

        @Override
        public Optional<PhysicalSelection> physicalSelection(String miningChainId) {
            return Optional.ofNullable(physicalSelections.get(miningChainId));
        }

        @Override
        public boolean publishPhysicalSelection(String miningChainId, PhysicalSelection selection) {
            if (require(miningChainId).epoch() != selection.epoch()
                    || !selection.tables().containsAll(requestedPhysicalTables(miningChainId))) {
                return false;
            }
            PhysicalSelection previous = physicalSelections.get(miningChainId);
            if (previous != null && previous.epoch() == selection.epoch()
                    && !java.util.Set.copyOf(previous.tables()).equals(java.util.Set.copyOf(selection.tables()))) {
                return false;
            }
            physicalSelections.put(miningChainId, selection);
            return true;
        }

        @Override
        public List<String> requestedPhysicalTables(String miningChainId) {
            return physicalRequests.getOrDefault(miningChainId, java.util.Set.of()).stream().sorted().toList();
        }

        @Override
        public boolean requestPhysicalTables(String miningChainId, long epoch, List<String> tables) {
            if (require(miningChainId).epoch() != epoch) {
                return false;
            }
            physicalRequests.computeIfAbsent(miningChainId, ignored -> new java.util.LinkedHashSet<>())
                    .addAll(tables);
            return true;
        }

        @Override
        public boolean replacePhysicalSelection(
                String miningChainId, PhysicalSelection expected, PhysicalSelection replacement) {
            if (require(miningChainId).epoch() != expected.epoch()
                    || !Objects.equals(physicalSelections.get(miningChainId), expected)
                    || replacement.revision() != expected.revision() + 1L
                    || !replacement.tables().containsAll(expected.tables())
                    || !replacement.tables().containsAll(requestedPhysicalTables(miningChainId))) {
                return false;
            }
            physicalSelections.put(miningChainId, replacement);
            return true;
        }

        @Override
        public void clearPhysicalRequests(String miningChainId, long epoch, List<String> tables) {
            if (require(miningChainId).epoch() == epoch) {
                java.util.Set<String> requested = physicalRequests.get(miningChainId);
                if (requested != null) {
                    requested.removeAll(tables);
                }
            }
        }

        @Override
        public synchronized Optional<SrsMeta> read(String miningChainId) {
            wholeRecordReads++;
            return Optional.ofNullable(records.get(miningChainId));
        }

        @Override
        public synchronized List<ConsumerOffset> consumerOffsets(String miningChainId) {
            cursorReads++;
            Optional<SrsMeta> record = read(miningChainId);
            // This double answers the narrow read out of the same map, so the line above counted a whole
            // record fetch that a real store would not have made. Take it back off: what this counter is
            // for is fetches made for their own sake.
            wholeRecordReads--;
            return record.map(SrsMeta::consumerOffsets).orElse(List.of());
        }

        @Override
        public void create(String miningChainId, String retention) {
            if (records.containsKey(miningChainId)) {
                throw new IllegalStateException("mining chain already seeded: " + miningChainId);
            }
            created.add(miningChainId);
            records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), List.of(), retention));
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            // No test on this double writes a position back; a call here is a wiring mistake, not a case.
            throw new UnsupportedOperationException("rewindSourceReadOffset");
        }

        @Override
        public synchronized void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), position, m.consumerOffsets(),
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        @Override
        public synchronized boolean advancePhysicalSourceReadOffset(
                String miningChainId, long epoch, ChainPosition position) {
            if (require(miningChainId).epoch() != epoch) {
                return false;
            }
            advanceSourceReadOffset(miningChainId, position);
            return true;
        }

        @Override
        public boolean physicalPrefixTrusted(String miningChainId) {
            return trustedPhysicalPrefixes.contains(miningChainId);
        }

        @Override
        public boolean establishPhysicalAnchor(String miningChainId, ChainPosition position) {
            SrsMeta current = require(miningChainId);
            if (current.epoch() != position.order().epoch()) {
                return false;
            }
            if (trustedPhysicalPrefixes.contains(miningChainId)) {
                return true;
            }
            if (current.sourceRead() != null) {
                return false;
            }
            advanceSourceReadOffset(miningChainId, position);
            trustedPhysicalPrefixes.add(miningChainId);
            return true;
        }

        @Override
        public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
            next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
            next.add(offset);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset c : m.consumerOffsets()) {
                if (c.pipelineId().equals(pipelineId)) {
                    existing = c;
                } else {
                    next.add(c);
                }
            }
            Map<String, Long> perTable = new LinkedHashMap<>(existing == null ? Map.of() : existing.perTableSeq());
            perTable.put(table, lastReadSeq);
            ChainPosition ack = existing == null ? null : existing.sinkAcked();
            next.add(new ConsumerOffset(
                    pipelineId,
                    perTable,
                    ack,
                    existing == null ? List.of() : existing.snapshotCompletedTables(),
                    existing == null ? null : existing.cdcStartPosition(),
                    existing == null ? 0L : existing.snapshotEpoch(),
                    existing == null ? null : existing.selectedTables(),
                    existing == null ? null : existing.selectedTablesEpoch(),
                    existing == null ? null : existing.cursorWriterToken()));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        @Override
        public void selectConsumerTables(
                String miningChainId, String pipelineId, List<String> tables, long epoch,
                String cursorWriterToken) {
            SrsMeta m = require(miningChainId);
            if (m.epoch() != epoch) {
                throw new IllegalStateException("consumer selection must match the open ring generation");
            }
            ConsumerOffset previous = m.consumerOffset(pipelineId).orElse(null);
            if (previous != null && !Objects.equals(previous.selectedTablesEpoch(), epoch)) {
                ringDone.remove(miningChainId + "/" + pipelineId);
            }
            Map<String, Long> retained = new LinkedHashMap<>();
            if (previous != null && Objects.equals(previous.selectedTablesEpoch(), epoch)
                    && Objects.equals(previous.cursorWriterToken(), cursorWriterToken)
                    && previous.selectedTables() != null) {
                for (String table : tables) {
                    if (previous.selectedTables().contains(table)
                            && previous.perTableSeq().containsKey(table)) {
                        retained.put(table, previous.perTableSeq().get(table));
                    }
                }
            }
            upsertConsumerOffset(miningChainId, new ConsumerOffset(
                    pipelineId, retained, previous == null ? null : previous.sinkAcked(),
                    previous == null ? List.of() : previous.snapshotCompletedTables(),
                    previous == null ? null : previous.cdcStartPosition(),
                    previous == null ? 0L : previous.snapshotEpoch(), tables, epoch,
                    cursorWriterToken));
        }

        @Override
        public void advanceConsumerReadSeq(
                String miningChainId, String pipelineId, String table, long epoch,
                String cursorWriterToken, long lastReadSeq) {
            ConsumerOffset current = require(miningChainId).consumerOffset(pipelineId).orElse(null);
            if (current != null && Objects.equals(current.selectedTablesEpoch(), epoch)
                    && Objects.equals(current.cursorWriterToken(), cursorWriterToken)
                    && current.selectedTables() != null && current.selectedTables().contains(table)) {
                advanceConsumerReadSeq(miningChainId, pipelineId, table, lastReadSeq);
            }
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset c : m.consumerOffsets()) {
                if (c.pipelineId().equals(pipelineId)) {
                    existing = c;
                } else {
                    next.add(c);
                }
            }
            Map<String, Long> perTable = existing == null ? Map.of() : existing.perTableSeq();
            next.add(new ConsumerOffset(
                    pipelineId,
                    perTable,
                    position,
                    existing == null ? List.of() : existing.snapshotCompletedTables(),
                    existing == null ? null : existing.cdcStartPosition(),
                    existing == null ? 0L : existing.snapshotEpoch(),
                    existing == null ? null : existing.selectedTables(),
                    existing == null ? null : existing.selectedTablesEpoch(),
                    existing == null ? null : existing.cursorWriterToken()));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        @Override
        public synchronized boolean advancePhysicalSinkAcked(
                String miningChainId, String pipelineId, long epoch, ChainPosition position) {
            SrsMeta current = require(miningChainId);
            if (current.epoch() != epoch) {
                return false;
            }
            ConsumerOffset consumer = current.consumerOffset(pipelineId).orElse(null);
            if (consumer != null && Objects.equals(consumer.selectedTablesEpoch(), epoch)) {
                advanceSinkAcked(miningChainId, pipelineId, position);
            }
            return true;
        }

        @Override
        public void setCdcStart(
                String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset consumer : m.consumerOffsets()) {
                if (consumer.pipelineId().equals(pipelineId)) {
                    existing = consumer;
                } else {
                    next.add(consumer);
                }
            }
            next.add(new ConsumerOffset(
                    pipelineId,
                    existing == null ? Map.of() : existing.perTableSeq(),
                    existing == null ? null : existing.sinkAcked(),
                    existing == null ? List.of() : existing.snapshotCompletedTables(),
                    cdcStartPosition,
                    snapshotEpoch,
                    existing == null ? null : existing.selectedTables(),
                    existing == null ? null : existing.selectedTablesEpoch(),
                    existing == null ? null : existing.cursorWriterToken()));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next,
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        @Override
        public long openEpoch(String miningChainId) {
            SrsMeta m = require(miningChainId);
            long opened = m.epoch() + 1;
            ringStarts.remove(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                    m.schemaHistory(), m.retention(), opened));
            return opened;
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            SrsMeta m = require(miningChainId);
            List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
            next.add(version);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                    next, m.retention(), m.epoch()));
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
            SrsMeta m = require(miningChainId);
            // Per pipeline, not per chain: the mark says this pipeline's sink took the table, and the
            // pipelines sharing a chain each write somewhere of their own.
            List<ConsumerOffset> consumers = new ArrayList<>();
            ConsumerOffset mine = null;
            for (ConsumerOffset consumer : m.consumerOffsets()) {
                if (consumer.pipelineId().equals(pipelineId)) {
                    mine = consumer;
                } else {
                    consumers.add(consumer);
                }
            }
            List<String> completed =
                    new ArrayList<>(mine == null ? List.of() : mine.snapshotCompletedTables());
            if (!completed.contains(table)) {
                completed.add(table);
            }
            consumers.add(new ConsumerOffset(pipelineId, mine == null ? Map.of() : mine.perTableSeq(),
                    mine == null ? null : mine.sinkAcked(), completed,
                    mine == null ? null : mine.cdcStartPosition(),
                    mine == null ? 0L : mine.snapshotEpoch(),
                    mine == null ? null : mine.selectedTables(),
                    mine == null ? null : mine.selectedTablesEpoch(),
                    mine == null ? null : mine.cursorWriterToken()));
            records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), consumers,
                    m.schemaHistory(), m.retention(), m.epoch()));
        }

        private SrsMeta require(String miningChainId) {
            SrsMeta m = records.get(miningChainId);
            if (m == null) {
                throw new IllegalStateException("mining chain not seeded: " + miningChainId);
            }
            return m;
        }
    }

    /**
     * A direct tail with nothing recorded yet begins where its author asked. {@code start_from} is that
     * ask, and on this path it names a position in the source's own log rather than a cursor into a replay
     * buffer -- there is no buffer here for it to point into.
     *
     * <p>The three forms are not interchangeable: {@code earliest} asks for the oldest change the source
     * still retains, an instant asks the source to resolve that moment to a position of its own, and
     * {@code latest} asks for only what is written from now on. Collapsing any of them into the present is
     * the silent form of ignoring the setting -- the tail comes up healthy having read a different stretch
     * than the one asked for, which is the same failure a start clamped to a buffer's head makes.
     */
    @Test
    void aDirectTailWithNothingRecordedBeginsWhereItsAuthorAsked() {
        Instant asked = Instant.parse("2026-09-01T00:00:00Z");

        assertThat(directTailStart(StartFrom.earliest(), "chain-first-earliest"))
                .as("earliest asks the source for the oldest change it still retains")
                .isEqualTo(CaptureStart.earliest());
        assertThat(directTailStart(StartFrom.at(asked), "chain-first-at"))
                .as("an instant is a start only the source can resolve to a position of its own")
                .isEqualTo(CaptureStart.at(asked));
        assertThat(directTailStart(StartFrom.latest(), "chain-first-latest"))
                .as("latest is the present moment: only changes written from now on")
                .isEqualTo(CaptureStart.present());
    }

    /**
     * A recorded position outranks {@code start_from} on a direct tail. The setting says where a read
     * begins, not where every later run of it begins: honoured again on the way back it would re-read the
     * stretch already read on every restart, and asking for the whole source again is a separate request
     * with its own verb.
     *
     * <p>This is what makes the case above a statement about a first run rather than about the setting
     * always winning, and the two readings are distinguishable only here: with nothing recorded they give
     * the same answer.
     */
    @Test
    void aRecordedPositionOutranksStartFromOnADirectTail() {
        InMemoryMeta meta = new InMemoryMeta();
        MiningChainId chainId = MiningChainId.resolve(config(), "chain-start-from-outranked");
        meta.create(chainId.value(), null);
        meta.advanceSourceReadOffset(chainId.value(), new ChainPosition(new SourceOrder(1L, 7L), "src-11"));

        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, meta).start(
                spec(ReadMode.CDC_ONLY, false, "chain-start-from-outranked", StartFrom.earliest()), e -> { });

        assertThat(port.cdcStart)
                .as("the position the last run reached wins; start_from named where the first one began")
                .isEqualTo(CaptureStart.resume(new SourcePosition("src-11")));
    }

    /**
     * A buffered tail's miner does not take {@code start_from}, because on that path the setting is this
     * one pipeline's cursor into the shared buffer and the miner is shared by all of them. The buffer is
     * mined once and each consumer finds its own start in it, so a miner that honoured one consumer's ask
     * would move where every other consumer's changes came from.
     *
     * <p>This is the control on the case above: it is what separates "the direct path resolves the ask"
     * from "the ask is resolved on every path", and only the buffered side can tell the two apart.
     */
    @Test
    void aBufferedTailsMinerDoesNotTakeStartFrom() {
        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, new InMemoryMeta()).start(
                spec(ReadMode.CDC_ONLY, true, "chain-miner-ignores-start-from", StartFrom.latest()), e -> { });

        assertThat(port.cdcStart)
                .as("the miner begins at the present with nothing recorded, whatever a consumer asked for")
                .isEqualTo(CaptureStart.present());
    }

    /**
     * A buffered tail refuses a past instant when this run is the one that starts the mining. Nothing has
     * been mined yet and nothing from before this moment ever will be, so the ask cannot be met -- and the
     * reader cannot see that for itself: an empty ring is the same shape whether its oldest change has
     * aged out or has simply not arrived, which is why the refusal belongs here, where the miner's own
     * start is decided.
     *
     * <p>Served rather than refused it is silent, and the silence is the whole of the defect: the pipeline
     * comes up healthy, reports running, and reads only what is written from now on -- every change between
     * the instant asked for and the moment the run came up is gone, with nothing thrown and nothing logged.
     */
    @Test
    void aBufferedTailRefusesAPastInstantWhenThisRunIsWhatStartsTheMining() {
        FakeSource port = new FakeSource(List.of(), List.of());

        TapstateException refused = catchThrowableOfType(() -> runUnit(port, new InMemoryMeta()).start(
                spec(ReadMode.CDC_ONLY, true, "chain-fresh-past-instant",
                        StartFrom.at(Instant.parse("2020-01-01T00:00:00Z"))), e -> { }),
                TapstateException.class);

        assertThat(refused.code()).isEqualTo(CaptureError.START_FROM_OUTSIDE_WINDOW);
        assertThat(refused.args())
                .as("the refusal names what was asked for and how far back this buffer goes")
                .containsEntry("requested", "2020-01-01T00:00:00Z")
                .containsEntry("retention", "unset")
                .containsKey("earliest");
        assertThat(port.cdcStarted)
                .as("it refuses before opening the source's stream, not after")
                .isFalse();
    }

    /**
     * The control on the case above: an instant this buffer will still cover is taken, not refused. Mining
     * begins now, so a moment at or after that is reachable by waiting rather than unreachable, and an
     * implementation that refused every instant on a fresh chain satisfies the case above while making the
     * setting unusable on exactly the path it was extended for.
     */
    @Test
    void aBufferedTailTakesAnInstantThisBufferWillStillCover() {
        FakeSource port = new FakeSource(List.of(), List.of());

        runUnit(port, new InMemoryMeta()).start(
                spec(ReadMode.CDC_ONLY, true, "chain-fresh-reachable-instant",
                        StartFrom.at(Instant.now().plusSeconds(3600))), e -> { });

        assertThat(port.cdcStarted)
                .as("nothing before the mining begins is missed, so there is nothing to refuse")
                .isTrue();
    }

    /**
     * A chain with a position to resume from is not refused, whatever instant was asked for. How far back
     * its buffer will reach is that recorded position -- the source's own opaque token, which says nothing
     * about a moment -- so the reachability this refusal turns on is not knowable here, and refusing on a
     * guess would fail runs that were going to be served.
     *
     * <p>This is what makes the refusal a statement about a chain whose mining starts at the present, and
     * not about past instants in general. The two readings agree everywhere except here.
     */
    @Test
    void aChainWithAPositionToResumeFromIsNotRefusedWhateverWasAskedFor() {
        InMemoryMeta meta = new InMemoryMeta();
        MiningChainId chainId = MiningChainId.resolve(config(), "chain-resumed-past-instant");
        meta.create(chainId.value(), null);
        meta.advanceSourceReadOffset(chainId.value(), new ChainPosition(new SourceOrder(1L, 7L), "src-11"));

        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, meta).start(
                spec(ReadMode.CDC_ONLY, true, "chain-resumed-past-instant",
                        StartFrom.at(Instant.parse("2020-01-01T00:00:00Z"))), e -> { });

        assertThat(port.cdcStart)
                .as("the miner resumes; where its buffer reaches back to is not a moment this can compare")
                .isEqualTo(CaptureStart.resume(new SourcePosition("src-11")));
    }

    /**
     * Where a direct tail with nothing recorded asks its source to begin, given one {@code start_from}.
     * A fresh store per call is what makes it a first run; the key keeps each call's chain its own.
     */
    private CaptureStart directTailStart(StartFrom startFrom, String srsKey) {
        FakeSource port = new FakeSource(List.of(), List.of());
        runUnit(port, new InMemoryMeta())
                .start(spec(ReadMode.CDC_ONLY, false, srsKey, startFrom), e -> { });
        return port.cdcStart;
    }
}
