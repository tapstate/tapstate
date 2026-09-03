package io.tapstate.runtime.srs;

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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The capture run unit assembles the snapshot phase, cdc phase, the self-built Jet ring source and the
 * mining-chain coordinator into one source run, dispatched by the pipeline's consumption plan (its read
 * mode and its {@code srs.enabled} flag). It runs over a single embedded Hazelcast member sized to the L1
 * hot-buffer shape (capacity 8): a real per-table change ring for the shared-ring cdc paths, and a mock
 * connector (a fixed snapshot batch and a fixed change stream) standing in for a real PDK source.
 */
class CaptureRunUnitTest {

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
        assertThat(run.snapshotCount()).isEqualTo(3);
        assertThat(run.snapshotCounts()).containsEntry("orders", 3L);
        assertThat(run.chainId()).isEmpty();
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isEmpty();
        assertThat(meta.created).isEmpty();
        assertThat(port.cdcStarted).isFalse();
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
        assertThat(meta.read(chainId)).get().extracting(SrsMeta::cdcStartPosition).isEqualTo("seam-0");

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
        assertThat(meta.read(chainId)).get().extracting(SrsMeta::cdcStartPosition).isNull();
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
     * The case above with its stand-in removed: no consumer has landed anything, so nothing is written down.
     *
     * <p>The two are one rule seen from both sides, and only together do they discriminate. An offset is a
     * claim that everything below it is safely out of the source's reach -- true only once a sink has taken
     * it, because the direct tail buffers nothing and a change it forwarded but nobody wrote is gone the
     * moment the process is. An implementation that recorded the read unconditionally passes the case above
     * and fails here, which is the only place that difference is visible.
     */
    @Test
    void aDirectTailRecordsNothingWhileNoSinkHasLandedAnything() {
        InMemoryMeta meta = new InMemoryMeta();
        MiningChainId chainId = MiningChainId.resolve(config(), "chain-direct-unacked");

        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false, "chain-direct-unacked"), e -> { });

        assertThat(meta.read(chainId.value()).orElseThrow().sourceReadOffset())
                .as("read is not written: an offset ahead of the sink would skip changes on the way back")
                .isNull();
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
        assertThat(passthrough).extracting(Envelope::src).containsExactly("orders", "customers");
        String chainId = run.chainId().orElseThrow().value();
        assertThat(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders")).tailSequence()).isEqualTo(0L);
        assertThat(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "customers")).tailSequence()).isEqualTo(0L);
    }

    @Test
    void theReadCursorPublisherResolvesTheStoreMemberSideAndAdvancesTheConsumerCursor() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-pub", null);
        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        try {
            SrsReadCursorPublisherFactory factory =
                    CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "orders");

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
            CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "orders")
                    .resolve(hz).accept(7L);
            CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "customers")
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
        SrsReadCursorPublisherFactory factory = CaptureRunUnit.readCursorPublisher("chain-x", "pipe-x", "orders");
        factory.resolve(hz).accept(3L);
    }

    /**
     * What a run of changes costs the coordination record must not grow with what the record has
     * accumulated. Both bounds a run applies -- how far ahead of its readers the ring may be written, and
     * how far the durable read offset may advance -- are functions of the consumer cursors alone, so a run
     * asks for those and nothing else.
     *
     * <p>The record also carries a schema history that grows by one entry per DDL and is never trimmed.
     * Fetching the whole record per run therefore carries that history back on every change, and the cost
     * of doing so climbs for the life of the chain: measured against a real endpoint, a chain with 500
     * DDLs behind it reads at 6.4 ms where the cursors alone read at 0.5 ms.
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
                        change(15), change(16), change(17), change(18), change(19))),
                many)
                .start(spec(ReadMode.CDC_ONLY, true, "chain-reads-many"), e -> { });

        // One cursor read per run of changes -- not two, which is what asking for each bound separately
        // costs when both come from the same record.
        assertThat(many.cursorReads - few.cursorReads)
                .as("cursor reads scale one-for-one with runs of changes")
                .isEqualTo(8);
        // And the whole record is fetched only by the start path, the same number of times either way:
        // eight more runs of changes fetch it not once more.
        assertThat(many.wholeRecordReads)
                .as("whole-record fetches do not scale with the number of change runs")
                .isEqualTo(few.wholeRecordReads);
    }

    @Test
    void theHeadroomBoundIsTheSlowestCursorAcrossTheChainsConsumers() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-min", null);
        // Two consumers on the chain: one has read orders up to 5, the other only to 2 -- the slowest bounds it.
        meta.advanceConsumerReadSeq("chain-min", "p1", "orders", 5L);
        meta.advanceConsumerReadSeq("chain-min", "p2", "orders", 2L);

        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-min"), "orders")).isEqualTo(2L);
    }

    @Test
    void theHeadroomBoundIsUnconstrainedWhenNoConsumerHasACursorYet() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-none", null);

        // No consumer has published a cursor: nothing constrains the ring, so the write gate sees no bound.
        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-none"), "orders"))
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void theHeadroomBoundTreatsAConsumerThatHasNotReadTheTableAsHavingReadNothing() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-other", null);
        // The consumer has a cursor on another table but none on orders: for orders it has read nothing (-1),
        // holding the orders ring at the head until it starts reading orders.
        meta.advanceConsumerReadSeq("chain-other", "p1", "customers", 9L);

        assertThat(CdcPhase.headroomBound(meta.consumerOffsets("chain-other"), "orders")).isEqualTo(-1L);
    }

    /** A mock connector: a fixed snapshot batch and a fixed change stream driven into the listener when cdc starts. */
    private static final class FakeSource implements CapturePort {
        private final List<Envelope> snapshotRows;
        private final List<Envelope> changes;
        private Throwable cdcError;
        boolean cdcStarted;
        /** Where the run asked this source to begin -- the whole of what a resume is observable as. */
        CaptureStart cdcStart;
        boolean cdcClosed;

        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes) {
            this.snapshotRows = snapshotRows;
            this.changes = changes;
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
                    : snapshotRows.stream().filter(row -> selected.contains(row.src())).toList());
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            cdcStarted = true;
            cdcStart = start;
            if (cdcError != null) {
                listener.onError(cdcError);
                return () -> cdcClosed = true;
            }
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

        FakeBatch(List<Envelope> events) {
            this.events = events.iterator();
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
            return Optional.of(new SourcePosition("seam-0"));
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
                    m.cdcStartPosition(), m.schemaHistory(), m.retention(), m.epoch(),
                    m.snapshotEpoch()));
        }

        final List<String> created = new ArrayList<>();
        /** How often the whole record was fetched, and how often the cursors alone were. */
        int wholeRecordReads;
        int cursorReads;
        private final Map<String, SrsMeta> records = new LinkedHashMap<>();

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            wholeRecordReads++;
            return Optional.ofNullable(records.get(miningChainId));
        }

        @Override
        public List<ConsumerOffset> consumerOffsets(String miningChainId) {
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
            records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), null, List.of(), retention));
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            // No test on this double writes a position back; a call here is a wiring mistake, not a case.
            throw new UnsupportedOperationException("rewindSourceReadOffset");
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), position, m.consumerOffsets(), m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
            next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
            next.add(offset);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next, m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
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
            next.add(new ConsumerOffset(pipelineId, perTable, ack));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next, m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
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
            next.add(new ConsumerOffset(pipelineId, perTable, position));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), next, m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(), cdcStartPosition,
                    m.schemaHistory(), m.retention(), m.epoch(), snapshotEpoch));
        }

        @Override
        public long openEpoch(String miningChainId) {
            SrsMeta m = require(miningChainId);
            long opened = m.epoch() + 1;
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(), m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), opened, m.snapshotEpoch()));
            return opened;
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            SrsMeta m = require(miningChainId);
            List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
            next.add(version);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(), m.cdcStartPosition(),
                    next, m.retention(), m.epoch(), m.snapshotEpoch()));
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
                    mine == null ? null : mine.sinkAcked(), completed));
            records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), consumers,
                    m.cdcStartPosition(), m.schemaHistory(), m.retention(), m.epoch(),
                    m.snapshotEpoch()));
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
