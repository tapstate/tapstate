package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reading a table is not writing it, and only the second is safe to skip a table on. A table whose bounded
 * snapshot read drained to its last row, but whose rows no sink has confirmed, is <em>not</em> complete: a
 * run that came back and skipped it would leave every one of its rows that has not changed since absent
 * from the target for good, because the tail only replays what changed after the seam. Nothing would be
 * thrown and nothing logged -- the run would look like a clean resume.
 *
 * <p>This is that rule seen from the side that matters, the next run's. Whether the mark is written is
 * already asserted where the phase writes its marks; what is asserted here is the consequence of its
 * absence, over one store carried across two runs: the same store the first run drained against decides
 * what the second one reads. That seam is where a reader that also marked tables complete would do its
 * damage, and it is invisible to a double whose recorded marks never reach what it reads back.
 *
 * <p>Both directions are asserted, and neither alone discriminates. Without the confirmed case, a phase
 * that re-read every table unconditionally -- resuming nothing, ever -- would satisfy the unconfirmed one.
 */
class ATableReadButNotAckedIsNotCompleteTest {

    private static final String CHAIN = "chain";

    private static CaptureConfig config() {
        return new CaptureConfig("mysql", Map.of(), List.of("orders"));
    }

    private static Envelope row(int id) {
        return Envelope.read(id, "orders", Map.of("id", id), Map.of());
    }

    @Test
    void aTableDrainedToItsLastRowIsStillOwedWhileNoSinkHasConfirmedIt() {
        ReadBackMeta meta = new ReadBackMeta();
        RereadablePort port = new RereadablePort(List.of(row(1), row(2)), "binlog.000042:1024");

        long first = SnapshotPhase.run(port, config(), CHAIN, List.of("orders"), 1L, meta, e -> { });

        // The read finished: every row of the table went downstream, and the batch reported its seam.
        assertThat(first).isEqualTo(2);
        // No sink confirmed those rows, so the one question this phase cannot answer stays unanswered.
        // Presence in this set means written, never merely read.
        assertThat(meta.read(CHAIN)).get()
                .extracting(SrsMeta::snapshotCompletedTables)
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.list(String.class))
                .isEmpty();

        // A restart: the durable record survives, the ring it ran under does not, so a new generation is
        // opened. The table is owed, so the run is a resume and keeps the generation its rows were pinned
        // to rather than taking the one running now.
        long second = SnapshotPhase.run(port, config(), CHAIN, List.of("orders"), 2L, meta, e -> { });

        // The whole of what this case is for: a table read and never written is read again. A run that
        // skipped it here would drop every row of it that has not changed since -- silently, and for good.
        assertThat(second)
                .as("a table no sink confirmed is read again rather than skipped")
                .isEqualTo(2);
        assertThat(port.asked).containsExactly(List.of("orders"), List.of("orders"));
    }

    @Test
    void aTableTheSinkConfirmedIsNotReadAgain() {
        ReadBackMeta meta = new ReadBackMeta();
        RereadablePort port = new RereadablePort(List.of(row(1), row(2)), "binlog.000042:1024");

        SnapshotPhase.run(port, config(), CHAIN, List.of("orders"), 1L, meta, e -> { });
        // Standing in for the sink: these runs have no sink of their own, and confirming the write is its
        // act, made where the frontier reaches that table's rows.
        meta.markSnapshotComplete(CHAIN, "orders");

        long second = SnapshotPhase.run(port, config(), CHAIN, List.of("orders"), 2L, meta, e -> { });

        // Not redoing the load is the whole of what resuming means. Without this half, a phase that never
        // resumed anything would pass the case above.
        assertThat(second)
                .as("a table the sink confirmed is not read a second time")
                .isZero();
        assertThat(port.asked).containsExactly(List.of("orders"));
    }

    /**
     * A capture port that yields a fresh bounded batch for every read, so one port serves the two runs a
     * restart is made of; it records the stream selection of each read it was asked for.
     */
    private static final class RereadablePort implements CapturePort {
        private final List<Envelope> events;
        private final String seam;
        final List<List<String>> asked = new ArrayList<>();

        RereadablePort(List<Envelope> events, String seam) {
            this.events = events;
            this.seam = seam;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            asked.add(config.streams());
            return new FixedBatch(events, seam);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            throw new UnsupportedOperationException("the tail is not exercised by this double");
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

    /** A bounded snapshot batch over a fixed list of events, reporting the seam its source sampled. */
    private static final class FixedBatch implements CaptureBatch {
        private final Iterator<Envelope> events;
        private final String seam;

        FixedBatch(List<Envelope> events, String seam) {
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
            return Optional.of(new SourcePosition(seam));
        }

        @Override
        public void close() {
            // Nothing to release: closing is asserted where the phase's resource handling is under test.
        }
    }

    /**
     * A meta store whose writes reach what it reads back. That is the whole point of it here: a double that
     * merely records the marks it was asked to make cannot show what the next run does with them, and every
     * question this case asks is about the next run.
     */
    private static final class ReadBackMeta implements SrsMetaStore {
        private final Map<String, SrsMeta> records = new LinkedHashMap<>();

        private SrsMeta of(String miningChainId) {
            return records.computeIfAbsent(miningChainId,
                    id -> new SrsMeta(id, null, List.of(), null, List.of(), null));
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            return Optional.of(of(miningChainId));
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            SrsMeta m = of(miningChainId);
            records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                    cdcStartPosition, m.schemaHistory(), m.retention(), m.snapshotCompletedTables(),
                    m.epoch(), snapshotEpoch));
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String table) {
            SrsMeta m = of(miningChainId);
            if (m.snapshotCompletedTables().contains(table)) {
                return;
            }
            List<String> next = new ArrayList<>(m.snapshotCompletedTables());
            next.add(table);
            records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                    m.cdcStartPosition(), m.schemaHistory(), m.retention(), next, m.epoch(),
                    m.snapshotEpoch()));
        }

        @Override
        public List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException("opening a generation is the coordinator's job");
        }

        @Override
        public void create(String miningChainId, String retention) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceConsumerReadSeq(
                String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException();
        }
    }
}
