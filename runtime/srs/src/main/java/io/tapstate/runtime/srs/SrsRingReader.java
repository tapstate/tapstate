package io.tapstate.runtime.srs;

import com.hazelcast.ringbuffer.StaleSequenceException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.SrsLogBatch;
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogStore;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.ObjLongConsumer;

/**
 * One consumer's reader over a per-table change ring. It tails the ring from a run-local cursor,
 * emitting each change once in sequence order and advancing the cursor as it goes. A fill drains a
 * bounded batch — from the cursor up to the ring tail, capped at the caller's max — so the reader yields
 * to the downstream between batches (Jet backpressure) and never blocks for a change that has not been
 * written yet.
 *
 * <p>As the cursor advances, the reader publishes its read progress: after each non-empty fill it reports
 * the last sequence it read to an {@code onAdvance} sink, one report per batch. That is the signal the
 * write-side headroom gate reads back as this consumer's progress, so a slow reader backpressures the
 * source rather than having its unread changes overwritten. A reader with no sink (the bare constructor)
 * simply does not report.
 *
 * <p>The cursor is run-local and never written into a Jet snapshot. A durable reader resumes at the
 * consumer's separately confirmed position and reads the recoverable log below the hot ring's head.
 * Reading a batch publishes only read progress; it does not confirm downstream effects. The overloads
 * without a log retain the volatile-ring behavior for callers that do not use durable shared capture.
 */
public final class SrsRingReader {

    private final SrsRingbuffer ring;
    private final LongConsumer onAdvance;
    private final String ringName;
    private final SrsLogStore log;
    private long cursor;

    /**
     * A reader that begins at {@code startSeq} — the next sequence it will read. At L1 a fresh reader
     * starts at the ring head to replay every buffered change. It reports no read progress.
     */
    public SrsRingReader(SrsRingbuffer ring, long startSeq) {
        this(ring, startSeq, seq -> { });
    }

    /**
     * A reader that begins at {@code startSeq} and reports its read progress to {@code onAdvance} — the
     * last sequence it read, published once after each non-empty fill.
     */
    public SrsRingReader(SrsRingbuffer ring, long startSeq, LongConsumer onAdvance) {
        this(ring, startSeq, onAdvance, null, null);
    }

    private SrsRingReader(SrsRingbuffer ring, long startSeq, LongConsumer onAdvance,
            String ringName, SrsLogStore log) {
        this.ring = Objects.requireNonNull(ring, "ring");
        this.onAdvance = Objects.requireNonNull(onAdvance, "onAdvance");
        this.ringName = ringName;
        this.log = log;
        this.cursor = startSeq;
    }

    /**
     * A reader positioned by a {@code start} point resolved against the ring: {@code earliest} at the head
     * (replay everything buffered), {@code latest} just past the tail (only changes appended from now on),
     * and an instant at the first change whose event time is at or after it. An instant older than every
     * buffered change is refused rather than served from the head, since coming up at the head would stream
     * a different stretch than the one asked for with nothing saying so; an instant newer than every
     * buffered change starts past the tail. It reports no read progress.
     */
    public static SrsRingReader from(SrsRingbuffer ring, StartFrom start) {
        return from(ring, start, seq -> { });
    }

    /** A reader positioned by {@code start} (as {@link #from(SrsRingbuffer, StartFrom)}) that reports its
     * read progress to {@code onAdvance}. */
    public static SrsRingReader from(SrsRingbuffer ring, StartFrom start, LongConsumer onAdvance) {
        return from(ring, start, onAdvance, null);
    }

    /**
     * As the three-argument form, with the chain's retention setting carried in so a start the buffer
     * can no longer reach can say how far back it is configured to go. The setting is only ever quoted
     * back in that refusal; it never decides where the reader begins.
     */
    public static SrsRingReader from(
            SrsRingbuffer ring, StartFrom start, LongConsumer onAdvance, String retention) {
        Objects.requireNonNull(ring, "ring");
        Objects.requireNonNull(start, "start");
        return new SrsRingReader(ring, resolveStartSeq(ring, start, retention), onAdvance);
    }

    /**
     * A reader that carries on just past {@code ackedSeq}: the ring sequence of the last change this
     * consumer's sink confirmed from this ring. This is where a run replacing one that died picks up, and
     * it is not the head: a ring outlives the runs that read it, so its head can sit far below what this
     * consumer already landed, and starting there hands the target every change it has again.
     *
     * <p>A next sequence below the volatile ring's head is missing history, even if another consumer
     * kept this same ring alive. Refuse it rather than skipping read-but-unconfirmed changes. A
     * confirmed sequence beyond the tail still falls back to the head of a rebuilt ring. Durable shared
     * capture uses the log-backed overload to recover history below the hot head.
     */
    public static SrsRingReader resumingAfter(SrsRingbuffer ring, long ackedSeq, LongConsumer onAdvance) {
        Objects.requireNonNull(ring, "ring");
        long head = ring.headSequence();
        long tail = ring.tailSequence();
        long next = ackedSeq + 1;
        if (next < head) {
            throw new TapstateException(CaptureError.RECOVERY_LOG_GAP, Map.of(
                    "ring", ring.name(), "sequence", next,
                    "reason", "the hot ring no longer retains the next change after this consumer's confirmed progress"
                            + ": head=" + head + ", tail=" + tail), null);
        }
        long start = next > tail + 1 ? head : next;
        return new SrsRingReader(ring, start, onAdvance);
    }

    /**
     * Opens a durable reader at the requested point in the retained log rather than at the hot buffer's
     * head. A start instant searches bounded batches; a hole or a record without its original generation
     * is an explicit recovery failure, never a reason to substitute a newer start.
     */
    public static SrsRingReader from(SrsRingbuffer ring, StartFrom start, LongConsumer onAdvance,
            String retention, String ringName, SrsLogStore log) {
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(ringName, "ringName");
        Objects.requireNonNull(log, "log");
        SrsRingReader reader = new SrsRingReader(ring, 0L, onAdvance, ringName, log);
        SrsLogBounds bounds = log.bounds(ringName);
        reader.cursor = switch (start) {
            case StartFrom.Earliest ignored -> nextSequence(bounds.trimmedThrough());
            case StartFrom.Latest ignored -> nextSequence(bounds.largestSequence());
            case StartFrom.At at -> reader.firstDurableSeqAtOrAfter(at, retention, bounds);
        };
        return reader;
    }

    /**
     * Resumes exactly after this consumer's confirmed sequence or explicit first-arrival marker. The
     * optional actual confirmation validates the retained record's original generation and token; a
     * marker has no such claim. A trimmed confirmed record needs no replay, but every sequence after it
     * must still be retained. In particular, neither a new hot head nor a later read cursor can advance
     * this recovery boundary.
     */
    public static SrsRingReader resumingAfter(SrsRingbuffer ring, long ackedSeq,
            ChainPosition confirmed, LongConsumer onAdvance, String ringName, SrsLogStore log) {
        Objects.requireNonNull(ringName, "ringName");
        Objects.requireNonNull(log, "log");
        SrsRingReader reader = new SrsRingReader(ring, 0L, onAdvance, ringName, log);
        SrsLogBounds bounds = log.bounds(ringName);
        if (confirmed != null && confirmed.order() != null && confirmed.order().seq() >= 0
                && confirmed.order().seq() > ackedSeq) {
            // A confirmation that advanced after graph assembly still proves those effects landed.
            ackedSeq = confirmed.order().seq();
        }
        if (ackedSeq < -1L || ackedSeq > bounds.largestSequence()) {
            throw reader.gap(ackedSeq, "the recovery cursor does not belong to this retained log: tail="
                    + bounds.largestSequence() + ", trimmedThrough=" + bounds.trimmedThrough());
        }
        reader.cursor = nextSequence(ackedSeq);
        reader.requireRetained(reader.cursor, bounds);
        if (confirmed != null && confirmed.order() == null) {
            throw reader.gap(ackedSeq, "the confirmed position has no verified original capture order");
        }
        if (confirmed != null && confirmed.order() != null
                && confirmed.order().seq() != SourceOrder.SNAPSHOT_SEQ) {
            SourceOrder order = confirmed.order();
            if (order.seq() != ackedSeq || order.epoch() < 1L) {
                throw reader.gap(ackedSeq, "the confirmed position does not prove this table's recovery cursor");
            }
            if (ackedSeq > bounds.trimmedThrough()) {
                SrsLogBatch batch = reader.readDurable(ackedSeq, 1);
                SrsLogRecord record = batch.records().get(ackedSeq);
                if (record.epoch() != order.epoch() || !Objects.equals(record.srcToken(), confirmed.token())) {
                    throw reader.gap(ackedSeq, "the confirmed position differs from the retained record's generation or token");
                }
            }
        }
        return reader;
    }

    private static long nextSequence(long sequence) {
        return Math.addExact(sequence, 1L);
    }

    private long firstDurableSeqAtOrAfter(StartFrom.At at, String retention, SrsLogBounds bounds) {
        long first = nextSequence(bounds.trimmedThrough());
        boolean oldest = true;
        while (first <= bounds.largestSequence()) {
            int size = (int) Math.min(256L, bounds.largestSequence() - first + 1L);
            SrsLogBatch batch = readDurable(first, size);
            for (Map.Entry<Long, SrsLogRecord> entry : batch.records().entrySet()) {
                if (oldest && entry.getValue().ts() > at.epochMilli()) {
                    throw new TapstateException(CaptureError.START_FROM_OUTSIDE_WINDOW, Map.of(
                            "requested", at.instant().toString(),
                            "earliest", Instant.ofEpochMilli(entry.getValue().ts()).toString(),
                            "retention", retention == null ? "unset" : retention), null);
                }
                oldest = false;
                if (entry.getValue().ts() >= at.epochMilli()) {
                    return entry.getKey();
                }
            }
            first += size;
        }
        return nextSequence(bounds.largestSequence());
    }

    private void requireRetained(long sequence, SrsLogBounds bounds) {
        if (sequence <= bounds.trimmedThrough()) {
            throw gap(sequence, "the retained log was trimmed beyond this consumer's confirmed progress");
        }
    }

    private SrsLogBatch readDurable(long first, int size) {
        SrsLogBatch batch = log.readBatch(ringName, first, size);
        requireRetained(first, batch.bounds());
        for (int i = 0; i < size; i++) {
            long sequence = first + i;
            SrsLogRecord record = batch.records().get(sequence);
            if (record == null) {
                throw gap(sequence, "the retained log has no record at the required sequence");
            }
            if (record.epoch() < 1L) {
                throw gap(sequence, "the retained record has no verified original capture generation");
            }
        }
        return batch;
    }

    private TapstateException gap(long sequence, String reason) {
        return new TapstateException(CaptureError.RECOVERY_LOG_GAP,
                Map.of("ring", ringName, "sequence", sequence, "reason", reason), null);
    }

    private static SrsItem itemOf(SrsLogRecord record) {
        return new SrsItem(record.srcToken() == null ? null : new SourcePosition(record.srcToken()),
                record.op(), record.ts(), record.before(), record.after(), record.schemaVer(),
                record.captureFence(), record.epoch());
    }

    private static long resolveStartSeq(SrsRingbuffer ring, StartFrom start, String retention) {
        return switch (start) {
            case StartFrom.Earliest ignored -> ring.headSequence();
            case StartFrom.Latest ignored -> ring.tailSequence() + 1;
            case StartFrom.At at ->
                    firstSeqAtOrAfter(ring, at.instant(), at.epochMilli(), retention);
        };
    }

    /**
     * The first sequence whose change is at or after {@code target}, or just past the tail when every
     * buffered change is older than it. {@code targetMillis} is {@code target} in the form changes are
     * timestamped by, converted once where the start was read and passed in rather than converted again
     * here -- so what is compared against is the value that was proved to be in range, and the instant
     * itself is kept only to quote back in the refusal below.
     *
     * <p>An instant the buffer can no longer reach is refused rather than served from the head. Serving
     * the head is silent: the reader comes up healthy and streams a different stretch than the one asked
     * for, and how different depends on how much the buffer happened to still hold, which moves with
     * load. A refusal names what was asked for, what is still held and the retention that decides how far
     * back that goes, so the caller can widen the retention, pick a reachable start, or read the source
     * directly.
     *
     * <p>An empty buffer is deliberately not refused. Nothing has been buffered yet, which is not the
     * same as having buffered it and dropped it -- refusing here would turn a fresh chain into a race
     * against its own miner. What such a reader misses instead depends on where mining began, which is
     * decided elsewhere and is not visible from here.
     */
    private static long firstSeqAtOrAfter(
            SrsRingbuffer ring, Instant target, long targetMillis, String retention) {
        long head = ring.headSequence();
        long tail = ring.tailSequence();
        if (head <= tail) {
            long oldest = ring.readOne(head).ts();
            if (oldest > targetMillis) {
                throw new TapstateException(CaptureError.START_FROM_OUTSIDE_WINDOW, Map.of(
                        "requested", target.toString(),
                        "earliest", Instant.ofEpochMilli(oldest).toString(),
                        "retention", retention == null ? "unset" : retention), null);
            }
        }
        for (long seq = head; seq <= tail; seq++) {
            if (ring.readOne(seq).ts() >= targetMillis) {
                return seq;
            }
        }
        return tail + 1;
    }

    /**
     * Drains up to {@code max} changes from the cursor to the ring tail, passing each to {@code out} with
     * the sequence the ring assigned it and advancing the cursor past it, and returns how many were
     * emitted. Bounded by {@code max} and by the tail, so it respects the downstream's pull and returns
     * promptly when the ring holds nothing new. When it emits at least one change it reports the last
     * sequence it read to the progress sink; an empty fill advanced nothing and reports nothing.
     *
     * <p>The sequence is handed over rather than left behind because the ring keeps it and the item does
     * not carry it, and it is the only monotonic order over a chain the engine has. A caller projecting
     * into the event currency needs it there; one that does not simply ignores it.
     */
    public int fill(ObjLongConsumer<SrsItem> out, int max) {
        Objects.requireNonNull(out, "out");
        long tail = ring.tailSequence();
        long head = log == null || cursor > tail || max < 1 ? 0L : ring.headSequence();
        int emitted = 0;
        while (cursor <= tail && emitted < max) {
            if (log != null && cursor < head) {
                int size = (int) Math.min((long) max - emitted, Math.min(tail - cursor + 1L,
                        head - cursor));
                SrsLogBatch batch = readDurable(cursor, size);
                for (int i = 0; i < size; i++) {
                    out.accept(itemOf(batch.records().get(cursor)), cursor);
                    cursor++;
                    emitted++;
                }
            } else {
                SrsItem item;
                try {
                    item = ring.readOne(cursor);
                } catch (StaleSequenceException missing) {
                    if (log == null) {
                        throw missing;
                    }
                    throw gap(cursor, "the hot ring lost the required record and the durable log could not serve it");
                }
                if (log != null && (item == null || item.epoch() < 1L)) {
                    throw gap(cursor, item == null ? "the required ring record is missing"
                            : "the retained record has no verified original capture generation");
                }
                out.accept(item, cursor);
                cursor++;
                emitted++;
            }
        }
        if (emitted > 0) {
            onAdvance.accept(cursor - 1);
        }
        return emitted;
    }
}
