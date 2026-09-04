package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;

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
 * <p>The cursor is run-local and deliberately not fault-tolerant: it is never written into a Jet
 * snapshot, so the source's position never lives in execution state. On an L1 restart the volatile ring
 * is gone; recovery re-mines the ring from the durable source read offset and this reader replays it from
 * the start. The offset truth stays in the durable coordination store, never in Jet.
 */
public final class SrsRingReader {

    private final SrsRingbuffer ring;
    private final LongConsumer onAdvance;
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
        this.ring = Objects.requireNonNull(ring, "ring");
        this.onAdvance = Objects.requireNonNull(onAdvance, "onAdvance");
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

    private static long resolveStartSeq(SrsRingbuffer ring, StartFrom start, String retention) {
        return switch (start) {
            case StartFrom.Earliest ignored -> ring.headSequence();
            case StartFrom.Latest ignored -> ring.tailSequence() + 1;
            case StartFrom.At at -> firstSeqAtOrAfter(ring, at.instant(), retention);
        };
    }

    /**
     * The first sequence whose change is at or after {@code target}, or just past the tail when every
     * buffered change is older than it.
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
    private static long firstSeqAtOrAfter(SrsRingbuffer ring, Instant target, String retention) {
        long head = ring.headSequence();
        long tail = ring.tailSequence();
        long targetMillis = target.toEpochMilli();
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
        int emitted = 0;
        while (cursor <= tail && emitted < max) {
            out.accept(ring.readOne(cursor), cursor);
            cursor++;
            emitted++;
        }
        if (emitted > 0) {
            onAdvance.accept(cursor - 1);
        }
        return emitted;
    }
}
