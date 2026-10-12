package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterRun;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * How far a pipeline has durably landed one table's changes when several writers land them: as far as the
 * slowest writer expected to receive them, and never further.
 *
 * <p>Each writer knows only its own progress, and a writer that is ahead proves nothing about one that is
 * behind: the changes it has not written yet are exactly the ones a resume would skip if the faster writer's
 * word stood for both. So the answer is the lowest of them, and there is no answer at all while any writer
 * the run expects has said nothing - a writer that has said nothing may be holding every change there is.
 *
 * <p>Where a read resumes from is a separate question, because only a position a source can resume from
 * carries a token, and a writer's progress may have moved on past its last one by being told nothing more is
 * coming. Any tokened position at or below the lowest progress is safe - every change at or below it is
 * durable at every writer - and the highest of them resumes with the least replayed.
 */
public final class SrsWriterFrontier {

    private SrsWriterFrontier() {
    }

    /**
     * How far every expected writer has landed a table: the lowest progress among them, and the highest
     * tokened position at or below it, or null where no writer has settled one that low.
     */
    public record Landed(SourceOrder durableThrough, ChainPosition resumableAt) {
        public Landed {
            Objects.requireNonNull(durableThrough, "durableThrough");
        }
    }

    /**
     * What {@code run}'s writers have jointly landed of {@code table}, or empty while the run expects no
     * writer for it or any expected writer has reported nothing.
     */
    public static Optional<Landed> landed(WriterRun run, String table) {
        return landed(run, table, run.expectedFor(table));
    }

    /**
     * As above, over {@code writers} only - the writers of one sink, say, where the question is whether that
     * sink has landed something rather than the whole pipeline.
     */
    public static Optional<Landed> landed(WriterRun run, String table, Collection<String> writers) {
        if (writers.isEmpty()) {
            return Optional.empty();
        }
        Map<String, WriterProgress> progress = run.progressFor(table);
        SourceOrder lowest = null;
        for (String writer : writers) {
            WriterProgress reported = progress.get(writer);
            if (reported == null) {
                return Optional.empty();
            }
            if (lowest == null || reported.durableThrough().compareTo(lowest) < 0) {
                lowest = reported.durableThrough();
            }
        }
        ChainPosition resumable = null;
        for (String writer : writers) {
            ChainPosition tokened = progress.get(writer).lastTokened();
            if (tokened != null && tokened.order().compareTo(lowest) <= 0
                    && (resumable == null || tokened.order().compareTo(resumable.order()) > 0)) {
                resumable = tokened;
            }
        }
        return Optional.of(new Landed(lowest, resumable));
    }

    /**
     * Whether {@code landed} has passed the pipeline's initial load: every row of the load sits at the one
     * reserved position beneath every change of its generation, so a progress at or past that position means
     * every writer has written every load row it was given. Never true for a pipeline that recorded no load.
     */
    public static boolean passedLoad(Landed landed, Long snapshotEpoch) {
        return snapshotEpoch != null
                && landed.durableThrough().compareTo(SourceOrder.snapshotRow(snapshotEpoch)) >= 0;
    }
}
