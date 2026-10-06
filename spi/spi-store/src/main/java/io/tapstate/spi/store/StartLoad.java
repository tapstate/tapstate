package io.tapstate.spi.store;

import io.tapstate.core.model.ReadMode;

import java.util.Collection;
import java.util.Objects;

/**
 * How a start loads a pipeline's targets: a new full load, a resume from what the pipeline has durably
 * recorded, or a read of changes only.
 *
 * <p><strong>One judgement, made in one place.</strong> Two sides ask it about the same start. The run's
 * preparation acts on the answer -- a new full load is what lets a target be cleared, or refused when it
 * already holds rows -- and the start verb predicts it before anything is written, so that it can tell the
 * person starting the pipeline what is about to happen to their target. If the two each worked it out for
 * themselves they would eventually disagree, and the disagreement is silent in both directions: a start
 * that was told "this resumes" and then clears a target, or one that asked about a full load nobody runs.
 *
 * <p>The answer reads nothing but the pipeline's own consumer records on the chains it reads: each of its
 * source nodes records under a consumer id of its own, and a record from before that is keyed by the
 * pipeline id alone. Any durable progress on any of them -- a read cursor into a ring, a position its sink
 * acknowledged, as a whole or per table, a table whose initial load landed, the seam a load began at --
 * makes the start a resume. Each is written by a run that already began, and a run that began has either
 * loaded what it owed or recorded where it stopped; reading that run's target as fresh would clear or
 * refuse rows it wrote itself.
 *
 * <p>Another pipeline's record on a shared chain says nothing about this one: snapshot completion and the
 * seam belong to the pipeline that recorded them, never to the chain.
 */
public enum StartLoad {

    /** A new initial load into the targets, the case a target's full-load policy governs. */
    FULL_LOAD,
    /** The run carries on from what the pipeline recorded; its targets are not prepared again. */
    RESUME,
    /** The pipeline reads changes only, so no start of it ever loads its targets. */
    CDC_ONLY;

    /**
     * Decides how a start of {@code pipelineId} loads its targets.
     *
     * @param readMode     the pipeline's read mode, null for the default
     * @param pipelineId   the pipeline being started
     * @param chainRecords the durable records of every chain that can hold the pipeline's progress; records
     *                     of other chains must not be passed, and a record that carries no entry for this
     *                     pipeline contributes nothing
     */
    public static StartLoad of(ReadMode readMode, String pipelineId, Collection<SrsMeta> chainRecords) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(chainRecords, "chainRecords");
        if (readMode == ReadMode.CDC_ONLY) {
            return CDC_ONLY;
        }
        boolean progressed = chainRecords.stream()
                .flatMap(record -> record.consumerOffsets().stream())
                .filter(consumer -> SrsConsumerId.belongsTo(consumer.pipelineId(), pipelineId))
                .anyMatch(StartLoad::hasDurableProgress);
        return progressed ? RESUME : FULL_LOAD;
    }

    /** Whether a consumer record carries anything a run already began wrote down. */
    static boolean hasDurableProgress(ConsumerOffset consumer) {
        return !consumer.perTableSeq().isEmpty()
                || consumer.sinkAcked() != null
                || !consumer.sinkAckedByTable().isEmpty()
                || !consumer.snapshotCompletedTables().isEmpty()
                || consumer.cdcStartPosition() != null;
    }
}
