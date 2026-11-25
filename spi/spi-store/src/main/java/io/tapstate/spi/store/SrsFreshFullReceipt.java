package io.tapstate.spi.store;

import java.util.Objects;

/** Exact result of one atomic baseline publication; a later cursor is not a replacement receipt. */
public record SrsFreshFullReceipt(SrsChainState chain, SrsConsumerProgress progress) {
    public SrsFreshFullReceipt {
        Objects.requireNonNull(chain, "chain"); Objects.requireNonNull(progress, "progress");
        var full = chain.pendingFreshFull();
        if (full == null || progress.snapshot() == null
                || !chain.miningChainId().equals(progress.context().key().miningChainId())
                || !full.origin().targetScope().pipelineId().equals(progress.context().key().pipelineId())
                || !full.origin().sourceId().equals(progress.context().key().sourceId())
                || full.origin().targetScope().stateGeneration() != progress.context().stateGeneration()
                || !full.models().equals(progress.snapshot().models())
                || full.snapshotEpoch() != progress.snapshot().snapshotEpoch()
                || !chain.resumeOverride().token().equals(progress.snapshot().sourcePosition())
                || !full.models().keySet().equals(progress.ringDoneThrough().keySet())
                || !progress.ringDoneThrough().equals(progress.cursor().perTableSeq())
                || progress.writerRun() != null || progress.cursor().sinkAcked() != null
                || !progress.cursor().sinkAckedByTable().isEmpty() || !progress.cursor().snapshotCompletedTables().isEmpty())
            throw new IllegalArgumentException("incoherent fresh full receipt");
    }
}
