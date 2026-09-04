package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/**
 * Where a pipeline picks up from, one entry per mining chain it reads — the document the read face hands
 * out and the one a write-back is edited out of. Read it, change where a chain resumes, hand it back.
 *
 * <p><strong>Per chain, never per table.</strong> A chain is one read of one source's change log, and the
 * position recorded against it is one position for every table on it. The metrics face projects that one
 * value onto each selected table, which is the right shape for reading a run and the wrong one for
 * editing: shown per table it invites setting two tables to two different places, which is not a state
 * the record can hold and not a request that can be refused halfway. What this face shows is the unit
 * that actually exists.
 *
 * <p><strong>A chain can be read by more than one pipeline</strong> — it is keyed by the source's physical
 * coordinates and deliberately excludes the table subset, so two pipelines on one database share it by
 * construction. {@code sharedWith} names the others, because moving where a chain resumes moves it for
 * all of them: what a write-back does to somebody else's pipeline has to be visible before it is asked
 * for, not discovered afterwards.
 *
 * @param pipelineId the pipeline this reading is for. Always set on a reading; on a write-back it is
 *                   whatever the caller sent back and is not read — the request names its pipeline in the
 *                   path, and a document that had to repeat it correctly would be one more thing to get
 *                   wrong on the way to a rewind
 * @param chains     one entry per chain it reads, in the order its sources are declared
 */
public record PipelinePosition(String pipelineId, List<Chain> chains) {

    public PipelinePosition {
        chains = chains == null ? List.of() : List.copyOf(chains);
    }

    /**
     * One chain's entry.
     *
     * @param chainId    the mining chain, and the only handle a write-back names a chain by
     * @param sourceId   the source resource this pipeline reads it through
     * @param tables     the tables selected on it, so the chain is recognisable without decoding its id
     * @param resumeFrom where a read of this chain would start now — the one editable thing here
     * @param recordedAt when {@code resumeFrom} was last written, as an ISO-8601 instant, or null on a
     *                   record that predates the stamp. It answers whether the position is still inside
     *                   the source's retention window, which is what decides whether resuming from it is
     *                   possible at all — an opaque token cannot be looked at and dated. Text rather than
     *                   a moment because it is read and echoed and never computed on, and because this
     *                   document crosses two Jackson configurations that would each need teaching a time
     *                   type
     * @param sinkAcked  how far this pipeline's own sink has confirmed writes, or null before its first
     *                   ack. Not editable, and here because it is the bound the chain's own advance is
     *                   held under: a resume point ahead of it is one this pipeline has not landed yet
     * @param sharedWith the other pipelines recorded on this chain, sorted; empty when it is this
     *                   pipeline's alone
     */
    public record Chain(
            String chainId,
            String sourceId,
            List<String> tables,
            Point resumeFrom,
            String recordedAt,
            Point sinkAcked,
            List<String> sharedWith) {

        public Chain {
            Objects.requireNonNull(chainId, "chainId");
            tables = tables == null ? List.of() : List.copyOf(tables);
            sharedWith = sharedWith == null ? List.of() : List.copyOf(sharedWith);
        }

        /** The chain named and nothing else asserted — the smallest write-back a caller can send. */
        public static Chain resumingAt(String chainId, String token) {
            return new Chain(chainId, null, List.of(), Point.at(token), null, null, List.of());
        }
    }

    /**
     * A position on a chain: the token a read resumes from, and the ring coordinate the engine observed
     * it at.
     *
     * <p>The coordinate is absent on a position that was written back. It says where the engine saw that
     * token go past in the ring, and a written-back position names a spot in the source's own log that no
     * ring here ever saw — so there is no value to put there, and inventing one would put a made-up
     * coordinate into the comparison that decides which changes are safe to forget.
     *
     * @param token the connector's own position, opaque here and the only half a read can start from
     * @param epoch the ring generation it was observed in, or null when it was not observed here
     * @param seq   its sequence within that generation, or null for the same reason
     */
    public record Point(String token, Long epoch, Long seq) {

        /** A position nobody has ranked — the shape a write-back names and the shape one reads back as. */
        public static Point at(String token) {
            return new Point(token, null, null);
        }
    }
}
