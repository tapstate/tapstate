package io.tapstate.control.core;

import java.util.List;

/**
 * The read face and the one write a pipeline's derived output columns have: what a step was recorded
 * producing, what it produces now, and what the table it writes into actually holds.
 *
 * <p><b>Why this exists at all.</b> A start is refused when a step's derived columns no longer match
 * the ones it was recorded producing. That refusal names what moved, and naming it is not the same as
 * being able to act on it: the question that decides whether the pipeline can carry on is not whether
 * our record agrees with our recomputation, it is whether the target table will hold the new values.
 * A column widened at the source and a target still declared at the old width writes rows that succeed
 * and lose data. So the comparison is three columns, not two.
 *
 * <p><b>The third column is best-effort and says so.</b> Target column types are read from the
 * discovery kept for the connection the pipeline writes through, which exists only if somebody
 * discovered it. Absent, {@link StepReport#targetKnown()} is false and the column reads as unknown
 * rather than as matching - an unknown reported as agreement is the one answer here that would send
 * someone to start a pipeline that then truncates.
 *
 * <p><b>Accepting is a separate, explicit act, and it is deliberately not part of applying.</b> Two
 * things could have carried it: re-applying the same document, or a flag on the start. Both let a
 * start that nobody looked at get past the check - re-applying an unchanged document says nothing
 * about having read the difference, and a flag on start is typed once and then lives in a script. What
 * makes this worth having is the one moment where a person looks at the difference and says to carry
 * on, and only a verb of its own is that moment.
 *
 * <p>Accepting does not touch the target table. Making the target able to hold the new shape is the
 * operator's, in their own database, and it belongs before the accept rather than after: accepting
 * first and altering later leaves a window in which the pipeline is running into a table that cannot
 * hold what it is being sent.
 */
public interface DerivedSchemas {

    /** One report per derived step of the pipeline, in the order the pipeline declares them. */
    List<StepReport> compare(String pipelineId);

    /**
     * Records what each derived step produces now as the shape to hold it to from here, so a start
     * refused over the difference is allowed through. It changes nothing about the pipeline, the
     * target table or what the step will produce - only what the next start is compared against.
     *
     * <p>Audited, and attributed to {@code principal}. Moving what a refusal is measured against is the
     * one act that can turn this check off for a pipeline, so who did it and when is worth as much as
     * the record itself.
     */
    void accept(String principal, String pipelineId);

    /**
     * One derived step: which step, which table it writes into, whether that table's columns could be
     * read at all, and the per-column comparison.
     */
    record StepReport(String step, String targetTable, boolean targetKnown, List<ColumnReport> columns) {
    }

    /**
     * One column across the three sides. Each side is the declared type, or {@code null} where that
     * side does not have the column at all - which is itself the answer for a column that appeared or
     * went, and is why an absent side is not folded into an empty string.
     */
    record ColumnReport(String column, String recorded, String derived, String target) {

        /** Whether the recorded and the recomputed sides agree - the difference a start is refused over. */
        public boolean drifted() {
            return recorded != null && !recorded.equals(derived);
        }
    }
}
