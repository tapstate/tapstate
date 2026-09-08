package io.tapstate.core.lifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The kinds of state a pipeline accumulates, and the one place the sentence about them is written. A
 * stop says what it is about to clear -- or keep -- by rendering the declarations it works through, so
 * the two cannot come to disagree.
 *
 * <p>Where each kind actually lives is not here. Namespaces depend on what a pipeline was built from,
 * so they are answered per pipeline by whatever keeps the state; this holds the vocabulary, which is
 * the same for every pipeline and is what a surface with no pipeline in hand can still speak from.
 *
 * <p>Nothing declared anywhere names the user's own database, and the sentence says so out loud. A stop
 * reaches what this product recorded about the pipeline; the rows the pipeline wrote to its target are
 * the user's, and no answer to a stop touches them.
 */
public final class PipelineStateInventory {

    /** Said by every surface that describes a stop, because it is the fear a stop actually raises. */
    public static final String TARGET_UNTOUCHED = "Your target database is not touched either way.";

    /**
     * What clearing costs, said as the thing a reader will actually notice rather than as the name of
     * what was dropped. It belongs to the clearing answer alone -- on the other one it would be false --
     * and it is here rather than at a surface because every surface that offers the choice owes it.
     *
     * <p>It says the position is gone, and stops there. "Reads its whole source again" is the sentence a
     * reader wants and it is not always true: which tables finished their initial load is recorded on
     * the chain, not on the pipeline, so a pipeline leaving a chain that others are still reading has
     * lost its position without that record going anywhere. Saying the stronger thing would be right for
     * the ordinary pipeline and wrong for exactly the arrangement shared mining exists to produce.
     */
    public static final String NEXT_RUN_HAS_NO_POSITION =
            "The run after this one has no position to carry on from.";

    /** What a nest assembled, the shape it assembled under, and the changes it could not assemble. */
    public static final PipelineStateHolding OPERATOR_STATE = PipelineStateHolding.named(
            "what its operators had assembled, and the changes they could not assemble",
            PipelineStateHolding.Scope.PIPELINE);

    /**
     * How far the pipeline's read had got and been confirmed -- what a resume starts from, and the one
     * thing a user notices at once when a stop clears it: the next run reads its whole source again.
     * Kept as a field on the chain's shared record rather than under a namespace of its own, so the act
     * that lets it go is wired where that store is reachable.
     */
    public static final PipelineStateHolding RESUME_POSITION = PipelineStateHolding.named(
            "the position it had read and confirmed up to",
            PipelineStateHolding.Scope.PIPELINE);

    /**
     * What the shared mining chain itself accumulated -- how far it had read, the seam its tail resumes
     * from, the schema it saw, and which tables finished their initial load. It belongs to the chain
     * rather than to any one pipeline on it, so a stop only takes it when the pipeline stopping was the
     * last one reading that chain. Its label says so, because a description that promised to clear it
     * unconditionally would be untrue for every pipeline that shares a chain -- which is the arrangement
     * shared mining exists to produce.
     */
    public static final PipelineStateHolding CHAIN_RECORD = PipelineStateHolding.named(
            "what the shared mining chain had read, once this is the last pipeline reading it",
            PipelineStateHolding.Scope.CHAIN);

    private static final List<PipelineStateHolding> VOCABULARY =
            List.of(OPERATOR_STATE, RESUME_POSITION, CHAIN_RECORD);

    /**
     * Every kind of state this product records about a running pipeline. What a surface says without a
     * pipeline in hand, and the set a per-pipeline answer draws its labels from.
     */
    public static List<PipelineStateHolding> vocabulary() {
        return VOCABULARY;
    }

    /**
     * What a stop is about to do about the state, item by item.
     *
     * <p>Rendered from the declarations rather than written out, so a component that starts keeping
     * state is spoken about by the same edit that gets it cleared. A sentence maintained separately
     * agrees with the clearing only for as long as somebody keeps checking, and when it stops agreeing
     * every word of it is still true -- it is the missing item that a reader has no way to notice.
     */
    public static String describe(boolean purgeState, List<PipelineStateHolding> holdings) {
        Objects.requireNonNull(holdings, "holdings");
        if (holdings.isEmpty()) {
            return purgeState ? "Clears nothing: this pipeline has accumulated none." : "Keeps nothing.";
        }
        String items = holdings.stream()
                .map(PipelineStateHolding::label)
                .collect(Collectors.joining("; "));
        return (purgeState ? "Clears " : "Keeps ") + items + ".";
    }

    /**
     * Both outcomes, for a surface that describes the verb rather than one call of it.
     *
     * <p>Both, not just the clearing one. A description that covers the destructive path and stays
     * silent about the other is harder to catch than one that has gone stale: every word of it is true,
     * and the reader cannot tell that the sentence they needed is the one that is missing.
     */
    public static String describeBothOutcomes() {
        return "purgeState true -- " + describe(true, VOCABULARY)
                + " purgeState false -- " + describe(false, VOCABULARY)
                + " " + TARGET_UNTOUCHED;
    }

    /**
     * The same answer as {@link #describe}, laid out for a surface that shows it rather than says it: a
     * lead line, then one line per item, then what a reader needs beyond the list itself.
     *
     * <p>The same declarations and the same labels, so a surface cannot come to speak about a different
     * set than the one being worked through. Only the lead line turns on the answer -- writing the two
     * outcomes as two texts is what lets the kept one quietly fall behind the cleared one, and the kept
     * one is the half a cautious reader is reading.
     */
    public static List<String> lines(boolean purgeState, List<PipelineStateHolding> holdings) {
        Objects.requireNonNull(holdings, "holdings");
        List<String> lines = new ArrayList<>();
        lines.add(purgeState
                ? "This clears what the pipeline accumulated:"
                : "This keeps what the pipeline accumulated:");
        holdings.forEach(holding -> lines.add("  - " + holding.label()));
        if (purgeState) {
            lines.add(NEXT_RUN_HAS_NO_POSITION);
        }
        lines.add(TARGET_UNTOUCHED);
        return List.copyOf(lines);
    }

    private PipelineStateInventory() {
    }
}
