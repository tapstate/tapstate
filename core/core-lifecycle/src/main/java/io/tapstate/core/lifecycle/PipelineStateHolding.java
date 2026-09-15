package io.tapstate.core.lifecycle;

import java.util.Objects;
import java.util.Set;

/**
 * One kind of state a pipeline accumulates while it runs, as the component that keeps it declares
 * itself: what to call it where a stop says what it is about to do, whose it is, and the namespaces it
 * is kept under.
 *
 * <p>Declared once and read by two, on purpose. A stop that was asked to clear works through the
 * namespaces; a stop's surfaces say what is about to be cleared, or kept, out of the labels. Written as
 * two lists instead, the pair drifts the first time a component is added -- and it drifts in the
 * direction nobody notices, because the text keeps being true about everything it still mentions. What
 * that produces is a stop reporting it cleared everything while leaving something behind, and nothing
 * ever names that state again: state is let go of by naming it, so what nothing names is unreachable.
 *
 * <p>A holding may name no namespace at all. Not every kind of state is kept under one -- a resume
 * position is a field on a shared record rather than a namespace of its own -- and such a holding is
 * still declared so the surfaces speak about it, with the act that lets it go wired where its store is
 * reachable. A holding that does name namespaces needs nothing wired: it is dropped by being named,
 * which is what lets a new one arrive as a declaration and nothing else.
 */
public record PipelineStateHolding(String label, Scope scope, Set<String> namespaces) {

    public PipelineStateHolding {
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(scope, "scope");
        namespaces = Set.copyOf(Objects.requireNonNull(namespaces, "namespaces"));
        if (label.isBlank()) {
            throw new IllegalArgumentException("a state holding needs a non-blank label");
        }
    }

    /** A kind of state, named but not yet located: the declaration a surface can speak from. */
    public static PipelineStateHolding named(String label, Scope scope) {
        return new PipelineStateHolding(label, scope, Set.of());
    }

    /**
     * The same kind of state, located in one pipeline's namespaces. Derived from the declaration rather
     * than written out again, so what a surface calls it and what a stop drops stay one string.
     */
    public PipelineStateHolding in(Set<String> namespaces) {
        return new PipelineStateHolding(label, scope, namespaces);
    }

    /** Whose the state is, which decides when clearing it is this pipeline's to do. */
    public enum Scope {

        /** This pipeline's alone. Clearing it reaches nothing anybody else reads. */
        PIPELINE,

        /**
         * The mining chain's, shared with every other pipeline reading it. Only the last pipeline to
         * leave the chain may clear it: one that leaves while others read would take away what they
         * are reading from, which is the difference between stopping a pipeline and deleting a source.
         */
        CHAIN
    }
}
