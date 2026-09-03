package io.tapstate.cli;

import java.util.List;

/**
 * The outcome of a remote resume-point read or write-back ({@code /api/pipelines/{id}/position}). Either
 * the server answered with the document, or it refused with a coded reason, or it could not be reached.
 */
sealed interface PositionOutcome {

    /**
     * The server's document, kept twice over.
     *
     * <p>{@code document} is the body exactly as it arrived, and printing that rather than something
     * re-rendered is what makes the round trip exact: what is saved to a file, edited and sent back
     * differs from what the server holds only where the author changed it, so the write can refuse every
     * other difference by name instead of guessing which ones this side introduced.
     *
     * <p>{@code chains} is the same thing read far enough to say a sentence about it — which chain now
     * resumes where, and who else reads it. Nothing is decided from it.
     */
    record Found(String document, List<Chain> chains) implements PositionOutcome {
    }

    /** One chain out of the document: enough to say what happened to it and whose else it is. */
    record Chain(String chainId, String token, List<String> sharedWith) {
    }

    /** The server refused, with a coded reason. */
    record Rejected(String code, String message) implements PositionOutcome {
    }

    /** The landing node could not answer. */
    record Unreachable() implements PositionOutcome {
    }
}
