package io.tapstate.e2e;

/**
 * The words a person types that the product spells with more than one verb.
 *
 * <p>{@code restart} is one command at the terminal and is not a fifth lifecycle verb: it expands
 * into a stop and a start, and the only thing that differs between its two forms is the answer the
 * stop carries. So it cannot be spelled from the product's verb enum the way {@link StreamVerb} is,
 * and it does not belong in that enum either — the verb set is four, and a gate says so.
 *
 * <p>It is here rather than left out because the step vocabulary follows what a user can type, not
 * what the control plane exposes. A specification that wants to say "restart it and carry on" has to
 * be able to write the word the person writes; spelling it as the pair it expands into would be a
 * specification about the implementation, and would go on passing if {@code restart} stopped
 * expanding that way.
 *
 * <p>The {@code --rerun} form keeps the flag in its name on purpose. It is the same command with a
 * different answer, and writing it as a separate word ({@code rerun}) would put a verb in the
 * vocabulary that nobody can type.
 */
public enum ComposedVerb {

    /** Cycle the pipeline and carry on from where it got to: the stop keeps what it has. */
    RESTART("restart", false),

    /** Cycle the pipeline and read the whole source again: the stop clears. */
    RESTART_RERUN("restart --rerun", true);

    private final String word;
    private final boolean rereadsEverything;

    ComposedVerb(String word, boolean rereadsEverything) {
        this.word = word;
        this.rereadsEverything = rereadsEverything;
    }

    /** The word an author writes, spelled the way the terminal spells it. */
    public String word() {
        return word;
    }

    /**
     * Whether the stop this expands into clears what the pipeline accumulated. This is the entire
     * difference between the two forms, and it is the answer the product requires a stop to carry.
     */
    public boolean rereadsEverything() {
        return rereadsEverything;
    }
}
