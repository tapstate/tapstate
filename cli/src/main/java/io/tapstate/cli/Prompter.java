package io.tapstate.cli;

import java.util.List;

/**
 * The wizard's input seam: every interactive question goes through this interface, so the question
 * flow is driven by a real JLine line reader in production and by a scripted fake in tests (the
 * symmetric counterpart to {@link CliIo}'s output writers). The wizard never touches a terminal
 * directly.
 */
interface Prompter {

    /** Free-text answer; returns the raw reply (empty string if the user just accepts / skips). The
     *  {@code defaultValue} is shown as a hint — the caller decides what an empty reply means. */
    String ask(String question, String defaultValue);

    /** Masked free-text answer (e.g. a password); returns the raw reply, empty when skipped. */
    String secret(String question);

    /** Pick exactly one of {@code options}; returns the chosen option. */
    String choose(String question, List<String> options);

    /**
     * Pick exactly one of {@code options}, with {@code defaultOption} marked as the one an empty reply
     * takes. The two-argument form reads an empty reply as the last option because the wizards' lists
     * end with a skip sentinel; a picker whose default is not last uses this one. Implementations that
     * cannot show a default fall back to the two-argument form, which is what this default does.
     */
    default String choose(String question, List<String> options, String defaultOption) {
        return choose(question, options);
    }

    /**
     * Capture a multi-line block (e.g. SQL or a JS body); returns the joined lines with no trailing
     * newline (the caller normalizes block-scalar layout). An immediately-finished / skipped block
     * returns the empty string.
     */
    String lines(String question);
}
