package io.tapstate.cli;

import java.util.ArrayList;
import java.util.List;

/**
 * Question primitives shared by the resource wizards, so an inline definition and a standalone one ask
 * the same way. Every primitive routes through a {@link Prompter}, never a terminal directly.
 */
final class WizardPrompts {

    /** The sentinel choice meaning "none of the listed sources — let me type an id myself". */
    private static final String OTHER = "(other)";

    private WizardPrompts() {
    }

    /**
     * Ask for a top-level id, re-prompting until it is legal: no dot (the parser forbids it).
     * {@code suggested} must itself be dot-free, so a blank (or exhausted) answer settles the loop on
     * the default.
     */
    static String askId(Prompter prompter, String question, String suggested) {
        while (true) {
            String answer = prompter.ask(question, suggested);
            String id = answer == null || answer.isBlank() ? suggested : answer;
            if (!id.contains(".")) {
                return id;
            }
        }
    }

    /** Normalize a free-text answer: a blank reply becomes {@code null} (the "skip" of an optional field). */
    static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    /** Ask a view's primary key (optional), so an inline view and a standalone one ask it identically. */
    static String askPrimaryKey(Prompter prompter) {
        // Re-asked rather than allowed to be blank: the key is what the view's sink indexes uniquely,
        // so a view scaffolded without one is a document this CLI's own validate refuses.
        String answer = prompter.ask("Primary key", null);
        while (answer == null || answer.isBlank()) {
            answer = prompter.ask("Primary key (required)", null);
        }
        return answer;
    }

    /**
     * Ask for a source id, offering the workspace's existing {@code kind: source} ids as choices when
     * there are any, plus an {@code (other)} escape to free-text. When {@code required}, a blank
     * free-text answer is re-asked — a reference written as an empty id crashes validate.
     */
    static String askSourceRef(Prompter prompter, List<String> existingSourceIds,
                               String question, boolean required) {
        if (existingSourceIds.isEmpty()) {
            return askFreeTextRef(prompter, question, required);
        }
        List<String> options = new ArrayList<>(existingSourceIds);
        options.add(OTHER);
        String chosen = prompter.choose(question, options);
        return OTHER.equals(chosen) ? askFreeTextRef(prompter, question, required) : chosen;
    }

    private static String askFreeTextRef(Prompter prompter, String question, boolean required) {
        String answer = prompter.ask(question, null);
        while (required && (answer == null || answer.isBlank())) {
            answer = prompter.ask(question + " (required)", null);
        }
        return answer;
    }
}
