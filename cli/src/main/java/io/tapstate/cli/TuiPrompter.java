package io.tapstate.cli;

import java.util.List;
import java.util.Objects;

/**
 * Non-blocking fallback for prompts issued by a command dispatched from the full-screen command bar.
 * The first TUI slice keeps input single-line; empty answers make password and form flows report their
 * normal usage diagnostics instead of dereferencing a missing prompter or stealing the raw terminal.
 */
final class TuiPrompter implements Prompter {

    static final TuiPrompter INSTANCE = new TuiPrompter();

    @FunctionalInterface
    interface TextPrompt {
        String read(String question, String defaultValue, boolean secret);
    }

    @FunctionalInterface
    interface SecretPrompt {
        String read(String question);
    }

    @FunctionalInterface
    interface ChoicePrompt {
        String choose(String question, List<String> options);
    }

    @FunctionalInterface
    interface LinesPrompt {
        String read(String question);
    }

    private final TextPrompt textPrompt;
    private final SecretPrompt secretPrompt;
    private final ChoicePrompt choicePrompt;
    private final LinesPrompt linesPrompt;

    private TuiPrompter() {
        this((question, defaultValue, secret) -> "",
                (question, options) -> options == null || options.isEmpty() ? "" : options.get(options.size() - 1),
                question -> "");
    }

    TuiPrompter(TextPrompt textPrompt, ChoicePrompt choicePrompt, LinesPrompt linesPrompt) {
        this(textPrompt, question -> textPrompt.read(question, "", true), choicePrompt, linesPrompt);
    }

    TuiPrompter(TextPrompt textPrompt, SecretPrompt secretPrompt, ChoicePrompt choicePrompt,
                LinesPrompt linesPrompt) {
        this.textPrompt = Objects.requireNonNull(textPrompt, "textPrompt");
        this.secretPrompt = Objects.requireNonNull(secretPrompt, "secretPrompt");
        this.choicePrompt = Objects.requireNonNull(choicePrompt, "choicePrompt");
        this.linesPrompt = Objects.requireNonNull(linesPrompt, "linesPrompt");
    }

    @Override
    public String ask(String question, String defaultValue) {
        return textPrompt.read(question, defaultValue, false);
    }

    @Override
    public String secret(String question) {
        return secretPrompt.read(question);
    }

    @Override
    public String choose(String question, List<String> options) {
        return choicePrompt.choose(question, options);
    }

    @Override
    public String lines(String question) {
        return linesPrompt.read(question);
    }
}
