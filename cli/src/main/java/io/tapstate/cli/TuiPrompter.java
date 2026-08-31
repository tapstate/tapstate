package io.tapstate.cli;

import java.util.List;

/**
 * Non-blocking fallback for prompts issued by a command dispatched from the full-screen command bar.
 * The first TUI slice keeps input single-line; empty answers make password and form flows report their
 * normal usage diagnostics instead of dereferencing a missing prompter or stealing the raw terminal.
 */
final class TuiPrompter implements Prompter {

    static final TuiPrompter INSTANCE = new TuiPrompter();

    private TuiPrompter() {
    }

    @Override
    public String ask(String question, String defaultValue) {
        return "";
    }

    @Override
    public String secret(String question) {
        return "";
    }

    @Override
    public String choose(String question, List<String> options) {
        return options == null || options.isEmpty() ? "" : options.get(options.size() - 1);
    }

    @Override
    public String lines(String question) {
        return "";
    }
}
