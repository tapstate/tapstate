package io.tapstate.cli;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A test {@link Prompter} that replays a fixed script of answers in order, so the wizard's question
 * flow can be unit-tested without a terminal. It is lenient when the script is exhausted: a free-text
 * question yields an empty answer (the wizard treats that as "skip"), and a choice yields the last
 * option (the wizard's option lists end with a skip / "(none)" sentinel) — so an integration test
 * need only script the answers it cares about and let the rest skip, without coupling to a connector's
 * exact field count.
 */
final class ScriptedPrompter implements Prompter {

    private final Deque<String> answers;

    /** The option lists passed to each {@link #choose} call, in order — for asserting what was offered. */
    final List<List<String>> offered = new ArrayList<>();

    /** The questions routed through {@link #secret} — for asserting masked prompting was used. */
    final List<String> secretQuestions = new ArrayList<>();

    /**
     * The free-text questions asked, in order — for asserting that a path which must not stop and wait
     * did not ask one. A prompt that reaches a process with nothing on its input blocks until something
     * times it out, and a blocked run and a slow one look the same from outside.
     */
    final List<String> questions = new ArrayList<>();

    ScriptedPrompter(String... scripted) {
        this.answers = new ArrayDeque<>(List.of(scripted));
    }

    @Override
    public String ask(String question, String defaultValue) {
        questions.add(question);
        return answers.isEmpty() ? "" : answers.removeFirst();
    }

    @Override
    public String secret(String question) {
        secretQuestions.add(question);
        return answers.isEmpty() ? "" : answers.removeFirst();
    }

    @Override
    public String choose(String question, List<String> options) {
        offered.add(options);
        return answers.isEmpty() ? options.get(options.size() - 1) : answers.removeFirst();
    }

    @Override
    public String lines(String question) {
        // a whole multi-line block is scripted as one answer (newlines embedded); exhausted = empty block
        return answers.isEmpty() ? "" : answers.removeFirst();
    }
}
