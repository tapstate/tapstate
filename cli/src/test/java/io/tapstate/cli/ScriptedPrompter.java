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
 *
 * <p>That leniency is bounded. A prompt that re-asks until it gets a non-blank answer never gets one
 * from an exhausted script, so it spins forever: no output, no allocation, no timeout. Measured once
 * at nine hours before anyone looked. Past {@link #EXHAUSTED_LIMIT} answers the script cannot supply,
 * this fails instead, naming the question that kept coming back.
 */
final class ScriptedPrompter implements Prompter {

    /** How many answers to improvise before treating the caller as stuck in a re-ask loop. */
    private static final int EXHAUSTED_LIMIT = 50;

    private final Deque<String> answers;

    private int improvised;

    /** One improvised answer, or a failure naming the question if the caller will not stop asking. */
    private String whenExhausted(String question, String answer) {
        if (++improvised > EXHAUSTED_LIMIT) {
            throw new IllegalStateException(
                    "the script ran out and \"" + question + "\" has been asked " + improvised
                            + " times: the wizard is re-asking until it gets an answer this prompter "
                            + "cannot give. Script an answer for it, or the test hangs rather than fails.");
        }
        return answer;
    }

    /** The option lists passed to each {@link #choose} call, in order — for asserting what was offered. */
    final List<List<String>> offered = new ArrayList<>();

    /** The questions routed through {@link #secret} — for asserting masked prompting was used. */
    final List<String> secretQuestions = new ArrayList<>();

    ScriptedPrompter(String... scripted) {
        this.answers = new ArrayDeque<>(List.of(scripted));
    }

    @Override
    public String ask(String question, String defaultValue) {
        return answers.isEmpty() ? whenExhausted(question, "") : answers.removeFirst();
    }

    @Override
    public String secret(String question) {
        secretQuestions.add(question);
        return answers.isEmpty() ? whenExhausted(question, "") : answers.removeFirst();
    }

    @Override
    public String choose(String question, List<String> options) {
        offered.add(options);
        return answers.isEmpty()
                ? whenExhausted(question, options.get(options.size() - 1))
                : answers.removeFirst();
    }

    @Override
    public String lines(String question) {
        // a whole multi-line block is scripted as one answer (newlines embedded); exhausted = empty block
        return answers.isEmpty() ? whenExhausted(question, "") : answers.removeFirst();
    }
}
