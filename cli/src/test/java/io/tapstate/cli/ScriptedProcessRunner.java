package io.tapstate.cli;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ProcessRunner} that runs nothing: it writes down every command it is handed, with the
 * directory it was to run in, and answers what the test scripted for that command line - exit 0 and no
 * output unless told otherwise. A hook fires when a command runs, so a test can make the world change
 * the way a real {@code docker compose up -d} changes it (the health probe starts answering).
 */
final class ScriptedProcessRunner implements ProcessRunner {

    /** Every command run, as {@code <dir>: <words joined by spaces>}, in order. */
    final List<String> calls = new ArrayList<>();

    private final Map<String, Result> answers = new LinkedHashMap<>();
    private final Map<String, Runnable> hooks = new LinkedHashMap<>();
    private IOException failure;

    /** Answers {@code command} (the words joined by spaces) with {@code result} instead of exit 0. */
    ScriptedProcessRunner answer(String command, Result result) {
        answers.put(command, result);
        return this;
    }

    /** Runs {@code hook} whenever {@code command} (the words joined by spaces) is run. */
    ScriptedProcessRunner when(String command, Runnable hook) {
        hooks.put(command, hook);
        return this;
    }

    /** Makes every run fail the way a process that cannot be started fails. */
    ScriptedProcessRunner failing(IOException failure) {
        this.failure = failure;
        return this;
    }

    @Override
    public Result run(Path dir, List<String> command) throws IOException {
        String words = String.join(" ", command);
        calls.add(dir + ": " + words);
        if (failure != null) {
            throw failure;
        }
        Runnable hook = hooks.get(words);
        if (hook != null) {
            hook.run();
        }
        return answers.getOrDefault(words, new Result(0, "", ""));
    }
}
