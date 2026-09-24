package io.tapstate.cli;

import java.io.IOException;
import java.nio.file.Files;
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
 *
 * <p>One thing it does not script away: a directory that does not exist. The real runner cannot start
 * any program there and throws before the program is looked up at all, so this one throws the same way,
 * or a caller that runs its preflight in a directory it has not created yet would only fail in production.
 */
final class ScriptedProcessRunner implements ProcessRunner {

    /** Every command run, as {@code <dir>: <words joined by spaces>}, in order; {@code (cwd)} for no directory. */
    final List<String> calls = new ArrayList<>();

    private final Map<String, Result> answers = new LinkedHashMap<>();
    private final Map<String, Runnable> hooks = new LinkedHashMap<>();
    private IOException failure;

    /** Answers {@code command} (the words joined by spaces) with {@code result} instead of exit 0. */
    ScriptedProcessRunner answer(String command, Result result) {
        answers.put(command, result);
        return this;
    }

    /** The same, unless the test already scripted that command - for a helper's defaults. */
    ScriptedProcessRunner answerUnlessScripted(String command, Result result) {
        answers.putIfAbsent(command, result);
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
        calls.add((dir == null ? "(cwd)" : dir.toString()) + ": " + words);
        if (failure != null) {
            throw failure;
        }
        if (dir != null && !Files.isDirectory(dir)) {
            throw new IOException("Cannot run program \"" + command.get(0) + "\" (in directory \"" + dir
                    + "\"): error=2, No such file or directory");
        }
        Runnable hook = hooks.get(words);
        if (hook != null) {
            hook.run();
        }
        return answers.getOrDefault(words, new Result(0, "", ""));
    }
}
