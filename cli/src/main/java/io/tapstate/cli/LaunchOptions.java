package io.tapstate.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The options that shape how the CLI starts, parsed off the front of the top-level arguments before a
 * command is chosen. They are a launch concern rather than a verb's: {@code -w} says where a session
 * begins, and {@code -c} / {@code -u} say that it should begin already connected and
 * signed in, so a whole session's worth of work fits on one line.
 *
 * <p>{@code stopAtPositional} is what makes that safe: everything from the first non-option token on is
 * captured verbatim as the command, so a verb's own options ({@code ls -o json}) are handed to the
 * command table untouched rather than being parsed as launch options here.
 *
 * <p>Credentials given this way are never written anywhere — they live in the session's memory for as
 * long as the process does. The password comes from {@code TAPSTATE_PASSWORD} or a masked prompt, so
 * it is never accepted through the process arguments.
 */
@Command(name = "tapstate")
final class LaunchOptions {

    @Option(names = {"-c", "--connect"}, paramLabel = "URL")
    String connect;

    @Option(names = "--context", paramLabel = "NAME")
    String context;

    @Option(names = {"-u", "--user"}, paramLabel = "NAME")
    String user;

    @Option(names = {"-w", "--workdir"}, paramLabel = "DIR",
            defaultValue = "${env:TAPSTATE_WORKDIR:-tap-work}")
    String workdir;

    /** The command to run and its arguments; empty when the CLI was started to open a session. */
    @Parameters
    List<String> command = List.of();

    /** The environment {@code TAPSTATE_PASSWORD} is read from; a scripted stand-in is used in tests. */
    private UnaryOperator<String> env = System::getenv;

    /** The workspace a session starts in. */
    Path root() {
        return Path.of(workdir);
    }

    /** The seed list to reach, or null when this launch does not connect. */
    String connect() {
        return connect;
    }

    /** The durable context explicitly selected for this process, or null when none was named. */
    String context() {
        return context;
    }

    /** The user to sign in as, or null when this launch only connects. */
    String user() {
        return user;
    }

    /** The command to run, empty when a session was asked for. */
    List<String> command() {
        return command;
    }

    /** Replaces the environment the password falls back to; for tests. */
    LaunchOptions withEnv(UnaryOperator<String> replacement) {
        this.env = replacement;
        return this;
    }

    /** Reads one launch-related environment variable through the injectable process environment. */
    String environment(String name) {
        return env.apply(name);
    }

    /** Whether mutually exclusive temporary and durable target flags were supplied together. */
    boolean hasConflictingTargets() {
        return connects() && context != null && !context.isBlank();
    }

    /**
     * The password to sign in with: {@code TAPSTATE_PASSWORD}, else a masked prompt. This avoids
     * exposing a password through the process list or shell history.
     */
    String resolvePassword(Supplier<Prompter> prompter) {
        String fromEnv = env.apply("TAPSTATE_PASSWORD");
        if (fromEnv != null && !fromEnv.isEmpty()) {
            return fromEnv;
        }
        return prompter.get().secret("Password");
    }

    /** Whether this launch asked to reach a server before doing anything else. */
    boolean connects() {
        return connect != null && !connect.isBlank();
    }

    /** Whether this launch asked to sign in as well as connect. */
    boolean signsIn() {
        return connects() && user != null && !user.isBlank();
    }

    /** Whether a command was given, meaning run it once and exit rather than open a session. */
    boolean isOneShot() {
        return !command.isEmpty();
    }

    /**
     * Whether these arguments put {@code -w} before a command, which the command table must reject.
     * Parsing it here would bind the directory to the launch and leave the command on its own default,
     * silently dropping the one the user named; letting the table see the arguments instead produces the
     * usage error that sends them to {@code tapstate <command> -w DIR}, which does work.
     */
    boolean misplacesTheWorkspaceOption(String... args) {
        return isOneShot() && List.of(args).stream()
                .takeWhile(arg -> !command.get(0).equals(arg))
                .anyMatch(arg -> arg.equals("-w") || arg.equals("--workdir")
                        || arg.startsWith("-w=") || arg.startsWith("--workdir="));
    }

    /** Parses the launch options off the front of the arguments. Throws if they are malformed. */
    static LaunchOptions parse(String... args) {
        LaunchOptions options = new LaunchOptions();
        new CommandLine(options)
                // everything from the first non-option token on is the command, captured verbatim: a
                // verb's own options must reach the command table, not be parsed as launch options here
                .setStopAtPositional(true)
                .parseArgs(args);
        options.rejectBlankContext();
        options.consumeAuthContextAfterAction();
        return options;
    }

    /** Rejects an explicitly supplied root selector that cannot name a context. */
    private void rejectBlankContext() {
        if (context != null && context.isBlank()) {
            throw new IllegalArgumentException("--context needs a non-empty name");
        }
    }

    /**
     * Lets one-shot auth follow the documented {@code auth <action> --context NAME} spelling without
     * making unrelated command options look like launch options. The scanner rejects incomplete, empty,
     * and duplicate selectors before resolving a context, so malformed input cannot reach a server.
     */
    private void consumeAuthContextAfterAction() {
        if (command.size() < 2 || !command.getFirst().equals("auth")) {
            return;
        }
        if (command.get(1).equals("--context") || command.get(1).startsWith("--context=")) {
            throw new IllegalArgumentException("auth action must precede --context");
        }
        List<String> remaining = new ArrayList<>(command.size());
        remaining.add(command.getFirst());
        remaining.add(command.get(1));
        for (int index = 2; index < command.size(); index++) {
            String word = command.get(index);
            if (word.equals("--context") && index + 1 < command.size()
                    && !command.get(index + 1).startsWith("-")) {
                selectTrailingAuthContext(command.get(++index));
                continue;
            }
            if (word.startsWith("--context=")) {
                selectTrailingAuthContext(word.substring("--context=".length()));
                continue;
            }
            if (word.equals("--context")) {
                throw new IllegalArgumentException("auth --context needs a non-empty name");
            }
            remaining.add(word);
        }
        command = List.copyOf(remaining);
    }

    /** Records exactly one non-empty trailing context selector for an auth command. */
    private void selectTrailingAuthContext(String value) {
        if (value.isBlank()) {
            throw new IllegalArgumentException("auth --context needs a non-empty name");
        }
        if (context != null) {
            throw new IllegalArgumentException("auth accepts only one --context selector");
        }
        context = value;
    }
}
