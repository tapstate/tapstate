package io.tapstate.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The options that shape how the CLI starts, parsed off the front of the top-level arguments before a
 * command is chosen. They are a launch concern rather than a verb's: {@code -w} says where a session
 * begins, and {@code -c} / {@code -u} / {@code -p} say that it should begin already connected and
 * signed in, so a whole session's worth of work fits on one line.
 *
 * <p>{@code stopAtPositional} is what makes that safe: everything from the first non-option token on is
 * captured verbatim as the command, so a verb's own options ({@code ls -o json}) are handed to the
 * command table untouched rather than being parsed as launch options here.
 *
 * <p>Credentials given this way are never written anywhere — they live in the session's memory for as
 * long as the process does. A password in {@code -p} is visible to anything that can read the process
 * list, and stays in the shell's history, which is why omitting it falls back to
 * {@code TAPSTATE_PASSWORD} and then to asking.
 */
@Command(name = "tapstate")
final class LaunchOptions {

    @Option(names = {"-c", "--connect"}, paramLabel = "URL")
    String connect;

    @Option(names = "--context", paramLabel = "NAME")
    String context;

    @Option(names = {"-u", "--user"}, paramLabel = "NAME")
    String user;

    // -p always takes its value. An optional-value form was tried and removed: `-p` followed by a verb
    // swallowed the verb as the password, so `-c URL -u admin -p ls` signed in with the password "ls"
    // and ran nothing. Omitting -p already means "ask me", so the optional form bought nothing for it.
    @Option(names = {"-p", "--password"}, paramLabel = "PW")
    String password;

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
     * The password to sign in with, in the order the ways of giving one should win: the value passed to
     * {@code -p}, else {@code TAPSTATE_PASSWORD}, else asked for.
     *
     * <p>The last two exist because the first is the one that leaks. A password in {@code -p} is in the
     * shell's history and in the process list, readable by anything on the machine; it is supported
     * because a one-line launch is the point of these options, but a script that can avoid it should.
     * Asking is the safest and stays the default when nothing else is given.
     */
    String resolvePassword(Supplier<Prompter> prompter) {
        if (password != null) {
            return password;
        }
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
        return options;
    }
}
