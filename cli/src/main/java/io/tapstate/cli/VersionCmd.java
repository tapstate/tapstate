package io.tapstate.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

/**
 * Reports the versions in play: this CLI, and the server it is connected to when it is connected to
 * one. Both, from one command, because they are separate builds — the CLI is installed by one path and
 * the server pulled by another — and a reader asked for "the version" has no way to know which of the
 * two a question is about. Offline it says so rather than guessing or staying silent: "not connected"
 * is an answer, and a blank where a number belongs is not.
 *
 * <p>This is the offline half. Connected, the session answers the same word and adds the server's
 * number to the same two lines, so what a reader pastes into a report has one shape either way.
 */
@Command(name = "version", mixinStandardHelpOptions = true,
        description = "Report this CLI's version, and the connected server's when there is one.")
final class VersionCmd implements Callable<Integer> {

    @Spec
    CommandSpec spec;

    /**
     * The one place the pair's shape is decided, shared with the connected path. Two fixed lines, each
     * naming which half it is: this output exists to be pasted into a bug report, and a reader who has
     * to explain which number is which has already lost what the command was for.
     */
    static void render(PrintWriter out, String serverLine) {
        out.println("cli    " + Cli.VERSION_NUMBER);
        out.println("server " + serverLine);
        out.flush();
    }

    @Override
    public Integer call() {
        render(CliIo.out(spec), "not connected");
        return Cli.EXIT_OK;
    }
}
