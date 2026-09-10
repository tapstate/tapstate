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
 * number, plus the two things only a server knows: the authoring grammars it accepts and the schema
 * version of the system data it is running against. Those two are left out entirely when offline rather
 * than reported as unknown — nothing about them is knowable without a server, and a line saying so
 * would describe one that was never asked.
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
    static void render(PrintWriter out, String serverLine, String dslLine, String dataLine) {
        out.println("cli    " + Cli.VERSION_NUMBER);
        out.println("server " + serverLine);
        if (dslLine != null) {
            out.println("dsl    " + dslLine);
        }
        if (dataLine != null) {
            out.println("data   " + dataLine);
        }
        out.flush();
    }

    @Override
    public Integer call() {
        render(CliIo.out(spec), "not connected", null, null);
        return Cli.EXIT_OK;
    }
}
