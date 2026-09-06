package io.tapstate.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * The command-table face of {@code up}: the flags and the help. The verb itself runs in the session,
 * which is where the connection, the saved sign-in and the calls the stages make all live — the same
 * place every other online verb runs from. This class is reached only when there is no session to
 * route through, so it says what is true then: a connection is needed.
 *
 * <p>The stages are named in the description because they are the words a failure names; a reader
 * who meets "apply workspace failed" should be able to find, from the help alone, where in the order
 * that was.
 *
 * <p>This verb owns every contact the CLI makes with a server outside the verbs that talk to one: the
 * first run on an unbound workspace asks which server to use, starts the local development stack
 * behind that default when nothing is listening, signs in and binds. The scaffolding verbs write
 * files and learn nothing.
 */
@Command(mixinStandardHelpOptions = true,
        description = "Stages, in order: preflight, apply sources, discover, apply workspace, start. "
                + "Runs again safely: an applied, running workspace is left as it is.")
final class UpCmd implements Callable<Integer> {

    /** The stage names, in the order they run; a failure names exactly one of them. */
    static final String STAGE_PREFLIGHT = "preflight";
    static final String STAGE_APPLY_SOURCES = "apply sources";
    static final String STAGE_DISCOVER = "discover";
    static final String STAGE_APPLY_WORKSPACE = "apply workspace";
    static final String STAGE_START = "start";

    @Spec
    CommandSpec spec;

    @Option(names = "--server", paramLabel = "URL",
            description = "Reach this server for this run instead of the one the workspace is bound to; "
                    + "the binding is not changed.")
    String server;

    @Option(names = {"-y", "--yes"},
            description = "Never prompt. The workspace must already be bound, or --server must name a "
                    + "server; the local development stack is never started without --start-local.")
    boolean yes;

    @Option(names = "--start-local",
            description = "Start the local development stack in Docker when nothing is listening on "
                    + ServerBinding.DEFAULT_SERVER_TEXT + ". Never started without it in a script.")
    boolean startLocal;

    @Option(names = {"-u", "--user"}, paramLabel = "NAME",
            description = "Sign in as this user when the workspace is bound for the first time "
                    + "(default admin). The password comes from $" + ServerBinding.PASSWORD_ENV
                    + " or a masked prompt.")
    String user;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format: text (default), json or yaml.")
    OutputFormat output = OutputFormat.TEXT;

    @Option(names = {"-w", "--workdir"}, paramLabel = "DIR",
            description = "The workspace to bring up (default: the session workspace).")
    Path workdir;

    @Override
    public Integer call() {
        Diagnostics.printText(CliIo.err(spec), CliError.NOT_CONNECTED, Map.of("verb", spec.name()));
        return Cli.EXIT_VERB_UNAVAILABLE;
    }
}
