package io.tapstate.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Unmatched;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * The handler shared by every server-state verb (registered under each name in {@link Cli}). Without a
 * session the verb is known but unreachable, so it reports a coded diagnostic naming itself — a
 * discoverable affordance, not a missing command. It is reached only when no connection is in play:
 * inside the REPL a connected session routes these verbs to the server instead.
 */
@Command(mixinStandardHelpOptions = true, description = "(requires a connection to a Tapstate server)")
final class ConnectedVerb implements Callable<Integer> {

    /** Preserve option value boundaries while checking for help before a session exists. */
    static void recognizeValueOptionsForHelp(CommandLine verb) {
        // These separate arguments are normally consumed by the session. The help parse only needs
        // to know where they go, including paths like "--help". The regular command table stays intact.
        verb.getCommandSpec().addOption(OptionSpec.builder(
                        "-f", "--file", "-w", "--workdir", "-o", "--output", "--if-match", "--scope",
                        "--decide", "--from", "--to", "--resolution", "--limit", "--cursor", "--table")
                .arity("1").type(String[].class).hidden(true).build());
        verb.setAllowOptionsAsOptionParameters(true);
    }

    @Spec
    CommandSpec spec;

    /**
     * Swallows whatever the verb was given. These verbs are typed with their real operands long before
     * a session exists ({@code apply src.tap.yml}), and the server owns their argument grammar; parsing
     * it here would be a second, drifting copy. Rejecting the operands instead would answer a question
     * about a missing connection with a usage error, which is the one thing this handler exists to
     * prevent — so the arguments are accepted, ignored, and the diagnostic always wins.
     *
     * <p>The single exception is the standard help mixin above, whose options are declared and so are
     * matched before anything reaches here. {@code --help} is the one request whose answer does not
     * depend on holding a connection, and it is asked precisely when the user has not got one; letting
     * it fall through to the diagnostic would refuse to describe a verb on the grounds that it cannot
     * run yet.
     */
    @Unmatched
    List<String> ignored;

    @Override
    public Integer call() {
        Diagnostics.printText(CliIo.err(spec), CliError.NOT_CONNECTED, Map.of("verb", spec.name()));
        return Cli.EXIT_VERB_UNAVAILABLE;
    }
}
