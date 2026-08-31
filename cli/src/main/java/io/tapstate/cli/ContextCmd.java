package io.tapstate.cli;

import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

/** Help model for the interactive context manager; process dispatch is owned by the REPL surface. */
@Command(name = "context", mixinStandardHelpOptions = true,
        customSynopsis = "tapstate context [-hV]",
        description = "Create, choose, edit, bind, unbind, or delete saved contexts.")
final class ContextCmd implements Callable<Integer> {

    @Override
    public Integer call() {
        return Cli.EXIT_VERB_UNAVAILABLE;
    }
}
