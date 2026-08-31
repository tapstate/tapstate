package io.tapstate.cli;

import picocli.CommandLine.Command;

/** The discoverable command specification for the interactive full-screen dashboard. */
@Command(name = "tui", mixinStandardHelpOptions = true,
        description = {
                "Open the full-screen terminal dashboard.",
                "Use the command bar to run the same commands as the regular session."})
final class TuiCmd implements Runnable {

    @Override
    public void run() {
        // The real command is intercepted by Cli.main. This no-op keeps tui help and version on the
        // shared picocli command table without making the normal one-shot path enter raw mode.
    }
}
