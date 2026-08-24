package io.tapstate.cli;

import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

/** Help model for persistent human authentication; process dispatch is owned by the shared session service. */
@Command(name = "auth", mixinStandardHelpOptions = true,
        customSynopsis = "tapstate auth <login|status|logout> [ARGS...] [-hV]",
        description = {
                "Sign in to a named context, inspect its cached session, or revoke it.",
                "login stores only an opaque owner-only session; logout revokes it remotely by default.",
                "Use logout --local-only only when the server is unreachable; the remote session then",
                "remains valid until it expires."
        })
final class AuthCmd implements Callable<Integer> {

    @Override
    public Integer call() {
        return Cli.EXIT_VERB_UNAVAILABLE;
    }
}
