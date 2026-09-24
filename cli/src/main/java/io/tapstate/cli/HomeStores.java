package io.tapstate.cli;

import java.nio.file.Path;
import java.time.Clock;
import java.util.function.UnaryOperator;

/**
 * The per-user stores under one home directory - the saved servers and the saved sign-ins - composed
 * in one place.
 *
 * <p>Every entry point that needs them asks here rather than naming the store classes itself: the
 * launch line's session and the guided {@code new} both did their own wiring, which is two answers
 * to which files hold a user's state. Composing them once also keeps the guided first run honest
 * about how it reaches that state - through {@link ContextManager}, {@link ContextResolver} and
 * {@link AuthService} - so that the questions it asks can never write a context or a session file
 * behind those services' backs.
 */
final class HomeStores {

    private HomeStores() {
    }

    /** The saved servers, and the directories bound to them, for a home directory. */
    static ContextManager contexts(Path home) {
        return new ContextManager(ContextConfigStore.underHome(home));
    }

    /** What resolves a verb's target from the saved servers, the environment and the workspace. */
    static ContextResolver resolver(Path home, UnaryOperator<String> env) {
        return new ContextResolver(ContextConfigStore.underHome(home), env);
    }

    /** Sign-in and resume over the saved sessions for a home directory. */
    static AuthService auth(Path home, ControlPlaneClient controlPlane, Clock clock) {
        return new AuthService(controlPlane, AuthFileStore.underHome(home), clock);
    }
}
