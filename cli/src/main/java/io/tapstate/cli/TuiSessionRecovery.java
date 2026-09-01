package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs discovery and cached-session exchange away from the terminal loop. Its only output is an
 * immutable reducer action, so stale workers cannot write terminal output or mutate application state.
 */
final class TuiSessionRecovery {

    private final Executor executor;
    private final Function<ResolvedContext.Named, Attempt> recovery;
    private final ConcurrentMap<Long, Handoff> handoffs = new ConcurrentHashMap<>();

    TuiSessionRecovery(AuthService authService, Executor executor) {
        this(recoverWith(Objects.requireNonNull(authService, "authService")), executor);
    }

    private static Function<ResolvedContext.Named, Attempt> recoverWith(AuthService auth) {
        return context -> {
            IssuerBinding.Verified discovery = auth.discover(context);
            return new Attempt(discovery.issuer(), auth.resume(context).orElse(null));
        };
    }

    TuiSessionRecovery(Function<ResolvedContext.Named, Attempt> recovery, Executor executor) {
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    void start(TuiContextSessionState state, Consumer<TuiEvent> events) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(events, "events");
        if (!state.recoveryRequested() || state.context() == null) {
            return;
        }
        long generation = state.generation();
        ResolvedContext.Named context = state.context();
        discardExcept(generation);
        executor.execute(() -> {
            TuiContextSessionAction.Recovery completed;
            try {
                Attempt result = recovery.apply(context);
                if (result.activeSession() != null) {
                    handoffs.put(generation, new Handoff(context, result.activeSession()));
                    completed = new TuiContextSessionAction.Recovery.Online(
                            result.activeSession().record().principal(), result.issuer());
                } else {
                    completed = new TuiContextSessionAction.Recovery.SignedOut(result.issuer());
                }
            } catch (TapstateException failure) {
                completed = recoveryState(failure);
            } catch (RuntimeException failure) {
                // A transport adapter may surface an unchecked network failure; never leave the UI
                // in connecting state because a worker failed before it could post its completion.
                completed = new TuiContextSessionAction.Recovery.Offline(
                        CliError.AUTH_SESSION_UNREACHABLE.code());
            }
            events.accept(new TuiEvent.ContextSessionPosted(
                    new TuiContextSessionAction.RecoveryCompleted(generation, completed)));
        });
    }

    Optional<AuthService.ActiveSession> take(TuiContextSessionState state) {
        Objects.requireNonNull(state, "state");
        discardExcept(state.generation());
        Handoff handoff = handoffs.remove(state.generation());
        if (handoff == null || state.connection() != TuiDashboard.Connection.ONLINE
                || !handoff.context().equals(state.context())) {
            return Optional.empty();
        }
        return Optional.of(handoff.activeSession());
    }

    void clear() {
        handoffs.clear();
    }

    private void discardExcept(long generation) {
        handoffs.keySet().removeIf(candidate -> candidate != generation);
    }

    private static TuiContextSessionAction.Recovery recoveryState(TapstateException failure) {
        if (failure.code() == CliError.AUTH_ISSUER_MISMATCH) {
            return new TuiContextSessionAction.Recovery.IssuerMismatch(failure.code().code());
        }
        if (failure.code() == CliError.AUTH_SESSION_REJECTED) {
            return new TuiContextSessionAction.Recovery.SessionExpired(null, null);
        }
        return new TuiContextSessionAction.Recovery.Offline(failure.code().code());
    }

    /** Surface-neutral discovery and cached-session result supplied by the auth service primitives. */
    record Attempt(String issuer, AuthService.ActiveSession activeSession) {
        Attempt {
            if (issuer == null || issuer.isBlank()) {
                throw new IllegalArgumentException("issuer is required");
            }
        }
    }

    /** The active access grant remains private until the UI thread accepts its matching state event. */
    private record Handoff(ResolvedContext.Named context, AuthService.ActiveSession activeSession) {
    }
}
