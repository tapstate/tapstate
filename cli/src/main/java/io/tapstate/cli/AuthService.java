package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Shared human-login, cached-session recovery, status, and logout service for every CLI face. */
final class AuthService {

    private final ControlPlaneClient client;
    private final AuthFileStore store;
    private final SessionState sessionState;
    private final Clock clock;
    private final Map<UUID, AuthSessionRecord> processOnly = new ConcurrentHashMap<>();

    AuthService(ControlPlaneClient client, AuthFileStore store, Clock clock) {
        this.client = client;
        this.store = store;
        this.clock = clock;
        this.sessionState = new SessionState(client, clock);
    }

    LoginResult login(ResolvedContext.Named context, String username, String password, boolean interactiveTerminal) {
        IssuerBinding.Verified verified = new IssuerBinding(client).verify(context.definition(), null);
        LoginOutcome outcome = verified.withCredential(password,
                (seed, credential) -> client.login(seed, username, credential, true));
        return switch (outcome) {
            case LoginOutcome.Success success -> loginSuccess(context, verified, success, interactiveTerminal);
            case LoginOutcome.Rejected rejected ->
                    new LoginResult.Rejected(safeLoginCode(rejected.code()), username);
            case LoginOutcome.Unreachable ignored -> new LoginResult.Unreachable();
        };
    }

    private LoginResult loginSuccess(ResolvedContext.Named context, IssuerBinding.Verified verified,
                                     LoginOutcome.Success success, boolean interactiveTerminal) {
        if (!success.hasPersistentSession()) {
            return new LoginResult.Unreachable();
        }
        if (!verified.issuer().equals(success.issuer())) {
            throw new TapstateException(CliError.AUTH_ISSUER_MISMATCH,
                    Map.of("expected", "discovered issuer", "actual", "a different issuer",
                            "seed", "login response"), null);
        }
        if (success.accessExpiresAt() == null || !success.accessExpiresAt().isAfter(clock.instant())
                || success.sessionIdleExpiresAt() == null
                || success.sessionAbsoluteExpiresAt() == null
                || success.sessionIdleExpiresAt().isBefore(clock.instant())
                || success.sessionAbsoluteExpiresAt().isBefore(success.sessionIdleExpiresAt())) {
            return new LoginResult.Unreachable();
        }
        AuthSessionRecord record = new AuthSessionRecord(
                AuthSessionRecord.CURRENT_VERSION,
                context.definition().authRef(),
                context.definition().id(),
                success.issuer(),
                success.principal(),
                success.scopes(),
                success.sessionToken(),
                clock.instant(),
                success.sessionIdleExpiresAt(),
                success.sessionAbsoluteExpiresAt());
        AuthFileStore.SaveResult saved = store.save(record, interactiveTerminal);
        if (saved == AuthFileStore.SaveResult.MEMORY_ONLY) {
            processOnly.put(record.authRef(), record);
        } else {
            processOnly.remove(record.authRef());
        }
        sessionState.remember(record, success.token(), success.accessExpiresAt());
        return new LoginResult.Success(
                new ActiveSession(verified.seed(), success.token(), record), saved);
    }

    Optional<ActiveSession> resume(ResolvedContext.Named context) {
        Optional<AuthSessionRecord> cached = cached(context);
        if (cached.isEmpty()) {
            return Optional.empty();
        }
        AuthSessionRecord record = cached.orElseThrow();
        IssuerBinding.Verified verified = new IssuerBinding(client).verify(context.definition(), record.issuer());
        return Optional.of(new ActiveSession(
                verified.seed(), sessionState.accessToken(verified.seed(), record), record));
    }

    Status status(ResolvedContext.Named context) {
        return resume(context).<Status>map(Status.SignedIn::new).orElseGet(Status.SignedOut::new);
    }

    LogoutResult logout(ResolvedContext.Named context, boolean localOnly) {
        Optional<AuthSessionRecord> cached = cached(context);
        if (cached.isEmpty()) {
            return new LogoutResult.SignedOut();
        }
        AuthSessionRecord record = cached.orElseThrow();
        boolean memoryOnly = record.equals(processOnly.get(record.authRef()));
        if (!localOnly) {
            IssuerBinding.Verified verified = new IssuerBinding(client).verify(context.definition(), record.issuer());
            SessionLogoutOutcome outcome = verified.withCredential(record.sessionToken(), client::logoutSession);
            switch (outcome) {
                case SessionLogoutOutcome.Success ignored -> {
                }
                case SessionLogoutOutcome.Rejected rejected -> {
                    return new LogoutResult.Rejected(SessionState.safeCode(rejected.code()), record.principal());
                }
                case SessionLogoutOutcome.Unreachable ignored -> {
                    return new LogoutResult.Unreachable();
                }
            }
        }
        if (memoryOnly) {
            if (!processOnly.remove(record.authRef(), record)
                    && processOnly.containsKey(record.authRef())) {
                return new LogoutResult.CacheChanged();
            }
        } else if (store.delete(record) == AuthFileStore.DeleteResult.CHANGED) {
            return new LogoutResult.CacheChanged();
        }
        processOnly.remove(record.authRef(), record);
        return new LogoutResult.Removed(localOnly);
    }

    private Optional<AuthSessionRecord> cached(ResolvedContext.Named context) {
        AuthSessionRecord memory = processOnly.get(context.definition().authRef());
        if (memory != null) {
            if (!memory.contextId().equals(context.definition().id())) {
                throw new TapstateException(CliError.AUTH_CACHE_INVALID,
                        Map.of("path", "process memory", "reason", "context binding mismatch"), null);
            }
            return Optional.of(memory);
        }
        return store.load(context.definition().authRef(), context.definition().id());
    }

    private static String safeLoginCode(String code) {
        return "control.auth-failed".equals(code) ? code : "unknown";
    }

    record ActiveSession(URI seed, String accessToken, AuthSessionRecord record) {

        @Override
        public String toString() {
            return "ActiveSession[seed=" + seed + ", accessToken=<redacted>, record=" + record + ']';
        }
    }

    sealed interface LoginResult {
        record Success(ActiveSession session, AuthFileStore.SaveResult storage) implements LoginResult {
        }

        record Rejected(String code, String principal) implements LoginResult {
        }

        record Unreachable() implements LoginResult {
        }
    }

    sealed interface Status {
        record SignedIn(ActiveSession session) implements Status {
        }

        record SignedOut() implements Status {
        }
    }

    sealed interface LogoutResult {
        record Removed(boolean localOnly) implements LogoutResult {
        }

        record SignedOut() implements LogoutResult {
        }

        record Rejected(String code, String principal) implements LogoutResult {
        }

        record Unreachable() implements LogoutResult {
        }

        record CacheChanged() implements LogoutResult {
        }
    }
}
