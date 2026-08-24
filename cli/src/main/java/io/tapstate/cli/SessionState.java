package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Process-local access-token state derived from an owner-only opaque session cache. */
final class SessionState {

    private static final Duration REFRESH_SKEW = Duration.ofSeconds(60);
    private static final Set<String> DISPLAYABLE_REJECTION_CODES = Set.of(
            "control.unauthenticated",
            "control.auth-session-expired",
            "control.auth-session-revoked",
            "control.auth-session-identity-mismatch",
            "control.auth-session-invalid-grant");

    private final ControlPlaneClient client;
    private final Clock clock;
    private final Map<AuthSessionRecord, AccessGrant> grants = new HashMap<>();

    SessionState(ControlPlaneClient client, Clock clock) {
        this.client = client;
        this.clock = clock;
    }

    /** Seeds process memory with the short access grant returned alongside a new persistent session. */
    synchronized void remember(AuthSessionRecord cached, String token, Instant expiresAt) {
        if (token == null || token.isBlank() || expiresAt == null || !expiresAt.isAfter(clock.instant())) {
            throw rejected(cached, "control.auth-session-invalid-grant");
        }
        grants.put(cached, new AccessGrant(token, expiresAt));
    }

    synchronized String accessToken(URI baseUrl, AuthSessionRecord cached) {
        Instant now = clock.instant();
        if (!cached.idleExpiresAt().isAfter(now) || !cached.absoluteExpiresAt().isAfter(now)) {
            throw rejected(cached, "control.auth-session-expired");
        }
        AccessGrant current = grants.get(cached);
        if (current != null && current.expiresAt().minus(REFRESH_SKEW).isAfter(now)) {
            return current.token();
        }
        return refresh(baseUrl, cached);
    }

    private String refresh(URI baseUrl, AuthSessionRecord cached) {
        return switch (client.exchangeSession(baseUrl, cached.sessionToken())) {
            case SessionExchangeOutcome.Success success -> accept(cached, success);
            case SessionExchangeOutcome.Rejected rejected -> throw rejected(cached, rejected.code());
            case SessionExchangeOutcome.Unreachable ignored -> throw new TapstateException(
                    CliError.AUTH_SESSION_UNREACHABLE, Map.of("principal", cached.principal()), null);
        };
    }

    private String accept(AuthSessionRecord cached, SessionExchangeOutcome.Success success) {
        if (!cached.issuer().equals(success.issuer())) {
            throw new TapstateException(CliError.AUTH_ISSUER_MISMATCH,
                    Map.of("expected", "cached issuer", "actual", "a different issuer",
                            "seed", "session exchange"),
                    null);
        }
        if (!cached.principal().equals(success.principal()) || !sameScopes(cached.scopes(), success.scopes())) {
            throw rejected(cached, "control.auth-session-identity-mismatch");
        }
        if (success.token() == null || success.token().isBlank() || success.accessExpiresAt() == null
                || !success.accessExpiresAt().isAfter(clock.instant())) {
            throw rejected(cached, "control.auth-session-invalid-grant");
        }
        grants.put(cached, new AccessGrant(success.token(), success.accessExpiresAt()));
        return success.token();
    }

    private static boolean sameScopes(List<String> expected, List<String> actual) {
        return expected.size() == new HashSet<>(expected).size()
                && actual.size() == new HashSet<>(actual).size()
                && new HashSet<>(expected).equals(new HashSet<>(actual));
    }

    private static TapstateException rejected(AuthSessionRecord cached, String code) {
        return new TapstateException(CliError.AUTH_SESSION_REJECTED,
                Map.of("code", safeCode(code), "principal", cached.principal()), null);
    }

    static String safeCode(String code) {
        return DISPLAYABLE_REJECTION_CODES.contains(code) ? code : "unknown";
    }

    private record AccessGrant(String token, Instant expiresAt) {

        @Override
        public String toString() {
            return "AccessGrant[token=<redacted>, expiresAt=" + expiresAt + ']';
        }
    }
}
