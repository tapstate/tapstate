package io.tapstate.control.core;

import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SessionStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Issues and authenticates revocable opaque user sessions. These credentials are never accepted on the
 * API bearer surface; they only mint short-lived access tokens through the auth endpoints.
 */
public final class SessionService {

    public static final Duration IDLE_TTL = Duration.ofDays(30);
    public static final Duration ABSOLUTE_TTL = Duration.ofDays(90);

    private static final String TOKEN_PREFIX = "tss_";
    private static final char SEPARATOR = '.';

    private final SessionStore sessions;
    private final TokenSecrets secrets;
    private final AccessTokenService accessTokens;
    private final Clock clock;

    public SessionService(
            SessionStore sessions, TokenSecrets secrets, AccessTokenService accessTokens, Clock clock) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.secrets = Objects.requireNonNull(secrets, "secrets");
        this.accessTokens = Objects.requireNonNull(accessTokens, "accessTokens");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CreatedSession create(String principal, Scope scope, String issuer) {
        Objects.requireNonNull(scope, "scope");
        requireText(principal, "principal");
        requireText(issuer, "issuer");
        GeneratedSecret minted = secrets.generate();
        Instant now = clock.instant();
        Instant idleExpiresAt = now.plus(IDLE_TTL);
        Instant absoluteExpiresAt = now.plus(ABSOLUTE_TTL);
        sessions.save(new SessionRecord(
                minted.tokenId(), minted.secretHash(), principal, scope.name(), issuer, false,
                now, now, idleExpiresAt, absoluteExpiresAt));
        return new CreatedSession(TOKEN_PREFIX + minted.tokenId() + SEPARATOR + minted.secret(),
                idleExpiresAt, absoluteExpiresAt);
    }

    public Optional<AccessTokenGrant> exchange(String presented, String expectedIssuer) {
        requireText(expectedIssuer, "expectedIssuer");
        return parse(presented).flatMap(parsed -> {
            Instant now = clock.instant();
            Optional<SessionRecord> touched = sessions.exchange(
                    parsed.sessionId(), secrets.hash(parsed.secret()), expectedIssuer, now, now.plus(IDLE_TTL));
            return touched.map(live -> accessTokens.issue(live.principal(), scopeOf(live.scope())));
        });
    }

    public boolean logout(String presented, String expectedIssuer) {
        requireText(expectedIssuer, "expectedIssuer");
        return parse(presented)
                .map(parsed -> sessions.revoke(parsed.sessionId(), secrets.hash(parsed.secret()),
                        expectedIssuer, clock.instant()))
                .orElse(false);
    }

    public static boolean isSessionToken(String presented) {
        return presented != null && presented.startsWith(TOKEN_PREFIX);
    }

    private static Optional<ParsedSessionToken> parse(String presented) {
        if (!isSessionToken(presented)) {
            return Optional.empty();
        }
        String body = presented.substring(TOKEN_PREFIX.length());
        int separator = body.indexOf(SEPARATOR);
        if (separator <= 0 || separator >= body.length() - 1) {
            return Optional.empty();
        }
        String sessionId = body.substring(0, separator);
        String secret = body.substring(separator + 1);
        if (secret.indexOf(SEPARATOR) >= 0 || !isTokenPart(sessionId) || !isTokenPart(secret)) {
            return Optional.empty();
        }
        return Optional.of(new ParsedSessionToken(sessionId, secret));
    }

    private static boolean isTokenPart(String value) {
        return value.codePoints().allMatch(c -> (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '-'
                || c == '_');
    }

    private static Scope scopeOf(String scope) {
        try {
            return Scope.valueOf(scope);
        } catch (IllegalArgumentException unknown) {
            throw new IllegalStateException("unrecognized stored session scope: " + scope, unknown);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("session " + field + " must be non-blank");
        }
    }

    private record ParsedSessionToken(String sessionId, String secret) {
    }
}
