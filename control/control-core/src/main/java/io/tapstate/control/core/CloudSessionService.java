package io.tapstate.control.core;

import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.common.TapstateException;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;

/** Cluster-local sliding sessions: the Cloud JWT lifetime is relevant only when the login is created. */
public final class CloudSessionService {

    public static final Duration IDLE_TTL = Duration.ofMinutes(30);
    private static final String PREFIX = "tcs_";
    private final CloudSessionStore sessions;
    private final CloudSessionIdentity identity;
    private final TokenSecrets secrets;
    private final Clock clock;

    public CloudSessionService(
            CloudSessionStore sessions, CloudSessionIdentity identity, TokenSecrets secrets, Clock clock) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.secrets = Objects.requireNonNull(secrets, "secrets");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CloudSessionIdentity identity() {
        return identity;
    }

    public static boolean isSessionToken(String presented) {
        return presented != null && presented.startsWith(PREFIX);
    }

    /** Creates one cookie only from a verified, still-in-window proof for this exact deployment. */
    public Optional<CreatedCloudSession> create(CloudLoginIdentity login) {
        Objects.requireNonNull(login, "login");
        var now = clock.instant();
        if (!identity.equals(login.deployment()) || !login.jwtExpiresAt().isAfter(now)
                || login.scope() == Scope.ADMIN) {
            return Optional.empty();
        }
        GeneratedSecret secret = secrets.generate();
        var expires = now.plus(IDLE_TTL);
        CloudSessionRecord record = new CloudSessionRecord(identity, login.jwtId(), secret.secretHash(),
                login.userId(), login.scope().name(), false, now, now, expires);
        if (!sessions.create(record)) {
            return Optional.empty();
        }
        String id = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(login.jwtId().getBytes(StandardCharsets.UTF_8));
        return Optional.of(new CreatedCloudSession(PREFIX + id + "." + secret.secret(), expires));
    }

    /** Every successful local authentication refreshes idle expiry, without consulting Cloud or the JWT. */
    public Optional<VerifiedToken> authenticate(String cookie) {
        return parse(cookie).flatMap(parsed -> {
            var now = clock.instant();
            return sessions.authenticate(identity, parsed.jwtId(), secrets.hash(parsed.secret()),
                    now, now.plus(IDLE_TTL)).map(record -> new VerifiedToken(record.userId(), scopeOf(record.scope())));
        });
    }

    public boolean logout(String cookie) {
        return parse(cookie).map(parsed -> sessions.logout(
                identity, parsed.jwtId(), secrets.hash(parsed.secret()), clock.instant())).orElse(false);
    }

    /** Caller must already have authenticated the Cloud back-channel callback. */
    public void invalidateJwt(String jwtId) {
        if (jwtId == null || jwtId.isBlank()) {
            throw new IllegalArgumentException("jwtId must be non-blank");
        }
        sessions.invalidate(identity, jwtId, clock.instant());
    }

    private static Optional<CookieParts> parse(String cookie) {
        if (cookie == null || !cookie.startsWith(PREFIX)) {
            return Optional.empty();
        }
        String body = cookie.substring(PREFIX.length());
        int split = body.indexOf('.');
        if (split <= 0 || split == body.length() - 1 || body.indexOf('.', split + 1) >= 0) {
            return Optional.empty();
        }
        String encodedId = body.substring(0, split);
        String secret = body.substring(split + 1);
        if (!tokenPart(encodedId) || !tokenPart(secret)) {
            return Optional.empty();
        }
        try {
            byte[] decoded = Base64.getUrlDecoder().decode(encodedId);
            String jwtId = new String(decoded, StandardCharsets.UTF_8);
            if (jwtId.isBlank() || !encodedId.equals(Base64.getUrlEncoder().withoutPadding().encodeToString(
                    jwtId.getBytes(StandardCharsets.UTF_8)))) {
                return Optional.empty();
            }
            return Optional.of(new CookieParts(jwtId, secret));
        } catch (IllegalArgumentException malformed) {
            return Optional.empty();
        }
    }

    private static boolean tokenPart(String value) {
        return !value.isEmpty() && value.codePoints().allMatch(code -> code >= 'A' && code <= 'Z'
                || code >= 'a' && code <= 'z' || code >= '0' && code <= '9' || code == '-' || code == '_');
    }

    private static Scope scopeOf(String stored) {
        try {
            Scope scope = Scope.valueOf(stored);
            if (scope == Scope.ADMIN) {
                throw unreadableScope();
            }
            return scope;
        } catch (IllegalArgumentException unknown) {
            // A persisted value is untrusted storage data, not a programmer invariant. Enum parser
            // messages quote that value, so never attach the parser cause or value to the diagnostic.
            throw unreadableScope();
        }
    }

    private static TapstateException unreadableScope() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "cloud-session", "field", "scope"), null);
    }

    private record CookieParts(String jwtId, String secret) { }
}
