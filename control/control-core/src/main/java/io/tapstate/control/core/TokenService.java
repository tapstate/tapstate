package io.tapstate.control.core;

import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The machine-token flow: issue, revoke, list and authenticate long-lived scoped tokens. Unlike the
 * short-lived human session token — a stateless signed credential the {@link TokenSigner} mints and
 * verifies on its own — a machine token is opaque and revocable, so its truth lives in the
 * {@link TokenStore} and every authentication consults it. The service holds no state; it composes
 * the token store, the secret minter and a clock, each bound at the assembly root.
 *
 * <p>The presented token is {@code cyxt_<id>.<secret>}: a fixed scheme prefix, the public id used to
 * look the token up and revoke it, and the one-time bearer secret. Only the secret's hash is stored,
 * so reading the store yields no usable token; the scope is stored server-side and authoritative, so
 * it cannot be forged by tampering with the credential.
 *
 * <p>Authentication does not run a fixed-cost dummy comparison the way the human login does: a login
 * protects a low-entropy username against enumeration, but a token id is a high-entropy handle that
 * cannot be guessed, so an unknown id short-circuits without leaking anything an attacker could use.
 */
public final class TokenService {

    /** The scheme prefix that marks a Tapstate machine token, distinguishing it from a human session token. */
    static final String TOKEN_PREFIX = "cyxt_";

    /** Separates the public id from the secret in a presented token; a character outside the base64url alphabet. */
    private static final char SEPARATOR = '.';

    private final TokenStore tokenStore;
    private final TokenSecrets secrets;
    private final Clock clock;

    public TokenService(TokenStore tokenStore, TokenSecrets secrets, Clock clock) {
        this.tokenStore = Objects.requireNonNull(tokenStore, "tokenStore");
        this.secrets = Objects.requireNonNull(secrets, "secrets");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Whether a presented credential is shaped as a Tapstate machine token. */
    public static boolean isMachineToken(String presented) {
        return presented != null && presented.startsWith(TOKEN_PREFIX);
    }

    /**
     * Issues a new scoped machine token and returns the one-time presented token string. That string
     * is the only time the secret is visible — the store keeps only its hash — so a caller that loses
     * it must issue a fresh token, never recover this one.
     */
    public String create(Scope scope) {
        return persist(prepare(scope));
    }

    /** Prepares a token so an audited caller can record its id before the store mutation. */
    PendingToken prepare(Scope scope) {
        Objects.requireNonNull(scope, "scope");
        return new PendingToken(scope, secrets.generate(), clock.instant());
    }

    /** Persists a prepared token and returns its one-time bearer credential. */
    String persist(PendingToken pending) {
        Objects.requireNonNull(pending, "pending");
        GeneratedSecret minted = pending.secret();
        tokenStore.save(new TokenRecord(
                minted.tokenId(), pending.scope().name(), minted.secretHash(), false, pending.createdAt()));
        return TOKEN_PREFIX + minted.tokenId() + SEPARATOR + minted.secret();
    }

    /**
     * Revokes the token with {@code tokenId} so it can no longer authenticate. Idempotent: revoking an
     * unknown or already-revoked token is a no-op success — revocation only has to guarantee the token
     * cannot authenticate, which is already true in both cases.
     */
    public void revoke(String tokenId) {
        Objects.requireNonNull(tokenId, "tokenId");
        tokenStore.revoke(tokenId);
    }

    /**
     * Authenticates a presented machine token, returning its verified content, or empty when the token
     * is malformed, unknown, revoked, or its secret does not match. Never throws on a bad token — an
     * unauthenticatable token is simply absent, mirroring {@link TokenSigner#verify}.
     */
    public Optional<VerifiedToken> authenticate(String presented) {
        return authenticateBound(presented).map(Authenticated::identity);
    }

    public record Authenticated(VerifiedToken identity, String configurationBinding) {
        public Authenticated {
            Objects.requireNonNull(identity, "identity");
            if (configurationBinding == null || !configurationBinding.matches("[a-f0-9]{64}")) {
                throw new IllegalArgumentException("authenticated token record binding required");
            }
        }
        @Override public String toString() { return "AuthenticatedMachineToken[verified]"; }
    }

    /** Identity and record binding come from the same verified read, never a second lookup by id. */
    public Optional<Authenticated> authenticateBound(String presented) {
        if (!isMachineToken(presented)) {
            return Optional.empty();
        }
        String body = presented.substring(TOKEN_PREFIX.length());
        int separator = body.indexOf(SEPARATOR);
        // A well-formed body is <id>.<secret> with both halves present.
        if (separator <= 0 || separator >= body.length() - 1) {
            return Optional.empty();
        }
        String tokenId = body.substring(0, separator);
        String secret = body.substring(separator + 1);
        Optional<TokenRecord> found = tokenStore.find(tokenId);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        TokenRecord record = found.get();
        if (record.revoked() || !secrets.matches(secret, record.secretHash())) {
            return Optional.empty();
        }
        return Optional.of(new Authenticated(new VerifiedToken(record.tokenId(), scopeOf(record.scope())), record.authenticationBinding()));
    }

    /** All issued tokens as safe, secret-free descriptors, for the {@code token.list} operation. */
    public List<TokenInfo> list() {
        List<TokenInfo> out = new ArrayList<>();
        for (TokenRecord record : tokenStore.list()) {
            out.add(new TokenInfo(
                    record.tokenId(), scopeOf(record.scope()), record.revoked(), record.createdAt()));
        }
        return List.copyOf(out);
    }

    /**
     * Maps a stored scope string to its grade. The store is written and read only here, so an
     * unrecognized value is not a caller outcome but a data-integrity fault seeded into the store: it
     * crashes bare (an invariant violation) rather than being laundered into a user-facing error that
     * would hide the corruption.
     */
    private static Scope scopeOf(String scope) {
        try {
            return Scope.valueOf(scope);
        } catch (IllegalArgumentException unknown) {
            throw new IllegalStateException("unrecognized stored token scope: " + scope, unknown);
        }
    }

    record PendingToken(Scope scope, GeneratedSecret secret, java.time.Instant createdAt) {
        PendingToken {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(secret, "secret");
            Objects.requireNonNull(createdAt, "createdAt");
        }

        String tokenId() {
            return secret.tokenId();
        }
    }
}
