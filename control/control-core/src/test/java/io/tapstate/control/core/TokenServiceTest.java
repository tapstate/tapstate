package io.tapstate.control.core;

import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The machine-token flow: issue an opaque scoped token, authenticate it on later requests, revoke it,
 * and list the issued tokens. Unlike the human login, the token's truth is server-side: only the
 * secret's hash is stored, the scope is authoritative in the store, and revocation takes effect at
 * once because every authentication consults the store. A malformed, unknown, revoked or
 * wrong-secret token authenticates to nothing rather than throwing, and an unrecognized stored scope
 * is a data fault that crashes bare rather than being dressed up as a user error.
 *
 * <p>Both collaborators are test fakes: an in-memory token store, and a secret minter whose generated
 * parts are deterministic and whose {@code matches} is a reversible hash, so a test can assert the
 * exact stored record and round-trip an issued token.
 */
class TokenServiceTest {

    private static final Instant NOW = Instant.parse("2026-07-08T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void authenticatedBindingComesFromTheSameReadWhoseSecretWasVerified() {
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var original = new TokenRecord("token", "ADMIN", "hash:valid", false, NOW);
        var replacement = new TokenRecord("token", "ADMIN", "hash:replacement", false, NOW.plusSeconds(1));
        TokenStore store = new TokenStore() {
            @Override public Optional<TokenRecord> find(String id) { return Optional.of(reads.incrementAndGet() == 1 ? original : replacement); }
            @Override public void save(TokenRecord record) { throw new UnsupportedOperationException(); }
            @Override public void revoke(String id) { throw new UnsupportedOperationException(); }
            @Override public List<TokenRecord> list() { return List.of(); }
        };
        var authenticated = new TokenService(store, new FakeTokenSecrets(), CLOCK).authenticateBound("cyxt_token.valid").orElseThrow();
        assertThat(reads).hasValue(1);
        assertThat(authenticated.identity()).isEqualTo(new VerifiedToken("token", Scope.ADMIN));
        assertThat(authenticated.configurationBinding()).isEqualTo(original.authenticationBinding()).isNotEqualTo(replacement.authenticationBinding());
        assertThat(authenticated.toString()).doesNotContain("hash:valid", "hash:replacement", authenticated.configurationBinding());
    }

    @Test
    void createIssuesAPrefixedTokenAndStoresOnlyTheHashedSecret() {
        FakeTokenStore store = new FakeTokenStore();
        FakeTokenSecrets secrets = new FakeTokenSecrets();
        TokenService service = new TokenService(store, secrets, CLOCK);

        String presented = service.create(Scope.WRITE);

        // The presented token binds the scheme prefix, the public id, and the one-time secret.
        assertThat(presented).isEqualTo("cyxt_tok-1.sec-1");
        TokenRecord stored = store.find("tok-1").orElseThrow();
        assertThat(stored.scope()).isEqualTo("WRITE");
        assertThat(stored.revoked()).isFalse();
        assertThat(stored.createdAt()).isEqualTo(NOW);
        // Only the hash is persisted — the raw secret never lands in the store.
        assertThat(stored.secretHash()).isEqualTo("hash:sec-1");
        assertThat(stored.secretHash()).isNotEqualTo("sec-1"); // the stored value is a hash, not the secret
    }

    @Test
    void authenticateAcceptsAFreshTokenAndReturnsItsIdAndScope() {
        TokenService service = new TokenService(new FakeTokenStore(), new FakeTokenSecrets(), CLOCK);

        String presented = service.create(Scope.ADMIN);
        Optional<VerifiedToken> verified = service.authenticate(presented);

        assertThat(verified).contains(new VerifiedToken("tok-1", Scope.ADMIN));
    }

    @Test
    void authenticateRejectsAnUnknownToken() {
        TokenService service = new TokenService(new FakeTokenStore(), new FakeTokenSecrets(), CLOCK);

        assertThat(service.authenticate("cyxt_nobody.whatever")).isEmpty();
    }

    @Test
    void authenticateRejectsARevokedToken() {
        TokenService service = new TokenService(new FakeTokenStore(), new FakeTokenSecrets(), CLOCK);
        String presented = service.create(Scope.WRITE);

        service.revoke("tok-1");

        assertThat(service.authenticate(presented)).isEmpty();
    }

    @Test
    void authenticateRejectsAWrongSecret() {
        TokenService service = new TokenService(new FakeTokenStore(), new FakeTokenSecrets(), CLOCK);
        service.create(Scope.WRITE); // stores tok-1 with hash:sec-1

        // Same id, tampered secret: the hash will not match.
        assertThat(service.authenticate("cyxt_tok-1.forged")).isEmpty();
    }

    @Test
    void authenticateRejectsMalformedTokens() {
        TokenService service = new TokenService(new FakeTokenStore(), new FakeTokenSecrets(), CLOCK);
        service.create(Scope.WRITE); // stores tok-1

        assertThat(service.authenticate(null)).as("null").isEmpty();
        assertThat(service.authenticate("tok-1.sec-1")).as("no scheme prefix").isEmpty();
        assertThat(service.authenticate("cyxt_tok-1")).as("no separator").isEmpty();
        assertThat(service.authenticate("cyxt_.sec-1")).as("empty id").isEmpty();
        assertThat(service.authenticate("cyxt_tok-1.")).as("empty secret").isEmpty();
    }

    @Test
    void revokeIsIdempotentForAnUnknownToken() {
        FakeTokenStore store = new FakeTokenStore();
        TokenService service = new TokenService(store, new FakeTokenSecrets(), CLOCK);

        assertThatCode(() -> service.revoke("never-issued")).doesNotThrowAnyException();
        assertThat(store.list()).isEmpty();
    }

    @Test
    void listReturnsSecretFreeDescriptorsWithRevocationState() {
        FakeTokenStore store = new FakeTokenStore();
        TokenService service = new TokenService(store, new FakeTokenSecrets(), CLOCK);
        service.create(Scope.WRITE);  // tok-1
        service.create(Scope.ADMIN);  // tok-2
        service.revoke("tok-1");

        List<TokenInfo> listed = service.list();

        assertThat(listed).containsExactlyInAnyOrder(
                new TokenInfo("tok-1", Scope.WRITE, true, NOW),
                new TokenInfo("tok-2", Scope.ADMIN, false, NOW));
    }

    @Test
    void anUnrecognizedStoredScopeCrashesBareRatherThanAsACodedError() {
        FakeTokenStore store = new FakeTokenStore();
        // Seed a corrupt record directly: a scope string that is no grade. Reached only after the
        // secret verifies, so it is a data-integrity fault, not an authentication outcome.
        store.save(new TokenRecord("tok-x", "SUPERUSER", "hash:sec-x", false, NOW));
        TokenService service = new TokenService(store, new FakeTokenSecrets(), CLOCK);

        Throwable thrown = catchThrowableOfType(RuntimeException.class,
                () -> service.authenticate("cyxt_tok-x.sec-x"));

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(thrown).isNotInstanceOf(io.tapstate.core.common.TapstateException.class);
    }

    /** An in-memory token store keyed by token id. */
    private static final class FakeTokenStore implements TokenStore {
        private final Map<String, TokenRecord> byId = new LinkedHashMap<>();

        @Override
        public void save(TokenRecord record) {
            byId.put(record.tokenId(), record);
        }

        @Override
        public Optional<TokenRecord> find(String tokenId) {
            return Optional.ofNullable(byId.get(tokenId));
        }

        @Override
        public void revoke(String tokenId) {
            TokenRecord existing = byId.get(tokenId);
            if (existing != null) {
                byId.put(tokenId, new TokenRecord(existing.tokenId(), existing.scope(),
                        existing.secretHash(), true, existing.createdAt()));
            }
        }

        @Override
        public List<TokenRecord> list() {
            return new ArrayList<>(byId.values());
        }
    }

    /**
     * A deterministic secret minter: each generate() hands out the next tok-N / sec-N pair with a
     * reversible hash, so a test can predict the issued token and the stored record; matches() reverses
     * that hash.
     */
    private static final class FakeTokenSecrets implements TokenSecrets {
        private int counter;

        @Override
        public GeneratedSecret generate() {
            counter++;
            return new GeneratedSecret("tok-" + counter, "sec-" + counter, "hash:sec-" + counter);
        }

        @Override
        public boolean matches(String presentedSecret, String storedHash) {
            return storedHash.equals("hash:" + presentedSecret);
        }
    }
}
