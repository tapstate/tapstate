package io.tapstate.app;

import io.tapstate.control.core.Scope;
import io.tapstate.control.core.VerifiedToken;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HMAC session-token signer: an issued token round-trips its subject and grade through
 * verification; a token signed with a different secret, a tampered token, or an expired token all
 * verify to empty. Time is driven by an injected clock so expiry is deterministic.
 */
class HmacTokenSignerTest {

    private static final byte[] SECRET = "top-secret-signing-key".getBytes(StandardCharsets.UTF_8);
    private static final Instant T0 = Instant.parse("2026-07-08T12:00:00Z");

    private static HmacTokenSigner signerAt(Instant now, byte[] secret) {
        return new HmacTokenSigner(secret, Duration.ofMinutes(15), Clock.fixed(now, ZoneOffset.UTC));
    }

    @Test
    void accessTokensRemainBoundToTheirOriginalAuthenticationGeneration() {
        var scope = new io.tapstate.spi.store.CacheAuthScope("cluster", 1,
                io.tapstate.spi.store.StorageBinding.Mode.OP, "8a5bbd9e-e330-4238-a001-98f5f104ef2c", 1);
        var clock = Clock.fixed(T0, ZoneOffset.UTC);
        var original = new HmacTokenSigner(SECRET, Duration.ofMinutes(15), clock, scope);
        String token = original.issue("alice", Scope.ADMIN);
        assertThat(original.verify(token)).contains(new VerifiedToken("alice", Scope.ADMIN));
        var next = new io.tapstate.spi.store.CacheAuthScope(scope.clusterId(), scope.bindingVersion(),
                scope.mode(), scope.cacheIncarnation(), 2);
        var replacement = new HmacTokenSigner(SECRET, Duration.ofMinutes(15), clock, next);
        assertThat(replacement.verify(token)).isEmpty();
        assertThat(original.verify(replacement.issue("alice", Scope.ADMIN))).isEmpty();
        assertThat(replacement.verify(signerAt(T0, SECRET).issue("alice", Scope.ADMIN))).isEmpty();
        assertThat(signerAt(T0, SECRET).verify(token)).isEmpty();
    }

    @Test
    void sameGenerationFromAnotherCacheOrClusterCannotAuthorizeAnAccessToken() {
        var clock = Clock.fixed(T0, ZoneOffset.UTC);
        var mode = io.tapstate.spi.store.StorageBinding.Mode.OP;
        String incarnation = "8a5bbd9e-e330-4238-a001-98f5f104ef2c";
        var original = new HmacTokenSigner(SECRET, Duration.ofMinutes(15), clock,
                new io.tapstate.spi.store.CacheAuthScope("cluster", 1, mode, incarnation, 1));
        String token = original.issue("alice", Scope.ADMIN);
        for (var other : java.util.List.of(
                new io.tapstate.spi.store.CacheAuthScope("other", 1, mode, incarnation, 1),
                new io.tapstate.spi.store.CacheAuthScope("cluster", 2, mode, incarnation, 1),
                new io.tapstate.spi.store.CacheAuthScope("cluster", 1, mode, "ca6be84e-d0e4-4449-8b8f-4a29f6f57583", 1))) {
            assertThat(new HmacTokenSigner(SECRET, Duration.ofMinutes(15), clock, other).verify(token)).isEmpty();
        }
    }

    @Test
    void issuedTokenRoundTripsSubjectAndScope() {
        HmacTokenSigner signer = signerAt(T0, SECRET);

        String token = signer.issue("alice", Scope.ADMIN);

        assertThat(token).isNotBlank();
        assertThat(signer.verify(token)).contains(new VerifiedToken("alice", Scope.ADMIN));
    }

    @Test
    void aTokenSignedWithADifferentSecretDoesNotVerify() {
        String token = signerAt(T0, SECRET).issue("alice", Scope.READ);

        HmacTokenSigner otherKey = signerAt(T0, "a-different-key".getBytes(StandardCharsets.UTF_8));

        assertThat(otherKey.verify(token)).isEmpty();
    }

    @Test
    void aTamperedPayloadDoesNotVerify() {
        HmacTokenSigner signer = signerAt(T0, SECRET);
        String token = signer.issue("alice", Scope.READ);

        // Swap the claims segment for one minted for a higher grade under the same signer: the
        // signature no longer matches the altered claims, so the token is rejected.
        String forgedClaims = signer.issue("alice", Scope.ADMIN).split("\\.")[1];
        String[] parts = token.split("\\.");
        String tampered = parts[0] + "." + forgedClaims + "." + parts[2];

        assertThat(signer.verify(tampered)).isEmpty();
    }

    @Test
    void anExpiredTokenDoesNotVerify() {
        String token = signerAt(T0, SECRET).issue("alice", Scope.WRITE);

        // Advance the clock past the 15-minute lifetime: verification against a later clock is empty.
        HmacTokenSigner later = signerAt(T0.plus(Duration.ofMinutes(16)), SECRET);

        assertThat(later.verify(token)).isEmpty();
    }

    @Test
    void aTokenStillWithinItsLifetimeVerifies() {
        String token = signerAt(T0, SECRET).issue("alice", Scope.WRITE);

        HmacTokenSigner justBeforeExpiry = signerAt(T0.plus(Duration.ofMinutes(14)), SECRET);

        assertThat(justBeforeExpiry.verify(token)).contains(new VerifiedToken("alice", Scope.WRITE));
    }

    @Test
    void aStructurallyMalformedTokenDoesNotVerify() {
        HmacTokenSigner signer = signerAt(T0, SECRET);

        assertThat(signer.verify("not-a-token")).isEmpty();
        assertThat(signer.verify("only.two")).isEmpty();
        // Three segments, but the signature segment is not valid base64url: the decode fails, and a
        // failed decode must be rejected as unverifiable rather than escaping as an exception.
        assertThat(signer.verify("aGVhZGVy.Y2xhaW1z.@@not-base64@@")).isEmpty();
        Optional<VerifiedToken> nullToken = signer.verify(null);
        assertThat(nullToken).isEmpty();
    }
}
