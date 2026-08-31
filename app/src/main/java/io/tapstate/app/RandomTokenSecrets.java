package io.tapstate.app;

import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.TokenSecrets;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Mints opaque machine-token secrets from a secure random source and hashes them with SHA-256, bound
 * behind the control ring's {@link TokenSecrets} port at the assembly root. The token id and secret
 * are high-entropy random base64url strings; the stored hash is base64url(SHA-256(secret)). A
 * high-entropy secret needs only a fast one-way hash — unlike a low-entropy password it is not
 * brute-forceable — so the per-request verification stays cheap while a store read still yields no
 * usable token.
 *
 * <p>{@link #matches} recomputes the hash and compares it in constant time (never a short-circuiting
 * equality, which would leak the hash byte by byte through timing). The token id is a public lookup
 * handle (128 bits, ample to be unguessable); the secret is the bearer proof (256 bits, so it resists
 * guessing without a slow hash).
 */
public final class RandomTokenSecrets implements TokenSecrets {

    private static final int ID_BYTES = 16;
    private static final int SECRET_BYTES = 32;
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final Base64.Encoder B64_URL = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64_URL_DEC = Base64.getUrlDecoder();

    private final SecureRandom random = new SecureRandom();

    @Override
    public GeneratedSecret generate() {
        String tokenId = randomBase64Url(ID_BYTES);
        String secret = randomBase64Url(SECRET_BYTES);
        return new GeneratedSecret(tokenId, secret, hash(secret));
    }

    @Override
    public String hash(String presentedSecret) {
        if (presentedSecret == null) {
            throw new IllegalArgumentException("presented secret must not be null");
        }
        return B64_URL.encodeToString(sha256(presentedSecret));
    }

    @Override
    public boolean matches(String presentedSecret, String storedHash) {
        if (presentedSecret == null || storedHash == null) {
            return false;
        }
        byte[] expected;
        try {
            expected = B64_URL_DEC.decode(storedHash);
        } catch (IllegalArgumentException malformedHash) {
            return false;
        }
        // Constant-time comparison: a byte-by-byte short-circuit would leak the stored hash through timing.
        return MessageDigest.isEqual(expected, sha256(presentedSecret));
    }

    private String randomBase64Url(int bytes) {
        byte[] buffer = new byte[bytes];
        random.nextBytes(buffer);
        return B64_URL.encodeToString(buffer);
    }

    private static byte[] sha256(String value) {
        try {
            // MessageDigest is not reusable across calls without reset; a fresh instance per hash is simplest.
            return MessageDigest.getInstance(HASH_ALGORITHM).digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException unavailable) {
            // SHA-256 is guaranteed on every JVM; its absence is a broken platform invariant, not a
            // user-facing condition, so it crashes bare rather than being coded.
            throw new IllegalStateException("SHA-256 unavailable", unavailable);
        }
    }
}
