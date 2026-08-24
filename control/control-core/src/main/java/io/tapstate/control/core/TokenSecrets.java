package io.tapstate.control.core;

/**
 * Mints and verifies the secret half of an opaque machine token. A port so the control layer stays
 * framework-free: the random source and the hash algorithm are bound at the assembly root, never
 * referenced here.
 *
 * <p>{@link #generate} produces a fresh public token id, its one-time bearer secret, and the hash of
 * that secret to persist; {@link #matches} verifies a presented secret against a stored hash in
 * constant time. Only the hash is ever stored, so a store read yields no usable token. A machine
 * token secret is high-entropy, so a fast one-way hash suffices — unlike a low-entropy password, it
 * is not brute-forceable — which is why this is a separate port from {@link PasswordHasher} rather
 * than the same slow adaptive hash reused per request.
 */
public interface TokenSecrets {

    /** Generates a fresh token id, its one-time secret, and the secret's stored hash. */
    GeneratedSecret generate();

    /** Hashes a presented high-entropy secret into the canonical value used for conditional storage mutations. */
    default String hash(String presentedSecret) {
        throw new IllegalStateException("token secret implementation does not expose its canonical hash");
    }

    /** Whether {@code presentedSecret} hashes to {@code storedHash}; a constant-time comparison. */
    boolean matches(String presentedSecret, String storedHash);
}
