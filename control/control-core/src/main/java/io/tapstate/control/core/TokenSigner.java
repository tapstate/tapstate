package io.tapstate.control.core;

import java.util.Optional;

/**
 * Issues and verifies session tokens. A port so the control layer stays framework-free: the concrete
 * token format and signing algorithm are bound at the assembly root, never referenced here.
 *
 * <p>{@link #issue} mints a signed token binding a subject to a capability grade for a bounded
 * lifetime; {@link #verify} returns the token's verified content, or empty when the signature does
 * not check out, the token has expired, or it is malformed — verification never throws on a bad
 * token, an unverifiable token is simply absent.
 */
public interface TokenSigner {

    /** Mints a signed session token binding {@code subject} to {@code scope} for a bounded lifetime. */
    String issue(String subject, Scope scope);

    /** Returns the verified content of {@code token}, or empty if it is unsigned, expired, or malformed. */
    Optional<VerifiedToken> verify(String token);
    record Detailed(VerifiedToken identity, HumanTokenEvidence continuation) {
        public Detailed { java.util.Objects.requireNonNull(identity); java.util.Objects.requireNonNull(continuation); }
    }
    /** Implementations without verified continuation claims do not authorize asynchronous work. */
    default Optional<Detailed> verifyDetailed(String token) { return Optional.empty(); }
}
