package io.tapstate.control.core;

import java.util.Objects;
import java.util.Optional;

/**
 * Resolves a presented credential to a verified token through the verifier selected by the deployment.
 * The on-prem constructor routes a {@code cyxt_} machine token to the revocable token store and every
 * other credential to the local signer. A managed deployment supplies its own verifier instead, so an
 * unavailable Cloud verifier cannot silently fall back to either local mechanism. Every successful path
 * converges on one {@link VerifiedToken}; the dispatcher authorizes the grade, not the provider.
 *
 * <p>A bad credential is never an exception, only an absence: a malformed, unknown, revoked, expired or
 * unsigned credential resolves to empty, and the surface turns that into an unauthenticated refusal. The
 * local routing is by prefix and therefore exclusive — a machine token is never verified as a session token,
 * and a session token never touches the token store — so neither mechanism can be probed through the other.
 */
public final class CredentialAuthenticator {

    private final CredentialVerifier verifier;

    public CredentialAuthenticator(TokenService tokenService, TokenSigner tokenSigner) {
        Objects.requireNonNull(tokenService, "tokenService");
        Objects.requireNonNull(tokenSigner, "tokenSigner");
        this.verifier = presented -> TokenService.isMachineToken(presented)
                ? tokenService.authenticate(presented)
                : tokenSigner.verify(presented);
    }

    /** Uses the selected deployment's credential verifier, such as the managed Cloud JWT verifier. */
    public CredentialAuthenticator(CredentialVerifier verifier) {
        this.verifier = Objects.requireNonNull(verifier, "verifier");
    }

    /** Refuses every credential until a required external verifier has been integrated. */
    public static CredentialAuthenticator refusing() {
        return new CredentialAuthenticator(credential -> Optional.empty());
    }

    /**
     * Authenticates {@code presented} and returns its verified content, or empty when it is absent or does
     * not check out. The selected verifier owns provider-specific routing and verification.
     */
    public Optional<VerifiedToken> authenticate(String presented) {
        if (presented == null || presented.isBlank()) {
            return Optional.empty();
        }
        return verifier.verify(presented);
    }
}
