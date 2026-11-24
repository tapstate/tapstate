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

    public enum Request { ORDINARY, PIPELINE_START, PIPELINE_STOP }

    private final java.util.function.BiFunction<String, Request, Optional<CredentialAuthentication>> contexts;
    private final CredentialVerifier verifier;

    public CredentialAuthenticator(TokenService tokenService, TokenSigner tokenSigner) {
        Objects.requireNonNull(tokenService, "tokenService");
        Objects.requireNonNull(tokenSigner, "tokenSigner");
        this.verifier = presented -> TokenService.isMachineToken(presented)
                ? tokenService.authenticate(presented)
                : tokenSigner.verify(presented);
        this.contexts = null;
    }

    /** Uses the selected deployment's verifier, such as the Cluster-local managed-session verifier. */
    public CredentialAuthenticator(CredentialVerifier verifier) {
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        this.contexts = null;
    }

    private CredentialAuthenticator(java.util.function.BiFunction<String, Request, Optional<CredentialAuthentication>> contexts) {
        this.contexts = Objects.requireNonNull(contexts, "contexts");
        this.verifier = credential -> contexts.apply(credential, Request.ORDINARY).map(CredentialAuthentication::token);
    }

    public static CredentialAuthenticator scopedRequests(java.util.function.BiFunction<String, Request, Optional<CredentialAuthentication>> contexts) {
        return new CredentialAuthenticator(contexts);
    }

    /** The selected provider supplies a fixed scope and an original-credential guard, never a latest-scope lookup. */
    public static CredentialAuthenticator scoped(java.util.function.Function<String, Optional<CredentialAuthentication>> contexts) {
        Objects.requireNonNull(contexts, "contexts");
        return scopedRequests((value, request) -> contexts.apply(value));
    }

    /** Keeps the selected request admission separate from later credential revalidation. */
    public static CredentialAuthenticator scoped(java.util.function.Function<String, Optional<CredentialAuthentication>> contexts,
            java.util.function.Function<String, Optional<CredentialAuthentication>> startContexts) {
        Objects.requireNonNull(contexts, "contexts");
        Objects.requireNonNull(startContexts, "startContexts");
        return scopedRequests((value, request) -> (request == Request.PIPELINE_START ? startContexts : contexts).apply(value));
    }

    public Optional<CredentialAuthentication> authenticateStartContext(String presented) {
        return authenticateContext(presented, Request.PIPELINE_START);
    }

    public Optional<CredentialAuthentication> authenticateContext(String presented) {
        return authenticateContext(presented, Request.ORDINARY);
    }

    public Optional<CredentialAuthentication> authenticateContext(String presented, Request request) {
        if (presented == null || presented.isBlank()) return Optional.empty();
        if (contexts != null) return contexts.apply(presented, request);
        return verifier.verify(presented).map(original -> new CredentialAuthentication(original,
                Optional.empty(), Optional.empty(), () -> {
                    if (!verifier.verify(presented).filter(original::equals).isPresent())
                        throw new io.tapstate.core.common.TapstateException(ControlError.UNAUTHENTICATED, java.util.Map.of(), null);
                }));
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
