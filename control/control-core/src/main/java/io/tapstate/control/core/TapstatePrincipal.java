package io.tapstate.control.core;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Framework-free identity established by a verified control-plane credential.
 *
 * <p>The principal carries every scope implied by the credential grade so presentation adapters can map it
 * to their own authority representation without reimplementing the {@link Scope} hierarchy. A machine
 * credential retains its stable token id for audit correlation; a human JWT deliberately has no token id.
 */
public record TapstatePrincipal(
        String subject,
        CredentialType credentialType,
        Set<Scope> scopes,
        Optional<String> tokenId) {

    public TapstatePrincipal {
        if (subject == null || subject.isBlank()) {
            throw new IllegalArgumentException("principal subject must be non-blank");
        }
        credentialType = Objects.requireNonNull(credentialType, "credentialType");
        if (scopes == null || scopes.isEmpty()) {
            throw new IllegalArgumentException("principal scopes must be non-empty");
        }
        scopes = Set.copyOf(scopes);
        tokenId = Objects.requireNonNull(tokenId, "tokenId");
        if ((credentialType == CredentialType.HUMAN_JWT || credentialType == CredentialType.CLOUD_SESSION)
                && tokenId.isPresent()) {
            throw new IllegalArgumentException("a human JWT must not carry a machine token id");
        }
        if (credentialType == CredentialType.MACHINE_TOKEN && tokenId.isEmpty()) {
            throw new IllegalArgumentException("a machine token must carry its token id");
        }
    }

    /** Builds the principal for a verified signed human session token. */
    public static TapstatePrincipal humanJwt(VerifiedToken verified) {
        Objects.requireNonNull(verified, "verified");
        return new TapstatePrincipal(
                verified.subject(), CredentialType.HUMAN_JWT, scopesFor(verified.scope()), Optional.empty());
    }

    /** Builds the principal for a verified, revocable machine token. */
    public static TapstatePrincipal machineToken(VerifiedToken verified) {
        Objects.requireNonNull(verified, "verified");
        return new TapstatePrincipal(
                verified.subject(), CredentialType.MACHINE_TOKEN, scopesFor(verified.scope()),
                Optional.of(verified.subject()));
    }

    /** Builds a principal whose authority is the local managed session, not a browser-held Cloud JWT. */
    public static TapstatePrincipal cloudSession(VerifiedToken verified) {
        Objects.requireNonNull(verified, "verified");
        return new TapstatePrincipal(verified.subject(), CredentialType.CLOUD_SESSION,
                scopesFor(verified.scope()), Optional.empty());
    }

    /** Whether this verified identity carries the requested operation grade. */
    public boolean permits(Scope required) {
        return scopes.contains(Objects.requireNonNull(required, "required"));
    }

    private static Set<Scope> scopesFor(Scope grade) {
        Objects.requireNonNull(grade, "grade");
        Set<Scope> granted = EnumSet.noneOf(Scope.class);
        for (Scope scope : Scope.values()) {
            if (grade.permits(scope)) {
                granted.add(scope);
            }
        }
        return granted;
    }
}
