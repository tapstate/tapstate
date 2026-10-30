package io.tapstate.control.core;

import io.tapstate.spi.store.CacheAuthScope;
import java.util.Objects;
import java.util.Optional;

/** The original server-selected authentication context, retained for the lifetime of one request. */
public record CredentialAuthentication(VerifiedToken token, Optional<CacheAuthScope> storageScope,
        Optional<String> organizationId, Runnable requireActive) {
    public CredentialAuthentication {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(storageScope, "storageScope");
        Objects.requireNonNull(organizationId, "organizationId");
        Objects.requireNonNull(requireActive, "requireActive");
        if (organizationId.isPresent() && organizationId.get().isBlank())
            throw new IllegalArgumentException("verified organization must not be blank");
    }
    @Override public String toString() { return "CredentialAuthentication[verified]"; }
}
