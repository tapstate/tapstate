package io.tapstate.control.core;

import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.StorageSetupScope;
import java.util.Objects;
import java.util.Optional;

/** The original server-selected authentication context, retained for the lifetime of one request. */
public record CredentialAuthentication(VerifiedToken token, Optional<CacheAuthScope> storageScope,
        Optional<String> organizationId, Runnable requireActive, Optional<CredentialEvidence> evidence,
        Optional<StorageSetupScope> setupScope) {
    public CredentialAuthentication(VerifiedToken token, Optional<CacheAuthScope> storageScope,
            Optional<String> organizationId, Runnable requireActive, Optional<CredentialEvidence> evidence) {
        this(token, storageScope, organizationId, requireActive, evidence, Optional.empty());
    }
    public CredentialAuthentication(VerifiedToken token, Optional<CacheAuthScope> storageScope,
            Optional<String> organizationId, Runnable requireActive) {
        this(token, storageScope, organizationId, requireActive, Optional.empty());
    }
    public CredentialAuthentication {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(storageScope, "storageScope");
        Objects.requireNonNull(organizationId, "organizationId");
        Objects.requireNonNull(requireActive, "requireActive");
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(setupScope, "setupScope");
        if (organizationId.isPresent() && organizationId.get().isBlank())
            throw new IllegalArgumentException("verified organization must not be blank");
    }
    @Override public String toString() { return "CredentialAuthentication[verified]"; }
}
