package io.tapstate.control.core;

import java.util.Objects;
import java.util.Optional;

/** Server-established provenance and a process-local guard, never a client supplied authority flag. */
public record CredentialEvidence(CredentialType type, Optional<String> configurationTokenBinding,
        Runnable requireLocallyActive) {
    public CredentialEvidence {
        Objects.requireNonNull(type, "type"); Objects.requireNonNull(configurationTokenBinding, "configurationTokenBinding");
        Objects.requireNonNull(requireLocallyActive, "requireLocallyActive");
        if ((type == CredentialType.MACHINE_TOKEN) != configurationTokenBinding.isPresent()
                || configurationTokenBinding.filter(value -> !value.matches("[a-f0-9]{64}")).isPresent()) {
            throw new IllegalArgumentException("credential evidence does not match its authenticated mechanism");
        }
    }
    @Override public String toString() { return "CredentialEvidence[verified]"; }
}
