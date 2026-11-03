package io.tapstate.control.core;

import java.time.Instant;
import java.util.Objects;

/** Nonsecret claims captured by the same successful signature verification; never carries a bearer token. */
public record HumanTokenEvidence(Instant expiresAt, String verifierBinding) {
    public HumanTokenEvidence {
        Objects.requireNonNull(expiresAt, "expiresAt");
        if (verifierBinding == null || !verifierBinding.matches("[a-f0-9]{64}")) { throw new IllegalArgumentException("verified signer binding required"); }
    }
    @Override public String toString() { return "HumanTokenEvidence[verified]"; }
}
