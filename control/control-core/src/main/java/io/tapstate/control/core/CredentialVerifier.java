package io.tapstate.control.core;

import java.util.Optional;

/** Verifies one presented workload credential without exposing its provider-specific implementation. */
@FunctionalInterface
public interface CredentialVerifier {

    Optional<VerifiedToken> verify(String credential);
}
