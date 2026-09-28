package io.tapstate.control.core;

import io.tapstate.spi.store.CloudSessionIdentity;

import java.util.Optional;

/** Initial online SDK-to-Cloud validation; claims are trusted only after this call succeeds. */
@FunctionalInterface
public interface CloudJwtValidator {
    Optional<CloudLoginIdentity> validate(String rawJwt, CloudSessionIdentity expectedDeployment);
}
