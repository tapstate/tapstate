package io.tapstate.control.core;

import io.tapstate.spi.store.CloudSessionIdentity;

import java.util.Optional;

/** Initial SDK JWT verification; claims are trusted only after this call succeeds. */
@FunctionalInterface
public interface CloudJwtValidator {
    Optional<CloudLoginIdentity> validate(String rawJwt, CloudSessionIdentity expectedDeployment);
}
