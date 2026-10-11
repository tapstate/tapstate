package io.tapstate.control.core;

import io.tapstate.spi.store.CloudSessionIdentity;

import java.util.Optional;

/** Initial SDK JWT verification scoped to the current request's validated audience. */
@FunctionalInterface
public interface CloudJwtValidator {
    Optional<CloudLoginIdentity> validate(
            String rawJwt, CloudSessionIdentity expectedDeployment, String expectedAudience);
}
