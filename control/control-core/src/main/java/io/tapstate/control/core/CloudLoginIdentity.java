package io.tapstate.control.core;

import io.tapstate.spi.store.CloudSessionIdentity;

import java.time.Instant;
import java.util.Objects;

/** Verified login proof returned only after the SDK's initial JWT verification. */
public record CloudLoginIdentity(
        CloudSessionIdentity deployment, String userId, String organizationId, String clusterId,
        String jwtId, Scope scope, Instant jwtExpiresAt) {

    public CloudLoginIdentity(CloudSessionIdentity deployment, String userId, String jwtId,
            Scope scope, Instant jwtExpiresAt) {
        this(deployment, userId, null, null, jwtId, scope, jwtExpiresAt);
    }

    public CloudLoginIdentity {
        Objects.requireNonNull(deployment, "deployment");
        requireText(userId, "userId");
        requireText(jwtId, "jwtId");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(jwtExpiresAt, "jwtExpiresAt");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Cloud login identity " + name + " must be non-blank");
        }
    }
}
