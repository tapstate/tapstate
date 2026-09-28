package io.tapstate.control.core;

import java.time.Instant;
import java.util.Objects;

/**
 * A Cloud-issued user credential held only behind the Cluster session boundary. Its string form never
 * includes the bearer bytes.
 */
public final class CloudUserToken {

    private final String bearer;
    private final String userId;
    private final String organizationId;
    private final String clusterId;
    private final Instant expiresAt;

    public CloudUserToken(
            String bearer, String userId, String organizationId, String clusterId, Instant expiresAt) {
        this.bearer = requireText(bearer, "bearer");
        this.userId = requireText(userId, "userId");
        this.organizationId = requireText(organizationId, "organizationId");
        this.clusterId = requireText(clusterId, "clusterId");
        this.expiresAt = Objects.requireNonNull(expiresAt, "expiresAt");
    }

    public String bearer() {
        return bearer;
    }

    public String userId() {
        return userId;
    }

    public String organizationId() {
        return organizationId;
    }

    public String clusterId() {
        return clusterId;
    }

    public Instant expiresAt() {
        return expiresAt;
    }

    boolean sameIdentity(CloudUserToken other) {
        return userId.equals(other.userId)
                && organizationId.equals(other.organizationId)
                && clusterId.equals(other.clusterId);
    }

    @Override
    public String toString() {
        return "CloudUserToken[userId=" + userId + ", organizationId=" + organizationId
                + ", clusterId=" + clusterId + ", expiresAt=" + expiresAt + ", bearer=<redacted>]";
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Cloud user token " + field + " must be non-blank");
        }
        return value;
    }
}
