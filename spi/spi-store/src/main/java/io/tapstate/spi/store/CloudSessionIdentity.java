package io.tapstate.spi.store;

/** The trusted deployment identity to which a local managed session is bound. */
public record CloudSessionIdentity(String issuer, String organizationId, String clusterId) {

    public CloudSessionIdentity {
        requireText(issuer, "issuer");
        requireText(organizationId, "organizationId");
        requireText(clusterId, "clusterId");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Cloud session identity " + name + " must be non-blank");
        }
    }
}
