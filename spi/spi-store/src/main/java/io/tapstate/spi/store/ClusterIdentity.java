package io.tapstate.spi.store;

import java.util.Objects;

/** The one durable identifier from which a Tapstate cluster's issuer is formed. */
public record ClusterIdentity(String clusterId) {

    public static final String ISSUER_PREFIX = "urn:tapstate:cluster:";

    public ClusterIdentity {
        Objects.requireNonNull(clusterId, "clusterId");
        if (clusterId.isBlank() || clusterId.indexOf(':') >= 0) {
            throw new IllegalArgumentException("clusterId must be non-blank and contain no colon");
        }
    }

    public String issuer() {
        return ISSUER_PREFIX + clusterId;
    }
}
