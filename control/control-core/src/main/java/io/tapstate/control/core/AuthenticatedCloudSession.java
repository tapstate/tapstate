package io.tapstate.control.core;

import io.tapstate.spi.store.CloudSessionContext;
import java.util.Objects;

/** Identity and organization context from the same successful durable session authentication. */
public record AuthenticatedCloudSession(VerifiedToken identity, CloudSessionContext clusterContext) {
    public AuthenticatedCloudSession {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(clusterContext, "clusterContext");
    }
}
