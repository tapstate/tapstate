package io.tapstate.control.core;

/** Revokes the Cluster-side session when its Cloud credential can no longer be renewed. */
@FunctionalInterface
public interface CloudSessionInvalidator {

    void invalidate(String sessionId);
}
