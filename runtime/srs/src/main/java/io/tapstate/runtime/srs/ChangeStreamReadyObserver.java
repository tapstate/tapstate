package io.tapstate.runtime.srs;

import java.time.Instant;

/** Acknowledges the original readiness fact once the member's publication is durable. */
@FunctionalInterface
public interface ChangeStreamReadyObserver {
    String USER_CONTEXT_KEY = "tapstate.srs.change-stream-ready";

    boolean ready(Object executionIdentity, long jobId, long executionId, Instant readyAt);
}
