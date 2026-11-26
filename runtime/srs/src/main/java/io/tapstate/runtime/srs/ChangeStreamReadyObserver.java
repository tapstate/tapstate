package io.tapstate.runtime.srs;

/** Member-local callback once a source processor can consume changes for this execution. */
@FunctionalInterface
public interface ChangeStreamReadyObserver {
    String USER_CONTEXT_KEY = "tapstate.srs.change-stream-ready";

    void ready(Object executionIdentity, long jobId, long executionId);
}
