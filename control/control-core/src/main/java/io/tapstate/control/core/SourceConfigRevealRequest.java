package io.tapstate.control.core;

/** Reserved request shape for a future short-lived, single-Source reveal grant. */
public record SourceConfigRevealRequest(String id, String grant) {
}
