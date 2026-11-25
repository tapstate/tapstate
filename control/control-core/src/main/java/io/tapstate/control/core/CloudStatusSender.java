package io.tapstate.control.core;

/** Provider boundary for sending one nonce-protected C2 status report. */
@FunctionalInterface
public interface CloudStatusSender {

    void send(String clusterId, String nonce, CloudRuntimeStatus status);
}
