package io.tapstate.control.core;

import java.util.Objects;
import java.util.function.Supplier;

/** Builds and sends one C2 status report; scheduling and provider retry remain outside this core. */
public final class CloudStatusReporter {

    private final String clusterId;
    private final CloudRuntimeStatusProvider provider;
    private final CloudStatusSender sender;
    private final Supplier<String> nonces;

    public CloudStatusReporter(
            String clusterId,
            CloudRuntimeStatusProvider provider,
            CloudStatusSender sender,
            Supplier<String> nonces) {
        if (clusterId == null || clusterId.isBlank()) {
            throw new IllegalArgumentException("clusterId must be non-blank");
        }
        this.clusterId = clusterId;
        this.provider = Objects.requireNonNull(provider, "provider");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.nonces = Objects.requireNonNull(nonces, "nonces");
    }

    public CloudRuntimeStatus report() {
        CloudRuntimeStatus status = Objects.requireNonNull(provider.snapshot(), "status snapshot");
        String nonce = Objects.requireNonNull(nonces.get(), "nonce");
        if (nonce.isBlank()) {
            throw new IllegalStateException("status nonce must be non-blank");
        }
        sender.send(clusterId, nonce, status);
        return status;
    }
}
