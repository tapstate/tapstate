package io.tapstate.runtime.srs;

import java.util.List;
import java.util.Objects;

/**
 * What provisioning a cdc source did to its mining chain. {@code merged} is false when this source opened
 * the chain and true when the chain already existed and the source was force-merged onto it — the signal a
 * caller surfaces as "this config coincides with an already-running capture, so it shares that chain rather
 * than mining the source a second time". {@code tables} is the chain's table set after this source's streams
 * were unioned in. {@code epoch} is the ring generation this source now reads under — a new one when this
 * source opened the chain, the one already running when it merged onto an open one.
 */
public record ProvisionOutcome(MiningChainId chainId, boolean merged, List<String> tables, long epoch) {

    public ProvisionOutcome {
        Objects.requireNonNull(chainId, "chainId");
        if (epoch < 1) {
            throw new IllegalArgumentException("a provisioned chain reads under a generation, got " + epoch);
        }
        tables = List.copyOf(tables);
    }
}
