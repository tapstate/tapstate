package io.tapstate.spi.store;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** One atomic dependency view for impact confirmation; its revision does not advance on source batches. */
public record SrsConsumerTopology(CacheBusinessScope scope, String miningChainId, long revision,
                                   List<SrsConsumerMembership> consumers) {
    public SrsConsumerTopology {
        Objects.requireNonNull(scope, "scope");
        consumers = List.copyOf(Objects.requireNonNull(consumers, "consumers"));
        if (miningChainId == null || miningChainId.isBlank() || revision < 0 || revision == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid consumer topology");
        }
        var identities = new HashSet<String>();
        for (var consumer : consumers) {
            if (consumer.detached() || !miningChainId.equals(consumer.context().key().miningChainId())
                    || !identities.add(consumer.context().key().consumerId())) throw new IllegalArgumentException("incoherent consumer topology");
        }
    }
}
