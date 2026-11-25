package io.tapstate.spi.store;

import java.util.Objects;

/** The fixed business scope's SRS facets, assembled together on one storage owner. */
public record SrsStores(SrsChainStore chains, SrsModelStore models, SrsLogStore log, SrsConsumerStore consumers) {
    public SrsStores {
        Objects.requireNonNull(chains, "chains"); Objects.requireNonNull(models, "models");
        Objects.requireNonNull(log, "log"); Objects.requireNonNull(consumers, "consumers");
    }
}
