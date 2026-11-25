package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.Map;
import java.util.Objects;

/** One pending source boundary names only tables changed in that batch, never its event bodies. */
public record DirectBatch(DirectChannelContext context,long epoch,long ordinal,ChainPosition boundary,Map<String,Long> targets) {
    public DirectBatch {
        Objects.requireNonNull(context,"context"); Objects.requireNonNull(boundary,"boundary");
        targets = Map.copyOf(Objects.requireNonNull(targets,"targets"));
        if (epoch < 1 || epoch == Long.MAX_VALUE || ordinal < 1 || ordinal == Long.MAX_VALUE || boundary.order().epoch() != epoch
                || boundary.order().seq() < 0 || boundary.order().seq() == Long.MAX_VALUE || boundary.token() == null || boundary.token().isBlank()
                || targets.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || entry.getValue() < 0 || entry.getValue() > boundary.order().seq()))
            throw new IllegalArgumentException("invalid pending direct batch");
    }
}
