package io.tapstate.runtime.engine;

import com.hazelcast.config.MapConfig;
import io.tapstate.runtime.engine.join.JoinMaps;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.OperatorPlacement;
import java.io.Serializable;
import java.util.Objects;

/** The immutable capacity of one stable cold position, independent of its hot execution attempts. */
public record OperatorMapCapacity(OperatorPlacement placement,long entriesHeldInMemory) implements Serializable {
    public OperatorMapCapacity {
        Objects.requireNonNull(placement,"placement");
        if (entriesHeldInMemory < 1 || entriesHeldInMemory > Integer.MAX_VALUE)
            throw new IllegalArgumentException("operator map capacity must be a positive supported entry count");
        if (placement.resource() == OperatorPlacement.Resource.NEST_STATE)
            NestSettings.defaults().withEntriesHeldInMemory(entriesHeldInMemory);
    }
    public MapConfig configuration(String splitBrainProtection) {
        String pattern = OperatorMapBinding.configurationPattern(placement);
        MapConfig config = placement.resource() == OperatorPlacement.Resource.NEST_STATE
                ? NestSettings.defaults().withEntriesHeldInMemory(entriesHeldInMemory).backedStateMaps(pattern,placement.database())
                : JoinMaps.backedStateMaps(pattern,entriesHeldInMemory);
        config.getMapStoreConfig().getProperties().setProperty(OperatorMapBindings.SCOPED_PROPERTY,"true");
        if (splitBrainProtection != null) config.setSplitBrainProtectionName(splitBrainProtection);
        return config;
    }
}
