package io.tapstate.runtime.engine;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestMemoryBudget;
import io.tapstate.spi.store.IoError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Prepares one member's stable configurations before publishing the original run's local bindings. */
public final class OperatorMapConfigurations {
    private OperatorMapConfigurations() {}

    public static void install(HazelcastInstance member,OperatorRunManifest original,Collection<OperatorMapCapacity> capacities) {
        Objects.requireNonNull(member,"member"); Objects.requireNonNull(original,"original");
        List<OperatorMapCapacity> captured = List.copyOf(capacities);
        var wantedPositions = new HashSet<>(original.bindings().stream().map(OperatorMapBinding::placement).toList());
        var actualPositions = new HashSet<io.tapstate.spi.store.OperatorPlacement>();
        for (var capacity : captured) if (!actualPositions.add(capacity.placement())) throw fenced();
        if (!wantedPositions.equals(actualPositions)) throw fenced();
        var registry = OperatorMapBindings.resolve(member,true);
        synchronized (registry) {
            Object protection = member.getUserContext().get(NestMemoryBudget.SPLIT_BRAIN_PROTECTION_CONTEXT_KEY);
            if (protection != null && (!(protection instanceof String name) || name.isBlank())) throw fenced();
            // Configuration alone grants no cold access. A failed preparation may retain these stable patterns.
            try {
                for (var config : verify(member,registry,original,captured,(String)protection,false)) member.getConfig().addMapConfig(config);
            } catch (InvalidConfigurationException conflicting) { throw fenced(); }
            registry.install(original.bindings(),() -> verify(member,registry,original,captured,(String)protection,true));
        }
    }

    private static List<MapConfig> verify(HazelcastInstance member,OperatorMapBindings registry,OperatorRunManifest original,
            List<OperatorMapCapacity> capacities,String protection,boolean requireInstalled) {
        // Regenerate rather than compare against objects handed to the mutable substrate configuration.
        var configs = capacities.stream().map(capacity -> capacity.configuration(protection)).toList();
        var context = member.getUserContext(); var configuration = member.getConfig();
        Set<String> live = new HashSet<>();
        for (DistributedObject object : member.getDistributedObjects()) if (object instanceof IMap<?,?>) live.add(object.getName());
        if (!Objects.equals(protection,context.get(NestMemoryBudget.SPLIT_BRAIN_PROTECTION_CONTEXT_KEY))) throw fenced();
        var known = configuration.getMapConfigs(); var prospectiveNames = new HashSet<>(known.keySet());
        configs.forEach(config -> prospectiveNames.add(config.getName()));
        var bindings = new HashMap<String,OperatorMapBinding>();
        original.bindings().forEach(binding -> bindings.put(binding.configurationPattern(),binding));
        List<MapConfig> missing = new ArrayList<>();
        for (var wanted : configs) {
            var binding = bindings.get(wanted.getName()); String prefix = wanted.getName().substring(0,wanted.getName().length()-1);
            boolean alreadyBound = registry.alreadyInstalled(binding);
            if (live.stream().anyMatch(name -> name.startsWith(prefix) && (!alreadyBound || !name.equals(binding.mapName())))) throw fenced();
            var pinned = known.get(wanted.getName());
            if (pinned == null) {
                if (requireInstalled) throw fenced();
                missing.add(wanted);
            } else if (!pinned.equals(wanted)) throw fenced();
            // Exact hot configs and more specific patterns must never override the stable typed pattern.
            try {
                if (known.containsKey(binding.mapName()) || !wanted.getName().equals(
                        configuration.getConfigPatternMatcher().matches(prospectiveNames,binding.mapName()))) throw fenced();
            } catch (InvalidConfigurationException ambiguous) { throw fenced(); }
        }
        return missing;
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
}
