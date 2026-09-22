package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.common.TapstateException;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Pins each nest namespace to its resolved operator-state database before the map is created. */
public final class NestStatePlacement {

    private NestStatePlacement() {
    }

    public static void applyTo(HazelcastInstance member, Map<String, String> databases,
            NestSettings settings) {
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(databases, "databases");
        Objects.requireNonNull(settings, "settings");
        Set<String> alreadyMade = mapsAlreadyOn(member);
        databases.forEach((namespace, database) -> {
            MapConfig inherited = member.getConfig().findMapConfig(namespace);
            // A database target has meaning only when a cold layer is actually bound behind the map.
            // Adding a backed configuration to a heap-only member would enable eviction with nowhere to
            // reload from and would also make the factory fail over a store that was deliberately absent.
            if (!inherited.getMapStoreConfig().isEnabled()) {
                return;
            }
            MapConfig wanted = settings.backedStateMaps(namespace, database);
            MapConfig pinned = member.getConfig().getMapConfigs().get(namespace);
            if (pinned != null) {
                refuseIfDifferent(namespace, pinned, wanted);
                return;
            }
            if (alreadyMade.contains(namespace)) {
                refuseIfDifferent(namespace, inherited, wanted);
                return;
            }
            // Repeating the pattern under an exact name would buy no placement or capacity change, but
            // the exact entry cannot be removed while this member lives. Leaving it unpinned is what lets
            // a purged map take a different budget on a later run without requiring a process restart.
            if (equivalent(inherited, wanted)) {
                return;
            }
            member.getConfig().addMapConfig(wanted);
        });
    }

    /** The database an already configured namespace resolves to, or {@code defaultDatabase}. */
    public static String databaseOf(
            HazelcastInstance member, String namespace, String defaultDatabase) {
        String configured = NestMaps.stateDatabase(member.getConfig().findMapConfig(namespace));
        return configured == null ? defaultDatabase : configured;
    }

    private static Set<String> mapsAlreadyOn(HazelcastInstance member) {
        Set<String> names = new HashSet<>();
        for (DistributedObject object : member.getDistributedObjects()) {
            if (object instanceof IMap<?, ?>) {
                names.add(object.getName());
            }
        }
        return names;
    }

    private static void refuseIfDifferent(String namespace, MapConfig configured, MapConfig wanted) {
        String configuredDatabase = NestMaps.stateDatabase(configured);
        String requestedDatabase = NestMaps.stateDatabase(wanted);
        if (!Objects.equals(configuredDatabase, requestedDatabase)) {
            refuseDatabase(namespace, configuredDatabase, requestedDatabase);
        }
        int configuredEntries = configured.getEvictionConfig().getSize();
        int requestedEntries = wanted.getEvictionConfig().getSize();
        if (configuredEntries != requestedEntries) {
            throw new TapstateException(NestError.MEMORY_BUDGET_CHANGED_WHILE_RUNNING,
                    Map.of("namespace", namespace,
                            "configured", (long) configuredEntries,
                            "requested", (long) requestedEntries), null);
        }
    }

    private static boolean equivalent(MapConfig configured, MapConfig wanted) {
        return Objects.equals(NestMaps.stateDatabase(configured), NestMaps.stateDatabase(wanted))
                && configured.getEvictionConfig().getSize() == wanted.getEvictionConfig().getSize();
    }

    private static void refuseDatabase(String namespace, String configured, String requested) {
        throw new TapstateException(NestError.STATE_DATABASE_CHANGED_WHILE_RUNNING,
                Map.of("namespace", namespace,
                        "configured", configured == null ? "default" : configured,
                        "requested", requested), null);
    }
}
