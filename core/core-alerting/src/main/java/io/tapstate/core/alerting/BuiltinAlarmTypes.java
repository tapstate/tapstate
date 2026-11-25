package io.tapstate.core.alerting;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/** The built-in type descriptions. Actual provider capabilities must be supplied separately. */
public final class BuiltinAlarmTypes {
    private static final String RETIRED_NODE_UNAVAILABLE = "cluster.node-unavailable";

    private BuiltinAlarmTypes() { }

    /** A known retired type is still recognizable in historical configuration and delivery records. */
    public static boolean retired(String typeId) {
        return RETIRED_NODE_UNAVAILABLE.equals(typeId);
    }
    public static AlarmTypeCatalog catalog() {
        return new AlarmTypeCatalog(List.of(
                state("pipeline.failed", "pipeline", "pipeline-lifecycle", "condition", AlarmSeverity.MAJOR),
                new AlarmTypeCatalog.Type("pipeline.observation-stale", 1, AlarmTypeCatalog.Kind.STATEFUL, Set.of("pipeline"),
                        "pipeline-observations", "freshness", List.of(seconds("max-age", 5, 3600, 120),
                        seconds("startup-grace", 0, 3600, 120)), AlarmSeverity.WARNING, true,
                        "pipeline.observation-stale.condition", "pipeline.observation-stale.recovery", "pipeline.observation-stale.guidance"),
                state("source.connection-failed", "source", "source-operational-health", "condition", AlarmSeverity.MAJOR),
                state("cluster.storage-unavailable", "cluster-storage", "cluster-storage-health", "condition", AlarmSeverity.CRITICAL),
                state("cluster.config-structure-drift", "cluster", "cluster-structure-inspection", "condition", AlarmSeverity.MAJOR),
                state("cluster.cache-structure-drift", "cluster", "cluster-structure-inspection", "condition", AlarmSeverity.MAJOR)));
    }
    private static AlarmTypeCatalog.Type state(String id, String resource, String provider, String evaluator, AlarmSeverity severity) {
        return new AlarmTypeCatalog.Type(id, 1, AlarmTypeCatalog.Kind.STATEFUL, Set.of(resource), provider, evaluator,
                List.of(seconds("trigger-for", 0, 3600, 0), seconds("resolve-for", 0, 3600, 0)), severity, true,
                id + ".condition", id + ".recovery", id + ".guidance");
    }
    private static AlarmTypeCatalog.Parameter seconds(String name, int min, int max, int defaultValue) {
        return new AlarmTypeCatalog.Parameter(name, AlarmTypeCatalog.ParameterKind.DURATION_SECONDS, "s",
                BigDecimal.valueOf(min), BigDecimal.valueOf(max), BigDecimal.valueOf(defaultValue));
    }
}
