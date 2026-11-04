package io.tapstate.core.alerting;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/** The initial five type descriptions. Actual provider capabilities must be supplied separately. */
public final class BuiltinAlarmTypes {
    private BuiltinAlarmTypes() { }
    public static AlarmTypeCatalog catalog() {
        return new AlarmTypeCatalog(List.of(
                state("pipeline.failed", "pipeline", "pipeline-lifecycle", "condition", AlarmSeverity.MAJOR),
                new AlarmTypeCatalog.Type("pipeline.observation-stale", 1, AlarmTypeCatalog.Kind.STATEFUL, Set.of("pipeline"),
                        "pipeline-observations", "freshness", List.of(seconds("max-age", 5, 3600, 120),
                        seconds("startup-grace", 0, 3600, 120)), AlarmSeverity.WARNING, true,
                        "pipeline.observation-stale.condition", "pipeline.observation-stale.recovery", "pipeline.observation-stale.guidance"),
                state("source.connection-failed", "source", "source-operational-health", "condition", AlarmSeverity.MAJOR),
                state("cluster.node-unavailable", "cluster-node", "cluster-member-health", "condition", AlarmSeverity.MAJOR),
                state("cluster.storage-unavailable", "cluster-storage", "cluster-storage-health", "condition", AlarmSeverity.CRITICAL)));
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
