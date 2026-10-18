package io.tapstate.app;

import io.tapstate.runtime.engine.nest.NestStateLedger;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** The planner's shape comparison over one pass's fixed Pipeline scope and resolved CACHE databases. */
final class ScopedNestStateLedger implements NestStateLedger {
    private static final long serialVersionUID = 1L;
    private final transient PipelineConvergencePass pass;
    private final String defaultDatabase;
    private final Map<String, String> databases;

    ScopedNestStateLedger(PipelineConvergencePass pass, String defaultDatabase, Map<String, String> databases) {
        this.pass = Objects.requireNonNull(pass, "pass"); this.defaultDatabase = Objects.requireNonNull(defaultDatabase, "defaultDatabase");
        this.databases = Map.copyOf(databases);
    }

    @Override public Set<String> recall(String pipelineId, String nodeId) {
        pass.requirePipeline(pipelineId); return pass.nestShapes(database(nodeId)).read(nodeId).orElse(Set.of());
    }

    @Override public void record(String pipelineId, String nodeId, Set<String> paths) {
        pass.recordNestShape(database(nodeId), pipelineId, nodeId, paths);
    }

    private String database(String nodeId) { return databases.getOrDefault(nodeId, defaultDatabase); }
}
