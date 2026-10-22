package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkTargetIdentityPlanner;
import io.tapstate.adapters.pdk.TargetIdentityPlan;
import io.tapstate.control.core.PipelineTargetIdentities;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StorePort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Joins topology-derived target models to the same PDK assessment used by actual writers. */
final class StoreBackedTargetIdentities implements PipelineTargetIdentities {
    private final StorePort store;
    private final ConnectorProvisioner connectors;
    private final ArtifactStore artifacts;

    StoreBackedTargetIdentities(StorePort store, ConnectorProvisioner connectors) {
        this(store, connectors, store.artifacts());
    }

    StoreBackedTargetIdentities(StorePort store, ConnectorProvisioner connectors, ArtifactStore artifacts) {
        this.store = store;
        this.connectors = connectors;
        this.artifacts = artifacts;
    }

    record Resolved(Assessment assessment, String stream, TargetIdentityPlan plan) { }

    @Override
    public List<Assessment> assess(PipelineResource definition, boolean fullLoad) {
        return resolve(definition, fullLoad).stream().map(Resolved::assessment).distinct().toList();
    }

    List<Resolved> resolve(PipelineResource definition, boolean fullLoad) {
        var source = new StoreBackedDagSource(store, NestSettings.defaults(), artifacts);
        List<Resolved> results = new ArrayList<>();
        source.writeTargets(definition, (coordinate, stream, target) -> {
            if (target.unwindIdentity() == null) return;
            SourceResource connection = StoredArtifacts.requireSource(artifacts, coordinate.connection());
            SinkConfig config = new SinkConfig(connection.connector(), connection.config(), WriteMode.UPSERT,
                    DdlPolicy.FAIL, target, null, OnFullLoad.valueOf(coordinate.onFullLoad().name()), fullLoad);
            TargetIdentityPlan plan = new PdkTargetIdentityPlanner(connectors)
                    .assess(config, Map.of(target.name(), target)).get(target.name());
            Assessment assessment = new Assessment(coordinate.element(), coordinate.connection(), coordinate.table(),
                    plan.status(), plan.reason(), plan.sourceField(), plan.preservedField(), plan.writeKey());
            results.add(new Resolved(assessment, stream, plan));
        });
        return List.copyOf(results);
    }

    static Map<String, Object> payload(Resolved resolved) {
        Assessment assessment = resolved.assessment();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("element", assessment.element());
        values.put("stream", resolved.stream());
        values.put("connection", assessment.connection());
        values.put("table", assessment.table());
        values.put("status", assessment.status());
        values.put("reason", assessment.reason());
        values.put("sourceField", assessment.sourceField());
        values.put("preservedField", assessment.preservedField());
        values.put("writeKey", assessment.writeKey());
        values.put("generatedFields", resolved.plan().automatic() ? List.of(resolved.plan().sourceField()) : List.of());
        return values;
    }
}
