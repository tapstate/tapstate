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
    public boolean relevant(PipelineResource definition) {
        if (definition.serve() == null || definition.transforms() == null || definition.transforms().isEmpty()) return false;
        Map<String, io.tapstate.core.model.Resource> resources = new LinkedHashMap<>();
        boolean hasExpansion = false;
        for (var step : definition.transforms()) {
            io.tapstate.core.model.TransformBody body;
            if (step instanceof io.tapstate.core.model.Step.Inline inline) body = inline.body();
            else {
                var use = (io.tapstate.core.model.Step.Use) step;
                var resource = referenced(use.use());
                resources.put(resource.id(), resource);
                body = resource instanceof io.tapstate.core.model.TransformResource transform ? transform.body() : null;
            }
            hasExpansion |= body instanceof io.tapstate.core.model.TransformBody.Unwind;
        }
        if (!hasExpansion) return false;
        for (var source : definition.sources()) resources.put(source.id(), referenced(source.id()));
        if (definition.serve() instanceof io.tapstate.core.model.ServeBlock.Use use) {
            resources.put(use.use(), referenced(use.use()));
        }
        return io.tapstate.core.dsl.UnwindRules.reachesSyncTarget(definition, resources);
    }

    private io.tapstate.core.model.Resource referenced(String id) {
        return artifacts.get(id).orElseThrow(() -> new io.tapstate.core.common.TapstateException(
                io.tapstate.core.dsl.DslError.MISSING_REFERENCE, Map.of("path", "unwind target identity", "ref", id), null));
    }

    @Override
    public List<Assessment> assess(PipelineResource definition, boolean fullLoad) {
        return resolve(definition, fullLoad).stream().map(Resolved::assessment).distinct().toList();
    }

    List<Resolved> resolve(PipelineResource definition, boolean fullLoad) {
        if (!relevant(definition)) return List.of();
        var source = new StoreBackedDagSource(store, NestSettings.defaults(), artifacts);
        List<Resolved> results = new ArrayList<>();
        source.writeTargets(definition, (coordinate, stream, target) -> {
            SourceResource connection = StoredArtifacts.requireSource(artifacts, coordinate.connection());
            SinkConfig config = new SinkConfig(connection.connector(), connection.config(), WriteMode.UPSERT,
                    DdlPolicy.FAIL, target, null, OnFullLoad.valueOf(coordinate.onFullLoad().name()), fullLoad);
            TargetIdentityPlan plan = new PdkTargetIdentityPlanner(connectors)
                    .assess(config, Map.of(target.name(), target)).get(target.name());
            Assessment assessment = new Assessment(coordinate.element(), coordinate.connection(), coordinate.table(),
                    plan.status(), plan.reason(), plan.sourceField(), plan.preservedField(), plan.writeKey());
            results.add(new Resolved(assessment, stream, plan));
        });
        return reconcile(results);
    }

    static List<Resolved> reconcile(List<Resolved> results) {
        Map<String, Map<String, TargetIdentityPlan>> byConnection = new LinkedHashMap<>();
        for (int i = 0; i < results.size(); i++) {
            var result = results.get(i);
            byConnection.computeIfAbsent(result.assessment().connection(), ignored -> new LinkedHashMap<>())
                    .put(Integer.toString(i), result.plan());
        }
        byConnection.replaceAll((connection, plans) -> io.tapstate.adapters.pdk.TargetIdentityResolver.reconcile(plans));
        List<Resolved> reconciled = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            var result = results.get(i);
            var assessment = result.assessment();
            var plan = byConnection.get(assessment.connection()).get(Integer.toString(i));
            reconciled.add(new Resolved(new Assessment(assessment.element(), assessment.connection(), assessment.table(),
                    plan.status(), plan.reason(), plan.sourceField(), plan.preservedField(), plan.writeKey()), result.stream(), plan));
        }
        return List.copyOf(reconciled);
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
        values.put("generatedFields", resolved.plan().isAutomatic() ? List.of(resolved.plan().sourceField()) : List.of());
        return values;
    }
}
