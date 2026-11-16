package io.tapstate.app;

import io.tapstate.control.core.PipelineWriteTargets;
import io.tapstate.control.core.StartPlanner;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.PipelinePreparationInputs;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SrsStores;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Names a pipeline's write targets through the topology build's own derivation, so a start check reads
 * the table a sink will write and not one worked out a second way.
 */
final class StoreBackedPipelineWriteTargets implements PipelineWriteTargets {

    private final StoreBackedDagSource source;

    StoreBackedPipelineWriteTargets(StorePort storePort) {
        this(storePort, false);
    }

    StoreBackedPipelineWriteTargets(StorePort storePort, boolean requireMarkedStore) {
        this.source = new StoreBackedDagSource(Objects.requireNonNull(storePort, "storePort"),
                NestSettings.defaults(), StoreReachability.assumingReachable(),
                SourcePlacement.anyMember(), requireMarkedStore);
    }

    @Override
    public List<WriteTarget> of(PipelineResource pipeline) {
        Objects.requireNonNull(pipeline, "pipeline");
        return source.writeTargets(pipeline);
    }

    StartPlanner.Reads capture(ThreeLayerStorePort store, StoreBackedPipelineChains chains, PipelineResource pipeline,
            Optional<io.tapstate.spi.store.PipelineIntentScope> original) {
        var scope = original.map(store::planningScope).orElseGet(() -> store.planningScope(pipeline.id()));
        var live = store.artifacts();
        var resources = new HashMap<String, Optional<Resource>>();
        live.startPlanningInventory(pipeline).ifPresent(inventory -> {
            io.tapstate.core.dsl.ReferenceGraph.declaredReferences(pipeline).forEach(edge ->
                    resources.put(edge.id(), Optional.empty()));
            inventory.resources().forEach(resource -> resources.put(resource.id(), Optional.of(resource)));
        });
        resources.put(pipeline.id(), Optional.of(pipeline));
        ArtifactStore captured = new ArtifactStore() {
            private Optional<String> managed = Optional.empty();
            private boolean managedLoaded;
            private List<Resource> all;
            @Override public Optional<String> managedDefaultSourceId() {
                if (!managedLoaded) {
                    managed = live.managedDefaultSourceId();
                    managedLoaded = true;
                }
                return managed;
            }
            @Override public Optional<Resource> get(String id) { return resources.computeIfAbsent(id, live::get); }
            @Override public List<Resource> list() {
                if (all == null) all = live.list();
                return all;
            }
            @Override public void saveAll(List<Resource> values) { throw new UnsupportedOperationException(); }
        };
        var models = new HashMap<SourceResource, Optional<SourceModel>>();
        Function<SourceResource, SourceModel> discovery = definition -> models.computeIfAbsent(definition,
                key -> Optional.ofNullable(original.isPresent()
                        ? SourceDiscovery.matched(store.startSchemas().get(key.id()), key)
                        : SourceDiscovery.model(store, key))).orElse(null);
        Supplier<SrsStores> progress = new Supplier<>() {
            private SrsStores stores;
            @Override public SrsStores get() {
                if (stores == null) stores = store.startPlanningSrs();
                return stores;
            }
        };
        return new StartPlanner.Reads(() -> chains.progressChainsOf(pipeline.id(), captured, discovery, progress),
                chain -> store.startProgress(chain, progress.get()), () -> source.startWriteTargets(pipeline, captured, discovery),
                scope.verify(), () -> {
                    var expanded = PipelineInlining.inline(pipeline, captured);
                    var sources = new LinkedHashMap<String, SourceResource>();
                    var tables = new LinkedHashMap<String, Set<String>>();
                    for (String id : pipeline.sourceIds()) {
                        var sourceDefinition = StoredArtifacts.requireSource(captured, id);
                        sources.put(id, sourceDefinition);
                        tables.put(id, Set.copyOf(PipelineTableSelection.resolve(expanded, sourceDefinition,
                                discovery.apply(sourceDefinition))));
                    }
                    return Optional.of(new DesiredStore.StartPreparation(scope.scope(),
                            PipelinePreparationInputs.of(scope.scope(), pipeline, sources, tables)));
                }, id -> captured.get(id).filter(SourceResource.class::isInstance).map(SourceResource.class::cast)
                        .filter(resolved -> id.equals(resolved.id())));
    }
}
