package io.tapstate.app;

import com.hazelcast.config.MapConfig;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.OperatorMapBinding;
import io.tapstate.runtime.engine.OperatorMapCapacity;
import io.tapstate.runtime.engine.OperatorMapCoordinates;
import io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.runtime.engine.join.JoinMaps;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.OperatorPlacement.Resource;
import io.tapstate.spi.store.PipelineIntentScope;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Immutable candidate coordinates, shared by capacity, execution bindings and exact retirement. */
final class OperatorCoordinatePlan {
    record NestStep(String database, NestTopology topology, NestSettings settings) {
        NestStep {
            Objects.requireNonNull(database,"database"); Objects.requireNonNull(topology,"topology");
            Objects.requireNonNull(settings,"settings");
        }
        Set<String> shape() { return topology.stateIdentityPaths(); }
    }
    private record StateMap(OperatorPlacement placement, long entriesHeldInMemory) {}
    private final PipelineIntentScope scope;
    private final Map<String,NestStep> nests;
    private final List<StateMap> maps;
    private final List<OperatorPlacement> placements;

    private OperatorCoordinatePlan(PipelineIntentScope scope,Map<String,NestStep> nests,List<StateMap> maps) {
        this.scope = scope; this.nests = Map.copyOf(nests);
        this.maps = maps.stream().sorted(java.util.Comparator.comparing(map -> map.placement().mapName())).toList();
        this.placements = this.maps.stream().map(StateMap::placement).toList();
        var names = new HashSet<String>();
        for (var placement : placements) if (!names.add(placement.mapName()))
            throw new IllegalArgumentException("duplicate compiled operator position");
    }

    static OperatorCoordinatePlan compile(PipelineIntentScope scope,PipelineResource pipeline,String defaultDatabase,
            Function<String,NestTable> tables,Map<String,? extends Collection<String>> joinDimensions,NestSettings base) {
        Objects.requireNonNull(scope,"scope"); Objects.requireNonNull(pipeline,"pipeline");
        Objects.requireNonNull(defaultDatabase,"defaultDatabase"); Objects.requireNonNull(tables,"tables");
        Objects.requireNonNull(base,"base"); Objects.requireNonNull(joinDimensions,"joinDimensions");
        if (!scope.pipelineId().equals(pipeline.id())) throw fenced();
        var expectedJoins = new HashSet<String>();
        if (pipeline.transforms() != null) for (Step step : pipeline.transforms())
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Join) expectedJoins.add(step.id());
        if (!expectedJoins.equals(joinDimensions.keySet())) throw new IllegalArgumentException("compiled joins must match the original pipeline");
        var nests = new LinkedHashMap<String,NestStep>(); var maps = new ArrayList<StateMap>();
        if (pipeline.transforms() != null) for (Step step : pipeline.transforms()) {
            if (!(step instanceof Step.Inline inline) || !(inline.body() instanceof TransformBody.Nest body)) continue;
            String database = body.state() == null ? defaultDatabase : body.state().database();
            NestTopology topology = NestTopology.compile(pipeline.id(),step.id(),body,tables);
            NestSettings settings = body.entriesInMemory() == null ? base : base.withEntriesHeldInMemory(body.entriesInMemory());
            if (body.maxElementsPerDocument() != null && !topology.isPassthrough())
                settings = settings.withElementLimit(topology.assembler().mapName(),body.maxElementsPerDocument());
            if (nests.putIfAbsent(step.id(),new NestStep(database,topology,settings)) != null || joinDimensions.containsKey(step.id()))
                throw new IllegalArgumentException("duplicate compiled operator step");
            long memory = settings.entriesHeldInMemory();
            for (var vertex : topology.vertices()) {
                maps.add(nestMap(scope,database,step.id(),vertex.pathId(),NestMap.STATE,null,memory));
                maps.add(nestMap(scope,database,step.id(),vertex.pathId(),NestMap.PARKING,null,memory));
            }
            for (var lookup : topology.lookups()) {
                maps.add(nestMap(scope,database,step.id(),lookup.pathId(),NestMap.LOOKUP,lookup.alias(),memory));
                maps.add(nestMap(scope,database,step.id(),lookup.pathId(),NestMap.REFERENCES,lookup.alias(),memory));
            }
        }
        joinDimensions.forEach((step,aliases) -> {
            maps.add(joinMap(scope,defaultDatabase,step,Resource.JOIN_FACTS,null));
            maps.add(joinMap(scope,defaultDatabase,step,Resource.JOIN_WRITERS,null));
            for (String alias : List.copyOf(aliases)) {
                maps.add(joinMap(scope,defaultDatabase,step,Resource.JOIN_DIMENSIONS,alias));
                maps.add(joinMap(scope,defaultDatabase,step,Resource.JOIN_INDEXES,alias));
            }
        });
        return new OperatorCoordinatePlan(scope,nests,maps);
    }

    private static StateMap nestMap(PipelineIntentScope scope,String database,String step,List<String> path,
            NestMap kind,String alias,long memory) {
        return new StateMap(OperatorMapCoordinates.nest(scope,database,step,path,kind,alias),memory);
    }
    private static StateMap joinMap(PipelineIntentScope scope,String database,String step,Resource kind,String alias) {
        return new StateMap(OperatorMapCoordinates.join(scope,database,step,kind,alias),JoinMaps.DEFAULT_ENTRIES_HELD_IN_MEMORY);
    }
    Map<String,NestStep> nests() { return nests; }
    List<OperatorPlacement> placements() { return placements; }
    Map<String,NestBinding.CompiledStep> compiledNests() {
        var compiled = new LinkedHashMap<String,NestBinding.CompiledStep>();
        nests.forEach((step,nest) -> compiled.put(step,new NestBinding.CompiledStep(nest.topology(),nest.settings())));
        return Map.copyOf(compiled);
    }
    Map<String,String> nestDatabases() {
        var databases = new LinkedHashMap<String,String>(); nests.forEach((step,nest) -> databases.put(step,nest.database()));
        return Map.copyOf(databases);
    }
    Map<String,Integer> blockingVertices() {
        var counts = new LinkedHashMap<String,Integer>();
        nests.forEach((step,nest) -> counts.put(step,nest.topology().vertices().size()+nest.topology().lookups().size()));
        return Map.copyOf(counts);
    }
    OperatorRunManifest bind(ExecutionFence original) {
        if (original == null || !scope.equals(original.scope())) throw fenced();
        return new OperatorRunManifest(scope.pipelineId(),original,placements.stream()
                .map(position -> new OperatorMapBinding(position,original,original.operatorExecutionKey())).toList());
    }
    List<OperatorMapCapacity> capacities() {
        return maps.stream().map(map -> new OperatorMapCapacity(map.placement(),map.entriesHeldInMemory())).toList();
    }
    /** Fresh mutable configurations; successive attempts share only the stable physical pattern. */
    List<MapConfig> configurations(String splitBrainProtection) {
        return capacities().stream().map(capacity -> capacity.configuration(splitBrainProtection)).toList();
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
}
