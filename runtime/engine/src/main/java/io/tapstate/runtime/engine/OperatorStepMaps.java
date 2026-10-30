package io.tapstate.runtime.engine;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.OperatorPlacement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** One original step's immutable lookup, built before any record takes a map handle. */
public final class OperatorStepMaps implements Serializable {
    private record At(OperatorPlacement.Resource resource,String path,String namespace) implements Serializable {}
    private final String pipeline;
    private final Map<At,OperatorMapBinding> maps;
    OperatorStepMaps(String pipeline,String step,List<OperatorMapBinding> originals) {
        this.pipeline = pipeline; Objects.requireNonNull(step,"step"); var index = new HashMap<At,OperatorMapBinding>();
        for (var binding : originals) {
            var at = binding.placement();
            if (step.equals(at.stepId()) && index.putIfAbsent(new At(at.resource(),at.pathId(),at.namespace()),binding) != null) throw refused();
        }
        this.maps = Map.copyOf(index);
    }
    public OperatorMapBinding nest(List<String> path,OperatorMapCoordinates.NestMap kind,String alias) {
        return require(new At(OperatorPlacement.Resource.NEST_STATE,OperatorMapCoordinates.path(path),OperatorMapCoordinates.nestNamespace(kind,alias)));
    }
    public OperatorMapBinding join(OperatorPlacement.Resource kind,String alias) {
        return require(new At(kind,"root",OperatorMapCoordinates.joinNamespace(kind,alias)));
    }
    private OperatorMapBinding require(At coordinate) { var found = maps.get(coordinate); if (found == null) throw refused(); return found; }
    private TapstateException refused() { return new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",pipeline),null); }
}
