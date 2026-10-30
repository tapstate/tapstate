package io.tapstate.runtime.engine;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import io.tapstate.core.common.TapstateException;
import java.util.Map;

/** Original run metadata, transported with the job rather than rebuilt from a later artifact. */
public record OperatorRunManifest(String pipelineId, Serializable executionIdentity,
        List<OperatorMapBinding> bindings) implements Serializable {
    public OperatorRunManifest {
        if (pipelineId == null || pipelineId.isBlank()) throw new IllegalArgumentException("missing pipeline identity");
        Objects.requireNonNull(executionIdentity, "executionIdentity"); bindings = List.copyOf(bindings);
        var cold = new HashSet<String>(); var hot = new HashSet<String>();
        if (!bindings.isEmpty()) {
            var scope = bindings.getFirst().placement().scope();
            String executionKey = bindings.getFirst().executionKey();
            for (var binding : bindings) {
                if (!pipelineId.equals(binding.placement().scope().pipelineId()) || !executionIdentity.equals(binding.executionIdentity())
                        || !scope.equals(binding.placement().scope()) || !executionKey.equals(binding.executionKey())
                        || !cold.add(binding.placement().mapName()) || !hot.add(binding.mapName())) {
                    throw new IllegalArgumentException("operator manifest requires one original run and unique physical positions");
                }
            }
        }
    }
    public OperatorStepMaps forStep(String pipeline,String step) {
        requirePipeline(pipeline); return new OperatorStepMaps(pipelineId,step,bindings);
    }
    public void requirePipeline(String pipeline) { if (!pipelineId.equals(pipeline)) throw new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",pipelineId),null); }
}
