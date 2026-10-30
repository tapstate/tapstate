package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.dsl.PipelineInputs;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Read-only names of configured outputs, without requiring a runnable write model. */
@FunctionalInterface
public interface PipelineOutputNames {
    List<Output> of(PipelineResource pipeline);

    record Output(String element, String kind, String connection, String table, String identity) { }

    record Unresolved(String pipelineId, String code, Map<String, Object> params) {
        public Unresolved {
            params = Map.copyOf(params);
        }
    }

    record Result(List<Output> outputs, List<Unresolved> unresolved) {
        public Result {
            outputs = List.copyOf(outputs);
            unresolved = List.copyOf(unresolved);
        }
    }

    /** Request-local lineage is kept separate from the output-name wire projection. */
    record Description(Result names, Map<String, Set<PipelineInputs.Input>> inputs, Set<String> qualifiedTables) {
        public Description(Result names, Map<String, Set<PipelineInputs.Input>> inputs) { this(names, inputs, Set.of()); }
        public Description {
            var copy = new LinkedHashMap<String, Set<PipelineInputs.Input>>();
            inputs.forEach((key, value) -> copy.put(key, Set.copyOf(value)));
            inputs = Map.copyOf(copy);
            qualifiedTables = Set.copyOf(qualifiedTables);
        }
    }

    /** Providers without physical input metadata leave freshness explicitly unavailable. */
    default Description describe(PipelineResource pipeline, Map<String, Resource> resources) {
        return new Description(resolve(pipeline), Map.of());
    }

    default Result resolve(PipelineResource pipeline) {
        try {
            return new Result(of(pipeline), List.of());
        } catch (TapstateException unavailable) {
            return new Result(List.of(), List.of(new Unresolved(
                    pipeline.id(), unavailable.code().code(), unavailable.args())));
        }
    }
}
