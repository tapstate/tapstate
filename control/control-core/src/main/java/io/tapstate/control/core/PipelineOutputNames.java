package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
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

    default Result resolve(PipelineResource pipeline) {
        try {
            return new Result(of(pipeline), List.of());
        } catch (TapstateException unavailable) {
            return new Result(List.of(), List.of(new Unresolved(
                    pipeline.id(), unavailable.code().code(), unavailable.args())));
        }
    }
}
