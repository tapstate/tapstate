package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.NestInspection;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.TransformBody;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The nest inspect read: what one nest step holds for one root key, as a rendered view. Every way it can come
 * back empty is a refusal naming what to do instead - a step that is not a nest, a pipeline not running, a
 * key of the wrong width, a key nothing is held under - never an empty view, which would read as a document
 * with nothing in it.
 */
public final class NestInspectService {

    private final ArtifactQueryService artifacts;
    private final NestStateInspector inspector;

    public NestInspectService(ArtifactQueryService artifacts, NestStateInspector inspector) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.inspector = Objects.requireNonNull(inspector, "inspector");
    }

    /** What {@code step} of {@code pipelineId} holds under {@code rootKey}, one value per root key column. */
    public NestInspection inspect(String pipelineId, String step, List<String> rootKey) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(rootKey, "rootKey");
        PipelineResource pipeline = artifacts.getResource(pipelineId)
                .map(StoredResource::resource)
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast)
                .orElseThrow(() -> new TapstateException(LifecycleError.UNKNOWN_PIPELINE,
                        Map.of("pipeline", pipelineId), null));
        Map<String, TransformBody.Nest> nests = NestProgressService.nests(pipeline);
        if (nests.isEmpty()) {
            throw new TapstateException(MonitorError.NO_NEST_STEP, Map.of("pipeline", pipelineId), null);
        }
        if (!nests.containsKey(step)) {
            throw new TapstateException(MonitorError.UNKNOWN_NEST_STEP, Map.of("pipeline", pipelineId,
                    "step", step, "steps", String.join(", ", nests.keySet())), null);
        }
        NestStateInspector.Result result = inspector.inspect(pipelineId, step, rootKey);
        return switch (result) {
            case NestStateInspector.Result.Held held -> held.inspection();
            case NestStateInspector.Result.NotRunning ignored -> throw new TapstateException(
                    MonitorError.NEST_NOT_RUNNING, Map.of("pipeline", pipelineId, "step", step), null);
            case NestStateInspector.Result.WrongWidth wrong -> throw new TapstateException(
                    MonitorError.NEST_ROOT_KEY_WIDTH, Map.of("pipeline", pipelineId, "step", step,
                            "columns", String.join(", ", wrong.columns()), "given", rootKey.size()), null);
            case NestStateInspector.Result.NotHeld none -> throw new TapstateException(
                    MonitorError.NEST_ROOT_KEY_NOT_HELD, Map.of("pipeline", pipelineId, "step", step,
                            "key", String.join(", ", rootKey), "columns", String.join(", ", none.columns())),
                    null);
        };
    }
}
