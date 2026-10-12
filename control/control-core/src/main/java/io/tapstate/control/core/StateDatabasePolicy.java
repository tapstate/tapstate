package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;

import java.util.Collection;
import java.util.Map;

/** Deployment admission for authored Nest state placement; it does not change the DSL or route state. */
public enum StateDatabasePolicy {
    ON_PREM,
    CLOUD;

    /** Checks the same dependency closure that apply validates, before any artifact or audit write. */
    public void validate(Collection<Resource> resources) {
        if (this == ON_PREM) {
            return;
        }
        for (Resource resource : resources) {
            if (resource instanceof TransformResource transform) {
                validateBody(transform.body());
            } else if (resource instanceof PipelineResource pipeline && pipeline.transforms() != null) {
                for (Step step : pipeline.transforms()) {
                    if (step instanceof Step.Inline inline) {
                        validateBody(inline.body());
                    }
                }
            }
        }
    }

    private static void validateBody(TransformBody body) {
        if (body instanceof TransformBody.Nest nest && nest.state() != null) {
            // Even a value equal to the deployment default is authored placement and must be refused,
            // rather than ignored: the Cloud database is deployment-owned, not a per-Nest choice.
            throw new TapstateException(ControlError.STATE_DATABASE_UNAVAILABLE, Map.of(), null);
        }
    }
}
