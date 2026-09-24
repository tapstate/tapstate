package io.tapstate.control.restapi;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;

import java.util.Map;

/**
 * The body of a stop request: whether stopping also clears what the pipeline has accumulated — its
 * resume position and its operators' state.
 *
 * <p>A {@code Boolean} rather than a {@code boolean} so that "did not say" and "said false" stay
 * different values. Bound to a primitive they arrive identical, and telling them apart is the whole
 * reason this body exists: the two answers leave the pipeline in states that differ by hours of
 * re-reading, and neither is safe to assume for a caller who never expressed one.
 */
record PipelineStopRequest(Boolean purgeState) {

    /** The stated answer, or a coded refusal naming the pipeline it was aimed at. */
    static boolean purgeStateOf(PipelineStopRequest request, String pipelineId) {
        if (request == null || request.purgeState() == null) {
            throw new TapstateException(
                    LifecycleError.PURGE_STATE_NOT_STATED, Map.of("pipeline", pipelineId), null);
        }
        return request.purgeState();
    }
}
