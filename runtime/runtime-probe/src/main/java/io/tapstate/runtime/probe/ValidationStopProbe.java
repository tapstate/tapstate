package io.tapstate.runtime.probe;

/** Signal the original run only; neither scheduling nor the production Pipeline is stopped here. */
public interface ValidationStopProbe {
    ValidationOperation.Response stop(ValidationOperation original);
}
