package io.tapstate.runtime.probe;

/** Fixed original start only; an unknown result never grants a substitute reservation. */
public interface ValidationStartProbe {
    ValidationOperation.Response start(ValidationOperation original);
}
