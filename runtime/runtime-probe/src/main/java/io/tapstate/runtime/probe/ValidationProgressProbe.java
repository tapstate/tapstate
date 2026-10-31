package io.tapstate.runtime.probe;

/** Passive original-run lookup; missing local handles never rebuild execution. */
public interface ValidationProgressProbe {
    ValidationOperation.Response read(ValidationOperation original);
}
