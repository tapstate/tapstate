package io.tapstate.runtime.probe;

/** Independent metadata evaluation only; never saves a definition or starts a validation run. */
public interface ValidationEvaluationProbe {
    ValidationEvaluation evaluate(ValidationEvaluation.Request request);
}
