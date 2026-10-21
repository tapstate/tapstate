package io.tapstate.core.validation;

import java.util.Objects;

/** Original run and root revision carried by every query and result callback. */
public record ValidationObservation(ValidationRunIdentity run, ValidationRoot root, long revision) {
    public ValidationObservation {
        Objects.requireNonNull(run, "run");
        Objects.requireNonNull(root, "root");
        ValidationError.positive(revision, "observation.revision");
    }

    public boolean isCurrentFor(ValidationObservation expected) {
        return equals(expected);
    }
}
