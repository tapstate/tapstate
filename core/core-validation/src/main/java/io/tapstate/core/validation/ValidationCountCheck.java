package io.tapstate.core.validation;

import java.util.Objects;

/** Complete native scalar observations for one table mapping; agreement says nothing about keys or values. */
public record ValidationCountCheck(ValidationRunIdentity run, String mappingId, String outputId,
                                   long sourceCount, long targetCount,
                                   ValidationCheckEvent.ReadBoundary sourceRead,
                                   ValidationCheckEvent.ReadBoundary targetRead) {
    public ValidationCountCheck {
        Objects.requireNonNull(run); Objects.requireNonNull(sourceRead); Objects.requireNonNull(targetRead);
        mappingId = ValidationError.identifier(mappingId, "count.mapping");
        outputId = ValidationError.identifier(outputId, "count.output");
        if (run.mode() != ValidationMode.FULL || sourceCount < 0 || targetCount < 0)
            throw ValidationError.invalid("count.observation");
    }
    public boolean sameCount() { return sourceCount == targetCount; }
    public long difference() { return sourceCount - targetCount; }
}
