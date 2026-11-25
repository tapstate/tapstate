package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.DesiredState;
import java.util.Objects;

/** One latest intent revision, or its small deletion marker; this is not a command history. */
public record PipelineIntentSnapshot(PipelineIntentScope scope, long intentRevision, DesiredState desired) {
    public PipelineIntentSnapshot {
        Objects.requireNonNull(scope, "scope");
        if (intentRevision < 1 || intentRevision == Long.MAX_VALUE
                || (desired != null && !scope.pipelineId().equals(desired.pipelineId()))) {
            throw new IllegalArgumentException("invalid Pipeline intent snapshot");
        }
    }
    public boolean deleted() { return desired == null; }
}
