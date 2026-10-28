package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Observation-only routing through the exact execution carried by the current engine job. */
final class RunBoundCaptureReadings implements PipelineCaptureCoordinator {
    private final PipelineCaptureCoordinator captures;
    private final Function<String, Optional<ExecutionFence>> current;
    private final Function<String, CaptureReading> arrivals;

    RunBoundCaptureReadings(PipelineCaptureCoordinator captures, Function<String, Optional<ExecutionFence>> current,
            Function<String, CaptureReading> arrivals) {
        this.captures = Objects.requireNonNull(captures, "captures");
        this.current = Objects.requireNonNull(current, "current");
        this.arrivals = Objects.requireNonNull(arrivals, "arrivals");
    }

    @Override public SnapshotReading snapshotProgress(String pipeline) {
        return read(pipeline, SnapshotReading.NONE, original -> view(original).snapshotProgress(pipeline));
    }
    @Override public SnapshotReading runSnapshotProgress(String pipeline) {
        return read(pipeline, SnapshotReading.NONE, original -> view(original).runSnapshotProgress(pipeline));
    }
    @Override public io.tapstate.core.lifecycle.InitialLoad initialLoad(String pipeline,
            io.tapstate.core.lifecycle.ExecutionPlan plan, java.time.Instant at) {
        return read(pipeline, null, original -> view(original).initialLoad(pipeline, plan, at));
    }
    @Override public CaptureReading capturedRows(String pipeline) {
        return read(pipeline, CaptureReading.NONE, original -> arrivals.apply(pipeline));
    }

    private <T> T read(String pipeline, T absent, Function<ExecutionFence, T> operation) {
        var selected = Objects.requireNonNull(current.apply(pipeline), "run identity");
        if (selected.isEmpty()) return absent;
        var original = selected.orElseThrow();
        if (original.scope() == null || !original.pipelineId().equals(pipeline)) throw refused();
        T result = operation.apply(original);
        if (!selected.equals(Objects.requireNonNull(current.apply(pipeline), "run identity"))) throw refused();
        return result;
    }

    private PipelineCaptureCoordinator view(ExecutionFence original) {
        var claim = new WorkloadClaimFence(new WorkloadClaimKey(original.scope().clusterId(),
                WorkloadClaimType.PIPELINE_ACTUATION, original.pipelineId()), new WorkloadOwner(original.ownerNodeId(), original.ownerBootId()),
                original.claimGeneration(), original.executionGeneration(), original.topologyRevision());
        return captures.forRun(original.scope(), claim, original);
    }

    @Override public void startCapture(String pipeline) { throw new UnsupportedOperationException("observation-only capture view"); }
    @Override public void stopCapture(String pipeline, boolean purge) { throw new UnsupportedOperationException("observation-only capture view"); }
    private static TapstateException refused() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
}
