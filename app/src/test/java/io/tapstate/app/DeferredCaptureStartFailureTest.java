package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeferredCaptureStartFailureTest {

    private static final String PIPELINE = "waiting-pipe";

    @Test
    void aWaitIgnoresOnlyItsOldJobAndStillReportsCaptureFailureAndEngineLoss() {
        Engine engine = mock(Engine.class);
        TapstateException oldFailure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                Map.of("pipeline", PIPELINE), null);
        when(engine.failureOf(PIPELINE)).thenReturn(Optional.of(oldFailure));
        when(engine.failureOf("other-pipe")).thenReturn(Optional.of(oldFailure));
        WaitingCapture captures = new WaitingCapture();
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(
                engine, new IdleDagSource(), captures, teardown());

        actuator.start(PIPELINE);

        assertThat(actuator.failure(PIPELINE)).isEmpty();
        assertThat(actuator.failure("other-pipe")).contains(oldFailure);
        captures.failure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                Map.of("pipeline", PIPELINE), null);
        assertThat(actuator.failure(PIPELINE)).contains(captures.failure);

        TapstateException lost = new TapstateException(EngineError.OUT_OF_MEMORY,
                Map.of("pipeline", PIPELINE), new OutOfMemoryError("fixture"));
        when(engine.isLost()).thenReturn(true);
        when(engine.failureOf(PIPELINE)).thenReturn(Optional.of(lost));
        assertThat(actuator.failure(PIPELINE)).contains(lost);
    }

    @Test
    void aStopEndsTheWaitSoLaterFailureChecksCanSeeTheEngineAgain() {
        Engine engine = mock(Engine.class);
        TapstateException oldFailure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                Map.of("pipeline", PIPELINE), null);
        when(engine.failureOf(PIPELINE)).thenReturn(Optional.of(oldFailure));
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(
                engine, new IdleDagSource(), new WaitingCapture(), teardown());

        actuator.start(PIPELINE);
        assertThat(actuator.failure(PIPELINE)).isEmpty();

        actuator.stop(PIPELINE, false);

        assertThat(actuator.failure(PIPELINE)).contains(oldFailure);
    }

    @Test
    void aFencedCaptureWaitKeepsCurrentAndInheritedFailuresInTheirClaimScope() {
        Engine engine = mock(Engine.class);
        WorkloadClaimKey key = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
        WorkloadClaim first = new WorkloadClaim(key, new WorkloadOwner("a", "boot-a"), 11, 7, 3,
                Instant.now().plusSeconds(60), 0, 0, Set.of(), 0, false, 2);
        WorkloadClaim takeover = new WorkloadClaim(key, new WorkloadOwner("a", "boot-a"), 12, 7, 3,
                Instant.now().plusSeconds(60), 0, 0, Set.of(), 0, false, 2);
        AtomicReference<WorkloadClaim> current = new AtomicReference<>(first);
        PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        when(ownership.isFenced()).thenReturn(true);
        when(ownership.currentClaim(PIPELINE)).thenAnswer(call -> Optional.of(current.get()));
        when(ownership.heldExecutionGeneration(PIPELINE)).thenReturn(7L);
        when(ownership.mayStart(PIPELINE)).thenReturn(true);
        WaitingCapture captures = new WaitingCapture();
        captures.failure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                Map.of("pipeline", PIPELINE), null);
        TapstateException inherited = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                Map.of("pipeline", PIPELINE), null);
        PipelineExecutionAdmission admission = new PipelineExecutionAdmission() {
            @Override public boolean prepare(String id, DagSource.PlannedStart planned,
                    PipelineActuationOwnership owner) { return true; }
            @Override public PipelineActuationOwnership.Execution begin(String id, DagSource.PlannedStart planned,
                    PipelineActuationOwnership owner) {
                return new PipelineActuationOwnership.Execution(true, new ExecutionFence(id, 11, 7, 2), 3L);
            }
            @Override public Optional<Throwable> inheritedFailure(String id, WorkloadClaim expected) {
                return expected == takeover ? Optional.of(inherited) : Optional.empty();
            }
        };
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(engine, new IdleDagSource(), captures,
                teardown(), ownership, ExecutionPlanRecorder.NONE, Clock.systemUTC(), ConnectorReadiness.NONE, admission);
        actuator.start(PIPELINE);

        assertThat(actuator.failure(PIPELINE))
                .as("a deferred profile wait must not publish the older unscoped capture frame").isEmpty();
        assertThat(captures.observedFence).isEqualTo(WorkloadClaimFence.from(first));
        captures.scopedFailure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                Map.of("pipeline", PIPELINE), null);
        assertThat(actuator.failure(PIPELINE)).contains(captures.scopedFailure);
        captures.scopedFailure = null;
        current.set(takeover);
        assertThat(actuator.failure(PIPELINE)).as("same execution under a new claim retains inherited diagnosis")
                .contains(inherited);
        assertThat(captures.observedFence).isEqualTo(WorkloadClaimFence.from(takeover));
    }

    private static NestStateTeardown teardown() {
        NestStateTeardown teardown = mock(NestStateTeardown.class);
        when(teardown.defaultDatabase()).thenReturn("default");
        return teardown;
    }

    private static final class WaitingCapture implements PipelineCaptureCoordinator {
        private Throwable failure;
        private Throwable scopedFailure;
        private WorkloadClaimFence observedFence;

        @Override
        public void startCapture(String pipelineId) {
            throw new RingNotOpenYet(CaptureId.of(
                    new CaptureConfig("fake", Map.of("host", "fixture"), List.of("orders")), null));
        }

        @Override
        public void stopCapture(String pipelineId, boolean purgeState) {
        }

        @Override
        public Optional<Throwable> captureFailure(String pipelineId) {
            return Optional.ofNullable(failure);
        }

        @Override
        public Optional<Throwable> captureFailure(String pipelineId, WorkloadClaimFence pipelineClaim) {
            observedFence = pipelineClaim;
            return Optional.ofNullable(scopedFailure);
        }
    }
}
