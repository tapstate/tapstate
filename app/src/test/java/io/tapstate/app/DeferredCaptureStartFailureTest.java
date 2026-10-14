package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.capture.CaptureConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private static NestStateTeardown teardown() {
        NestStateTeardown teardown = mock(NestStateTeardown.class);
        when(teardown.defaultDatabase()).thenReturn("default");
        return teardown;
    }

    private static final class WaitingCapture implements PipelineCaptureCoordinator {
        private Throwable failure;

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
    }
}
