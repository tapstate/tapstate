package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.NEW;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The convergence driver ticks the framework-free converger over every desired pipeline. It reconciles
 * each one toward its intent, and isolates a per-pipeline failure so one bad pipeline cannot starve the
 * rest of the pass.
 */
class ConvergenceDriverTest {

    private static final Instant T0 = Instant.parse("2026-07-01T00:00:00Z");

    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final InMemoryObservationStore observations = new InMemoryObservationStore();
    private final PipelineConverger converger =
            new PipelineConverger(desired, state, new NoOpActuator(), Clock.fixed(T0, ZoneOffset.UTC));
    private final ConvergenceDriver driver =
            new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));

    @Test
    void reconcileConvergesEveryDesiredPipeline() {
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        desired.save(new DesiredState("users", RUNNING, "rev-1"));

        driver.reconcile();

        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.read("users").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        // Each converged pipeline also has its observation published for the read faces to serve.
        assertThat(observations.read("orders").orElseThrow().state()).isEqualTo(RUNNING);
        assertThat(observations.read("users").orElseThrow().state()).isEqualTo(RUNNING);
    }

    @Test
    void reconcileIsolatesAPerPipelineFailure() {
        desired.save(new DesiredState("broken", RUNNING, "rev-1"));
        desired.save(new DesiredState("healthy", RUNNING, "rev-1"));
        state.failFor("broken");

        driver.reconcile();

        // The healthy pipeline still converges even though the broken one threw mid-pass.
        assertThat(state.read("healthy").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
    }

    @Test
    void aPipelineWhoseReconcileKeepsThrowingBecomesObservableWithAClimbingErrorCount() {
        // A pipeline whose converge pass throws every tick (its store is unreachable) never reaches the
        // publish call, so with no error observation it is indistinguishable from one that is merely slow to
        // converge. The driver must count the consecutive failures and surface them, so "permanently broken"
        // reads differently from "still converging" on the observation read face.
        desired.save(new DesiredState("broken", RUNNING, "rev-1"));
        state.failFor("broken");

        driver.reconcile();
        driver.reconcile();
        driver.reconcile();

        Observation observed = observations.read("broken").orElseThrow();
        // Never converged, so no lifecycle state was ever witnessed: the projection stays NEW rather than a
        // fabricated FAILED, and the climbing errorCount is what marks it broken.
        assertThat(observed.state()).isEqualTo(NEW);
        assertThat(observed.metrics()).containsOnly(entry("errorCount", 3L));
    }

    @Test
    void theReconcileFailureStreakClearsThenRestartsAfterThePipelineRecovers() {
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        state.failFor("orders");
        driver.reconcile();
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 2L));

        // The store recovers: the next pass converges normally and republishes the real state, so the error
        // count drops back to zero rather than staying stuck at the earlier failure total.
        state.recover("orders");
        driver.reconcile();
        Observation recovered = observations.read("orders").orElseThrow();
        assertThat(recovered.state()).isEqualTo(RUNNING);
        assertThat(recovered.metrics()).containsOnly(entry("errorCount", 0L));

        // A later failure starts a fresh streak from one, not from the earlier total of two, so the clean pass
        // in between must have cleared the counter.
        state.failFor("orders");
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 1L));
    }

    @Test
    void aFailingPipelineDroppedFromTheDesiredSetDoesNotKeepItsStreak() {
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        state.failFor("orders");
        driver.reconcile();
        driver.reconcile(); // the streak climbs to two

        // The pipeline is deleted while still failing; the pass that no longer sees it prunes its counter so a
        // deleted-while-failing pipeline cannot leak a streak that nothing will ever clear.
        desired.remove("orders");
        driver.reconcile();

        // Re-applied later and still failing, it starts a fresh streak from one rather than resuming at three.
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().metrics()).containsOnly(entry("errorCount", 1L));
    }

    @Test
    void aReadFaceThatRejectsTheErrorObservationDoesNotStarveTheOtherPipelines() {
        // The broken pipeline's converge keeps throwing and the read face also rejects its error observation
        // (the same store backs both and is down). Publishing the failure is best-effort: it must be swallowed
        // so a second, healthy pipeline still converges and is observed in the same pass. Without that, one
        // unreachable read face would starve every pipeline reconciled after it.
        desired.save(new DesiredState("broken", RUNNING, "rev-1"));
        desired.save(new DesiredState("healthy", RUNNING, "rev-1"));
        state.failFor("broken");
        observations.failSaveFor("broken");

        driver.reconcile(); // must return normally rather than propagating the rejected save

        assertThat(observations.read("healthy").orElseThrow().state()).isEqualTo(RUNNING);
        // The broken pipeline's error observation could not be written, but that did not abort the pass.
        assertThat(observations.read("broken")).isEmpty();
    }

    @Test
    void logsEmittedDuringAPipelinesPassAreAttributedToThatPipeline() {
        // Feed the node-local sink from the driver's own logger via the pipeline appender; make one pipeline
        // fail so the driver logs a warning during that pipeline's turn. The warning must land under that
        // pipeline id, proving the driver sets the pipeline attribution slot around each pipeline's work.
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Logger driverLogger = (Logger) LoggerFactory.getLogger(ConvergenceDriver.class);
        PipelineLogAppender appender = new PipelineLogAppender(sink, new io.tapstate.core.logging.SecretRedactor());
        appender.setContext(driverLogger.getLoggerContext());
        appender.start();
        driverLogger.addAppender(appender);
        try {
            desired.save(new DesiredState("broken", RUNNING, "rev-1"));
            state.failFor("broken");

            driver.reconcile();
        } finally {
            driverLogger.detachAppender(appender);
            appender.stop();
        }

        assertThat(sink.tail("broken")).extracting(LogLine::level).containsExactly("WARN");
        // The attribution slot is cleared after the pass, so an unrelated later log is not misattributed.
        assertThat(MDC.get("pipeline_id")).isNull();
    }

    @Test
    void aPipelineWhoseJobDiedIsDrivenToFailedAndLogged() {
        // A job that dies on its own does not throw into the reconcile pass — the converge side moves the
        // pipeline to FAILED and returns it. The driver must log that, so a dead job is no longer the
        // silent "found 0" it used to be, and the observable state reflects the failure.
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Logger driverLogger = (Logger) LoggerFactory.getLogger(ConvergenceDriver.class);
        PipelineLogAppender appender = new PipelineLogAppender(sink, new io.tapstate.core.logging.SecretRedactor());
        appender.setContext(driverLogger.getLoggerContext());
        appender.start();
        driverLogger.addAppender(appender);

        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        try {
            desired.save(new DesiredState("orders", RUNNING, "rev-1"));
            driver.reconcile(); // orders -> RUNNING while the job is healthy
            actuator.failWith(new RuntimeException("sink write failed"));

            driver.reconcile(); // the job has died: orders -> FAILED, and the driver logs it
        } finally {
            driverLogger.detachAppender(appender);
            appender.stop();
        }

        assertThat(sink.tail("orders")).extracting(LogLine::level).contains("WARN");
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
    }

    @Test
    void aPipelineWhoseJobDiedPublishesTheCodedReasonAlongsideTheFailedState() {
        // The state says the run died and the error count says it was counted; neither says why. The driver
        // holds the only copy of the cause, so it must carry it into the published observation -- otherwise
        // the reason exists solely as a log line and no read face can answer for it.
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));

        driver.reconcile();

        ObservationFailure failure = observations.read("orders").orElseThrow().failure();
        assertThat(failure).isNotNull();
        assertThat(failure.code()).isEqualTo("engine.job-failed");
        assertThat(failure.params()).containsEntry("cause", "sink write failed");
    }

    @Test
    void aPipelineStillFailedOnALaterPassKeepsReportingWhyItDied() {
        // PipelineConverger reports the cause only on the one pass that drives RUNNING -> FAILED; every
        // later pass over an already-FAILED checkpoint returns a bare CONVERGED with no cause attached (it
        // did not just fail again, it is still failed from before). Without the driver carrying it forward,
        // a pipeline dead for an hour would say why for exactly the one second it transitioned in.
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));
        driver.reconcile(); // orders -> FAILED, cause published

        driver.reconcile(); // still FAILED, unchanged
        driver.reconcile(); // still FAILED, unchanged again

        ObservationFailure failure = observations.read("orders").orElseThrow().failure();
        assertThat(failure).isNotNull();
        assertThat(failure.code()).isEqualTo("engine.job-failed");
        assertThat(failure.params()).containsEntry("cause", "sink write failed");
    }

    @Test
    void aPipelineStillFailedAfterARestartKeepsReportingWhyItDied() {
        // A restart rebuilds the driver but not the stores: the FAILED checkpoint and its published reason
        // both survive in durable state, so the first pass of the new process must keep answering with them.
        // Anything the old driver only held in memory is gone here -- if the reason rode only there, this
        // pass would republish the still-FAILED pipeline reasonless, the exact erasure the carry-forward
        // exists to prevent.
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));
        driver.reconcile(); // orders -> FAILED, cause published
        assertThat(observations.read("orders").orElseThrow().failure()).isNotNull();

        ConvergenceDriver restarted =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        restarted.reconcile();

        ObservationFailure failure = observations.read("orders").orElseThrow().failure();
        assertThat(failure).isNotNull();
        assertThat(failure.code()).isEqualTo("engine.job-failed");
        assertThat(failure.params()).containsEntry("cause", "sink write failed");
    }

    @Test
    void aPipelineThatRecoversStopsReportingTheFailureThatKilledItsPreviousRun() {
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().failure()).isNotNull();

        // A failed run stays failed on its own (PipelineConverger never re-drives FAILED toward RUNNING);
        // the only real recovery path is an explicit stop, then a fresh start. Once that happens the
        // observation is current-state again, so the stale reason must not keep being served.
        actuator.failWith(null);
        desired.save(new DesiredState("orders", STOPPED, "rev-2"));
        driver.reconcile();
        desired.save(new DesiredState("orders", RUNNING, "rev-3"));
        driver.reconcile();

        Observation recovered = observations.read("orders").orElseThrow();
        assertThat(recovered.state()).isEqualTo(RUNNING);
        assertThat(recovered.failure()).isNull();
    }

    /** A no-op actuator whose failure() a test can arm, to drive a pipeline to FAILED without a real job. */
    private static final class FailingActuator implements LifecycleActuator {
        private Throwable failure;

        void failWith(Throwable cause) {
            this.failure = cause;
        }

        @Override
        public void start(String pipelineId) {
        }

        @Override
        public void pause(String pipelineId) {
        }

        @Override
        public void resume(String pipelineId) {
        }

        @Override
        public void stop(String pipelineId, boolean purgeState) {
        }

        @Override
        public Optional<Throwable> failure(String pipelineId) {
            return Optional.ofNullable(failure);
        }

        /** Always carrying: these cases are about what the driver does with a converge result. */
        @Override
        public boolean isCarryingAJob(String pipelineId) {
            return true;
        }
    }
}
