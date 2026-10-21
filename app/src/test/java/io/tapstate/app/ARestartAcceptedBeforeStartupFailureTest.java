package io.tapstate.app;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.spi.store.AuditRecord;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

class ARestartAcceptedBeforeStartupFailureTest {

    private static final String PIPELINE = "orders-pipe";

    @Test
    void aRerunStillClearsAndStartsANewRunWhenThePreviousRunFails() {
        Clock clock = Clock.fixed(Instant.parse("2026-10-01T15:22:32Z"), ZoneOffset.UTC);
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders-pipe
                source: orders_src
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: /.*/
                  sync:
                    - id: sink
                      source: orders_dest
                      write_mode: upsert
                      ddl: apply
                """));
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        InMemoryStateStore state = new InMemoryStateStore();
        InMemoryObservationStore observations = new InMemoryObservationStore();
        List<AuditRecord> audit = new ArrayList<>();
        PipelineLifecycleService lifecycle = new PipelineLifecycleService(
                new ArtifactQueryService(artifacts), desired, new AuditGate(audit::add, clock),
                id -> state.read(id).map(CheckpointDoc::epoch));
        StartingRun actuator = new StartingRun();
        ConvergenceDriver driver = new ConvergenceDriver(
                new PipelineConverger(desired, state, actuator, clock), desired,
                new ObservationPublisher(state, observations));

        lifecycle.start("alice", PIPELINE);
        driver.reconcile();
        CheckpointDoc starting = state.read(PIPELINE).orElseThrow();
        assertThat(starting.stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(actuator.starts).isEqualTo(1);
        assertThat(actuator.failure(PIPELINE)).isEmpty();

        // Both halves of restart --rerun are accepted while the first run is still coming up.
        // No converge pass sees the stop before the start supersedes it.
        DesiredState stopped = lifecycle.stop("alice", PIPELINE, true);
        DesiredState restarted = lifecycle.start("alice", PIPELINE);
        assertThat(stopped.targetState()).isEqualTo(STOPPED);
        assertThat(restarted.targetState()).isEqualTo(RUNNING);
        assertThat(restarted.reassemble()).isTrue();
        assertThat(restarted.purgeState()).isTrue();
        assertThat(restarted.rebuiltAtStateEpoch()).isEqualTo(starting.epoch());
        assertThat(desired.read(PIPELINE)).contains(restarted);
        assertThat(audit).extracting(AuditRecord::operationId)
                .containsExactly("pipeline.start", "pipeline.stop", "pipeline.start");

        // Release the previous run's failure only after both requests have landed. Driving later
        // ticks as well distinguishes a delayed restart from one that has been lost permanently.
        actuator.failStartup();
        for (int tick = 0; tick < 5; tick++) {
            driver.reconcile();
        }

        Observation observed = observations.read(PIPELINE).orElseThrow();
        assertSoftly(softly -> {
            softly.assertThat(actuator.starts).as("the accepted restart starts a second run").isEqualTo(2);
            softly.assertThat(actuator.stops)
                    .as("the rerun's clearing reaches the actuator")
                    .contains(true);
            softly.assertThat(state.read(PIPELINE).orElseThrow().stateJson())
                    .as("the previous run's late failure cannot consume the newer start")
                    .isEqualTo(StateJson.of(RUNNING));
            softly.assertThat(observed.state()).as("the read face reports the fresh run").isEqualTo(RUNNING);
            softly.assertThat(observed.failure()).as("the previous run's failure is no longer current").isNull();
        });
    }

    /** A data-plane boundary whose first run fails only when the test releases its startup result. */
    private static final class StartingRun implements LifecycleActuator {
        private int starts;
        private final List<Boolean> stops = new ArrayList<>();
        private boolean carrying;
        private Throwable failure;

        void failStartup() {
            failure = new TapstateException(CaptureError.RECOVERY_PROGRESS_UNPROVEN,
                    Map.of("pipeline", PIPELINE, "source", "orders_src"), null);
            carrying = false;
        }

        @Override
        public void start(String pipelineId) {
            starts++;
            carrying = true;
            failure = null;
        }

        @Override
        public void stop(String pipelineId, boolean purgeState) {
            stops.add(purgeState);
            carrying = false;
            failure = null;
        }

        @Override
        public void pause(String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resume(String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Throwable> failure(String pipelineId) {
            return Optional.ofNullable(failure);
        }

        @Override
        public Optional<Throwable> lost(String pipelineId) {
            return Optional.empty();
        }

        @Override
        public boolean isCarryingAJob(String pipelineId) {
            return carrying;
        }
    }
}
