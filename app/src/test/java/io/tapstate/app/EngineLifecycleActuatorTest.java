package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The assembly-layer binding from the lifecycle actuator seam to the Jet engine and the capture coordinator,
 * driven against a real embedded member. It runs a pipeline through start -> pause -> resume -> stop over an
 * idle stand-in topology and proves the verb composition: start fills the capture before submitting the job
 * that reads it; pause and resume are engine-only (the capture keeps running); stop cancels the job before
 * stopping the capture behind it. The same actuator drives the store-backed topology production runs, by
 * pipeline id alone.
 */
class EngineLifecycleActuatorTest {

    private static final String PIPE = "orders-pipe";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("start captures then submits, pause/resume touch only the engine, stop cancels then stops capture")
    void composesCaptureAndEngineAcrossTheFullLifecycle() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        LifecycleActuator actuator =
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());
        // At stop the coordinator awaits the pipeline's job going terminal; that only happens if the cancel ran
        // before the capture stop, so it discriminates the stop ordering rather than racing it.
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        // Start fills the capture, then submits the job that reads the ring the capture fills.
        assertThat(events).containsExactly("startCapture:" + PIPE, "submit:" + PIPE);
        Job job = member.getJet().getJob(PIPE);
        assertThat(job).as("start submits a job named by the pipeline id").isNotNull();
        awaitStatus(job, JobStatus.RUNNING);

        actuator.pause(PIPE);
        awaitStatus(job, JobStatus.SUSPENDED);
        actuator.resume(PIPE);
        awaitStatus(job, JobStatus.RUNNING);
        // Pause and resume are engine-only: the capture keeps running, so the coordinator is never touched.
        assertThat(events).containsExactly("startCapture:" + PIPE, "submit:" + PIPE);

        actuator.stop(PIPE);
        awaitStatus(job, JobStatus.FAILED); // Jet reports a cancelled job as FAILED
        // Stop cancels the job, then stops the capture behind it: the job was already terminal when capture stopped.
        assertThat(events).containsExactly(
                "startCapture:" + PIPE, "submit:" + PIPE, "stopCapture:" + PIPE + "[jobTerminal]");
    }

    @Test
    void surfacesACaptureFailureThroughTheSeamWhenTheEngineJobReportsNone() {
        RuntimeException boom = new RuntimeException("cdc tail died");
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        coordinator.captureFailure = boom;
        LifecycleActuator actuator = new EngineLifecycleActuator(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        // No job was submitted, so the engine reports no failure; the cdc capture's death still surfaces through
        // the seam the converge loop reads, which is what drives a pipeline whose tail died into FAILED even
        // though its Jet job keeps running over a ring gone quiet.
        assertThat(actuator.failure(PIPE)).contains(boom);
    }

    @Test
    void reportsNoFailureWhenNeitherTheJobNorTheCaptureHasFailed() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        LifecycleActuator actuator = new EngineLifecycleActuator(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        assertThat(actuator.failure(PIPE)).isEmpty();
    }

    /**
     * The state teardown these cases run against: the stand-in topology keeps no state, so what this drops
     * is nothing. It is here because the actuator will not be built without one, which is the point.
     */
    private NestStateTeardown teardown() {
        return new NestStateTeardown(member, new InMemoryKeyedStateStore());
    }

    private static void awaitStatus(Job job, JobStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = job.getStatus();
            if (last == expected) {
                return;
            }
            sleep();
        }
        throw new AssertionError("job did not reach " + expected + " within budget; last status was " + last);
    }

    /** Polls for the job going terminal within a budget; true if it does, false on timeout. */
    private static boolean awaitTerminal(Job job) {
        if (job == null) {
            return false;
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (job.getStatus().isTerminal()) {
                return true;
            }
            sleep();
        }
        return false;
    }

    private static void sleep() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Records each capture verb into a shared event log; at stop it records whether the job was already terminal. */
    private static final class RecordingCaptureCoordinator implements PipelineCaptureCoordinator {

        private final List<String> events;
        private Supplier<Boolean> jobTerminalProbe = () -> false;
        private Throwable captureFailure;

        RecordingCaptureCoordinator(List<String> events) {
            this.events = events;
        }

        @Override
        public void startCapture(String pipelineId) {
            events.add("startCapture:" + pipelineId);
        }

        @Override
        public void stopCapture(String pipelineId) {
            events.add("stopCapture:" + pipelineId + (jobTerminalProbe.get() ? "[jobTerminal]" : "[jobLive]"));
        }

        @Override
        public Optional<Throwable> captureFailure(String pipelineId) {
            return Optional.ofNullable(captureFailure);
        }
    }

    /** Records each topology request into the shared log and returns the idle stand-in topology. */
    private static final class RecordingDagSource implements DagSource {
    /** Keeps no state, so there is nothing for a budget to be applied to. */
    @Override
    public NestCapacity capacityOf(String pipelineId) {
        return NestCapacity.none();
    }


        private final List<String> events;
        private final IdleDagSource idle = new IdleDagSource();

        RecordingDagSource(List<String> events) {
            this.events = events;
        }

        @Override
        public DAG dagFor(String pipelineId) {
            events.add("submit:" + pipelineId);
            return idle.dagFor(pipelineId);
        }

        @Override
        public Set<String> stateNamespacesOf(String pipelineId) {
            return idle.stateNamespacesOf(pipelineId);
        }
    }
}
