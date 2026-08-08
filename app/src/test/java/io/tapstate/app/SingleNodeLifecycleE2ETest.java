package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * A single-node integration of the lifecycle chain the {@code --role=all} process runs: desired intent ->
 * converge loop -> Jet job, and converge loop -> observation -> read faces, over one embedded member and
 * the in-memory store the assembly wires the same way. It drives a pipeline through the four verbs (and a
 * re-dig) and witnesses, at each step, that the mapped Jet operation actually took effect, that the actual
 * state converged with a strictly increasing fencing epoch, and that the store-backed read faces serve the
 * converged state. The errorCount metric reads 0 for the healthy run here; the remaining metrics and the
 * snapshot face are honestly empty, their sources being the capture and transform planes, which merge
 * later. The artificial-failover fencing witness (epoch monotonic under a
 * competing writer, a stale write rejected) lives in the converger and core unit tests; this integration
 * witnesses the epoch advancing once per real actuated transition.
 */
class SingleNodeLifecycleE2ETest {

    private static final String PIPE = "orders-pipe";
    private static final String REV = "rev-1";
    private static final Instant T0 = Instant.parse("2026-07-13T00:00:00Z");

    private HazelcastInstance member;
    private InMemoryStorePort storePort;
    private ConvergenceDriver driver;
    private PipelineObservationQueryService readFaces;

    @BeforeEach
    void wireTheChain() {
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

        Clock clock = Clock.fixed(T0, ZoneOffset.UTC);
        storePort = new InMemoryStorePort();
        Engine engine = new Engine(member);
        EngineLifecycleActuator actuator =
                new EngineLifecycleActuator(engine, new IdleDagSource(), new NoOpCaptureCoordinator(),
                        new NestStateTeardown(member, storePort.keyedState()));
        PipelineConverger converger = new PipelineConverger(storePort.desired(), storePort.state(), actuator, clock);
        ObservationPublisher publisher = new ObservationPublisher(storePort.state(), storePort.observations());
        driver = new ConvergenceDriver(converger, storePort.desired(), publisher);
        readFaces = new PipelineObservationQueryService(
                new ArtifactQueryService(storePort.artifacts()), storePort.observations());
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("start -> pause -> resume -> stop drives the Jet job, converges actual state with a monotonic epoch, and serves the read faces")
    void theFullLifecycleDrivesJetAndConvergesAndServesTheReadFaces() {
        desire(RUNNING);
        Job job = member.getJet().getJob(PIPE);
        assertThat(job).as("start submits the pipeline's Jet job").isNotNull();
        awaitStatus(job, JobStatus.RUNNING);
        assertActualState(RUNNING, 1);
        assertThat(readFaces.status(PIPE)).isEqualTo(new PipelineStatus(PIPE, RUNNING));
        assertThat(readFaces.metrics(PIPE).metrics())
                .as("errorCount is wired: a healthy run reports zero errors")
                .containsOnly(entry("errorCount", 0L));
        assertThat(readFaces.snapshot(PIPE).snapshot()).as("snapshot source is not wired yet").isEmpty();

        desire(PAUSED);
        awaitStatus(job, JobStatus.SUSPENDED);
        assertActualState(PAUSED, 2);
        assertThat(readFaces.status(PIPE)).isEqualTo(new PipelineStatus(PIPE, PAUSED));

        desire(RUNNING);
        awaitStatus(job, JobStatus.RUNNING);
        assertActualState(RUNNING, 3);
        assertThat(readFaces.status(PIPE)).isEqualTo(new PipelineStatus(PIPE, RUNNING));

        desire(STOPPED);
        awaitStatus(job, JobStatus.FAILED); // Jet reports a cancelled job as FAILED
        assertActualState(STOPPED, 4);
        assertThat(readFaces.status(PIPE)).isEqualTo(new PipelineStatus(PIPE, STOPPED));
    }

    @Test
    @DisplayName("a re-dig — stop then start — cancels the job and submits a fresh running one")
    void aReDigCancelsThenSubmitsAFreshJob() {
        desire(RUNNING);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        desire(STOPPED);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.FAILED);

        desire(RUNNING); // the re-dig: stop discarded the old job, start replays from the source

        Job fresh = member.getJet().getJob(PIPE);
        awaitStatus(fresh, JobStatus.RUNNING);
        assertActualState(RUNNING, 3); // 1 = RUNNING, 2 = STOPPED, 3 = RUNNING again
    }

    /** Saves the pipeline's desired target and runs one reconcile pass, the tick the scheduled driver makes. */
    private void desire(PipelineState target) {
        storePort.desired().save(new DesiredState(PIPE, target, REV));
        driver.reconcile();
    }

    private void assertActualState(PipelineState expected, long epoch) {
        CheckpointDoc doc = storePort.state().read(PIPE).orElseThrow();
        assertThat(doc.stateJson()).isEqualTo(StateJson.of(expected));
        assertThat(doc.epoch()).as("the fencing epoch advances once per converged transition").isEqualTo(epoch);
    }

    private static void awaitStatus(Job job, JobStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = job.getStatus();
            if (last == expected) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("job did not reach " + expected + " within budget; last status was " + last);
    }
}
