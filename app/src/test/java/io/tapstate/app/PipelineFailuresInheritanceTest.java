package io.tapstate.app;

import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.ClusterCapacityReservation;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** A controller that did not submit a run may report its failure only through the exact retained receipt. */
class PipelineFailuresInheritanceTest {
    private static final Instant NOW = Instant.parse("2026-10-12T00:00:00Z");
    private static final String PIPELINE = "orders";
    private static final ClusterExecutionProfile PROFILE =
            new ClusterExecutionProfile("east", 1, new ExecutionProfile(1, Map.of("build", "one")));
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
    private static final IllegalStateException FAILURE = new IllegalStateException("the native job failed");
    private static final WorkloadOwner OWNER = new WorkloadOwner("a", "boot-a");
    private static final ClusterCapacityDemand DEMAND = new ClusterCapacityDemand(3, 1, 1, 2, 200, 400);

    @Test
    void theExactFailedRunNamedByTheSubmittedReceiptIsInherited() {
        WorkloadClaim claim = new Claim().build();
        Engine engine = engine(run("job-7", JobStatus.FAILED), run("job-7", JobStatus.FAILED));
        assertThat(PipelineFailures.inherited(PIPELINE, claim, engine, capacity(claim, new Receipt().build())))
                .contains(FAILURE);
    }

    @Test
    void anIncompleteOrForeignExecutionIdentityInheritsNothing() {
        Map<String, Consumer<Claim>> incomplete = new LinkedHashMap<>();
        incomplete.put("recovery claim", claim -> claim.key =
                new WorkloadClaimKey("east", WorkloadClaimType.CLUSTER_RECOVERY, "east"));
        incomplete.put("another pipeline", claim -> claim.key =
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "payments"));
        incomplete.put("no profile generation", claim -> claim.profileGeneration = 0);
        incomplete.put("no execution", claim -> {
            claim.executionGeneration = 0;
            claim.contextExecutionGeneration = 0;
        });
        incomplete.put("context of another execution", claim -> claim.contextExecutionGeneration = 6);
        incomplete.put("no execution claim", claim -> claim.executionClaimGeneration = 0);
        incomplete.put("the submitting authority itself", claim -> claim.claimGeneration = 2);
        incomplete.put("failure already claimed", claim -> claim.failureClaimGeneration = 3);
        incomplete.put("no execution profile", claim -> claim.executionProfile = null);
        incomplete.put("profile of another generation", claim -> claim.executionProfile =
                new ClusterExecutionProfile("east", 2, new ExecutionProfile(1, Map.of("build", "one"))));
        incomplete.put("no execution topology", claim -> claim.executionTopologyRevision = null);
        incomplete.put("no incarnation", claim -> claim.incarnation = null);
        incomplete.put("no revision", claim -> claim.revision = null);
        incomplete.put("blank incarnation", claim -> claim.incarnation = " ");
        incomplete.put("blank revision", claim -> claim.revision = "");
        incomplete.put("members unknown", claim -> claim.members = Map.of());
        assertThat(incomplete).hasSize(16);
        Engine engine = engine(run("job-7", JobStatus.FAILED), run("job-7", JobStatus.FAILED));
        incomplete.forEach((name, change) -> {
            WorkloadClaim claim = new Claim().with(change).build();
            assertThat(PipelineFailures.inherited(PIPELINE, claim, engine, capacity(claim, new Receipt().build())))
                    .as(name).isEmpty();
        });
        WorkloadClaim complete = new Claim().build();
        assertThat(PipelineFailures.inherited(PIPELINE, null, engine, capacity(complete, new Receipt().build()))).isEmpty();
        assertThat(PipelineFailures.inherited(PIPELINE, complete, engine, null)).isEmpty();
    }

    @Test
    void aRunThatIsNotTheFailedSubmittedExecutionInheritsNothing() {
        WorkloadClaim claim = new Claim().build();
        Map<String, Engine.NativeRun> otherRuns = new LinkedHashMap<>();
        otherRuns.put("still running", run("job-7", JobStatus.RUNNING));
        otherRuns.put("another authority", new Engine.NativeRun("job-7", 3, 7, 1, JobStatus.FAILED, Optional.empty()));
        otherRuns.put("another execution", new Engine.NativeRun("job-7", 2, 8, 1, JobStatus.FAILED, Optional.empty()));
        otherRuns.put("another profile", new Engine.NativeRun("job-7", 2, 7, 2, JobStatus.FAILED, Optional.empty()));
        assertThat(otherRuns).hasSize(4);
        otherRuns.forEach((name, run) -> assertThat(PipelineFailures.inherited(PIPELINE, claim, engine(run, run),
                capacity(claim, new Receipt().build()))).as(name).isEmpty());
        assertThat(PipelineFailures.inherited(PIPELINE, claim, engine(null, null),
                capacity(claim, new Receipt().build()))).isEmpty();

        Engine replaced = engine(run("job-7", JobStatus.FAILED), run("job-8", JobStatus.FAILED));
        assertThat(PipelineFailures.inherited(PIPELINE, claim, replaced, capacity(claim, new Receipt().build())))
                .as("the job name moved to another run while its result was read").isEmpty();
        Engine noResult = engine(run("job-7", JobStatus.FAILED), run("job-7", JobStatus.FAILED));
        when(noResult.failureOf(PIPELINE)).thenReturn(Optional.empty());
        assertThat(PipelineFailures.inherited(PIPELINE, claim, noResult, capacity(claim, new Receipt().build())))
                .isEmpty();
    }

    @Test
    void aReceiptThatDoesNotNameThisExactSubmissionInheritsNothing() {
        WorkloadClaim claim = new Claim().build();
        WorkloadClaimFence otherOwner = fence(new WorkloadOwner("b", "boot-b"), 2, 7, 1);
        WorkloadClaimFence restartedOwner = fence(new WorkloadOwner("a", "boot-restarted"), 2, 7, 1);
        Map<String, Consumer<Receipt>> foreign = new LinkedHashMap<>();
        foreign.put("another incarnation", receipt -> receipt.incarnation = "other");
        foreign.put("another profile", receipt -> receipt.profile =
                new ClusterExecutionProfile("east", 1, new ExecutionProfile(1, Map.of("build", "two"))));
        foreign.put("another execution", receipt -> receipt.executionGeneration = 8L);
        foreign.put("another job", receipt -> receipt.nativeJobId = "job-8");
        foreign.put("another submitting authority", receipt -> receipt.pipelineClaim = fence(OWNER, 3, 7, 1));
        foreign.put("authority of another execution", receipt -> receipt.pipelineClaim = fence(OWNER, 2, 8, 1));
        foreign.put("authority of another profile", receipt -> receipt.pipelineClaim = fence(OWNER, 2, 7, 2));
        foreign.put("demand on other members", receipt -> receipt.demand = Map.of("b", DEMAND));
        foreign.put("submitted by a non-member", receipt -> receipt.pipelineClaim = otherOwner);
        foreign.put("submitted by a restarted member", receipt -> receipt.pipelineClaim = restartedOwner);
        assertThat(foreign).hasSize(10);
        Engine engine = engine(run("job-7", JobStatus.FAILED), run("job-7", JobStatus.FAILED));
        foreign.forEach((name, change) -> assertThat(PipelineFailures.inherited(PIPELINE, claim, engine,
                capacity(claim, new Receipt().with(change).build()))).as(name).isEmpty());
        ClusterCapacityStore none = mock(ClusterCapacityStore.class);
        when(none.submittedExecution(claim)).thenReturn(Optional.empty());
        assertThat(PipelineFailures.inherited(PIPELINE, claim, engine, none)).isEmpty();
    }

    @Test
    void theCurrentAuthorityReportsOnlyItsOwnRunOrTheLossItObserved() {
        PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
        WorkloadClaim claim = new Claim().build();
        Engine own = engine(new Engine.NativeRun("job-7", 3, 7, 1, JobStatus.FAILED, Optional.empty()), null);
        assertThat(PipelineFailures.current(PIPELINE, claim, true, own, captures)).contains(FAILURE);
        assertThat(PipelineFailures.current(PIPELINE, null, true, own, captures))
                .as("a fenced controller without authority reports nothing").isEmpty();
        assertThat(PipelineFailures.current(PIPELINE, null, false, own, captures)).contains(FAILURE);
        assertThat(PipelineFailures.current(PIPELINE, new Claim().with(c -> c.profileGeneration = 0).build(),
                true, own, captures)).contains(FAILURE);

        Engine retained = engine(run("job-7", JobStatus.FAILED), null);
        assertThat(PipelineFailures.current(PIPELINE, claim, true, retained, captures))
                .as("the older authority's run cannot classify this one").isEmpty();
        TapstateException lost = new TapstateException(EngineError.NO_SUCH_JOB, Map.of("pipeline", PIPELINE), null);
        when(retained.lost(PIPELINE)).thenReturn(Optional.of(lost));
        assertThat(PipelineFailures.current(PIPELINE, claim, true, retained, captures)).contains(lost);
    }

    private static Engine.NativeRun run(String job, JobStatus status) {
        return new Engine.NativeRun(job, 2, 7, 1, status, Optional.empty());
    }

    private static Engine engine(Engine.NativeRun first, Engine.NativeRun after) {
        Engine engine = mock(Engine.class);
        when(engine.nativeRun(PIPELINE)).thenReturn(Optional.ofNullable(first), Optional.ofNullable(after));
        when(engine.failureOf(PIPELINE)).thenReturn(Optional.of(FAILURE));
        return engine;
    }

    private static ClusterCapacityStore capacity(WorkloadClaim claim, ClusterCapacityReservation reservation) {
        ClusterCapacityStore capacity = mock(ClusterCapacityStore.class);
        when(capacity.submittedExecution(claim))
                .thenReturn(Optional.of(new ClusterCapacityStore.SubmittedExecution(reservation, false)));
        return capacity;
    }

    private static WorkloadClaimFence fence(WorkloadOwner owner, long claimGeneration, long execution, long profile) {
        return new WorkloadClaimFence(KEY, owner, claimGeneration, execution, 2, profile);
    }

    /** The current authority over a run another member submitted, changed one fact at a time. */
    private static final class Claim {
        WorkloadClaimKey key = KEY;
        long claimGeneration = 3;
        long executionGeneration = 7;
        long contextExecutionGeneration = 7;
        long executionClaimGeneration = 2;
        long failureClaimGeneration;
        long profileGeneration = 1;
        ClusterExecutionProfile executionProfile = PROFILE;
        Long executionTopologyRevision = 2L;
        String incarnation = "incarnation";
        String revision = "revision";
        Map<String, ClusterExecutionMember> members = Map.of("a", new ClusterExecutionMember("a", "boot-a", "uuid-a"));

        Claim with(Consumer<Claim> change) {
            change.accept(this);
            return this;
        }

        WorkloadClaim build() {
            return new WorkloadClaim(key, new WorkloadOwner("c", "boot-c"), claimGeneration, executionGeneration, 2,
                    NOW, contextExecutionGeneration, executionClaimGeneration, Set.of("a"), failureClaimGeneration,
                    false, profileGeneration, executionProfile, executionTopologyRevision, incarnation, revision, members);
        }
    }

    /** The retained submission receipt of that run, changed one fact at a time. */
    private static final class Receipt {
        String incarnation = "incarnation";
        ClusterExecutionProfile profile = PROFILE;
        WorkloadClaimFence pipelineClaim = fence(OWNER, 2, 7, 1);
        Map<String, ClusterCapacityDemand> demand = Map.of("a", DEMAND);
        Long executionGeneration = 7L;
        String nativeJobId = "job-7";

        Receipt with(Consumer<Receipt> change) {
            change.accept(this);
            return this;
        }

        ClusterCapacityReservation build() {
            return new ClusterCapacityReservation("reservation-1", "east", PIPELINE, incarnation, "intent", profile,
                    pipelineClaim, demand, NOW, NOW.plusSeconds(30), executionGeneration, nativeJobId);
        }
    }
}
