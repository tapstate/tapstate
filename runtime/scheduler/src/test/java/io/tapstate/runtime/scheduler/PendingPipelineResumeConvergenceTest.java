package io.tapstate.runtime.scheduler;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PendingPipelineResume;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static io.tapstate.runtime.scheduler.ConvergeStatus.CONVERGED;
import static io.tapstate.runtime.scheduler.ConvergeStatus.SUPERSEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PendingPipelineResumeConvergenceTest {
    private static final String PIPELINE = "orders";
    private static final Instant NOW = Instant.parse("2026-10-11T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final DesiredState RESUME = new DesiredState(PIPELINE, RUNNING, "a".repeat(64));
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile("cluster", 2,
            new ExecutionProfile(1, Map.of("runtime.protocol", "1")));
    private static final Map<String, ClusterExecutionMember> MEMBERS = Map.of(
            "a", new ClusterExecutionMember("a", "boot-a", "00000000-0000-4000-8000-000000000001"),
            "b", new ClusterExecutionMember("b", "boot-b", "00000000-0000-4000-8000-000000000002"),
            "c", new ClusterExecutionMember("c", "boot-c", "00000000-0000-4000-8000-000000000003"));
    private static final WorkloadClaim ORIGINAL = claim(7, NOW.plusSeconds(60), MEMBERS);

    @Test void aReplacementConvergerRetriesTheReceiptAcceptedBeforeActuation() {
        Fixture fixture = new Fixture(PAUSED);
        AssertionError interrupted = new AssertionError("Interrupted after accepting the state transition");
        fixture.state.afterAccepted = () -> { throw interrupted; };

        assertThatThrownBy(() -> fixture.converger(fixture.actuator).converge(PIPELINE)).isSameAs(interrupted);
        PendingPipelineResume accepted = fixture.state.pendingResume(PIPELINE).orElseThrow();
        assertThat(accepted.stateEpoch()).isEqualTo(1);
        assertThat(accepted.intentFingerprint()).isEqualTo(DesiredStateFingerprint.of(RESUME));
        assertThat(accepted.originalClaim()).isEqualTo(ORIGINAL);
        assertThat(accepted.originalNativeJobId()).isEqualTo("42");
        assertThat(accepted.originalRuntimeExecutionId()).isEqualTo("43");
        assertThat(fixture.actuator.resumes).isZero();
        assertThat(fixture.state.read(PIPELINE).orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));

        RecordingActuator replacement = new RecordingActuator(fixture.state);
        replacement.completeOnResume = true;
        ConvergeResult retried = fixture.converger(replacement).converge(PIPELINE);

        assertThat(retried.status()).isEqualTo(CONVERGED);
        assertThat(replacement.prepares).isZero();
        assertThat(replacement.resumes).isEqualTo(1);
        assertThat(replacement.observedResumes).containsExactly(accepted);
        assertThat(replacement.starts).isZero();
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(2);
        assertThat(fixture.state.pendingResume(PIPELINE)).isEmpty();
        assertThat(fixture.state.plainExpectedEpochs).containsExactly(1L);
    }

    @Test void aReplacementConsumesCompletedResumeProofWithoutResumingTwice() {
        Fixture fixture = new Fixture(PAUSED);
        fixture.actuator.completeOnResume = true;
        AssertionError interrupted = new AssertionError("Interrupted after the resumed run completed initialization");
        fixture.actuator.afterResume = () -> { throw interrupted; };

        assertThatThrownBy(() -> fixture.converger(fixture.actuator).converge(PIPELINE)).isSameAs(interrupted);
        PendingPipelineResume accepted = fixture.state.pendingResume(PIPELINE).orElseThrow();
        assertThat(fixture.actuator.resumes).isEqualTo(1);
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(1);

        RecordingActuator replacement = new RecordingActuator(fixture.state);
        replacement.completedRequest = fixture.actuator.completedRequest;
        ConvergeResult acknowledged = fixture.converger(replacement).converge(PIPELINE);

        assertThat(acknowledged.status()).isEqualTo(CONVERGED);
        assertThat(replacement.resumes).isZero();
        assertThat(replacement.starts).isZero();
        assertThat(replacement.completedRequest).isEqualTo(accepted);
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(2);
        assertThat(fixture.state.rawReceipt(PIPELINE)).isEmpty();
        assertThat(fixture.state.plainExpectedEpochs).containsExactly(1L);
    }

    @Test void aWaitingResumeRetainsItsAcceptedEpochAndDoesNotStartAMissingJob() {
        Fixture fixture = new Fixture(PAUSED);
        fixture.actuator.carryingJob = false;

        fixture.converger(fixture.actuator).converge(PIPELINE);
        PendingPipelineResume accepted = fixture.state.pendingResume(PIPELINE).orElseThrow();
        ConvergeResult waiting = fixture.converger(fixture.actuator).converge(PIPELINE);

        assertThat(waiting.status()).isEqualTo(CONVERGED);
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(1);
        assertThat(fixture.state.pendingResume(PIPELINE)).contains(accepted);
        assertThat(fixture.state.plainExpectedEpochs).isEmpty();
        assertThat(fixture.actuator.prepares).isEqualTo(1);
        assertThat(fixture.actuator.resumes).isEqualTo(2);
        assertThat(fixture.actuator.starts).isZero();
        assertThat(fixture.actuator.carryingChecks).isZero();
    }

    @Test void aNewerPauseFencesCompletionWithoutRebasingTheAcknowledgement() {
        assertSupersedingTransition(PAUSED);
    }

    @Test void aNewerStopFencesCompletionWithoutRebasingTheAcknowledgement() {
        assertSupersedingTransition(STOPPED);
    }

    @Test void aLaterResumeWithTheSameIntentFingerprintKeepsItsOwnReceipt() {
        Fixture fixture = waitingResume();
        PendingPipelineResume first = fixture.state.pendingResume(PIPELINE).orElseThrow();
        PendingPipelineResume later = new PendingPipelineResume(3, first.intentFingerprint(), ORIGINAL,
                first.originalNativeJobId(), first.originalRuntimeExecutionId());
        fixture.actuator.completedRequest = first;
        fixture.state.beforePlain = () -> {
            fixture.state.competingTransition(PAUSED, null);
            fixture.state.competingTransition(RUNNING, later);
        };

        ConvergeResult stale = fixture.converger(fixture.actuator).converge(PIPELINE);

        assertThat(stale.status()).isEqualTo(SUPERSEDED);
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(3);
        assertThat(fixture.state.pendingResume(PIPELINE)).contains(later);
        assertThat(later.intentFingerprint()).isEqualTo(first.intentFingerprint());
        assertThat(later.sameRequestAs(first)).isFalse();
        assertThat(fixture.state.plainExpectedEpochs).containsExactly(1L);
        assertThat(fixture.actuator.resumes).isEqualTo(1);
        assertThat(fixture.actuator.starts).isZero();
    }

    @Test void aSupersedingRunningIntentPreventsAcknowledgingItsPreviousResume() {
        Fixture fixture = waitingResume();
        PendingPipelineResume accepted = fixture.state.pendingResume(PIPELINE).orElseThrow();
        fixture.actuator.completedRequest = accepted;
        fixture.actuator.beforeCompletion = () -> fixture.desired.save(new DesiredState(PIPELINE, RUNNING, "b".repeat(64)));

        ConvergeResult superseded = fixture.converger(fixture.actuator).converge(PIPELINE);

        assertThat(superseded.status()).isEqualTo(SUPERSEDED);
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(1);
        assertThat(fixture.state.rawReceipt(PIPELINE)).contains(accepted);
        assertThat(fixture.state.plainExpectedEpochs).isEmpty();
        assertThat(fixture.actuator.resumes).isEqualTo(1);
    }

    @Test void anOrdinaryRunningCheckpointWithoutAJobDoesNotInventAResumeReceipt() {
        Fixture fixture = new Fixture(RUNNING);
        fixture.actuator.carryingJob = false;

        ConvergeResult started = fixture.converger(fixture.actuator).converge(PIPELINE);

        assertThat(started.status()).isEqualTo(CONVERGED);
        assertThat(fixture.actuator.starts).isEqualTo(1);
        assertThat(fixture.actuator.resumes).isZero();
        assertThat(fixture.actuator.prepares).isZero();
        assertThat(fixture.state.pendingResume(PIPELINE)).isEmpty();
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isZero();
    }

    @Test void aStoreWithoutAtomicReceiptSupportRefusesBeforeAdvancingOrResuming() {
        InMemoryStateStore ordinary = new InMemoryStateStore();
        ordinary.create(PIPELINE, StateJson.of(PAUSED), NOW);
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        desired.save(RESUME);
        RecordingActuator actuator = new RecordingActuator(ordinary);
        PipelineConverger converger = new PipelineConverger(desired, ordinary, actuator, CLOCK);

        assertThatThrownBy(() -> converger.converge(PIPELINE)).isInstanceOfSatisfying(TapstateException.class,
                refused -> assertThat(refused.code()).isEqualTo(IoError.STORE_UNAVAILABLE));

        assertThat(ordinary.read(PIPELINE).orElseThrow().stateJson()).isEqualTo(StateJson.of(PAUSED));
        assertThat(ordinary.read(PIPELINE).orElseThrow().epoch()).isZero();
        assertThat(ordinary.swapAttempts()).isZero();
        assertThat(actuator.resumes).isZero();
        assertThat(actuator.starts).isZero();
    }

    @Test void renewalTopologyRefreshAndReservationLinkDoNotFenceTheSameAcceptedRequest() {
        Fixture fixture = waitingResume();
        PendingPipelineResume accepted = fixture.state.pendingResume(PIPELINE).orElseThrow();
        PendingPipelineResume observed = new PendingPipelineResume(accepted.stateEpoch(), accepted.intentFingerprint(),
                claim(8, NOW.plusSeconds(120), MEMBERS), accepted.originalNativeJobId(),
                accepted.originalRuntimeExecutionId(), "reservation-99");
        Map<String, ClusterExecutionMember> changedMembers = new HashMap<>(MEMBERS);
        changedMembers.put("b", new ClusterExecutionMember("b", "boot-b-next", "00000000-0000-4000-8000-000000000004"));
        PendingPipelineResume anotherContext = new PendingPipelineResume(accepted.stateEpoch(), accepted.intentFingerprint(),
                claim(8, NOW.plusSeconds(120), changedMembers), accepted.originalNativeJobId(),
                accepted.originalRuntimeExecutionId(), "reservation-99");
        assertThat(accepted.sameRequestAs(observed)).isTrue();
        assertThat(observed.sameRequestAs(accepted)).isTrue();
        assertThat(accepted.sameRequestAs(anotherContext)).isFalse();
        fixture.actuator.completedRequest = accepted;
        fixture.actuator.beforeCompletion = () -> fixture.state.observeReceipt(observed);

        ConvergeResult acknowledged = fixture.converger(fixture.actuator).converge(PIPELINE);

        assertThat(acknowledged.status()).isEqualTo(CONVERGED);
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(2);
        assertThat(fixture.state.rawReceipt(PIPELINE)).isEmpty();
        assertThat(fixture.state.plainExpectedEpochs).containsExactly(1L);
        assertThat(fixture.actuator.resumes).isEqualTo(1);
    }

    private static void assertSupersedingTransition(PipelineState next) {
        Fixture fixture = waitingResume();
        PendingPipelineResume accepted = fixture.state.pendingResume(PIPELINE).orElseThrow();
        fixture.actuator.completedRequest = accepted;
        fixture.state.beforePlain = () -> {
            fixture.desired.save(new DesiredState(PIPELINE, next, RESUME.revision()));
            fixture.state.competingTransition(next, null);
        };

        ConvergeResult stale = fixture.converger(fixture.actuator).converge(PIPELINE);

        assertThat(stale.status()).isEqualTo(SUPERSEDED);
        assertThat(fixture.state.read(PIPELINE).orElseThrow().stateJson()).isEqualTo(StateJson.of(next));
        assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(2);
        assertThat(fixture.state.plainExpectedEpochs).containsExactly(1L);
        assertThat(fixture.actuator.resumes).isEqualTo(1);
        assertThat(fixture.actuator.starts).isZero();
        assertThat(fixture.actuator.stops).isZero();
    }

    private static Fixture waitingResume() {
        Fixture fixture = new Fixture(PAUSED);
        fixture.converger(fixture.actuator).converge(PIPELINE);
        assertThat(fixture.state.pendingResume(PIPELINE)).isPresent();
        return fixture;
    }

    private static WorkloadClaim claim(long acquisitionTopology, Instant leaseUntil,
            Map<String, ClusterExecutionMember> members) {
        return new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE),
                new WorkloadOwner("a", "boot-a"), 3, 9, acquisitionTopology, leaseUntil, 9, 3,
                Set.of("a", "b", "c"), 0, false, 2, PROFILE, 7L, "pipeline-incarnation", RESUME.revision(), members);
    }

    private static final class Fixture {
        private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
        private final ReceiptStateStore state = new ReceiptStateStore();
        private final RecordingActuator actuator = new RecordingActuator(state);

        private Fixture(PipelineState initial) {
            desired.save(RESUME);
            state.create(PIPELINE, StateJson.of(initial), NOW);
        }

        private PipelineConverger converger(RecordingActuator driving) {
            return new PipelineConverger(desired, state, driving, CLOCK);
        }
    }

    /** The existing EpochCas store and receipt share one synchronized acceptance boundary. */
    private static final class ReceiptStateStore implements StateStore {
        private final InMemoryStateStore values = new InMemoryStateStore();
        private final Map<String, PendingPipelineResume> receipts = new HashMap<>();
        private final List<Long> plainExpectedEpochs = new ArrayList<>();
        private Runnable afterAccepted = () -> { };
        private Runnable beforePlain = () -> { };

        @Override public synchronized Optional<CheckpointDoc> read(String pipeline) { return values.read(pipeline); }
        @Override public synchronized void create(String pipeline, String json, Instant touched) {
            values.create(pipeline, json, touched);
        }
        @Override public synchronized void delete(String pipeline) { values.delete(pipeline); }

        @Override public synchronized CasOutcome compareAndSwap(String pipeline, long epoch, String json, Instant touched) {
            plainExpectedEpochs.add(epoch);
            Runnable interleaving = beforePlain;
            beforePlain = () -> { };
            interleaving.run();
            CasOutcome result = values.compareAndSwap(pipeline, epoch, json, touched);
            if (result instanceof CasOutcome.Applied) { receipts.remove(pipeline); }
            return result;
        }

        @Override public synchronized CasOutcome compareAndSwap(String pipeline, long epoch, String json, Instant touched,
                PendingPipelineResume receipt) {
            if (receipt == null) { return compareAndSwap(pipeline, epoch, json, touched); }
            if (receipt.stateEpoch() != epoch + 1 || !receipt.originalClaim().key().resourceId().equals(pipeline)
                    || !json.equals(StateJson.of(RUNNING))) {
                throw new IllegalArgumentException("receipt must belong to its accepted RUNNING transition");
            }
            CasOutcome result = values.compareAndSwap(pipeline, epoch, json, touched);
            if (result instanceof CasOutcome.Applied) {
                receipts.put(pipeline, receipt);
                Runnable interruption = afterAccepted;
                afterAccepted = () -> { };
                interruption.run();
            }
            return result;
        }

        @Override public synchronized Optional<PendingPipelineResume> pendingResume(String pipeline) {
            CheckpointDoc current = values.read(pipeline).orElse(null);
            return Optional.ofNullable(receipts.get(pipeline)).filter(receipt -> current != null
                    && receipt.stateEpoch() == current.epoch() && current.stateJson().equals(StateJson.of(RUNNING)));
        }

        private synchronized Optional<PendingPipelineResume> rawReceipt(String pipeline) {
            return Optional.ofNullable(receipts.get(pipeline));
        }

        private synchronized void observeReceipt(PendingPipelineResume receipt) {
            CheckpointDoc current = values.read(PIPELINE).orElseThrow();
            if (receipt.stateEpoch() != current.epoch()) { throw new IllegalArgumentException("receipt epoch changed"); }
            receipts.put(PIPELINE, receipt);
        }

        private synchronized void competingTransition(PipelineState next, PendingPipelineResume receipt) {
            CheckpointDoc current = values.read(PIPELINE).orElseThrow();
            CasOutcome result = values.applySwap(PIPELINE, current.epoch(), StateJson.of(next), NOW);
            assertThat(result).isInstanceOf(CasOutcome.Applied.class);
            if (receipt == null) { receipts.remove(PIPELINE); }
            else { observeReceipt(receipt); }
        }
    }

    /** Records lifecycle calls; completion is the actuator port's separate, explicit observation. */
    private static final class RecordingActuator implements LifecycleActuator {
        private final StateStore state;
        private final List<PendingPipelineResume> observedResumes = new ArrayList<>();
        private int prepares;
        private int starts;
        private int resumes;
        private int stops;
        private int carryingChecks;
        private boolean carryingJob = true;
        private boolean completeOnResume;
        private PendingPipelineResume completedRequest;
        private Runnable afterResume = () -> { };
        private Runnable beforeCompletion = () -> { };

        private RecordingActuator(StateStore state) { this.state = state; }
        @Override public void start(String pipeline) { starts++; }
        @Override public void pause(String pipeline) { }
        @Override public void stop(String pipeline, boolean purgeState) { stops++; }
        @Override public Optional<Throwable> failure(String pipeline) { return Optional.empty(); }
        @Override public Optional<Throwable> lost(String pipeline) { return Optional.empty(); }
        @Override public boolean isCarryingAJob(String pipeline) { carryingChecks++; return carryingJob; }

        @Override public Optional<PendingPipelineResume> prepareResume(String pipeline, DesiredState intent, long epoch) {
            prepares++;
            return Optional.of(new PendingPipelineResume(epoch, DesiredStateFingerprint.of(intent), ORIGINAL,
                    "42", "43"));
        }

        @Override public boolean acceptsPendingResume(PendingPipelineResume receipt) {
            return receipt.originalClaim().executionProfile().equals(PROFILE)
                    && receipt.originalClaim().executionIncarnation().equals(ORIGINAL.executionIncarnation());
        }

        @Override public void resume(String pipeline) {
            PendingPipelineResume accepted = state.pendingResume(pipeline).orElseThrow();
            resumes++;
            observedResumes.add(accepted);
            if (completeOnResume) { completedRequest = accepted; }
            Runnable interruption = afterResume;
            afterResume = () -> { };
            interruption.run();
        }

        @Override public boolean resumeCompleted(PendingPipelineResume receipt) {
            Runnable competingIntent = beforeCompletion;
            beforeCompletion = () -> { };
            competingIntent.run();
            return completedRequest != null && completedRequest.sameRequestAs(receipt);
        }
    }
}
