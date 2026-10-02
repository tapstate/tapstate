package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorAdmission;
import io.tapstate.spi.store.SuccessorEnd;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real transactions keep one phased handoff and the existing execution sequence atomic. */
@RequiresDocker
class MongoDurableRebuildHandoffIT {
    private static final String CLUSTER = "handoff-cluster", PIPE = "orders", INC = "inc-a";
    private static final Instant AT = Instant.parse("2026-10-01T00:00:00Z");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey(CLUSTER, WorkloadClaimType.PIPELINE_ACTUATION, PIPE);
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test void continuationKeepsItsSourceUntilRealBindingAndReadableHandoff() {
        try (Fixture f = new Fixture()) {
            StopReservation stop = f.stop(false);
            StopReservation pending = f.state.markReplacementPending(stop, AT).orElseThrow();
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
            assertThat(pending.source()).isEqualTo(stop.source());
            assertThat(pending.originalDesired()).isEqualTo(stop.originalDesired());
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(1);
            SuccessorAdmission admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow();
            StopReservation target = admitted.reservation();
            assertThat(target.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_ADMITTED);
            assertThat(target.source().scope().executionGeneration()).isEqualTo(1);
            assertThat(target.writerAuthority().executionGeneration()).isEqualTo(2);
            assertThat(target.successor().scope().executionGeneration()).isEqualTo(2);
            assertThat(target.successor().job()).isNull();
            assertThat(admitted.advancedClaim()).isEmpty();
            assertThat(f.claims.read(KEY)).isEmpty();
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
            assertThatThrownBy(target::subject).isInstanceOf(IllegalStateException.class);
            StopReservation bound = f.bind(target);
            assertThat(f.actual()).isEqualTo(PipelineState.RUNNING);
            assertThat(bound.source()).isEqualTo(stop.source());
            assertThat(f.state.completeHandoff(bound, null, AT)).isEmpty();
            assertThat(f.state.readStopReservation(PIPE)).contains(bound);
            assertThat(f.state.completeHandoff(bound, bound.handoffIdentity(), AT)).isPresent();
            assertThat(f.state.readStopReservation(PIPE)).isEmpty();
            assertThat(f.actual()).isEqualTo(PipelineState.RUNNING);
        }
    }

    @Test void resetCompletesWithoutWaitingForTelemetry() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(true), AT).orElseThrow();
            StopReservation bound = f.bind(f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation());
            assertThat(bound.counterPolicy()).isEqualTo(StopReservation.CounterPolicy.RESET);
            assertThat(f.state.completeHandoff(bound, null, AT)).isPresent();
        }
    }

    @Test void coldEmptySlotRetiresWithoutLosingSourceOrReusingItsGeneration() {
        try (Fixture f = new Fixture()) {
            StopReservation stop = f.stop(false);
            StopReservation pending = f.state.markReplacementPending(stop, AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            StopReservation retired = f.state.retireSuccessor(admitted,
                    new SuccessorEnd.Absent(admitted.successor().scope(), "submit-b"), AT).orElseThrow();
            assertThat(retired.phase()).isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING);
            assertThat(retired.source()).isEqualTo(stop.source());
            assertThat(retired.token()).isEqualTo(stop.token());
            assertThat(retired.counterPolicy()).isEqualTo(StopReservation.CounterPolicy.CONTINUE);
            assertThat(retired.writerAuthority().executionGeneration()).isEqualTo(2);
            StopReservation next = f.state.admitSuccessor(retired, INC, "submit-c", AT).orElseThrow().reservation();
            assertThat(next.successor().scope().executionGeneration()).isEqualTo(3);
            assertThat(next.source().scope().executionGeneration()).isEqualTo(1);
            assertThat(f.state.bindSuccessor(admitted, admitted.successor().scope(),
                    new StopReservation.JobIdentity(CLUSTER, 42, "submit-b"), AT)).isEmpty();
        }
    }

    @Test void scopeOrBootMismatchCannotBindOrRetireAnOccupiedSlot() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            assertThat(f.state.bindSuccessor(admitted, admitted.successor().scope(),
                    new StopReservation.JobIdentity(CLUSTER, 42, "foreign-boot"), AT)).isEmpty();
            assertThat(f.state.retireSuccessor(admitted,
                    new SuccessorEnd.Absent(admitted.successor().scope(), "foreign-boot"), AT)).isEmpty();
            StopReservation bound = f.bind(admitted);
            assertThat(f.state.retireSuccessor(bound,
                    new SuccessorEnd.Terminal(bound.successor().scope(),
                            new StopReservation.JobIdentity(CLUSTER, 99, "submit-b")), AT)).isEmpty();
            assertThat(f.state.readStopReservation(PIPE)).contains(bound);
            assertThat(f.actual()).isEqualTo(PipelineState.RUNNING);
        }
    }

    @Test void aFinishedBoundSuccessorReturnsToPendingWithTheOriginalPolicy() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            StopReservation bound = f.bind(f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation());
            StopReservation next = f.state.retireSuccessor(bound,
                    new SuccessorEnd.Terminal(bound.successor().scope(), bound.successor().job()), AT).orElseThrow();
            assertThat(next.source()).isEqualTo(bound.source());
            assertThat(next.successor()).isNull();
            assertThat(next.phase()).isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING);
            assertThat(next.counterPolicy()).isEqualTo(bound.counterPolicy());
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
        }
    }

    @Test void claimedAdmissionReturnsTheExactAdvancedClaimWithoutExtendingItsLease() {
        try (Fixture f = new Fixture()) {
            WorkloadClaim acquired = f.claims.acquire(KEY, new WorkloadOwner("node-a", "boot-a"),
                    1, Duration.ofMinutes(2)).claim();
            WorkloadClaim first = f.claims.advanceUnderClaim(acquired, 1).orElseThrow();
            StopReservation pending = f.state.markReplacementPending(f.stop(false,
                    StopAuthority.claimed(WorkloadClaimFence.from(first))), AT).orElseThrow();
            SuccessorAdmission admitted = f.state.admitSuccessor(pending, INC, "submit-a", AT).orElseThrow();
            WorkloadClaim advanced = admitted.advancedClaim().orElseThrow();
            assertThat(advanced.executionGeneration()).isEqualTo(2);
            assertThat(advanced.leaseUntil()).isEqualTo(first.leaseUntil());
            assertThat(admitted.reservation().writerAuthority()).isEqualTo(StopAuthority.claimed(WorkloadClaimFence.from(advanced)));
            assertThat(f.claims.release(advanced)).isTrue();
            WorkloadClaim next = f.claims.acquire(KEY, new WorkloadOwner("node-b", "boot-b"),
                    2, Duration.ofMinutes(2)).claim();
            StopReservation rebound = f.state.rebindStop(admitted.reservation(),
                    StopAuthority.claimed(WorkloadClaimFence.from(next)), AT).orElseThrow();
            assertThat(rebound.source()).isEqualTo(admitted.reservation().source());
            assertThat(rebound.successor()).isEqualTo(admitted.reservation().successor());
            assertThat(f.state.bindSuccessor(admitted.reservation(), admitted.reservation().successor().scope(),
                    new StopReservation.JobIdentity(CLUSTER, 42, "submit-a"), AT)).isEmpty();
        }
    }

    @Test void claimedPhasedAdmissionResetsTheInheritedExecutionContextAtomically() {
        try (Fixture f = new Fixture(PipelineState.FAILED)) {
            WorkloadClaim acquired = f.claims.acquire(KEY, new WorkloadOwner("node-a", "boot-a"),
                    1, Duration.ofMinutes(2)).claim();
            Set<String> oldMembers = Set.of("node-a", "node-b");
            WorkloadClaim oldRun = f.claims.advanceExecution(acquired, 1, oldMembers).orElseThrow();
            WorkloadClaim failed = f.claims.recordExecutionFailure(oldRun, true).orElseThrow();
            assertThat(failed.failureClaimGeneration()).isEqualTo(oldRun.claimGeneration());
            assertThat(failed.failureAfterMemberLoss()).isTrue();
            assertThat(f.claims.release(failed)).isTrue();
            WorkloadClaim current = f.claims.acquire(KEY, new WorkloadOwner("node-b", "boot-b"),
                    2, Duration.ofMinutes(2)).claim();
            assertThat(current.claimGeneration()).isGreaterThan(current.executionClaimGeneration());
            assertThat(current.executionNodeIds()).isEqualTo(oldMembers);
            StopReservation pending = f.state.markReplacementPending(f.stop(true,
                    StopAuthority.claimed(WorkloadClaimFence.from(current))), AT).orElseThrow();
            Set<String> nextMembers = Set.of("node-b", "node-c");

            SuccessorAdmission admitted = f.state.admitSuccessor(pending, INC, "submit-b", nextMembers, AT).orElseThrow();

            WorkloadClaim advanced = admitted.advancedClaim().orElseThrow();
            StopReservation slot = admitted.reservation();
            assertThat(advanced.executionGeneration()).isEqualTo(current.executionGeneration() + 1);
            assertThat(slot.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_ADMITTED);
            assertThat(slot.successor().scope().executionGeneration()).isEqualTo(advanced.executionGeneration());
            assertThat(slot.successor().scope().pipelineIncarnationId()).isEqualTo(INC);
            assertThat(slot.successor().submissionBootId()).isEqualTo("submit-b");
            assertThat(slot.successor().job()).isNull();
            assertThat(advanced.contextExecutionGeneration()).isEqualTo(slot.successor().scope().executionGeneration());
            assertThat(advanced.executionClaimGeneration()).isEqualTo(current.claimGeneration());
            assertThat(advanced.executionNodeIds()).isEqualTo(nextMembers).isNotEqualTo(oldMembers);
            assertThat(advanced.failureClaimGeneration()).isZero();
            assertThat(advanced.failureAfterMemberLoss()).isFalse();
            assertThat(advanced.leaseUntil()).isEqualTo(current.leaseUntil());
            assertThat(f.claims.read(KEY).orElseThrow().claim()).isEqualTo(advanced);
            assertThat(f.state.readStopReservation(PIPE)).contains(slot);
            assertThat(f.state.read(PIPE).orElseThrow().epoch()).isEqualTo(slot.reservedEpoch());
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(slot.successor().scope().executionGeneration());
            assertThat(slot.writerAuthority()).isEqualTo(StopAuthority.claimed(WorkloadClaimFence.from(advanced)));
            assertThat(slot.source()).isEqualTo(pending.source());
            assertThat(slot.token()).isEqualTo(pending.token());
            assertThat(slot.originalDesired()).isEqualTo(pending.originalDesired());
            assertThat(slot.counterPolicy()).isEqualTo(pending.counterPolicy());
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
        }
    }

    @Test void changedIntentOrArtifactPreventsAdmissionAndConsumesNoGeneration() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            f.desired.save(new DesiredState(PIPE, PipelineState.STOPPED, "rev-new"));
            assertThat(f.state.admitSuccessor(pending, INC, "submit-b", AT)).isEmpty();
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(1);
            f.desired.save(pending.originalDesired());
            f.database.getCollection("artifacts").updateOne(new Document("_id", PIPE),
                    new Document("$set", new Document("pipelineIncarnationId", "inc-recreated")));
            assertThat(f.state.admitSuccessor(pending, INC, "submit-b", AT)).isEmpty();
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(1);
            assertThat(f.state.readStopReservation(PIPE)).contains(pending);
        }
    }

    @Test void oversizedSuccessorRollsBackItsGenerationAndEveryGuardWrite() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            Document before = f.database.getCollection("workload_claims").find().first();
            assertThatThrownBy(() -> f.state.admitSuccessor(pending, INC, "b".repeat(5000), AT))
                    .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE));
            assertThat(f.database.getCollection("workload_claims").find().first()).isEqualTo(before);
            assertThat(f.state.readStopReservation(PIPE)).contains(pending);
        }
    }

    @Test void legacyConversionProvesResumePolicyAndKeepsTheActualOldJob() {
        try (Fixture f = new Fixture()) {
            f.claims.advanceStandalone(CLUSTER, PIPE);
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-a");
            f.desired.save(intent);
            CheckpointDoc actual = f.state.read(PIPE).orElseThrow();
            StopReservation legacy = new StopReservation(PIPE, "legacy-work", actual.epoch(), actual.epoch() + 1,
                    intent, new StopReservation.ExistingJob(INC, 1,
                            new StopReservation.JobIdentity(CLUSTER, 11, "old-boot"), StopAuthority.standalone(CLUSTER, 1)));
            f.state.reserveStop(actual, legacy, AT).orElseThrow();
            StopReservation current = f.state.promoteStopReservation(legacy, intent, legacy.writerAuthority(), AT).orElseThrow();
            assertThat(current.formatVersion()).isEqualTo(16);
            assertThat(current.counterPolicy()).isEqualTo(StopReservation.CounterPolicy.CONTINUE);
            assertThat(current.source()).isEqualTo(legacy.source());
            assertThat(current.token()).isEqualTo(legacy.token());
            assertThat(current.sourceEpoch()).isEqualTo(legacy.sourceEpoch());
            assertThat(current.reservedEpoch()).isEqualTo(legacy.reservedEpoch() + 1);
        }
    }

    @Test void factualLegacyNoJobGetsOnlyItsGuardedArtifactScopeAndNoInventedJob() {
        try (Fixture f = new Fixture()) {
            f.claims.advanceStandalone(CLUSTER, PIPE);
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-a"); f.desired.save(intent);
            StopReservation legacy = new StopReservation(PIPE, "legacy-no-job", 0, 1, intent,
                    new StopReservation.NoJob(CLUSTER, StopAuthority.standalone(CLUSTER, 1)));
            f.state.reserveStop(f.state.read(PIPE).orElseThrow(), legacy, AT).orElseThrow();
            StopReservation current = f.state.promoteStopReservation(legacy, intent, legacy.writerAuthority(), AT).orElseThrow();
            assertThat(current.source().scope()).isEqualTo(new ObservationStore.Scope(INC, 1));
            assertThat(current.source().oldJob()).isNull();
        }
    }

    @Test void malformedCompactPhaseOrSuccessorIsACodedStorageFailure() {
        try (Fixture f = new Fixture()) {
            StopReservation stop = f.stop(false);
            f.database.getCollection("states").updateOne(new Document("_id", PIPE),
                    new Document("$set", new Document("stopReservation.p", "SUCCESSOR_BOUND")));
            assertThatThrownBy(() -> f.state.readStopReservation(PIPE))
                    .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(stop.source().scope().executionGeneration()).isEqualTo(1);
        }
    }

    @Test void aStampedRestartCannotBeDecodedAsContinuation() {
        try (Fixture f = new Fixture()) {
            f.stop(true);
            f.database.getCollection("states").updateOne(new Document("_id", PIPE),
                    new Document("$set", new Document("stopReservation.c", "CONTINUE")));
            assertThatThrownBy(() -> f.state.readStopReservation(PIPE))
                    .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @Test void continuationReservationRequiresTheActualPausedStateProof() {
        try (Fixture f = new Fixture()) {
            f.state.compareAndSwap(PIPE, 0, StateJson.of(PipelineState.RUNNING), AT);
            f.claims.advanceStandalone(CLUSTER, PIPE);
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-a"); f.desired.save(intent);
            CheckpointDoc before = f.state.read(PIPE).orElseThrow();
            StopReservation invalid = StopReservation.stopping(PIPE, "invalid-continue", before.epoch(), intent,
                    new StopReservation.Source(CLUSTER, new ObservationStore.Scope(INC, 1),
                            new StopReservation.JobIdentity(CLUSTER, 11, "old-boot")),
                    StopReservation.CounterPolicy.CONTINUE, StopAuthority.standalone(CLUSTER, 1));
            assertThatThrownBy(() -> f.state.reserveStop(before, invalid, AT)).isInstanceOf(IllegalArgumentException.class);
            assertThat(f.state.readStopReservation(PIPE)).isEmpty();
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(1);
        }
    }

    @Test void replacementPolicyMustMatchItsCurrentActualProof() {
        try (Fixture f = new Fixture()) {
            f.state.compareAndSwap(PIPE, 0, StateJson.of(PipelineState.RUNNING), AT);
            StopReservation old = f.stop(false);
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-new"); f.desired.save(intent);
            StopReservation invalid = StopReservation.stopping(PIPE, "invalid-replacement", old.reservedEpoch(), intent,
                    old.source(), StopReservation.CounterPolicy.CONTINUE, old.writerAuthority());
            assertThatThrownBy(() -> f.state.replaceStop(old, invalid, AT)).isInstanceOf(IllegalArgumentException.class);
            assertThat(f.state.readStopReservation(PIPE)).contains(old);
        }
    }

    @Test void knownNoJobAuthorityCannotBeRetiredAsAnUnadmittedZeroGeneration() {
        try (Fixture f = new Fixture()) {
            f.claims.advanceStandalone(CLUSTER, PIPE);
            DesiredState original = new DesiredState(PIPE, PipelineState.STOPPED, "rev-a"); f.desired.save(original);
            StopReservation old = new StopReservation(PIPE, "known-no-job", 0, 1, original,
                    new StopReservation.NoJob(CLUSTER, StopAuthority.standalone(CLUSTER, 1)));
            f.state.reserveStop(f.state.read(PIPE).orElseThrow(), old, AT).orElseThrow();
            f.database.getCollection("workload_claims").deleteMany(new Document());
            DesiredState successor = new DesiredState(PIPE, PipelineState.STOPPED, "rev-new"); f.desired.save(successor);
            assertThatThrownBy(() -> f.state.retireStop(old, successor, null, AT)).isInstanceOf(IllegalArgumentException.class);
            assertThat(f.database.getCollection("workload_claims").find().first()).isNull();
            assertThat(f.state.readStopReservation(PIPE)).contains(old);
        }
    }

    @Test void completedSuccessorRetainsItsBoundFloorUntilAckWithoutResurrection() {
        terminalSuccessorRetainsActual(PipelineState.COMPLETED);
    }

    @Test void failedSuccessorRetainsItsBoundFloorUntilAckWithoutResurrection() {
        terminalSuccessorRetainsActual(PipelineState.FAILED);
    }

    private void terminalSuccessorRetainsActual(PipelineState terminal) {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            StopReservation bound = f.bind(f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation());
            assertThat(f.state.recordSuccessorTerminal(bound, new SuccessorEnd.Terminal(bound.successor().scope(),
                    new StopReservation.JobIdentity(CLUSTER, 99, "submit-b")), terminal, AT)).isEmpty();
            StopReservation ended = f.state.recordSuccessorTerminal(bound,
                    new SuccessorEnd.Terminal(bound.successor().scope(), bound.successor().job()), terminal, AT).orElseThrow();
            assertThat(ended.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
            assertThat(ended.source()).isEqualTo(bound.source());
            assertThat(ended.successor()).isEqualTo(bound.successor());
            assertThat(ended.token()).isEqualTo(bound.token());
            assertThat(ended.reservedEpoch()).isEqualTo(bound.reservedEpoch() + 1);
            assertThat(f.actual()).isEqualTo(terminal);
            assertThat(f.state.recordSuccessorTerminal(ended,
                    new SuccessorEnd.Terminal(ended.successor().scope(), ended.successor().job()), terminal, AT)).contains(ended);
            assertThat(f.state.completeHandoff(bound, bound.handoffIdentity(), AT)).isEmpty();
            assertThat(f.state.completeHandoff(ended, null, AT)).isEmpty();
            assertThat(f.state.readStopReservation(PIPE)).contains(ended);
            assertThat(f.state.completeHandoff(ended, ended.handoffIdentity(), AT)).isPresent();
            assertThat(f.actual()).isEqualTo(terminal);
            assertThat(f.state.readStopReservation(PIPE)).isEmpty();
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(2);
        }
    }

    @Test void aColdDeleteCannotDiscardAnActiveContinuationOrInventAMissingCheckpoint() {
        try (Fixture f = new Fixture()) {
            StopReservation active = f.stop(false);
            f.database.getCollection("latest").insertOne(new Document("_id", PIPE).append("floor", 7L));
            assertThat(f.state.handoffWrites().withNoContinuationHandoff(PIPE, session -> {
                f.database.getCollection("latest").deleteOne(session, new Document("_id", PIPE)); return true;
            })).isEmpty();
            assertThat(f.database.getCollection("latest").find().first()).isNotNull();
            assertThat(f.state.readStopReservation(PIPE)).contains(active);
            f.state.delete(PIPE);
            assertThat(f.state.handoffWrites().withNoContinuationHandoff(PIPE, session -> true)).isEmpty();
            assertThat(f.state.read(PIPE)).isEmpty();
        }
    }

    @Test void conditionalColdWriteFailureAbortsItsCheckpointGuardToo() {
        try (Fixture f = new Fixture()) {
            Document before = f.database.getCollection("states").find().first();
            assertThat(f.state.handoffWrites().withNoContinuationHandoff(PIPE,
                    session -> { throw MongoStopReservationWrites.fencedHandoff(); })).isEmpty();
            assertThat(f.database.getCollection("states").find().first()).isEqualTo(before);
            assertThat(f.state.handoffWrites().withNoContinuationHandoff(PIPE, session -> true)).contains(true);
            assertThat(f.database.getCollection("states").find().first().get("stopGuardVersion")).isEqualTo(1L);
        }
    }

    @Test void refusalBeforeAdmissionRecordsFailedWithoutTakingAGenerationOrLease() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();

            CheckpointDoc failed = f.state.failReplacement(pending, Optional.empty(), AT).orElseThrow();

            assertThat(failed.stateJson()).isEqualTo(StateJson.of(PipelineState.FAILED));
            assertThat(failed.epoch()).isEqualTo(pending.reservedEpoch() + 1);
            assertThat(f.state.readStopReservation(PIPE)).isEmpty();
            assertThat(f.desired.read(PIPE)).contains(pending.originalDesired());
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(1);
            assertThat(f.claims.read(KEY)).isEmpty();
            assertThat(f.state.failReplacement(pending, Optional.empty(), AT)).isEmpty();
            assertThat(f.state.read(PIPE)).contains(failed);
        }
    }

    @Test void refusalOfAnAbsentAdmittedExecutionKeepsItsOneGenerationHole() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();

            CheckpointDoc failed = f.state.failReplacement(admitted, Optional.empty(), AT).orElseThrow();

            assertThat(failed.stateJson()).isEqualTo(StateJson.of(PipelineState.FAILED));
            assertThat(failed.epoch()).isEqualTo(admitted.reservedEpoch() + 1);
            assertThat(f.state.readStopReservation(PIPE)).isEmpty();
            assertThat(f.desired.read(PIPE)).contains(admitted.originalDesired());
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(2);
            assertThat(f.claims.read(KEY)).isEmpty();
            assertThat(f.state.failReplacement(admitted, Optional.empty(), AT)).isEmpty();
            assertThat(f.state.admitSuccessor(pending, INC, "submit-c", AT)).isEmpty();
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(2);
        }
    }

    @Test void refusalAfterActualSubmissionKeepsTheExactFailedBoundHandoff() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            StopReservation.JobIdentity actual = new StopReservation.JobIdentity(CLUSTER, 42, "submit-b");

            CheckpointDoc failed = f.state.failReplacement(admitted, Optional.of(actual), AT).orElseThrow();

            StopReservation bound = f.state.readStopReservation(PIPE).orElseThrow();
            assertThat(failed.stateJson()).isEqualTo(StateJson.of(PipelineState.FAILED));
            assertThat(bound.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
            assertThat(bound.reservedEpoch()).isEqualTo(admitted.reservedEpoch() + 1);
            assertThat(bound.token()).isEqualTo(admitted.token());
            assertThat(bound.source()).isEqualTo(admitted.source());
            assertThat(bound.originalDesired()).isEqualTo(admitted.originalDesired());
            assertThat(bound.counterPolicy()).isEqualTo(admitted.counterPolicy());
            assertThat(bound.writerAuthority()).isEqualTo(admitted.writerAuthority());
            assertThat(bound.successor().scope()).isEqualTo(admitted.successor().scope());
            assertThat(bound.successor().submissionBootId()).isEqualTo(admitted.successor().submissionBootId());
            assertThat(bound.successor().job()).isEqualTo(actual);
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(2);
            assertThat(f.state.failReplacement(admitted, Optional.of(actual), AT)).isEmpty();
            assertThat(f.state.compareAndSwap(PIPE, bound.reservedEpoch(), StateJson.of(PipelineState.RUNNING), AT))
                    .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Fenced.class);
            assertThat(f.state.completeHandoff(bound, null, AT)).isEmpty();
            assertThat(f.state.completeHandoff(bound, bound.handoffIdentity(), AT)).isPresent();
            assertThat(f.state.readStopReservation(PIPE)).isEmpty();
            assertThat(f.actual()).isEqualTo(PipelineState.FAILED);
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(2);
        }
    }

    @Test void replacementRefusalRejectsInvalidPhasesAndForeignPhysicalJobs() {
        try (Fixture f = new Fixture()) {
            StopReservation stopping = f.stop(false);
            assertThatThrownBy(() -> f.state.failReplacement(stopping, Optional.empty(), AT))
                    .isInstanceOf(IllegalArgumentException.class);
            StopReservation pending = f.state.markReplacementPending(stopping, AT).orElseThrow();
            assertThat(f.state.failReplacement(pending,
                    Optional.of(new StopReservation.JobIdentity(CLUSTER, 42, "old-boot")), AT)).isEmpty();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "old-boot", AT).orElseThrow().reservation();
            CheckpointDoc before = f.state.read(PIPE).orElseThrow();
            assertThat(f.state.failReplacement(admitted, Optional.of(stopping.source().oldJob()), AT)).isEmpty();
            assertThat(f.state.failReplacement(admitted,
                    Optional.of(new StopReservation.JobIdentity(CLUSTER, 42, "foreign-boot")), AT)).isEmpty();
            assertThat(f.state.failReplacement(admitted,
                    Optional.of(new StopReservation.JobIdentity("foreign-cluster", 42, "old-boot")), AT)).isEmpty();
            assertThat(f.state.read(PIPE)).contains(before);
            assertThat(f.state.readStopReservation(PIPE)).contains(admitted);
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(2);
        }
    }

    @Test void replacementRefusalCannotRebaseOntoANewerDesiredStamp() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            f.desired.save(new DesiredState(PIPE, PipelineState.RUNNING, "rev-a", false,
                    "assembly-a", true, pending.reservedEpoch()));
            Document before = f.database.getCollection("desired").find().first();

            assertThat(f.state.failReplacement(pending, Optional.empty(), AT)).isEmpty();

            assertThat(f.state.readStopReservation(PIPE)).contains(pending);
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
            assertThat(f.database.getCollection("desired").find().first()).isEqualTo(before);
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(1);
        }
    }

    @Test void replacementRefusalCannotOutliveItsExactAdmissionAuthority() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            f.claims.advanceStandalone(CLUSTER, PIPE);
            Document before = f.database.getCollection("desired").find().first();
            Document beforeClaim = f.database.getCollection("workload_claims").find().first();

            assertThat(f.state.failReplacement(admitted,
                    Optional.of(new StopReservation.JobIdentity(CLUSTER, 42, "submit-b")), AT)).isEmpty();

            assertThat(f.state.readStopReservation(PIPE)).contains(admitted);
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
            assertThat(f.database.getCollection("desired").find().first()).isEqualTo(before);
            assertThat(f.database.getCollection("workload_claims").find().first()).isEqualTo(beforeClaim);
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(3);
        }
    }

    @Test void replacementRefusalCannotAttachToARecreatedArtifact() {
        try (Fixture f = new Fixture()) {
            StopReservation pending = f.state.markReplacementPending(f.stop(false), AT).orElseThrow();
            StopReservation admitted = f.state.admitSuccessor(pending, INC, "submit-b", AT).orElseThrow().reservation();
            f.database.getCollection("artifacts").updateOne(new Document("_id", PIPE),
                    new Document("$set", new Document("pipelineIncarnationId", "inc-recreated")));
            Document before = f.database.getCollection("desired").find().first();
            Document beforeClaim = f.database.getCollection("workload_claims").find().first();

            assertThat(f.state.failReplacement(admitted, Optional.empty(), AT)).isEmpty();

            assertThat(f.state.readStopReservation(PIPE)).contains(admitted);
            assertThat(f.actual()).isEqualTo(PipelineState.STOPPED);
            assertThat(f.database.getCollection("desired").find().first()).isEqualTo(before);
            assertThat(f.database.getCollection("workload_claims").find().first()).isEqualTo(beforeClaim);
            assertThat(f.claims.currentGeneration(CLUSTER, PIPE)).hasValue(2);
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoDatabase database = client.getDatabase("handoff_" + UUID.randomUUID().toString().replace("-", ""));
        final MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(database.getCollection("workload_claims"));
        final MongoDesiredStore desired = new MongoDesiredStore(database.getCollection("desired"));
        final MongoStateStore state = new MongoStateStore(client, database.getCollection("states"),
                database.getCollection("desired"), database.getCollection("workload_claims"), database.getCollection("artifacts"));
        Fixture() { this(PipelineState.PAUSED); }
        Fixture(PipelineState initialState) {
            database.getCollection("artifacts").insertOne(new Document("_id", PIPE).append("kind", "pipeline")
                    .append("pipelineIncarnationId", INC));
            state.create(PIPE, StateJson.of(initialState), AT);
        }
        StopReservation stop(boolean reset) {
            claims.advanceStandalone(CLUSTER, PIPE);
            return stop(reset, StopAuthority.standalone(CLUSTER, 1));
        }
        StopReservation stop(boolean reset, StopAuthority authority) {
            DesiredState intent = new DesiredState(PIPE, PipelineState.RUNNING, "rev-a", false,
                    "assembly-a", reset, reset ? 0L : null);
            desired.save(intent);
            CheckpointDoc actual = state.read(PIPE).orElseThrow();
            StopReservation marker = StopReservation.stopping(PIPE, UUID.randomUUID().toString(), actual.epoch(), intent,
                    new StopReservation.Source(CLUSTER, new ObservationStore.Scope(INC, 1),
                            new StopReservation.JobIdentity(CLUSTER, 11, "old-boot")),
                    StopReservation.CounterPolicy.freeze(actual, intent), authority);
            return state.reserveStop(actual, marker, AT).orElseThrow();
        }
        StopReservation bind(StopReservation marker) {
            return state.bindSuccessor(marker, marker.successor().scope(),
                    new StopReservation.JobIdentity(CLUSTER, 42, marker.successor().submissionBootId()), AT).orElseThrow();
        }
        PipelineState actual() { return StateJson.parse(state.read(PIPE).orElseThrow().stateJson()); }
        @Override public void close() { database.drop(); client.close(); }
    }
}
