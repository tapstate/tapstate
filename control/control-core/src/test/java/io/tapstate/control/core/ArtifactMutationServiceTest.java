package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactMutationServiceTest {

    private static final String PRINCIPAL = "alice";

    private final InMemoryArtifactStore store = new InMemoryArtifactStore();
    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final InMemoryObservationStore observations = new InMemoryObservationStore();
    private final InMemorySrsMetaStore srsMeta = new InMemorySrsMetaStore();
    private final List<String> reclaimOrder = new ArrayList<>();
    private final RecordingAuditStore auditStore = new RecordingAuditStore();
    private final List<String> followsStopped = new ArrayList<>();
    private final ArtifactMutationService service = new ArtifactMutationService(
            store, desired, state, observations, srsMeta, new AuditGate(auditStore, FIXED_CLOCK),
            followsStopped::add);

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-08-11T09:00:00Z"), ZoneOffset.UTC);

    @Test
    @DisplayName("deleting a source stops whoever was following it, whichever door the delete came in")
    void deletingASourceStopsItsFollows() {
        SourceResource followed = source("src_a");
        store.saveAll(List.of(followed, source("src_b")));

        service.delete(PRINCIPAL, "src_a", hash(followed));

        assertThat(followsStopped)
                .as("a follow outlives the source it reads: nothing references it, so neither refusal "
                        + "sees it, and left running it keeps handing over rows from a source the "
                        + "caller has just been told is gone")
                .containsExactly("src_a");
    }

    @Test
    @DisplayName("deleting something that is not a source does not go near the follows")
    void deletingANonSourceLeavesTheFollowsAlone() {
        PipelineResource reader = pipelineReading("pl_1", "src_a");
        store.saveAll(List.of(source("src_a"), reader));

        service.delete(PRINCIPAL, "pl_1", hash(reader));

        assertThat(followsStopped)
                .as("the discriminating half: stopping every follow on any delete would pass the case "
                        + "above while cutting off readers of a source nobody touched")
                .isEmpty();
    }

    /** An audit store that captures every record it is asked to write. */
    private static final class RecordingAuditStore implements AuditStore {
        final List<AuditRecord> records = new ArrayList<>();

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }

    /** An audit store that always fails, standing in for an unavailable audit backend. */
    private static final class FailingAuditStore implements AuditStore {
        @Override
        public void record(AuditRecord record) {
            throw new IllegalStateException("audit backend down");
        }
    }

    @Test
    void deletesAnyKindThroughTheOneVerbAndRemovesOnlyThatArtifact() {
        List<Resource> kinds = List.of(
                source("orders"),
                pipeline("flow"),
                transform("mask"),
                view("orders_view"),
                serve("orders_api"));
        kinds.forEach(store::save);

        for (Resource resource : kinds) {
            service.delete(PRINCIPAL, resource.id(), hash(resource));
            assertThat(store.get(resource.id())).isEmpty();
        }
        assertThat(store.list()).isEmpty();
    }

    @Test
    void deleteWithoutAPreconditionIsRefusedWithTheStoreUntouched() {
        SourceResource orders = source("orders");
        store.save(orders);

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "orders", null),
                ArtifactError.PRECONDITION_REQUIRED,
                Map.of("id", "orders"));
        assertThat(store.get("orders")).isPresent();
    }

    @Test
    void deleteOfAnUnknownIdIsRefusedAsNotFound() {
        assertArtifactError(
                () -> service.delete(PRINCIPAL, "ghost", "0".repeat(64)),
                ArtifactError.NOT_FOUND,
                Map.of("id", "ghost"));
    }

    @Test
    void deleteWithAStaleHashIsRefusedWithTheStoredBytesUnchanged() {
        SourceResource orders = source("orders");
        store.save(orders);
        String before = hash(store.get("orders").orElseThrow());

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "orders", "0".repeat(64)),
                ArtifactError.VERSION_CONFLICT,
                Map.of("id", "orders"));
        assertThat(hash(store.get("orders").orElseThrow())).isEqualTo(before);
    }

    @Test
    void referencedDeleteIsRefusedWithSortedReferrersAndNoCascade() {
        SourceResource orders = source("orders");
        store.save(orders);
        store.save(pipelineReading("zeta", "orders"));
        store.save(pipelineReading("alpha", "orders"));

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "orders", hash(orders)),
                ArtifactError.IN_USE,
                Map.of("id", "orders", "referrers", List.of("alpha", "zeta")));
        assertThat(store.get("orders")).isPresent();
        assertThat(store.get("alpha")).isPresent();
        assertThat(store.get("zeta")).isPresent();
    }

    @Test
    void aPipelineThatHasNeverRunIsDeletable() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);

        service.delete(PRINCIPAL, "flow", hash(flow));

        assertThat(store.get("flow")).isEmpty();
    }

    @Test
    void aRunningPipelineIsRefusedEvenAfterStopWasRequested() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.RUNNING);
        desired.put("flow", PipelineState.STOPPED);

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "flow", hash(flow)),
                ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id", "flow", "actual", "RUNNING", "desired", "STOPPED"));
        assertThat(store.get("flow")).isPresent();
    }

    @Test
    void aStoppedPipelineThatWasAskedToStartIsRefused() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.STOPPED);
        desired.put("flow", PipelineState.RUNNING);

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "flow", hash(flow)),
                ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id", "flow", "actual", "STOPPED", "desired", "RUNNING"));
        assertThat(store.get("flow")).isPresent();
    }

    @Test
    void aPausedPipelineIsRefusedOnBothHalvesOfTheVerdict() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.PAUSED);
        desired.put("flow", PipelineState.PAUSED);

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "flow", hash(flow)),
                ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id", "flow", "actual", "PAUSED", "desired", "PAUSED"));
        assertThat(store.get("flow")).isPresent();
    }

    @Test
    void aSettledPipelineIsDeletableFromEveryRestingActualState() {
        for (PipelineState resting :
                List.of(PipelineState.NEW, PipelineState.STOPPED,
                        PipelineState.COMPLETED, PipelineState.FAILED)) {
            PipelineResource flow = pipeline("flow");
            store.save(flow);
            state.put("flow", resting);
            desired.put("flow", PipelineState.STOPPED);

            service.delete(PRINCIPAL, "flow", hash(flow));

            assertThat(store.get("flow")).isEmpty();
        }
    }

    @Test
    void theLifecycleGateJudgesPipelinesOnlyAndNotOtherKindsSharingTheirId() {
        SourceResource orders = source("orders");
        store.save(orders);
        // A non-pipeline resource is not run, so lifecycle documents left under its id say nothing
        // about whether it may be deleted.
        state.put("orders", PipelineState.RUNNING);
        desired.put("orders", PipelineState.RUNNING);

        service.delete(PRINCIPAL, "orders", hash(orders));

        assertThat(store.get("orders")).isEmpty();
    }

    @Test
    void deletingAPipelineReclaimsItsOwnBookkeepingSharedChainsFirst() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.STOPPED);
        desired.put("flow", PipelineState.STOPPED);
        observations.put("flow");
        srsMeta.seed("chain-a", consumer("flow"), consumer("other"));

        service.delete(PRINCIPAL, "flow", hash(flow));

        assertThat(desired.read("flow")).isEmpty();
        // The converge side reconciles exactly this set: an id left in it outlives the artifact and is
        // reconciled forever against something that no longer exists.
        assertThat(desired.pipelineIds()).doesNotContain("flow");
        assertThat(state.read("flow")).isEmpty();
        assertThat(observations.read("flow")).isEmpty();
        assertThat(srsMeta.consumerIds("chain-a")).containsExactly("other");
        // Shared first: it is the only residue that stalls a different pipeline, so a process that dies
        // mid-reclaim has already contained the damage that was not this pipeline's alone to suffer.
        assertThat(reclaimOrder).containsExactly("srs", "desired", "state", "observation");
    }

    @Test
    void deletingAPipelineDetachesItFromEveryChainAndNeverTouchesAnotherConsumerOrTheChain() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        ConsumerOffset survivor = consumer("other");
        srsMeta.seed("chain-a", consumer("flow"), survivor);
        srsMeta.seed("chain-b", consumer("flow"));
        srsMeta.seed("chain-c", survivor);

        service.delete(PRINCIPAL, "flow", hash(flow));

        // Every chain it consumed, not just the first one found.
        assertThat(srsMeta.consumerIds("chain-a")).containsExactly("other");
        assertThat(srsMeta.consumerIds("chain-b")).isEmpty();
        assertThat(srsMeta.consumerIds("chain-c")).containsExactly("other");
        // The chain record itself outlives its last consumer: it is keyed by the chain, not the pipeline,
        // and removing it would be cross-pipeline data loss dressed up as tidying.
        assertThat(srsMeta.read("chain-b")).isPresent();
        // The surviving consumer's cursor is not merely present but unchanged: a detach that rewrote a
        // neighbour's offsets would move the two minimums it is folded into.
        assertThat(srsMeta.consumerOffset("chain-a", "other")).isEqualTo(survivor);
    }

    @Test
    void deletingANonPipelineReclaimsNothingEvenWhenDocumentsShareItsId() {
        SourceResource orders = source("orders");
        store.save(orders);
        // Lifecycle documents are a pipeline's bookkeeping; ones filed under a source's id belong to
        // something else entirely and are not this removal's to reclaim.
        state.put("orders", PipelineState.STOPPED);
        desired.put("orders", PipelineState.STOPPED);
        observations.put("orders");
        srsMeta.seed("chain-a", consumer("orders"));

        service.delete(PRINCIPAL, "orders", hash(orders));

        assertThat(store.get("orders")).isEmpty();
        assertThat(desired.read("orders")).isPresent();
        assertThat(state.read("orders")).isPresent();
        assertThat(observations.read("orders")).isPresent();
        assertThat(srsMeta.consumerIds("chain-a")).containsExactly("orders");
    }

    @Test
    void aSuccessfulDeleteLeavesOneAuditRecordNamingTheCallerAndTheResource() {
        SourceResource orders = source("orders");
        store.save(orders);

        service.delete(PRINCIPAL, "orders", hash(orders));

        assertThat(auditStore.records).singleElement().satisfies(record -> {
            assertThat(record.principal()).isEqualTo(PRINCIPAL);
            assertThat(record.operationId()).isEqualTo("artifact.delete");
            assertThat(record.resourceId()).isEqualTo("orders");
            // A destroyed resource leaves nothing behind to compare a log entry against, so the version
            // the caller declared it was destroying is the only thing that can tie this entry to a
            // particular content later on.
            assertThat(record.expectedContentHash()).isEqualTo(hash(orders));
        });
    }

    /**
     * A refusal is not an attempt worth recording: nothing was destroyed, and a record for every rejected
     * delete would bury the ones that actually removed something. This also pins the ordering — the two
     * grounds are judged before the gate is entered, so an audit backend being down cannot change which
     * deletions are refused.
     */
    @Test
    void aDeleteRefusedOnEitherGroundLeavesNoAuditRecord() {
        SourceResource orders = source("orders");
        store.save(orders);
        store.save(pipelineReading("flow", "orders"));
        PipelineResource running = pipeline("live");
        store.save(running);
        state.put("live", PipelineState.RUNNING);

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "orders", hash(orders)),
                ArtifactError.IN_USE,
                Map.of("id", "orders", "referrers", List.of("flow")));
        assertArtifactError(
                () -> service.delete(PRINCIPAL, "live", hash(running)),
                ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id", "live", "actual", "RUNNING", "desired", "NEW"));
        assertArtifactError(
                () -> service.delete(PRINCIPAL, "orders", null),
                ArtifactError.PRECONDITION_REQUIRED,
                Map.of("id", "orders"));

        assertThat(auditStore.records).isEmpty();
    }

    /**
     * A stale precondition does leave a record, unlike the two refusal grounds. The split is deliberate
     * and follows from where each check can live: the grounds are judged from state this service already
     * holds, so they resolve before the gate; the version check is the store's own atomic compare, which
     * happens inside the write the gate exists to precede. Recording an attempt that the store then
     * refused is the honest reading of an audit-before-execute log — and the alternative, auditing after
     * the fact, would give up the guarantee that nothing is destroyed unrecorded.
     */
    @Test
    void aStalePreconditionLeavesTheAttemptOnTheAuditLogEvenThoughNothingWasRemoved() {
        SourceResource orders = source("orders");
        store.save(orders);

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "orders", "0".repeat(64)),
                ArtifactError.VERSION_CONFLICT,
                Map.of("id", "orders"));

        assertThat(store.get("orders")).isPresent();
        assertThat(auditStore.records).singleElement().satisfies(record -> {
            assertThat(record.operationId()).isEqualTo("artifact.delete");
            assertThat(record.resourceId()).isEqualTo("orders");
            // The load-bearing assertion for what this field means. The record is written before the store
            // compare runs, so the only version knowable at that moment is the one the caller declared —
            // here a stale one, and deliberately not the version the store actually holds. An
            // implementation that reads the current hash out of the store to fill this in would silently
            // redefine the field from "what was claimed" to "what was there", which is both unknowable at
            // audit time and, on this branch, a value the caller never mentioned.
            assertThat(record.expectedContentHash())
                    .isEqualTo("0".repeat(64))
                    .isNotEqualTo(hash(source("orders")));
        });
    }

    /**
     * No audit, no destruction. The record is written before the store delete runs, so an audit backend
     * that is down refuses the removal outright rather than destroying a resource that leaves no trace —
     * the one failure mode an audited destructive verb must not have.
     */
    @Test
    void anUnavailableAuditBackendRefusesTheDeleteAndLeavesTheArtifactByteForByte() {
        ArtifactMutationService blocked = new ArtifactMutationService(
                store, desired, state, observations, srsMeta,
                new AuditGate(new FailingAuditStore(), FIXED_CLOCK), followsStopped::add);
        SourceResource orders = source("orders");
        store.save(orders);
        String before = hash(orders);

        assertThatThrownBy(() -> blocked.delete(PRINCIPAL, "orders", before))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("control.audit-blocked"));

        assertThat(store.get("orders")).isPresent();
        assertThat(hash(store.get("orders").orElseThrow())).isEqualTo(before);
    }

    @Test
    void aRefusedDeleteReclaimsNothing() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.RUNNING);
        desired.put("flow", PipelineState.STOPPED);
        observations.put("flow");
        srsMeta.seed("chain-a", consumer("flow"));

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "flow", hash(flow)),
                ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id", "flow", "actual", "RUNNING", "desired", "STOPPED"));

        // A refusal is judged before anything is written, so the pipeline's bookkeeping is as intact as
        // the artifact: reclaiming here would strip a live pipeline of the state it is still running on.
        assertThat(desired.read("flow")).isPresent();
        assertThat(state.read("flow")).isPresent();
        assertThat(observations.read("flow")).isPresent();
        assertThat(srsMeta.consumerIds("chain-a")).containsExactly("flow");
        assertThat(reclaimOrder).isEmpty();
    }

    @Test
    void aStaleHashDeleteOfAPipelineRefusesWithoutReclaimingAnything() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.STOPPED);
        desired.put("flow", PipelineState.STOPPED);
        observations.put("flow");
        srsMeta.seed("chain-a", consumer("flow"));

        assertArtifactError(
                () -> service.delete(PRINCIPAL, "flow", "0".repeat(64)),
                ArtifactError.VERSION_CONFLICT,
                Map.of("id", "flow"));

        // Both gates passed here, so the only thing that refused was the store's own conditional delete —
        // someone else changed the pipeline while this caller held an old version. It still exists, so
        // reclaiming would strip a live pipeline of its state on the strength of a delete that failed.
        assertThat(store.get("flow")).isPresent();
        assertThat(desired.read("flow")).isPresent();
        assertThat(state.read("flow")).isPresent();
        assertThat(observations.read("flow")).isPresent();
        assertThat(srsMeta.consumerIds("chain-a")).containsExactly("flow");
        assertThat(reclaimOrder).isEmpty();
    }

    @Test
    void aReclaimFailureIsReportedWithoutPuttingTheArtifactBackOrSkippingTheRestOfTheReclaim() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.STOPPED);
        desired.put("flow", PipelineState.STOPPED);
        observations.put("flow");
        srsMeta.seed("chain-a", consumer("flow"));
        RuntimeException ioFailure = new IllegalArgumentException("desired store is down");
        desired.failDeleteWith(ioFailure);

        // Reported as its own code, not as one of the refusals: a refusal means nothing happened and
        // invites a retry, and this caller's retry can only ever answer artifact.not-found while the
        // residue below stays where it is. The underlying failure is kept as the cause, so the coded
        // report costs no diagnosis.
        assertThatThrownBy(() -> service.delete(PRINCIPAL, "flow", hash(flow)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ArtifactError.RECLAIM_INCOMPLETE);
                    assertThat(error.args()).containsEntry("id", "flow");
                    assertThat(error.args()).containsEntry("residue", List.of("desired"));
                    assertThat(error.args()).containsEntry("reason", "step-failed");
                })
                .hasCause(ioFailure);

        // Never resurrected: the removal did happen, and a caller told otherwise would re-apply a
        // resource it believes it deleted.
        assertThat(store.get("flow")).isEmpty();
        // The residue the report is about really is left behind — otherwise this witnesses nothing.
        assertThat(desired.read("flow")).isPresent();
        // Never abandoned partway: the steps after the failing one still ran, so the failure costs one
        // stale document rather than all of them — and the artifact is gone, so no retry can finish it.
        assertThat(state.read("flow")).isEmpty();
        assertThat(observations.read("flow")).isEmpty();
        assertThat(srsMeta.consumerIds("chain-a")).isEmpty();
    }

    @Test
    void aPipelineStartedInsideTheRemovalWindowKeepsItsCheckpointInsteadOfLosingItsFencingEpoch() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        state.put("flow", PipelineState.STOPPED);
        desired.put("flow", PipelineState.STOPPED);
        observations.put("flow");
        srsMeta.seed("chain-a", consumer("flow"));
        // A start lands after the lifecycle refusal has already passed and after the artifact's own
        // compare-and-swap — which cannot see it, since starting a pipeline leaves the canonical bytes
        // untouched. The reclaim that follows would otherwise delete the checkpoint of a job that is now
        // executing, discarding the fencing epoch that keeps a later pipeline under this id from sharing
        // a fencing sequence with it.
        store.afterDelete = () -> desired.put("flow", PipelineState.RUNNING);

        assertThatThrownBy(() -> service.delete(PRINCIPAL, "flow", hash(flow)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ArtifactError.RECLAIM_INCOMPLETE);
                    assertThat(error.args()).containsEntry("reason", "pipeline-live");
                });

        // The artifact is gone — the removal is not undone — and none of the live pipeline's own
        // bookkeeping was touched, so stopping it and clearing up by hand is still possible.
        assertThat(store.get("flow")).isEmpty();
        assertThat(state.read("flow")).isPresent();
        assertThat(desired.read("flow")).isPresent();
        assertThat(observations.read("flow")).isPresent();
        assertThat(srsMeta.consumerIds("chain-a")).containsExactly("flow");
    }

    @Test
    void aChainThatRefusesTheDetachNeverStopsTheRemainingChainsFromBeingDetached() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        // Seeded in iteration order, with the failure armed on the first: the two that follow are the
        // discriminating ones. A loop that gives up at its first failure never reaches them, and their
        // cursors are the residue that pins a shared chain's frontier for every other pipeline on it.
        srsMeta.seed("chain-a", consumer("flow"));
        srsMeta.seed("chain-b", consumer("flow"), consumer("other"));
        srsMeta.seed("chain-c", consumer("flow"));
        RuntimeException chainDown = new IllegalArgumentException("chain-a is unreachable");
        srsMeta.failDetachOn("chain-a", chainDown);

        assertThatThrownBy(() -> service.delete(PRINCIPAL, "flow", hash(flow)))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.args()).containsEntry("residue", List.of("mining-chain-consumer")))
                .hasCause(chainDown);

        assertThat(srsMeta.consumerIds("chain-b")).containsExactly("other");
        assertThat(srsMeta.consumerIds("chain-c")).isEmpty();
        // The armed chain really did keep the cursor, so the two above witness reach rather than a
        // failure that never happened.
        assertThat(srsMeta.consumerIds("chain-a")).containsExactly("flow");
        // The rest of the reclaim runs too: one unreachable chain must not cost the pipeline's own
        // bookkeeping as well, which no later run can clear because the artifact is already gone.
        assertThat(desired.read("flow")).isEmpty();
        assertThat(state.read("flow")).isEmpty();
        assertThat(observations.read("flow")).isEmpty();
    }

    @Test
    void everyChainFailureIsReportedAndNotJustTheFirst() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        srsMeta.seed("chain-a", consumer("flow"));
        srsMeta.seed("chain-b", consumer("flow"));
        RuntimeException first = new IllegalArgumentException("chain-a is unreachable");
        RuntimeException second = new IllegalArgumentException("chain-b is unreachable");
        srsMeta.failDetachOn("chain-a", first);
        srsMeta.failDetachOn("chain-b", second);

        // Both are carried out, not just the one that ended the loop: each names a different chain
        // whose cursor is still there, and a caller shown only the first would clear one and leave one.
        // The coded report carries them as its cause chain, so wrapping costs no diagnosis.
        assertThatThrownBy(() -> service.delete(PRINCIPAL, "flow", hash(flow)))
                .hasCause(first)
                .satisfies(thrown -> assertThat(thrown.getCause().getSuppressed()).containsExactly(second));
    }

    @Test
    void everyReclaimFailureIsReportedAndNotJustTheFirst() {
        PipelineResource flow = pipeline("flow");
        store.save(flow);
        RuntimeException first = new IllegalArgumentException("desired store is down");
        RuntimeException second = new IllegalArgumentException("state store is down");
        RuntimeException third = new IllegalArgumentException("observation store is down");
        desired.failDeleteWith(first);
        state.failDeleteWith(second);
        observations.failDeleteWith(third);

        // All three lifecycle steps are armed, not two: a step left out of the collecting wrapper
        // would abort the reclaim at itself, and the steps after it would go unattempted while the
        // artifact is already gone. The suppressed order also pins the sequence the reclaim runs in,
        // and the residue list names each of them — a report saying only "the reclaim failed" leaves
        // whoever has to clear it guessing which documents are still there.
        assertThatThrownBy(() -> service.delete(PRINCIPAL, "flow", hash(flow)))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args())
                        .containsEntry("residue", List.of("desired", "state", "observation")))
                .hasCause(first)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(second, third));
    }

    private static SourceResource source(String id) {
        return new SourceResource(
                id, null, "mysql",
                Map.of("host", "localhost", "port", "3306", "database", "orders", "username", "app"),
                SourceMode.SNAPSHOT, null, null, null);
    }

    private static PipelineResource pipeline(String id) {
        return pipelineReading(id, "orders_upstream");
    }

    private static PipelineResource pipelineReading(String id, String sourceId) {
        return new PipelineResource(id, null, List.of(sourceId), null, null, null, null, null);
    }

    private static TransformResource transform(String id) {
        return new TransformResource(id, null, new TransformBody.Js("function process(r) { return r; }"), null);
    }

    private static ViewResource view(String id) {
        return new ViewResource(id, null, null, null, null, null);
    }

    private static ServeResource serve(String id) {
        return new ServeResource(id, null, null, null, null, null);
    }

    private static String hash(Resource resource) {
        return CanonicalHash.of(new CanonicalWriter().write(resource));
    }

    private static void assertArtifactError(
            ThrowingAction action, ArtifactError code, Map<String, Object> args) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(code);
                    assertThat(error.args()).containsExactlyInAnyOrderEntriesOf(args);
                });
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run();
    }

    private static final class InMemoryArtifactStore implements ArtifactStore {

        private final Map<String, Resource> artifacts = new LinkedHashMap<>();
        /** Another caller acting in the window between the removal and the reclaim that follows it. */
        private Runnable afterDelete = null;

        @Override
        public synchronized ArtifactMutation delete(String id, String expectedContentHash) {
            Resource existing = artifacts.get(id);
            if (existing == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!hash(existing).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            artifacts.remove(id);
            if (afterDelete != null) {
                Runnable other = afterDelete;
                afterDelete = null;
                other.run();
            }
            return ArtifactMutation.DELETED;
        }

        @Override
        public synchronized void saveAll(List<Resource> resources) {
            resources.forEach(resource -> artifacts.put(resource.id(), resource));
        }

        @Override
        public synchronized Optional<Resource> get(String id) {
            return Optional.ofNullable(artifacts.get(id));
        }

        @Override
        public synchronized List<Resource> list() {
            return new ArrayList<>(artifacts.values());
        }
    }

    private static ConsumerOffset consumer(String pipelineId) {
        return new ConsumerOffset(pipelineId, Map.of("orders", 42L),
                new ChainPosition(new SourceOrder(1, 42), "srcpos-7"));
    }

    /** Records that a reclaim step ran, in the order the steps were taken, and fails it when armed. */
    private void step(String name, RuntimeException failure) {
        reclaimOrder.add(name);
        if (failure != null) {
            throw failure;
        }
    }

    private final class InMemoryDesiredStore implements DesiredStore {

        private final Map<String, DesiredState> docs = new LinkedHashMap<>();
        private RuntimeException deleteFailure;

        void put(String pipelineId, PipelineState target) {
            docs.put(pipelineId, new DesiredState(pipelineId, target, "0".repeat(64)));
        }

        void failDeleteWith(RuntimeException failure) {
            this.deleteFailure = failure;
        }

        @Override
        public void save(DesiredState state) {
            docs.put(state.pipelineId(), state);
        }

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }

        @Override
        public List<String> pipelineIds() {
            return new ArrayList<>(docs.keySet());
        }

        @Override
        public void delete(String pipelineId) {
            // Fail before mutating, so an armed failure leaves the document behind — the residue
            // the reporting is about.
            step("desired", deleteFailure);
            docs.remove(pipelineId);
        }
    }

    private final class InMemoryStateStore implements StateStore {

        private final Map<String, CheckpointDoc> docs = new LinkedHashMap<>();
        private RuntimeException deleteFailure;

        void put(String pipelineId, PipelineState actual) {
            docs.put(pipelineId, CheckpointDoc.initial(pipelineId, StateJson.of(actual), Instant.EPOCH));
        }

        void failDeleteWith(RuntimeException failure) {
            this.deleteFailure = failure;
        }

        @Override
        public Optional<CheckpointDoc> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }

        @Override
        public void create(String pipelineId, String stateJson, Instant touchTime) {
            docs.put(pipelineId, CheckpointDoc.initial(pipelineId, stateJson, touchTime));
        }

        @Override
        public CasOutcome compareAndSwap(
                String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
            throw new UnsupportedOperationException("the delete gate never writes state");
        }

        @Override
        public void delete(String pipelineId) {
            // Fail before mutating, so an armed failure leaves the document behind — the residue
            // the reporting is about.
            step("state", deleteFailure);
            docs.remove(pipelineId);
        }
    }

    private final class InMemoryObservationStore implements ObservationStore {

        private final Map<String, Observation> docs = new LinkedHashMap<>();
        private RuntimeException deleteFailure;

        void put(String pipelineId) {
            docs.put(pipelineId, new Observation(
                    pipelineId, PipelineState.STOPPED, Map.of(), Map.of(), Map.of(), null));
        }

        void failDeleteWith(RuntimeException failure) {
            this.deleteFailure = failure;
        }

        @Override
        public void save(Observation observation) {
            docs.put(observation.pipelineId(), observation);
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }

        @Override
        public void delete(String pipelineId) {
            // Fail before mutating, so an armed failure leaves the document behind — the residue
            // the reporting is about.
            step("observation", deleteFailure);
            docs.remove(pipelineId);
        }
    }

    private final class InMemorySrsMetaStore implements SrsMetaStore {

        private final Map<String, SrsMeta> chains = new LinkedHashMap<>();
        private final Map<String, RuntimeException> detachFailures = new LinkedHashMap<>();

        void seed(String miningChainId, ConsumerOffset... consumers) {
            chains.put(miningChainId,
                    new SrsMeta(miningChainId, "srcpos-1", List.of(consumers), null, List.of(), null));
        }

        /** Arms one chain to refuse a detach, standing in for a chain whose store is momentarily down. */
        void failDetachOn(String miningChainId, RuntimeException failure) {
            detachFailures.put(miningChainId, failure);
        }

        List<String> consumerIds(String miningChainId) {
            return chains.get(miningChainId).consumerOffsets().stream().map(ConsumerOffset::pipelineId).toList();
        }

        ConsumerOffset consumerOffset(String miningChainId, String pipelineId) {
            return chains.get(miningChainId).consumerOffsets().stream()
                    .filter(offset -> offset.pipelineId().equals(pipelineId))
                    .findFirst()
                    .orElseThrow();
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            return Optional.ofNullable(chains.get(miningChainId));
        }

        @Override
        public List<String> miningChainIdsWithConsumer(String pipelineId) {
            return chains.entrySet().stream()
                    .filter(entry -> entry.getValue().consumerOffsets().stream()
                            .anyMatch(offset -> offset.pipelineId().equals(pipelineId)))
                    .map(Map.Entry::getKey)
                    .toList();
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            // Fail before mutating, so an armed chain keeps the departing consumer's cursor — the residue
            // the report is about has to really be there for the assertions to witness anything.
            step("srs", detachFailures.get(miningChainId));
            SrsMeta chain = chains.get(miningChainId);
            List<ConsumerOffset> kept = chain.consumerOffsets().stream()
                    .filter(offset -> !offset.pipelineId().equals(pipelineId))
                    .toList();
            chains.put(miningChainId, new SrsMeta(chain.miningChainId(), chain.sourceReadOffset(), kept,
                    chain.cdcStartPosition(), chain.schemaHistory(), chain.retention()));
        }

        @Override
        public void create(String miningChainId, String retention) {
            throw new UnsupportedOperationException("the delete path never seeds a chain");
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition acked) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String table) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException("the delete path never advances a chain");
        }
    }
}
