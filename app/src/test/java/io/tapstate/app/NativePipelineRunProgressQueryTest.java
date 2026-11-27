package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoCachePipelineRunProgressStore;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelinePreparationInputs;
import io.tapstate.spi.store.PipelineStatePreparations;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NativePipelineRunProgressQueryTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
    private static final String HASH = "a".repeat(64);
    private final PipelineIntentStore intents = mock(PipelineIntentStore.class);
    private final PipelineStatePreparations preparations = mock(PipelineStatePreparations.class);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster", 1, "cache");
    private static final Instant STARTED_AT = Instant.parse("2026-11-26T10:00:00Z");
    private final NativePipelineRunProgressQuery query = new NativePipelineRunProgressQuery(BUSINESS, intents, preparations, () -> true);

    @Test
    void startIsReadableBeforePreparationAndTheSameRunSurvivesTheHandoff() {
        var intent = intent(4, PipelineState.RUNNING);
        var pending = new PipelineIntentStore.PendingStart(intent, new PipelinePreparationInputs(HASH, Map.of(), Map.of(), List.of()));
        when(intents.pendingStart("orders")).thenReturn(Optional.of(pending));
        when(preparations.read("orders")).thenReturn(Optional.of(new PipelinePreparation(
                "orders", 1, PipelinePreparation.Phase.UNASSIGNED, null, null, null)));
        var queued = query.find("orders").orElseThrow().progress();
        assertThat(queued.active()).containsExactly(
                new PipelineRunProgress.Activity(PipelineRunStage.WAITING_FOR_SCHEDULER, null));

        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 4)));
        var prepared = query.find("orders").orElseThrow().progress();
        assertThat(prepared.runId()).isEqualTo(queued.runId());
        assertThat(prepared.version()).isGreaterThan(queued.version());
        assertThat(prepared.active()).isEmpty();
    }

    @Test
    void aStoppedRunWithoutAProjectionDoesNotAwaitMilestones() {
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(5, PipelineState.STOPPED)));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 5)));
        var stopped = query.find("orders").orElseThrow();
        assertThat(stopped.awaitingMilestones()).isFalse();
        assertThat(stopped.progress().milestones()).extracting(PipelineRunProgress.Milestone::stage)
                .containsExactly(PipelineRunStage.START_ACCEPTED);
    }

    @Test
    void rejectsAConcurrentReplacementAndUsesItsOwnStartRevisionOnceTheReadAgrees() {
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(4, PipelineState.RUNNING)));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 4)));
        String previousRun = query.find("orders").orElseThrow().progress().runId();
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(6, 6)));
        assertThat(query.find("orders")).isEmpty();
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(6, PipelineState.RUNNING)));
        assertThat(query.find("orders").orElseThrow().progress().runId()).isNotEqualTo(previousRun);
    }

    @Test
    void keepsTheRunIdWhenItsPreparationIsIssuedAndWaitsForProofAfterCompletion() {
        var start = intent(4, PipelineState.RUNNING);
        var pending = new PipelineIntentStore.PendingStart(start, new PipelinePreparationInputs(HASH, Map.of(), Map.of(), List.of()));
        when(intents.pendingStart("orders")).thenReturn(Optional.of(pending));
        var issuer = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"),
                new WorkloadOwner("node", "boot"), 1, 0, 1);
        var preparationId = "81ed723a-c7bc-453c-9ed5-de25ac58fc20";
        when(preparations.read("orders")).thenReturn(Optional.of(new PipelinePreparation(
                "orders", 1, PipelinePreparation.Phase.ISSUED, SCOPE, preparationId, issuer)));
        var issued = query.find("orders").orElseThrow().progress();
        assertThat(issued.active()).containsExactly(
                new PipelineRunProgress.Activity(PipelineRunStage.RUN_RESOURCES_PREPARING, null));

        when(preparations.read("orders")).thenReturn(Optional.of(new PipelinePreparation(
                "orders", 1, PipelinePreparation.Phase.COMPLETE, SCOPE, preparationId, issuer)));
        assertThat(query.find("orders")).isEmpty();
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(start));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 4)));
        assertThat(query.find("orders").orElseThrow().progress().runId()).isEqualTo(issued.runId());
    }

    @Test
    void aStoppedProjectionRetainsTheSameVersionAndMilestonesWithoutAnyActivity() {
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(5, PipelineState.STOPPED)));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 5)));
        var saved = acceptedRun(4).enter(PipelineRunStage.WAITING_FOR_CAPTURE, STARTED_AT);

        var stopped = withProjection(4, saved).find("orders").orElseThrow();

        assertThat(stopped.awaitingMilestones()).isFalse();
        assertThat(stopped.progress().runId()).isEqualTo(saved.runId());
        assertThat(stopped.progress().version()).isEqualTo(saved.version());
        assertThat(stopped.progress().milestones()).isEqualTo(saved.milestones());
        assertThat(stopped.progress().active()).isEmpty();
    }

    @Test
    void aReplacementThatCommitsDuringTheCacheReadDoesNotExposeTheOldRun() {
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(4, PipelineState.RUNNING)));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 4)), Optional.of(bindings(6, 6)));
        var saved = acceptedRun(4).reach(PipelineRunStage.ENGINE_RUNNING, STARTED_AT);

        assertThat(withProjection(4, saved).find("orders")).isEmpty();
    }

    @Test
    void aRequestedHandoffFallsBackToItsOwnStartWhileTheCacheHoldsThePreviousRun() {
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(6, PipelineState.RUNNING)));
        var handoff = new ConnectorRunBindings.Handoff(6, ConnectorRunBindings.Phase.REQUESTED, List.of(), List.of());
        var current = new PipelineIntentStore.ExecutionBindings(new ConnectorRunBindings(SCOPE, 4, List.of(), handoff), 6);
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(current));

        var read = withProjection(4, acceptedRun(4)).find("orders").orElseThrow();

        assertThat(read.progress()).isEqualTo(acceptedRun(6));
        assertThat(read.awaitingMilestones()).isTrue();
    }

    @Test
    void aTemporarilyUnavailableStoreDoesNotExposeAStaleRun() {
        when(intents.loadForActuation("orders")).thenThrow(new TapstateException(IoError.STORE_UNAVAILABLE, Map.of(), null));

        assertThat(query.find("orders")).isEmpty();
    }

    private NativePipelineRunProgressQuery withProjection(long startRevision, PipelineRunProgress progress) {
        var cache = mock(MongoCacheDomain.class);
        var store = mock(MongoCachePipelineRunProgressStore.class);
        when(cache.pipelineRunProgress(eq(SCOPE), isNull(), any())).thenReturn(store);
        when(store.latest()).thenReturn(Optional.of(new MongoCachePipelineRunProgressStore.Snapshot(startRevision, progress)));
        return new NativePipelineRunProgressQuery(BUSINESS, intents, preparations, () -> true, cache);
    }

    private static PipelineRunProgress acceptedRun(long revision) {
        return NativePipelineRunProgressQuery.accepted(SCOPE, revision, null, 3);
    }

    private static PipelineIntentSnapshot intent(long revision, PipelineState state) {
        return new PipelineIntentSnapshot(SCOPE, revision, new DesiredState("orders", state, HASH));
    }

    private static PipelineIntentStore.ExecutionBindings bindings(long start, long revision) {
        return new PipelineIntentStore.ExecutionBindings(new ConnectorRunBindings(SCOPE, start, List.of(), null), revision);
    }
}
