package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineDeletionGuardTest {
    @ParameterizedTest @ValueSource(strings = {"none", "consumer-read", "consumer-detach", "notes-list", "connector-release", "note-completion"})
    void storageAndConnectorCallbacksUseLocalGuardsWhileDurableProofsRemainOutsideTransactions(String failingStage) {
        var inside = new AtomicBoolean();
        var currentStage = new AtomicReference<String>();
        var visited = new ArrayList<String>();
        var failure = new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        Runnable durable = () -> assertThat(inside.get()).as("a durable proof cannot open a nested transaction").isFalse();
        Runnable local = () -> { if (inside.get() && failingStage.equals(currentStage.get())) throw failure; };
        var configuration = mock(PipelineRetirements.class);
        var cache = mock(PipelineCacheRetirements.class);
        var consumers = mock(SrsConsumerStore.class);
        var notes = mock(RequiredNoteStore.class);
        var original = mock(PipelineRetirementPreparation.class);
        var receipt = mock(PipelineRetirementReceipt.class);
        var result = mock(PipelineRetirementResult.class);
        var scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        var request = new PipelineRetirementRequest("delete", scope, "a".repeat(64), Optional.empty());
        when(original.request()).thenReturn(request);
        when(configuration.begin(request)).thenReturn(original);
        when(configuration.complete(receipt)).thenReturn(result);
        when(cache.retire(eq(original), any(), eq(Optional.empty()))).thenReturn(receipt);
        var member = mock(SrsConsumerMembership.class);
        var consumerGuard = new AtomicReference<Runnable>();
        when(consumers.guardedBy(any())).thenAnswer(call -> { consumerGuard.set(call.getArgument(0)); return consumers; });
        when(consumers.forPipeline(scope)).thenAnswer(call -> {
            callback(inside, currentStage, visited, "consumer-read", consumerGuard.get()); return List.of(member);
        });
        when(consumers.detachForRetirement(eq(original), eq(member), any(), eq(Optional.empty()))).thenAnswer(call -> {
            callback(inside, currentStage, visited, "consumer-detach", consumerGuard.get()); return Optional.of(member);
        });
        var noteGuard = new AtomicReference<Runnable>();
        when(notes.guardedBy(any())).thenAnswer(call -> { noteGuard.set(call.getArgument(0)); return notes; });
        var retirement = mock(RequiredNoteRetirement.class);
        var owner = RequiredNoteOwner.forNode(1, new PipelineNode("flow", "source"), PipelineNode.Purpose.CAPTURE, 1);
        when(retirement.scope()).thenReturn(new RequiredNoteScope(owner, 1));
        when(retirement.origin()).thenReturn(Optional.of(new RequiredNoteOrigin("connector", "a".repeat(64), new byte[] {1})));
        when(notes.retirements(scope, "delete")).thenAnswer(call -> {
            callback(inside, currentStage, visited, "notes-list", noteGuard.get()); return List.of(retirement);
        });
        doAnswer(call -> { callback(inside, currentStage, visited, "note-completion", noteGuard.get()); return null; })
                .when(retirement).completeAfterVerifiedRelease();
        var releases = new PipelinePrivateNoteRetirement(notes, mock(PipelineStateRebuilds.class),
                (fixed, config, purpose, cluster, guard) -> {
                    callback(inside, currentStage, visited, "connector-release", guard); return Optional.empty();
                });
        var node = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"),
                new WorkloadOwner("node", "boot"), 1, 0, 1);
        var coordinator = new PipelineDeletionCoordinator(configuration, cache, consumers, releases, () -> node, () -> {},
                (fixed, manifest) -> new PipelineDeletionCoordinator.Isolated(Optional.empty(), durable, local));

        var stages = List.of("consumer-read", "consumer-detach", "notes-list", "connector-release", "note-completion");
        if (!failingStage.equals("none")) {
            assertThatThrownBy(() -> coordinator.delete(request, Optional.empty())).isSameAs(failure);
            int failedAt = stages.indexOf(failingStage);
            assertThat(visited).containsExactlyElementsOf(stages.subList(0, failedAt));
            verify(cache, times(failedAt < 2 ? 0 : 1)).retire(any(), any(), any());
            verify(configuration, never()).complete(any());
            verify(retirement, times(failingStage.equals("note-completion") ? 1 : 0)).completeAfterVerifiedRelease();
            if (failedAt < 2) verifyNoInteractions(notes, retirement);
        } else {
            assertThat(coordinator.delete(request, Optional.empty())).isSameAs(result);
            assertThat(visited).contains("consumer-read", "consumer-detach", "notes-list", "connector-release", "note-completion");
            verify(configuration, atLeastOnce()).requirePending(original);
            verify(retirement).completeAfterVerifiedRelease();
        }
    }

    private static void callback(AtomicBoolean inside, AtomicReference<String> currentStage, List<String> visited, String stage, Runnable guard) {
        inside.set(true); currentStage.set(stage);
        try { guard.run(); visited.add(stage); guard.run(); }
        finally { inside.set(false); currentStage.set(null); }
    }
}
