package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelinePrivateNoteRetirementTest {
    @Test void originalPurposesAreReleasedOnceAndFailureKeepsTheRemainingIdentity() {
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        var operations=mock(PipelineStateRebuilds.class);var original=mock(PipelineRebuildPreparation.class);var request=mock(PipelineRebuildRequest.class);
        var scope=new PipelineIntentScope("cluster",1,"incarnation","flow",1);
        when(original.request()).thenReturn(request);when(request.originalScope()).thenReturn(scope);when(request.operationId()).thenReturn("operation");
        var retired=new ArrayList<RequiredNoteRetirement>();
        for(var purpose:PipelineNode.Purpose.values()) {
            var item=mock(RequiredNoteRetirement.class);var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","same"),purpose,1),4);
            when(item.scope()).thenReturn(token);when(item.origin()).thenReturn(Optional.of(new RequiredNoteOrigin("original-connector","a".repeat(64),new byte[]{1})));
            retired.add(item);
        }
        when(notes.retirements(scope,"operation")).thenReturn(retired);
        var released=new ArrayList<PipelineNode.Purpose>();
        var failure=new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);
        var executor=new PipelinePrivateNoteRetirement(notes,operations,(item,config,purpose,cluster,guard) -> {
            guard.run();assertThat(config.node()).isEqualTo(new PipelineNode("flow","same"));assertThat(config.connectorId()).isEqualTo("original-connector");
            assertThat(config.settings()).isEmpty();assertThat(cluster).isEqualTo("cluster");released.add(purpose);
            return purpose==PipelineNode.Purpose.VIEW?Optional.of(failure):Optional.empty();
        });
        assertThatThrownBy(() -> executor.retire(original,() -> {})).isSameAs(failure);
        assertThat(released).containsExactly(PipelineNode.Purpose.CAPTURE,PipelineNode.Purpose.VIEW);
        verify(retired.getFirst()).completeAfterVerifiedRelease();verify(retired.get(1),never()).completeAfterVerifiedRelease();verify(retired.get(2),never()).completeAfterVerifiedRelease();
        when(retired.getFirst().completed()).thenReturn(true);
        var retry=new PipelinePrivateNoteRetirement(notes,operations,(item,config,purpose,cluster,guard) -> {guard.run();released.add(purpose);return Optional.empty();});
        retry.retire(original,() -> {});assertThat(released).containsExactly(PipelineNode.Purpose.CAPTURE,PipelineNode.Purpose.VIEW,PipelineNode.Purpose.VIEW,PipelineNode.Purpose.SYNC);
        verify(retired.getFirst(),times(1)).completeAfterVerifiedRelease();verify(retired.get(1)).completeAfterVerifiedRelease();verify(retired.get(2)).completeAfterVerifiedRelease();
    }
}
