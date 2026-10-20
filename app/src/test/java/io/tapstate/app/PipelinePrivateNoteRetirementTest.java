package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelinePrivateNoteRetirementTest {
    @Test void cacheReconciliationKeepsTheSameContextAndReleasesOnlyAChangedConnection() {
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);var operations=mock(PipelineStateRebuilds.class);
        var original=mock(PipelineRebuildPreparation.class);var request=mock(PipelineRebuildRequest.class);when(original.request()).thenReturn(request);
        var scope=new PipelineIntentScope("cluster",1,"cache","flow",1);when(request.originalScope()).thenReturn(scope);when(request.operationId()).thenReturn("operation");
        var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","source"),PipelineNode.Purpose.CAPTURE,1),7);
        var connection=Map.<String,Object>of("uri","database-x");var held=io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture("connector","a".repeat(64),connection);
        when(notes.privateContexts(scope,"operation")).thenReturn(List.of(new RequiredNoteContext(token,Optional.of(held),false)));
        var retirement=mock(RequiredNoteRetirement.class);when(notes.retirement(token,"operation")).thenReturn(retirement);when(retirement.scope()).thenReturn(token);when(retirement.origin()).thenReturn(Optional.of(held));
        var releases=new ArrayList<RequiredNoteScope>();
        var executor=new PipelinePrivateNoteRetirement(notes,operations,(id,settings) -> io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture(id,"a".repeat(64),settings),
                (fixed,config,purpose,cluster,guard) -> {guard.run();releases.add(fixed.scope());assertThat(fixed.origin()).contains(held);return Optional.empty();});
        var unchanged=new io.tapstate.core.model.SourceResource("source",null,"connector",connection,null,null,null,null);
        executor.reconcile(original,Map.of(token.owner().namespace(),unchanged),() -> {});
        assertThat(releases).isEmpty();verify(notes,never()).retirement(any(),anyString());
        var changed=new io.tapstate.core.model.SourceResource("source",null,"connector",Map.of("uri","database-y"),null,null,null,null);
        executor.reconcile(original,Map.of(token.owner().namespace(),changed),() -> {});
        assertThat(releases).containsExactly(token);verify(retirement).completeAfterVerifiedRelease();
    }

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
