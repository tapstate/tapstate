package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelinePrivateNoteRetirementTest {
    @Test void recoveredFullPreservesUnchangedPrivateOriginsAndDoesNotCompleteFailedRelease() {
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        var permission=mock(CacheRecoveryStore.FullPreparation.class);
        var previous=new PipelineIntentScope("cluster",1,"old","flow",1);var target=new PipelineIntentScope("cluster",1,"new","flow",2);
        when(permission.recovery()).thenReturn(new PipelineCacheRecovery(UUID.randomUUID().toString(),previous,target));
        var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","source"),PipelineNode.Purpose.CAPTURE,1),7);
        var connection=Map.<String,Object>of("uri","database-x");var held=io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture("connector","a".repeat(64),connection);
        when(notes.privateContexts(permission)).thenReturn(List.of(new RequiredNoteContext(token,Optional.of(held),false)));
        var retirement=mock(RequiredNoteRetirement.class);when(notes.retirement(token,permission)).thenReturn(retirement);
        when(retirement.scope()).thenReturn(token);when(retirement.origin()).thenReturn(Optional.of(held));
        var failure=new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);
        var executor=new PipelinePrivateNoteRetirement(notes,mock(PipelineStateRebuilds.class),
                (id,settings) -> io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture(id,"a".repeat(64),settings),
                (fixed,config,purpose,cluster,guard) -> {guard.run();return Optional.of(failure);});
        var unchanged=new io.tapstate.core.model.SourceResource("source",null,"connector",connection,null,null,null,null);
        executor.reconcileRecovered(permission,Map.of(token.owner().namespace(),unchanged),() -> {});
        verify(notes,never()).retirement(any(),any(CacheRecoveryStore.FullPreparation.class));
        var changed=new io.tapstate.core.model.SourceResource("source",null,"connector",Map.of("uri","database-y"),null,null,null,null);
        assertThatThrownBy(() -> executor.reconcileRecovered(permission,Map.of(token.owner().namespace(),changed),() -> {})).isSameAs(failure);
        verify(retirement,never()).completeAfterVerifiedRelease();
        doThrow(failure).when(permission).requireActive();
        assertThatThrownBy(() -> executor.reconcileRecovered(permission,Map.of(),() -> {})).isSameAs(failure);
    }
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
    @Test void clearingDirectPostgresSelectsOnlyItsPrivateCaptureAndPreservesFalsePolicy() {
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        var operations=mock(PipelineStateRebuilds.class);var original=mock(PipelineRebuildPreparation.class);
        var request=mock(PipelineRebuildRequest.class);when(original.request()).thenReturn(request);
        var scope=new PipelineIntentScope("cluster",1,"cache","flow",1);
        when(request.originalScope()).thenReturn(scope);when(request.operationId()).thenReturn("clear");
        var contexts=new ArrayList<RequiredNoteContext>();var selected=new HashMap<String,io.tapstate.core.model.SourceResource>();
        var retirements=new HashMap<String,RequiredNoteRetirement>();
        for(String id:List.of("pg","retained","mongo","view","foreign")) {
            String ownerId=id.equals("foreign")?"other":"flow";
            var purpose=id.equals("view")?PipelineNode.Purpose.VIEW:PipelineNode.Purpose.CAPTURE;
            var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode(ownerId,id),purpose,1),7);
            Map<String,Object> config=id.equals("retained")?Map.of("autoClearSlot",false):Map.of();
            String connector=id.equals("mongo")?"mongodb":"postgres";
            var source=new io.tapstate.core.model.SourceResource(id,null,connector,config,null,null,null,null);
            var origin=io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture(connector,"a".repeat(64),config);
            contexts.add(new RequiredNoteContext(token,Optional.of(origin),false));selected.put(token.owner().namespace(),source);
            var retirement=mock(RequiredNoteRetirement.class);when(retirement.scope()).thenReturn(token);
            when(retirement.origin()).thenReturn(Optional.of(origin));when(notes.retirement(token,"clear")).thenReturn(retirement);
            retirements.put(id,retirement);
        }
        when(notes.privateContexts(scope,"clear")).thenReturn(contexts);
        var releases=new ArrayList<String>();
        var executor=new PipelinePrivateNoteRetirement(notes,operations,
                (id,config) -> io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture(id,"a".repeat(64),config),
                (fixed,config,purpose,cluster,guard) -> {guard.run();releases.add(config.node().nodeId());return Optional.empty();});
        executor.retireDirectPostgres(original,selected,() -> {});
        assertThat(releases).containsExactly("pg");verify(retirements.get("pg")).completeAfterVerifiedRelease();
        for(String id:List.of("retained","mongo","view","foreign"))verify(retirements.get(id),never()).completeAfterVerifiedRelease();
    }

    @Test void failedDirectPostgresReleaseKeepsTheOriginalNotes() {
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        var operations=mock(PipelineStateRebuilds.class);var original=mock(PipelineRebuildPreparation.class);
        var request=mock(PipelineRebuildRequest.class);when(original.request()).thenReturn(request);
        var scope=new PipelineIntentScope("cluster",1,"cache","flow",1);
        when(request.originalScope()).thenReturn(scope);when(request.operationId()).thenReturn("clear");
        var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","pg"),PipelineNode.Purpose.CAPTURE,1),7);
        var origin=io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture("postgres","a".repeat(64),Map.of());
        when(notes.privateContexts(scope,"clear")).thenReturn(List.of(new RequiredNoteContext(token,Optional.of(origin),false)));
        var retirement=mock(RequiredNoteRetirement.class);when(retirement.origin()).thenReturn(Optional.of(origin));
        when(notes.retirement(token,"clear")).thenReturn(retirement);
        var failure=new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);
        var executor=new PipelinePrivateNoteRetirement(notes,operations,(id,config) -> origin,
                (fixed,config,purpose,cluster,guard) -> Optional.of(failure));
        var source=new io.tapstate.core.model.SourceResource("pg",null,"postgres",Map.of(),null,null,null,null);
        assertThatThrownBy(() -> executor.retireDirectPostgres(original,Map.of(token.owner().namespace(),source),() -> {})).isSameAs(failure);
        verify(retirement,never()).completeAfterVerifiedRelease();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={true,false})
    void theProductionPrivateReleaseUsesThePinnedPolicyBeforeRetiringNotes(boolean automatic) {
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        var operations=mock(PipelineStateRebuilds.class);var original=mock(PipelineRebuildPreparation.class);
        var request=mock(PipelineRebuildRequest.class);when(original.request()).thenReturn(request);
        var scope=new PipelineIntentScope("cluster",1,"cache","flow",1);
        when(request.originalScope()).thenReturn(scope);when(request.operationId()).thenReturn("clear");
        var token=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","pg"),PipelineNode.Purpose.CAPTURE,1),7);
        var origin=io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture("postgres","a".repeat(64),Map.of("autoClearSlot",automatic));
        when(notes.privateContexts(scope,"clear")).thenReturn(List.of(new RequiredNoteContext(token,Optional.of(origin),false)));
        var retirement=mock(RequiredNoteRetirement.class);when(retirement.scope()).thenReturn(token);
        when(retirement.origin()).thenReturn(Optional.of(origin));when(retirement.notes()).thenReturn(notes);
        when(notes.retirement(token,"clear")).thenReturn(retirement);
        when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0),1));
        var connectors=mock(io.tapstate.adapters.pdk.ConnectorProvisioner.class);
        var reference=new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"unused","2.0.8",null,null,"a".repeat(64),false);
        when(connectors.resolve("postgres")).thenReturn(reference);
        var port=mock(io.tapstate.adapters.pdk.PdkCapturePort.class);
        when(port.automaticallyClearsPrivatePostgres(origin)).thenReturn(automatic);
        when(port.releasePrivatePostgresSlot(any())).thenReturn(Optional.empty());
        try(var factory=mockStatic(io.tapstate.adapters.pdk.PdkCapturePort.class)) {
            factory.when(() -> io.tapstate.adapters.pdk.PdkCapturePort.withRequiredNotes(eq(connectors),isNull(),any()))
                    .thenReturn(port);
            var source=new io.tapstate.core.model.SourceResource("pg",null,"postgres",Map.of("autoClearSlot",automatic),null,null,null,null);
            new PipelinePrivateNoteRetirement(notes,operations,connectors).retireDirectPostgres(original,Map.of(token.owner().namespace(),source),() -> {});
            verify(port).automaticallyClearsPrivatePostgres(origin);
            verify(port,automatic?times(1):never()).releasePrivatePostgresSlot(any());
            verify(retirement,automatic?times(1):never()).completeAfterVerifiedRelease();
        }
    }

}
