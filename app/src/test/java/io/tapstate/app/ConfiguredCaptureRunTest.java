package io.tapstate.app;

import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfiguredCaptureRunTest {
    private static final String HASH="a".repeat(64);
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","flow",7);
    private static final PipelineNode NODE=new PipelineNode("flow","input");
    private static final ExecutionFence ORIGINAL=new ExecutionFence(SCOPE,new WorkloadClaimFence(
            new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("node","boot"),3,8,2));
    private static final ConnectorRef ARTIFACT=new ConnectorRef(List.of(),"must.not.load","2.0.8",null,null,HASH,false);

    @ParameterizedTest @ValueSource(booleans={false,true})
    void recoveredPrivateAndSharedCapturesUseThePreviouslyConfirmedArtifact(boolean shared) {
        var notes=notes();var config=config(shared);var origin=ConnectorNoteOrigin.capture("demo",HASH,config.settings());
        when(notes.origin(any())).thenReturn(origin);var provisioner=mock(ConnectorProvisioner.class);
        when(provisioner.resolve("demo",HASH)).thenReturn(ARTIFACT);
        var run=ConfiguredCaptureRun.prepare(ORIGINAL,config,notes,provisioner,() -> {},true);
        assertThat(run.artifactHash()).isEqualTo(HASH);verify(provisioner).resolve("demo",HASH);verify(provisioner,never()).resolve("demo");
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void pluginOpenUsesTheOriginalTokensAndExactArtifactWithoutOpeningCurrentScopes(boolean shared) {
        var notes=notes(); var provisioner=mock(ConnectorProvisioner.class);
        when(provisioner.resolve("demo")).thenReturn(ARTIFACT); when(provisioner.resolve("demo",HASH)).thenReturn(ARTIFACT);
        var config=config(shared); var run=ConfiguredCaptureRun.prepare(ORIGINAL,config,notes,provisioner,() -> {});
        var refusal=new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);
        when(notes.origin(run.primary())).thenThrow(refusal); clearInvocations(notes,provisioner);
        var port=run.port(config,notes,provisioner,() -> {});
        assertThatThrownBy(() -> port.snapshot(config)).isSameAs(refusal);
        verify(provisioner).resolve("demo",HASH); verify(provisioner,never()).resolve("demo");
        verify(notes).origin(run.primary()); verify(notes,never()).open(any()); verify(notes,never()).initialize(any());
    }
    @Test void aSharedDriveUsesItsCaptureGuardAfterItsOpeningPipelineStops() {
        var notes=notes(); var preparation=new AtomicBoolean(true); var capture=new AtomicBoolean(true);
        var config=config(true); var run=ConfiguredCaptureRun.prepare(ORIGINAL,config,notes,id -> ARTIFACT,() -> require(preparation));
        preparation.set(false); var refusal=new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);
        when(notes.origin(run.primary())).thenThrow(refusal);
        var port=run.port(config,notes,id -> ARTIFACT,() -> require(capture));
        assertThatThrownBy(() -> port.snapshot(config)).isSameAs(refusal);
        clearInvocations(notes); capture.set(false);
        assertThatThrownBy(() -> port.snapshot(config)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        verify(notes,never()).origin(any());
    }
    @Test void originalStartLossCannotPublishAnOrigin() {
        var notes=notes(); var active=new AtomicBoolean(true);
        assertThatThrownBy(() -> ConfiguredCaptureRun.prepare(ORIGINAL,config(false),notes,id -> { active.set(false); return ARTIFACT; },() -> require(active)))
                .isInstanceOf(TapstateException.class);
        verify(notes,never()).prepareOrigin(any(),any());
    }
    @Test void aCapturedRunCannotOpenAnotherNodeOrSharedNamespace() {
        var notes=notes(); var config=config(true); var run=ConfiguredCaptureRun.prepare(ORIGINAL,config,notes,id -> ARTIFACT,() -> {});
        assertThatThrownBy(() -> run.port(config.at(new PipelineNode("flow","other")),notes,id -> ARTIFACT,() -> {})).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> run.port(config.sharing(new SharedNotes("another-chain",List.of())),notes,id -> ARTIFACT,() -> {})).isInstanceOf(TapstateException.class);
    }
    private static CaptureConfig config(boolean shared) {
        var config=new CaptureConfig("demo",Map.of("password","secret-sentinel"),List.of("orders"),NODE);
        return shared?config.sharing(new SharedNotes("chain",List.of())):config;
    }
    private static RequiredNoteStore notes() {
        var notes=mock(RequiredNoteStore.class); when(notes.guardedBy(any())).thenReturn(notes);
        when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0),5));
        when(notes.prepareOrigin(any(),any())).thenAnswer(call -> call.getArgument(1)); return notes;
    }
    private static void require(AtomicBoolean active) { if (!active.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
}
