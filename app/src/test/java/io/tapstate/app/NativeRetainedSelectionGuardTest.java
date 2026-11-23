package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** A later edit must be checked from the captured start even when ordinary admission already succeeded. */
class NativeRetainedSelectionGuardTest {
    @Test
    void normalStartUsesTheCombinedCheckBeforePlanningItsModels() {
        var f = new Fixture();
        when(f.captures.retainedPlanningModels(eq(f.pass), eq(f.snapshot), eq(f.models), any()))
                .thenAnswer(call -> {
                    call.<PipelineCaptureCoordinator.RetainedStartCheck>getArgument(3).hasPreparedModels(() -> {});
                    return f.models;
                });
        doThrow(f.denied).when(f.preparer).verifyRetainedSourcesAndCheckSinkModels(eq(f.scope), eq(f.inputs),
                eq(f.fence), any());
        assertThatThrownBy(f::inspect).isSameAs(f.denied);
        verify(f.summaries).read("flow");
        verify(f.preparer, never()).verifyRetainedSources(any(), any());
        verify(f.configuration, never()).requiredNotes();

        doReturn(true).when(f.preparer).verifyRetainedSourcesAndCheckSinkModels(eq(f.scope), eq(f.inputs),
                eq(f.fence), any());
        var inspection = f.inspect();
        assertThat(inspection.models()).isSameAs(f.models);
        assertThat(inspection.connectorHashes()).isEmpty();
    }

    @Test
    void sourceRefusalStillWinsWhenTheRecoveryLookupFails() {
        var f = new Fixture();
        var unavailable = StorageRuntime.unavailable();
        doThrow(unavailable).when(f.summaries).read("flow");
        doThrow(f.denied).when(f.preparer).verifyRetainedSources(f.scope, f.inputs);
        assertThatThrownBy(f::inspect).isSameAs(f.denied);
        doNothing().when(f.preparer).verifyRetainedSources(f.scope, f.inputs);
        assertThatThrownBy(f::inspect).isSameAs(unavailable);
        verify(f.preparer, times(2)).verifyRetainedSources(f.scope, f.inputs);
        verifyNoInteractions(f.captures);
    }

    @Test
    void recoveryStillValidatesSourcesAndUsesThePinnedArtifact() {
        var f = new Fixture();
        var summary = new PipelineCacheRecovery(java.util.UUID.randomUUID().toString(), null, f.scope);
        when(f.summaries.read("flow")).thenReturn(Optional.of(summary));
        var notes = mock(RequiredNoteStore.class);
        when(f.configuration.requiredNotes()).thenReturn(notes);
        when(notes.guardedBy(any())).thenReturn(notes);
        when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0), 1));
        String hash = "a".repeat(64);
        when(notes.origin(any())).thenReturn(io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture(
                f.source.connector(), hash, f.source.config()));
        when(f.configuration.recoveryCommands()).thenReturn(mock(StorageRecoveryCommands.class));
        doThrow(f.denied).when(f.preparer).verifyRetainedSources(f.scope, f.inputs);
        assertThatThrownBy(f::inspect).isSameAs(f.denied);
        verify(f.configuration, never()).requiredNotes();
        doNothing().when(f.preparer).verifyRetainedSources(f.scope, f.inputs);
        assertThat(f.inspect().connectorHashes()).containsExactlyEntriesOf(Map.of(f.source.connector(), hash));
        verifyNoInteractions(f.captures);
        verify(f.preparer, never()).verifyRetainedSourcesAndCheckSinkModels(any(), any(), any(), any());
    }

    private static final class Fixture {
        final PipelineIntentScope scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        final WorkloadClaimFence fence = new WorkloadClaimFence(
                new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
                new WorkloadOwner("node", "boot"), 1, 1, 1);
        final CacheAuthScope auth = new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "cache", 1);
        final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
        final CacheRecoveryStore summaries = mock(CacheRecoveryStore.class);
        final PipelineFirstStatePreparer preparer = mock(PipelineFirstStatePreparer.class);
        final PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
        final PipelineConvergencePass pass = mock(PipelineConvergencePass.class);
        final TapstateException denied = new TapstateException(IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED,
                Map.of("pipeline", "flow", "tables", "input.customers"), null);
        final SourceResource source;
        final SourceModelSnapshot models;
        final ReadOnlyArtifactSnapshot snapshot;
        final PipelinePreparationInputs inputs;
        final NativePipelineRecovery recovery;

        Fixture() {
            var parser = new DslParser();
            source = (SourceResource) parser.parse("""
                    version: tapstate/v1
                    kind: source
                    id: input
                    connector: e2e_file
                    config: {uri: /fixture}
                    mode: cdc
                    tables: [orders, customers]
                    """);
            var pipeline = (PipelineResource) parser.parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: flow
                    source: [{id: input, srs: true}]
                    settings: {read_mode: snapshot_and_cdc}
                    """);
            var artifacts = mock(ArtifactStore.class);
            when(artifacts.list()).thenReturn(List.of(source, pipeline));
            snapshot = ReadOnlyArtifactSnapshot.capture(artifacts);
            models = SourceModelSnapshot.capture("flow", List.of(source), mock(SchemaStore.class));
            inputs = models.preparationInputs(scope, pipeline, pipeline, Map.of(source.id(), source));
            when(configuration.cacheRecoveries()).thenReturn(summaries);
            when(pass.scope()).thenReturn(scope);
            when(pass.claim()).thenReturn(fence);
            recovery = new NativePipelineRecovery(configuration, new CachePreparation("operation", auth), mock(StorePort.class), preparer,
                    mock(ConnectorProvisioner.class), mock(Engine.class), captures, mock(ExecutionAuthorization.class),
                    () -> {}, () -> true, mock(PipelineStoreViews.class));
        }

        StoreBackedDagSource.CapturedStartGuard.Inspection inspect() {
            return recovery.inspectCaptured(pass, snapshot, models);
        }
    }
}
