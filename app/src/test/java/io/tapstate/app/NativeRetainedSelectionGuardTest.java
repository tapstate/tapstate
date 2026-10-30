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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** A later edit must be checked from the captured start even when ordinary admission already succeeded. */
class NativeRetainedSelectionGuardTest {
    @Test
    void anOrdinaryCapturedStartChecksRetainedSelectionsBeforeRecoveryOrCaptureWork() {
        var scope = new PipelineIntentScope("cluster",1,"cache","flow",1);
        var auth = new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1);
        var parser = new DslParser();
        var source = (SourceResource) parser.parse("version: tapstate/v1\nkind: source\nid: input\nconnector: e2e_file\n"
                + "config: {uri: /fixture}\nmode: cdc\ntables: [orders, customers]\n");
        var pipeline = (PipelineResource) parser.parse("version: tapstate/v1\nkind: pipeline\nid: flow\n"
                + "source: [{id: input, srs: true}]\nsettings: {read_mode: snapshot_and_cdc}\n");
        var artifacts = mock(ArtifactStore.class);
        when(artifacts.list()).thenReturn(List.of(source,pipeline));
        var snapshot = ReadOnlyArtifactSnapshot.capture(artifacts);
        var models = SourceModelSnapshot.capture("flow",List.of(source),mock(SchemaStore.class));
        var expected = models.preparationInputs(scope,pipeline,pipeline,Map.of(source.id(),source));
        var configuration = mock(MongoConfigurationDomain.class);
        var summaries = mock(CacheRecoveryStore.class);
        when(configuration.cacheRecoveries()).thenReturn(summaries);
        when(summaries.read("flow")).thenReturn(Optional.empty());
        var preparer = mock(PipelineFirstStatePreparer.class);
        var denied = new TapstateException(IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED,
                Map.of("pipeline","flow","tables","input.customers"),null);
        doThrow(denied).when(preparer).verifyRetainedSources(eq(scope),any(PipelinePreparationInputs.class));
        var engine = mock(Engine.class);
        var captures = mock(PipelineCaptureCoordinator.class);
        var connectors = mock(ConnectorProvisioner.class);
        var store = mock(StorePort.class);
        var recovery = new NativePipelineRecovery(configuration,auth,store,preparer,connectors,engine,captures,
                mock(ExecutionAuthorization.class),() -> {},() -> true,mock(PipelineStoreViews.class));
        var pass = mock(PipelineConvergencePass.class);
        when(pass.scope()).thenReturn(scope);

        assertThatThrownBy(() -> recovery.inspectCaptured(pass,snapshot,models)).isSameAs(denied);

        verify(preparer).verifyRetainedSources(scope,expected);
        verify(preparer,never()).prepare(any(PipelineIntentScope.class),any(PipelinePreparationInputs.class));
        verify(summaries,never()).read(anyString());
        verifyNoInteractions(engine,captures,connectors,store);
    }
}
