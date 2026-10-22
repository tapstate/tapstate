package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.*;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.*;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineRecoveryInspectorTest {
    private static final CacheAuthScope AUTH=new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"new-cache",2);
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"new-cache","flow",2);
    private static final StorageRecoveryCommands.Selection SELECT=new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.PIPELINE,Set.of("flow"));
    private final MongoConfigurationDomain configuration=mock(MongoConfigurationDomain.class);
    private final CacheRecoveryStore recoveries=mock(CacheRecoveryStore.class);
    private final PipelineStatePreparations preparations=mock(PipelineStatePreparations.class);
    private final PipelineIntentStore intents=mock(PipelineIntentStore.class);
    private final StorageBindingStore bindings=mock(StorageBindingStore.class);
    private final InMemoryStorePort store=new InMemoryStorePort();
    private final Map<String,String> hashes=new HashMap<>(Map.of("mysql","a".repeat(64),"postgres","b".repeat(64)));
    private final Map<String,Set<String>> functions=new HashMap<>(Map.of("mysql",Set.of("batch_read_function","stream_read_function"),"postgres",Set.of("write_record_function","clear_table_function")));
    private final PipelineRecoveryInspector inspector;
    private SourceModel actualModel;
    private int probes;
    PipelineRecoveryInspectorTest() {
        when(configuration.cacheRecoveries()).thenReturn(recoveries);when(configuration.preparations()).thenReturn(preparations);
        when(configuration.bindings()).thenReturn(bindings);when(configuration.intents()).thenReturn(intents);
        when(configuration.capturePreparations()).thenReturn(mock(CaptureStatePreparations.class));
        when(bindings.cachePreparation()).thenReturn(Optional.of(new CachePreparation("auth",AUTH)));
        when(recoveries.read("flow")).thenReturn(Optional.of(new PipelineCacheRecovery(UUID.randomUUID().toString(),new PipelineIntentScope("cluster",1,"old-cache","flow",1),SCOPE)));
        when(preparations.read("flow")).thenReturn(Optional.of(new PipelinePreparation("flow",2,PipelinePreparation.Phase.UNASSIGNED,null,null,null)));
        var source=new SourceResource("src",null,"mysql",Map.of("host","source.invalid"),SourceMode.CDC,List.of(TableRef.literal("orders")),null,null);
        var target=new SourceResource("target",null,"postgres",Map.of("host","target.invalid"),null,null,null,null);
        store.artifacts().saveAll(List.of(source,target,pipeline(OnFullLoad.CLEAR,null)));model("id");
        ConnectorProvisioner connectors=id -> new ConnectorRef(List.of(),"fixture.Connector","2.0.8",null,null,hashes.get(id),false);
        inspector=new PipelineRecoveryInspector(configuration,store,connectors,(id,hash) -> {
            assertThat(hash).isEqualTo(hashes.get(id));return new ConnectorCapabilities(functions.get(id));
        },(item,hash) -> {probes++;return actualModel==null?store.schemas().get(item.id()).orElseThrow().model():actualModel;},() -> {});
    }
    private PipelineResource pipeline(OnFullLoad policy,ReadMode mode) {
        return new PipelineResource("flow",null,List.of(SourceRef.spec("src",true)),null,null,
                new ServeBlock.Inline("serve",FromRef.literal("orders"),List.of(new SyncElement("sync","target",null,null,null,policy)),null,null),
                mode==null?null:new Settings(null,null,null,null,mode,null),null);
    }
    private void model(String name) {
        store.schemas().save(new DiscoveredSourceModel("src","mysql",1,new SourceModel(List.of(new SourceTable("orders",
                List.of(new SourceField(name,"BIGINT",TapstateType.INT64)),List.of(name),List.of())))));
    }
    private void stopped() {
        var pipeline=(PipelineResource)store.artifacts().get("flow").orElseThrow();
        when(intents.load("flow")).thenReturn(Optional.of(new PipelineIntentSnapshot(SCOPE,2,new DesiredState("flow",PipelineState.STOPPED,
                CanonicalHash.of(pipeline),false,null,false,null))));
    }
    @Test void previewFixesTheSelectionArtifactAndModelWithoutPreparingOrWriting() {
        stopped();var before=store.artifacts().list();var result=inspector.inspect(AUTH,SELECT);
        assertThat(result.manifest().affectedPipelineIds()).containsExactly("flow");
        assertThat(result.manifest().definitionHashes()).containsOnlyKeys("flow","src","target");
        assertThat(result.targets()).containsExactly(new PipelineRecoveryInspector.Target("sync","target",OnFullLoad.CLEAR));
        assertThat(result.inputs().selectedTables()).containsEntry("src",Set.of("orders"));
        assertThat(result.privateContexts()).containsKeys("pdk.state.flow.capture.src","pdk.state.flow.sync.sync");
        assertThat(store.artifacts().list()).isEqualTo(before);
        verify(recoveries,never()).stageCluster(any());verify(recoveries,never()).authorizeFull(any(),anyString());
        verifyNoInteractionsExceptReads();
        model("changed");assertThat(inspector.inspect(AUTH,SELECT).manifest().dependencyHash()).isNotEqualTo(result.manifest().dependencyHash());
    }
    @Test void missingFullReadOrTargetClearCapabilityRefusesBeforeAnyGrant() {
        stopped();functions.put("mysql",Set.of("stream_read_function"));
        assertThatThrownBy(() -> inspector.inspect(AUTH,SELECT)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ConnectorError.CAPABILITY_MISSING));
        functions.put("mysql",Set.of("batch_read_function","stream_read_function"));functions.put("postgres",Set.of("write_record_function"));
        assertThatThrownBy(() -> inspector.inspect(AUTH,SELECT)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ConnectorError.CAPABILITY_MISSING));
        verify(recoveries,never()).authorizeFull(any(),anyString());
    }
    @Test void aViewRecoveryPreflightUsesItsDeclaredFullLoadPolicyAndRequiredCapability() {
        var view = new ViewBlock.Inline("view", FromRef.literal("orders"), "id", null, WriteMode.UPSERT, OnFullLoad.CLEAR);
        store.artifacts().saveAll(List.of(new PipelineResource("flow", null, List.of(SourceRef.spec("src", true)),
                null, view, null, null, null)));
        stopped();
        var artifacts = mock(ArtifactStore.class, org.mockito.AdditionalAnswers.delegatesTo(store.artifacts()));
        when(artifacts.managedDefaultSourceId()).thenReturn(Optional.of("target"));
        var wrapped = spy(store); doReturn(artifacts).when(wrapped).artifacts();
        ConnectorProvisioner connectors = id -> new ConnectorRef(List.of(), "fixture.Connector", "2.0.8", null, null, hashes.get(id), false);
        var reader = new PipelineRecoveryInspector(configuration, wrapped, connectors,
                (id, hash) -> new ConnectorCapabilities(functions.get(id)),
                (source, hash) -> store.schemas().get(source.id()).orElseThrow().model(), () -> {});
        assertThat(reader.inspect(AUTH, SELECT).targets())
                .containsExactly(new PipelineRecoveryInspector.Target("view", "target", OnFullLoad.CLEAR));
        functions.put("postgres", Set.of("write_record_function"));
        assertThatThrownBy(() -> reader.inspect(AUTH, SELECT)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ConnectorError.CAPABILITY_MISSING));
        verify(recoveries, never()).authorizeFull(any(), anyString());
    }
    @Test void cdcOnlyAnotherScopeAndMultiplePipelineSelectionsCannotAuthorizeAFull() {
        store.artifacts().saveAll(List.of(pipeline(OnFullLoad.APPEND,ReadMode.CDC_ONLY)));stopped();
        assertThatThrownBy(() -> inspector.inspect(AUTH,SELECT)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        assertThatThrownBy(() -> inspector.inspect(new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"old-cache",1),SELECT)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> inspector.inspect(AUTH,new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.PIPELINE,Set.of("flow","other"))))
                .isInstanceOf(TapstateException.class);
    }
    @Test void anotherRegisteredArtifactChangesTheConfirmationEvenWithTheSameSourceDefinition() {
        stopped();var original=inspector.inspect(AUTH,SELECT);hashes.put("mysql","c".repeat(64));
        assertThat(inspector.inspect(AUTH,SELECT).manifest().dependencyHash()).isNotEqualTo(original.manifest().dependencyHash());
    }
    @Test void aContinuationCanInspectPreparedStateButCannotCreateAnotherPreflight() {
        stopped();var initial=inspector.inspect(AUTH,SELECT);
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),new WorkloadOwner("node","boot"),1,0,1);
        when(preparations.read("flow")).thenReturn(Optional.of(new PipelinePreparation("flow",2,PipelinePreparation.Phase.COMPLETE,SCOPE,UUID.randomUUID().toString(),node)));
        when(intents.load("flow")).thenReturn(Optional.of(new PipelineIntentSnapshot(SCOPE,3,new DesiredState("flow",PipelineState.RUNNING,
                initial.inputs().pipelineHash(),false,null,false,null))));
        assertThatThrownBy(() -> inspector.inspect(AUTH,SELECT)).isInstanceOf(TapstateException.class);
        assertThat(inspector.inspectContinuation(AUTH,SELECT).manifest().dependencyHash()).isEqualTo(initial.manifest().dependencyHash());
    }
    @Test void liveStructureMismatchRefusesWhileTransactionRechecksNeverProbeOrSave() {
        stopped();var cached=store.schemas().get("src").orElseThrow();var before=inspector.inspect(AUTH,SELECT);assertThat(probes).isEqualTo(1);
        actualModel=new SourceModel(List.of(new SourceTable("orders",List.of(new SourceField("different","BIGINT",TapstateType.INT64)),List.of("different"),List.of())));
        assertThat(inspector.inspectCurrent(AUTH,SELECT).manifest().dependencyHash()).isEqualTo(before.manifest().dependencyHash());
        assertThat(probes).isEqualTo(1);
        assertThatThrownBy(() -> inspector.inspect(AUTH,SELECT)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED));
        assertThat(store.schemas().get("src")).contains(cached);verifyNoInteractionsExceptReads();
    }
    private void verifyNoInteractionsExceptReads() {
        verify(preparations,never()).issueFirstCreate(any(),any());verify(preparations,never()).complete(any());
        verify(intents,never()).compareAndSet(any(),anyLong(),any());
    }
}
