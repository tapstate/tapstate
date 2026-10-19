package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.spi.store.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfiguredSinkRunTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster-a",1,"cache-a","p1",7);
    private static final WorkloadClaimFence CLAIM=new WorkloadClaimFence(new WorkloadClaimKey("cluster-a",WorkloadClaimType.PIPELINE_ACTUATION,"p1"),new WorkloadOwner("node-a","boot-a"),3,4,1);
    private static final ExecutionFence FENCE=new ExecutionFence(SCOPE,CLAIM);
    private static final PipelineNode NODE=new PipelineNode("p1","sink");
    private static final String HASH="a".repeat(64);
    private static final TargetTable TARGET=new TargetTable("target",List.of(new TargetField("id","int",true)));
    private static PdkSinkWriterFactory factory(){return new PdkSinkWriterFactory("demo",Map.of(),WriteMode.UPSERT,DdlPolicy.FAIL,Map.of("orders",TARGET),NODE,OnFullLoad.CLEAR,true);}

    @Test void serializedFactoryUsesOnlyCapturedTokensExactArtifactAndIndependentCacheReceipts(@TempDir Path directory)throws Exception {
        var trace=Files.createFile(directory.resolve("trace"));var ref=new ConnectorRef(List.of(SyntheticSinkJars.preparationSink(directory,trace,true)),"synthetic.PreparationSink","2.0.8",null,null,HASH,false);
        var provisioner=mock(ConnectorProvisioner.class);when(provisioner.resolve("demo")).thenReturn(ref);when(provisioner.resolve("demo",HASH)).thenReturn(ref);
        var notes=notes();var original=factory();var run=ConfiguredSinkRun.prepare(FENCE,original.config(),PipelineNode.Purpose.SYNC,notes,provisioner,() -> {});
        var receipts=new Receipts();receipts.prepareTargets(original.preparationInputs(SCOPE));var cache=mock(PipelineStoreViews.class);
        when(cache.sinkPreparations(eq(SCOPE),eq(CLAIM),any())).thenAnswer(call -> {call.getArgument(2,Runnable.class).run();return receipts;});
        var auth=mock(ExecutionAuthorization.class);when(auth.require(FENCE)).thenReturn(CLAIM);
        var config=config();ScopedSinkStateProvider.bind(config,cache,notes);config.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY,auth);config.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY,provisioner);
        clearInvocations(notes,provisioner);
        var member=Hazelcast.newHazelcastInstance(config);
        try {
            var copy=copy(original.forRun(run));copy.prepareTargets(member);
            for(int i=0;i<3;i++)try(var ignored=copy.getEx()){}
            assertThat(Files.readAllLines(trace)).containsExactly("create:target","clear:target","stop","stop","stop","stop");
            verify(notes,never()).open(any());verify(notes,never()).initialize(any());verify(notes,never()).prepareOrigin(any(),any());
            verify(provisioner,never()).resolve("demo");verify(provisioner,atLeastOnce()).resolve("demo",HASH);
            assertThat(receipts.rows.values()).allSatisfy(row -> assertThat(row.phase()).isEqualTo(SinkPreparationStore.Phase.READY));
        }finally{member.shutdown();}
    }
    @Test void aRemoteOpenCannotFollowANewerClearGeneration(@TempDir Path directory)throws Exception {
        var trace=Files.createFile(directory.resolve("trace"));var ref=new ConnectorRef(List.of(SyntheticSinkJars.preparationSink(directory,trace,true)),"synthetic.PreparationSink","2.0.8",null,null,HASH,false);
        var notes=notes();var original=factory();var run=ConfiguredSinkRun.prepare(FENCE,original.config(),PipelineNode.Purpose.SYNC,notes,id -> ref,() -> {});
        when(notes.origin(any())).thenThrow(new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null));
        var auth=mock(ExecutionAuthorization.class);when(auth.require(FENCE)).thenReturn(CLAIM);var cache=mock(PipelineStoreViews.class);when(cache.sinkPreparations(any(),any(),any())).thenReturn(mock(SinkPreparationStore.class));
        var config=config();ScopedSinkStateProvider.bind(config,cache,notes);config.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY,auth);config.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY,(ConnectorProvisioner)(id -> ref));
        clearInvocations(notes);var member=Hazelcast.newHazelcastInstance(config);
        try{assertThatThrownBy(() -> copy(original.forRun(run)).prepareTargets(member)).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.NOTE_SCOPE_UNAVAILABLE));assertThat(Files.readAllLines(trace)).isEmpty();verify(notes,never()).open(any());verify(notes,never()).initialize(any());}finally{member.shutdown();}
    }
    @Test void theCoordinateDtoCarriesNoSettingsAndChecksBindingPurposeAndStateGeneration()throws Exception {
        var config=new SinkConfig("demo",Map.of("password","secret-sentinel"),WriteMode.UPSERT,DdlPolicy.FAIL,null,NODE);
        var notes=notes();var ref=new ConnectorRef(List.of(),"not.loaded","2.0.8",null,null,HASH,false);
        var run=ConfiguredSinkRun.prepare(FENCE,config,PipelineNode.Purpose.SYNC,notes,id -> ref,() -> {});var bytes=bytes(run);
        assertThat(new String(bytes,java.nio.charset.StandardCharsets.ISO_8859_1)).doesNotContain("secret-sentinel","password");
        assertThat(copy(run)).isEqualTo(run);
        assertThatThrownBy(() -> new ConfiguredSinkRun(FENCE,NODE,PipelineNode.Purpose.VIEW,run.primary(),run.global(),HASH)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConfiguredSinkRun(new ExecutionFence(new PipelineIntentScope("cluster-a",1,"cache-a","p1",8),CLAIM),NODE,PipelineNode.Purpose.SYNC,run.primary(),run.global(),HASH)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void originalGuardLossCannotPublishAnOriginOrAdoptANewerCommand() {
        var notes=notes();var ref=new ConnectorRef(List.of(),"not.loaded","2.0.8",null,null,HASH,false);var active=new AtomicLong();
        assertThatThrownBy(() -> ConfiguredSinkRun.prepare(FENCE,factory().config(),PipelineNode.Purpose.SYNC,notes,id -> {active.incrementAndGet();return ref;},() -> {if(active.get()>0)throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);})).isInstanceOf(TapstateException.class);
        verify(notes,never()).prepareOrigin(any(),any());
    }
    private static RequiredNoteStore notes() {
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0),5));
        when(notes.prepareOrigin(any(),any())).thenAnswer(call -> call.getArgument(1));
        when(notes.origin(any())).thenReturn(ConnectorNoteOrigin.capture("demo",HASH,Map.of()));when(notes.read(any(),anyString())).thenReturn(new RequiredNoteValue(0,null));return notes;
    }
    private static Config config(){var config=new Config().setClusterName("sink-"+UUID.randomUUID());config.setProperty("hazelcast.logging.type","none");config.setProperty("hazelcast.operation.thread.count","1");config.setProperty("hazelcast.operation.generic.thread.count","1");config.setProperty("hazelcast.io.thread.count","1");config.getJetConfig().setCooperativeThreadCount(1);config.getNetworkConfig().setPort(0).setPortAutoIncrement(false).getJoin().getAutoDetectionConfig().setEnabled(false);config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);return config;}
    private static byte[] bytes(Object value)throws Exception{var bytes=new ByteArrayOutputStream();try(var out=new ObjectOutputStream(bytes)){out.writeObject(value);}return bytes.toByteArray();}
    @SuppressWarnings("unchecked") private static <T>T copy(T value)throws Exception{try(var in=new ObjectInputStream(new ByteArrayInputStream(bytes(value)))){return(T)in.readObject();}}
    private static final class Receipts implements SinkPreparationStore {
        final Map<Input,Snapshot> rows=new HashMap<>();
        public void prepareTargets(List<Input> inputs){for(var input:inputs)rows.put(input,new Snapshot(input,UUID.randomUUID().toString(),0,Phase.PREPARING,null,null));}
        public Snapshot load(Input input){return Objects.requireNonNull(rows.get(input),"only original compiled targets may open");}
        public Attempt begin(Input input){var old=load(input);assertThat(old.revision()).isZero();var started=new Snapshot(input,old.operationId(),1,Phase.PREPARING,CLAIM,null);rows.put(input,started);return new Original(started);}
        public Snapshot complete(Attempt attempt,Result result){var old=attempt.original();assertThat(rows.get(old.input())).isEqualTo(old);var ready=new Snapshot(old.input(),old.operationId(),2,Phase.READY,CLAIM,result);rows.put(old.input(),ready);return ready;}
        private record Original(Snapshot original)implements Attempt{}
    }
}
