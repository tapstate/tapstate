package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapstate.core.common.NumericType;
import io.tapstate.core.common.StringType;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.spi.store.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ScopedTargetPreparationTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","pipeline",7);
    private static final PipelineNode NODE=new PipelineNode("pipeline","sink");
    private static final WorkloadClaimFence CLAIM=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),new WorkloadOwner("node","boot"),3,4,1);
    private static final TargetTable TARGET=new TargetTable("orders",List.of(new TargetField("id","bigint",true)),List.of(new TargetIndex(List.of("id"),true)));
    @Test void knownPreparationIsPublishedBeforeIndexAndRestartNeverClearsOrCreatesAgain()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var functions=functions(calls);
        functions.supportCreateIndex((context,table,event) -> {assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.READY);calls.add("index-failed");throw new IllegalStateException("index failed");});
        assertThatThrownBy(() -> preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET))).hasMessage("index failed");
        assertThat(calls).containsExactly("issued","create","clear","ready","index-failed");calls.clear();
        functions.supportCreateIndex((context,table,event) -> calls.add("index"));
        preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET));assertThat(calls).containsExactly("index");
    }
    @Test void unknownExternalClearCannotBeIssuedAgainAfterReopening()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var functions=functions(calls);
        functions.supportClearTable((context,event) -> {calls.add("clear-unknown");throw new IllegalStateException("response lost");});
        assertThatThrownBy(() -> preparation("mongodb",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET))).hasMessage("response lost");
        assertThat(calls).containsExactly("issued","clear-unknown");calls.clear();
        assertThatThrownBy(() -> preparation("mongodb",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET))).isInstanceOf(TapstateException.class);
        assertThat(calls).isEmpty();assertThat(store.held.revision()).isEqualTo(1);
    }
    @Test void losingOriginalAuthorityAfterTargetClearCannotPublishReady()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var active=new AtomicBoolean(true);var functions=functions(calls);
        functions.supportClearTable((context,event) -> {calls.add("clear");active.set(false);});
        assertThatThrownBy(() -> preparation("mysql",functions,store,() -> {if(!active.get())throw refused();}).prepare(TARGET,TargetTapTable.build(TARGET))).isInstanceOf(TapstateException.class);
        assertThat(calls).containsExactly("issued","create","clear");assertThat(store.held.phase()).isEqualTo(SinkPreparationStore.Phase.PREPARING);
    }
    @Test void preparedWriterRequiresReadyWithoutEverPreparingTheTarget() {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var binding=binding(store);
        assertThatThrownBy(() -> binding.requireReady(TARGET)).isInstanceOf(TapstateException.class);assertThat(calls).isEmpty();
        var attempt=store.begin(store.held.input());store.complete(attempt,new SinkPreparationStore.Result(true));calls.clear();binding.requireReady(TARGET);assertThat(calls).isEmpty();
    }
    @Test void createdPrimaryKeyResultIsReusedWhenIndexRetryOpensAgain()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var functions=functions(calls);
        functions.supportCreateTableV2((context,event) -> {calls.add("create-new");return new CreateTableOptions().tableExists(false);});
        preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET));assertThat(calls).containsExactly("issued","create-new","ready");calls.clear();
        preparation("mysql",functions,store,() -> {}).prepare(TARGET,TargetTapTable.build(TARGET));assertThat(calls).isEmpty();assertThat(store.held.result().created()).isTrue();
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"policy","node","purpose","mode"})
    void differentActualPreparationCannotConsumeTheOriginalReceipt(String change) {
        var calls=new ArrayList<String>();var store=new Receipts(calls);
        assertThatThrownBy(() -> new PdkTargetPreparation("mysql",null,functions(calls),
                change.equals("policy")?OnFullLoad.APPEND:OnFullLoad.CLEAR,!change.equals("mode"),
                change.equals("node")?new PipelineNode("pipeline","other"):NODE,null,false,
                change.equals("purpose")?PipelineNode.Purpose.VIEW:PipelineNode.Purpose.SYNC,() -> {},binding(store))
                .prepare(TARGET,TargetTapTable.build(TARGET))).isInstanceOf(TapstateException.class);
        assertThat(calls).isEmpty();assertThat(store.held.revision()).isZero();
    }
    @Test void sameNamedAliasWithAnotherModelCannotBypassThePreparedMemo()throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var preparation=preparation("mysql",functions(calls),store,() -> {});
        preparation.prepare(TARGET,TargetTapTable.build(TARGET));calls.clear();
        var changed=new TargetTable(TARGET.name(),List.of(new TargetField("another","bigint",true)));
        assertThatThrownBy(() -> preparation.prepare(changed,TargetTapTable.build(changed))).isInstanceOf(TapstateException.class);assertThat(calls).isEmpty();
    }
    @Test void nativeWriterCannotWriteAnUnmappedInputStream(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory)throws Throwable {
        var calls=new ArrayList<String>();var store=new Receipts(calls);var writes=new java.util.concurrent.atomic.AtomicInteger();
        var ref=new ConnectorRef(List.of(Synthetic.countingSink(directory)),"synthetic.CountingSink","2.0.8",null);
        var connector=PdkConnector.open("demo",ref,Map.of());
        var config=new SinkConfig("demo",Map.of(),WriteMode.UPSERT,DdlPolicy.FAIL,null,NODE,OnFullLoad.CLEAR,true);
        try(var writer=new PdkSinkWriter(connector,(context,rows,table,result) -> {writes.incrementAndGet();result.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>(1L,0L,0L));},config,Map.of("known",TARGET),null,true,null,PipelineNode.Purpose.SYNC,binding(store))) {
            assertThatThrownBy(() -> writer.write(List.of(io.tapstate.core.event.Envelope.insert(1,"unmapped",Map.of("id",1),null))).toCompletableFuture().get(5,java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TapstateException.class);assertThat(writes).hasValue(0);
        }
    }
    @Test void hostReceiptErrorKeepsItsIdentityThroughTheActualAsyncWriter(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory)throws Throwable {
        var failure=new AssertionError("host invariant");
        SinkPreparationStore broken=(SinkPreparationStore)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{SinkPreparationStore.class},(proxy,method,args) -> {throw failure;});
        var receipts=new PdkSinkPreparations(broken,SCOPE,NODE,PipelineNode.Purpose.SYNC,OnFullLoad.CLEAR,true);
        var connector=PdkConnector.open("demo",new ConnectorRef(List.of(Synthetic.countingSink(directory)),"synthetic.CountingSink","2.0.8",null),Map.of());
        var config=new SinkConfig("demo",Map.of(),WriteMode.UPSERT,DdlPolicy.FAIL,null,NODE,OnFullLoad.CLEAR,true);
        try(var writer=new PdkSinkWriter(connector,(context,rows,table,result) -> {throw new AssertionError("no target write is allowed");},config,Map.of("known",TARGET),null,false,null,PipelineNode.Purpose.SYNC,receipts)) {
            assertThatThrownBy(() -> writer.write(List.of(io.tapstate.core.event.Envelope.insert(1,"known",Map.of("id",1),null))).toCompletableFuture().get(5,java.util.concurrent.TimeUnit.SECONDS))
                    .cause().isSameAs(failure);
        }
    }
    @Test void targetHashRetainsAllOrderedFieldsIndexesAttributesAndLosslessText() {
        String original=PdkSinkPreparations.modelHash(TARGET);assertThat(original).matches("[0-9a-f]{64}");
        assertThat(PdkSinkPreparations.modelHash(new TargetTable(TARGET.name(),TARGET.fields(),TARGET.indexes()))).isEqualTo(original);
        var variants=List.of(new TargetTable("orders2",TARGET.fields(),TARGET.indexes()),new TargetTable("orders",List.of(new TargetField("id","bigint",false)),TARGET.indexes()),
                new TargetTable("orders",TARGET.fields(),List.of(new TargetIndex(List.of("id"),false))),new TargetTable("orders",List.of(new TargetField("id",null,true)),TARGET.indexes()),
                new TargetTable("orders",List.of(new TargetField("id","bigint",true,null,new NumericType(64,true,false,false,BigDecimal.ZERO,BigDecimal.TEN,18,0),null)),TARGET.indexes()),
                new TargetTable("orders",List.of(new TargetField("id","bigint",true,null,null,new StringType(4L,false,false,1L,2))),TARGET.indexes()));
        for(var target:variants)assertThat(PdkSinkPreparations.modelHash(target)).isNotEqualTo(original);
        assertThat(PdkSinkPreparations.modelHash(new TargetTable("x\ud800",List.of()))).isNotEqualTo(PdkSinkPreparations.modelHash(new TargetTable("x\udc00",List.of())));
        assertThat(PdkSinkPreparations.modelHash(new TargetTable("a",List.of(new TargetField("b:c","d",false))))).isNotEqualTo(PdkSinkPreparations.modelHash(new TargetTable("a",List.of(new TargetField("b","c:d",false)))));
    }
    private static PdkSinkPreparations binding(Receipts store){return new PdkSinkPreparations(store,SCOPE,NODE,PipelineNode.Purpose.SYNC,OnFullLoad.CLEAR,true);}
    private static PdkTargetPreparation preparation(String id,ConnectorFunctions functions,Receipts store,Runnable active){return new PdkTargetPreparation(id,null,functions,OnFullLoad.CLEAR,true,NODE,null,false,PipelineNode.Purpose.SYNC,active,binding(store));}
    private static ConnectorFunctions functions(List<String> calls){var functions=new ConnectorFunctions();functions.supportCreateTableV2((context,event) -> {calls.add("create");return new CreateTableOptions().tableExists(true);});functions.supportClearTable((context,event) -> calls.add("clear"));functions.supportCreateIndex((context,table,event) -> calls.add("index"));return functions;}
    private static TapstateException refused(){return new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",SCOPE.pipelineId()),null);}
    private static final class Receipts implements SinkPreparationStore {
        private final List<String> calls;private Snapshot held;
        Receipts(List<String> calls){this.calls=calls;held=new Snapshot(PdkSinkPreparations.input(SCOPE,NODE,PipelineNode.Purpose.SYNC,OnFullLoad.CLEAR,true,TARGET),UUID.randomUUID().toString(),0,Phase.PREPARING,null,null);}
        public void prepareTargets(List<Input> inputs){throw new AssertionError("plugin code cannot grant its own preparation roster");}
        public Snapshot load(Input input){if(!input.equals(held.input()))throw refused();return held;}
        public Attempt begin(Input input){load(input);if(held.revision()!=0)throw refused();held=new Snapshot(input,held.operationId(),1,Phase.PREPARING,CLAIM,null);calls.add("issued");return new Original(held);}
        public Snapshot complete(Attempt attempt,Result result){assertThat(attempt.original()).isEqualTo(held);held=new Snapshot(held.input(),held.operationId(),2,Phase.READY,CLAIM,result);calls.add("ready");return held;}
        private record Original(Snapshot original)implements Attempt{}
    }
}
