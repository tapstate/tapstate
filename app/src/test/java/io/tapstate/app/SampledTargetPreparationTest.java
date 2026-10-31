package io.tapstate.app;

import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SampledTargetPreparationTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","flow",1);
    private static final PipelineNode NODE=new PipelineNode("flow","out");
    private static final ExecutionFence ORIGINAL=new ExecutionFence(SCOPE,new WorkloadClaimFence(
            new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("node","boot"),1,1,1));
    private static final PipelinePreparationInputs AUTHORING=new PipelinePreparationInputs("a".repeat(64),Map.of("mongo","b".repeat(64),"sql","c".repeat(64)),
            Map.of("mongo",Set.of("orders"),"sql",Set.of("customers")),List.of());
    private static TargetTable table(String name,String type) {return new TargetTable(name,List.of(new TargetField("name",type,false)));}
    private static SinkPreparationStore.Prepared prepared(TargetTable table) {return PdkSinkPreparations.prepared(SCOPE,NODE,PipelineNode.Purpose.SYNC,OnFullLoad.APPEND,true,table);}
    private static StoreBackedDagSource.TargetPreparations plan(SinkPreparationStore store) {
        var snapshots=Map.of("mongo",new PipelineCaptureBindings.SnapshotOnlySource("mongo-chain",Set.of("orders"),1),
                "sql",new PipelineCaptureBindings.SnapshotOnlySource("sql-chain",Set.of("customers"),1));
        return new StoreBackedDagSource.TargetPreparations(store,new PipelineCaptureBindings(ORIGINAL,Map.of(),Map.of(),snapshots,true,AUTHORING,Set.of("mongo")));
    }
    @Test void samplingEvidenceCannotRestoreAnIndependentOrMixedDeclaredTarget() {
        var store=mock(SinkPreparationStore.class);var plan=plan(store);
        plan.observeOrigins("documents",Set.of("mongo"));plan.observeOrigins("declared",Set.of("sql"));plan.observeOrigins("mixed",Set.of("mongo","sql"));
        assertThat(plan.sampledStreams).containsExactly("documents");
        var old=table("orders","STRING(100)");var observed=table("orders","STRING(20480)");
        var sql=table("customers","varchar(99)");var original=prepared(old);
        when(store.retainedModels(anyCollection(),any(),anyBoolean(),eq(AUTHORING))).thenReturn(Map.of(original.input().key(),original));
        when(store.resolveModels(anyList(),eq(AUTHORING))).thenAnswer(call -> {
            List<SinkPreparationStore.Prepared> models=call.getArgument(0);assertThat(models).containsExactly(original);return models;
        });
        var notes=mock(RequiredNoteStore.class);when(notes.guardedBy(any())).thenReturn(notes);
        when(notes.open(any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0),1));
        when(notes.prepareOrigin(any(),any())).thenAnswer(call -> call.getArgument(1));
        var ref=new ConnectorRef(List.of(),"not.loaded","1.0",null,null,"d".repeat(64),false);
        var binder=new StoreBackedDagSource.PdkSinkWriterBinder(notes,id -> ref);
        var factory=new PdkSinkWriterFactory("mongodb",Map.of(),WriteMode.UPSERT,DdlPolicy.FAIL,
                Map.of("documents",observed,"declared",sql),NODE,OnFullLoad.APPEND,true);
        var bound=(PdkSinkWriterFactory)binder.scoped(factory,ORIGINAL,plan,() -> {});
        assertThat(bound.targets().get("documents")).isEqualTo(old);
        assertThat(bound.targets().get("declared")).isEqualTo(sql);
        assertThat(plan.models).containsExactly(original);
        assertThat(plan.inputs).contains(prepared(sql).input());
        assertThat(plan.inputs).doesNotContain(prepared(table("customers","varchar(40)")).input());
    }
    @Test void originalBodyIsRestoredBeforeAChangedSampleCanRefuseUnwindIdentity() {
        var store=mock(SinkPreparationStore.class);var plan=plan(store);plan.observeOrigins("rows",Set.of("mongo"));
        var identity=new UnwindIdentity(List.of("_id"),"source_id");
        var logical=new TargetTable("orders",List.of(new TargetField("_id","INT64",true),new TargetField("items.id","INT64",true)),List.of(),identity);
        var physical=new TargetTable("orders",List.of(new TargetField("source_id","INT64",true),new TargetField("items.id","INT64",true)));
        var collision=new TargetTable("orders",List.of(new TargetField("_id","STRING(100)",true),new TargetField("items.id","INT64",true),
                new TargetField("source_id","STRING(100)",false)),List.of(),identity);
        var original=prepared(physical);
        when(store.retainedModels(anyCollection(),any(),anyBoolean(),eq(AUTHORING))).thenReturn(Map.of(original.input().key(),original));
        when(store.resolveModels(anyList(),eq(AUTHORING))).thenAnswer(call -> call.getArgument(0));
        var notes=mock(RequiredNoteStore.class);ConnectorProvisioner connectors=id -> {throw new AssertionError("not a plugin test");};
        var run=mock(ConfiguredSinkRun.class);when(run.node()).thenReturn(NODE);when(run.purpose()).thenReturn(PipelineNode.Purpose.SYNC);
        when(run.assessTargets(any(),anyMap(),eq(notes),eq(connectors),any())).thenAnswer(call -> {
            Map<String,TargetTable> targets=call.getArgument(1);assertThat(targets).containsEntry("rows",logical);
            return Map.of("rows",new TargetIdentityPlan(TargetIdentityPlan.AUTOMATIC,"generated-target-identity",physical,"_id","source_id",List.of("source_id","items.id"),false,true));
        });
        try(var preparation=mockStatic(ConfiguredSinkRun.class)) {
            preparation.when(() -> ConfiguredSinkRun.prepare(eq(ORIGINAL),any(),eq(PipelineNode.Purpose.SYNC),eq(notes),eq(connectors),any(),eq(false))).thenReturn(run);
            var factory=new PdkSinkWriterFactory("mongodb",Map.of(),WriteMode.UPSERT,DdlPolicy.FAIL,Map.of("rows",collision),NODE,OnFullLoad.APPEND,true);
            var bound=(PdkSinkWriterFactory)new StoreBackedDagSource.PdkSinkWriterBinder(notes,connectors).scoped(factory,ORIGINAL,plan,() -> {});
            assertThat(bound.targets()).containsEntry("rows",logical);assertThat(plan.models).containsExactly(original);
            var order=inOrder(store,run);order.verify(store).retainedModels(anyCollection(),any(),anyBoolean(),eq(AUTHORING));
            order.verify(run).assessTargets(any(),anyMap(),eq(notes),eq(connectors),any());
        }
    }
}
