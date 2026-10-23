package io.tapstate.adapters.pdk;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.SinkPreparationStore;
import io.tapstate.core.common.TapstateException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;

/** One sink's immutable target inputs over the coordinator's already frozen complete roster. */
public final class PdkSinkPreparations {
    private final SinkPreparationStore store;
    private final PipelineIntentScope scope;
    private final PipelineNode node;
    private final PipelineNode.Purpose purpose;
    private final OnFullLoad policy;
    private final boolean fullLoad;
    public PdkSinkPreparations(SinkPreparationStore store,PipelineIntentScope scope,PipelineNode node,
            PipelineNode.Purpose purpose,OnFullLoad policy,boolean fullLoad) {
        this.store=Objects.requireNonNull(store,"store");this.scope=Objects.requireNonNull(scope,"scope");
        this.node=Objects.requireNonNull(node,"node");this.purpose=Objects.requireNonNull(purpose,"purpose");
        this.policy=Objects.requireNonNull(policy,"policy");this.fullLoad=fullLoad;
        if (!scope.pipelineId().equals(node.pipelineId())) throw new IllegalArgumentException("sink node belongs to another Pipeline");
    }
    void requireClaim(io.tapstate.spi.store.WorkloadClaimFence original) {
        if(original.key().type()!=io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION || original.executionGeneration()<1
                || !scope.clusterId().equals(original.key().clusterId()) || !scope.pipelineId().equals(original.key().resourceId()))throw fenced();
    }
    void requireInputs(PipelineNode actualNode,PipelineNode.Purpose actualPurpose,OnFullLoad actualPolicy,boolean actualFullLoad) {
        if(!node.equals(actualNode) || purpose!=actualPurpose || policy!=actualPolicy || fullLoad!=actualFullLoad)throw fenced();
    }
    /** Internal transport through connector catch boundaries; the public boundary restores the original host failure. */
    static final class HostInvariant extends RuntimeException {
        final RuntimeException original;
        HostInvariant(RuntimeException original){super(null,null,false,false);this.original=original;}
    }
    static <T>T host(java.util.function.Supplier<T> action) {
        try{return action.get();}catch(TapstateException coded){throw coded;}catch(RuntimeException invariant){throw new HostInvariant(invariant);}
    }
    private static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
    SinkPreparationStore.Snapshot load(TargetTable target){if(target==null)throw new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",scope.pipelineId()),null);return store.load(input(target));}
    SinkPreparationStore.Attempt begin(TargetTable target){return store.begin(input(target));}
    void complete(SinkPreparationStore.Attempt original,boolean created){store.complete(original,new SinkPreparationStore.Result(created));}
    void requireReady(TargetTable target){if(load(target).phase()!=SinkPreparationStore.Phase.READY)throw new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",scope.pipelineId()),null);}
    private SinkPreparationStore.Input input(TargetTable target){return input(scope,node,purpose,policy,fullLoad,target);}
    public static SinkPreparationStore.Input input(PipelineIntentScope scope,PipelineNode node,PipelineNode.Purpose purpose,
            OnFullLoad policy,boolean fullLoad,TargetTable target) {
        if(!scope.pipelineId().equals(node.pipelineId()))throw new IllegalArgumentException("sink node belongs to another Pipeline");
        return new SinkPreparationStore.Input(new SinkPreparationStore.Key(scope,purpose,node.nodeId(),target.name()),modelHash(target),
                io.tapstate.core.model.OnFullLoad.valueOf(policy.name()),fullLoad);
    }
    /** Explicit ordered structure, including nullable type attributes; strings retain every UTF-16 code unit. */
    public static String modelHash(TargetTable target) {
        Objects.requireNonNull(target,"target");
        try {
            var bytes=new ByteArrayOutputStream();
            try(var out=new DataOutputStream(bytes)) {
                out.writeInt(1);text(out,target.name());out.writeInt(target.fields().size());
                for(var field:target.fields()) {
                    text(out,field.name());text(out,field.type());out.writeBoolean(field.primaryKey());
                    text(out,field.inferredType()==null?null:field.inferredType().name());
                    var numeric=field.numericType();out.writeBoolean(numeric!=null);
                    if(numeric!=null){scalar(out,numeric.bit());scalar(out,numeric.fixed());scalar(out,numeric.unsigned());scalar(out,numeric.zerofill());scalar(out,numeric.minValue());scalar(out,numeric.maxValue());scalar(out,numeric.precision());scalar(out,numeric.scale());}
                    var string=field.stringType();out.writeBoolean(string!=null);
                    if(string!=null){scalar(out,string.bytes());scalar(out,string.fixed());scalar(out,string.doubleBytes());scalar(out,string.defaultValue());scalar(out,string.byteRatio());}
                }
                out.writeInt(target.indexes().size());
                for(var index:target.indexes()){out.writeInt(index.fields().size());for(String field:index.fields())text(out,field);out.writeBoolean(index.unique());out.writeBoolean(index.constrainsEveryRow());}
            }
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch(IOException|NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
    }
    private static void scalar(DataOutputStream out,Object value)throws IOException{text(out,value==null?null:value.toString());}
    private static void text(DataOutputStream out,String value)throws IOException{out.writeInt(value==null?-1:value.length());if(value!=null)out.writeChars(value);}
}
