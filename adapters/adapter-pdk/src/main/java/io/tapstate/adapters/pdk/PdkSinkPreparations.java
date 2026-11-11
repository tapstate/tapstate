package io.tapstate.adapters.pdk;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.SinkPreparationStore;
import io.tapstate.core.common.TapstateException;
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
    public static final class OpeningFacts {
        private final java.util.Set<SinkPreparationStore.Input> ready = new java.util.HashSet<>();
        private SharedSinkConnectors.NoteIdentity notes;
    }
    private final OpeningFacts opening;
    public PdkSinkPreparations(SinkPreparationStore store,PipelineIntentScope scope,PipelineNode node,
            PipelineNode.Purpose purpose,OnFullLoad policy,boolean fullLoad) {
        this(store, scope, node, purpose, policy, fullLoad, new OpeningFacts());
    }
    public PdkSinkPreparations(SinkPreparationStore store,PipelineIntentScope scope,PipelineNode node,
            PipelineNode.Purpose purpose,OnFullLoad policy,boolean fullLoad,OpeningFacts opening) {
        this.store=Objects.requireNonNull(store,"store");this.scope=Objects.requireNonNull(scope,"scope");
        this.node=Objects.requireNonNull(node,"node");this.purpose=Objects.requireNonNull(purpose,"purpose");
        this.policy=Objects.requireNonNull(policy,"policy");this.fullLoad=fullLoad;
        this.opening=Objects.requireNonNull(opening,"opening");
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
    void requireReady(java.util.Collection<TargetTable> targets) {
        var inputs = targets.stream().map(this::input).distinct().filter(input -> !opening.ready.contains(input)).toList();
        if (inputs.isEmpty()) return;
        var snapshots = store.loadMany(inputs);
        if (snapshots.size() != inputs.size() || snapshots.stream().anyMatch(row -> row.phase() != SinkPreparationStore.Phase.READY))
            throw new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",scope.pipelineId()),null);
        opening.ready.addAll(inputs);
    }
    SharedSinkConnectors.NoteIdentity openedNotes() { return opening.notes; }
    void rememberOpenedNotes(SharedSinkConnectors.NoteIdentity notes) { opening.notes = notes; }
    private SinkPreparationStore.Input input(TargetTable target){return input(scope,node,purpose,policy,fullLoad,target);}
    public static SinkPreparationStore.Input input(PipelineIntentScope scope,PipelineNode node,PipelineNode.Purpose purpose,
            OnFullLoad policy,boolean fullLoad,TargetTable target) {
        if(!scope.pipelineId().equals(node.pipelineId()))throw new IllegalArgumentException("sink node belongs to another Pipeline");
        return new SinkPreparationStore.Input(new SinkPreparationStore.Key(scope,purpose,node.nodeId(),target.name()),modelHash(target),
                io.tapstate.core.model.OnFullLoad.valueOf(policy.name()),fullLoad);
    }
    /** The original encoding and hash are shared with storage without an adapter dependency. */
    public static io.tapstate.core.common.PreparedTargetModel model(TargetTable target) {
        return new io.tapstate.core.common.PreparedTargetModel(target.name(),target.fields().stream().map(field ->
                new io.tapstate.core.common.PreparedTargetModel.Field(field.name(),field.type(),field.primaryKey(),field.inferredType(),field.numericType(),field.stringType())).toList(),
                target.indexes().stream().map(index -> new io.tapstate.core.common.PreparedTargetModel.Index(index.fields(),index.unique(),index.constrainsEveryRow())).toList());
    }
    public static String modelHash(TargetTable target) { return model(target).modelHash(); }
    public static TargetTable target(byte[] canonical,io.tapstate.spi.sink.UnwindIdentity identity) {
        var model=io.tapstate.core.common.PreparedTargetModel.decode(canonical);
        return new TargetTable(model.name(),model.fields().stream().map(field -> new io.tapstate.spi.sink.TargetField(
                field.name(),field.type(),field.primaryKey(),field.inferredType(),field.numericType(),field.stringType())).toList(),
                model.indexes().stream().map(index -> new io.tapstate.spi.sink.TargetIndex(index.fields(),index.unique(),index.constrainsEveryRow())).toList(),identity);
    }
    public static SinkPreparationStore.Prepared prepared(PipelineIntentScope scope,PipelineNode node,PipelineNode.Purpose purpose,
            OnFullLoad policy,boolean fullLoad,TargetTable target) {
        return new SinkPreparationStore.Prepared(input(scope,node,purpose,policy,fullLoad,target),model(target).canonical());
    }
}
