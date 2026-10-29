package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkValidationReads;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.*;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.ValidationReadHandle;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.*;

/** Serial direct-table scans and complete independent logical-key rereads on the original owner. */
final class NativeValidationPdkRows implements NativeValidationFullJob.Plan {
    private static final ValidationValues.Limits ROW=new ValidationValues.Limits(64*1024,8192,32);
    private static final long READ_BYTES=16L*1024*1024+PdkValidationReads.decodingWorkBytes(ROW)+2*ROW.bytes();
    private final NativeValidationOwnerReservations.Reservation original;
    private final Clock clock;
    private final String nodeId;
    private ValidationSavedDeclarations saved;
    private List<ValidationDirectMappings.Declared> declarations;
    private Map<String,ConnectorRef> artifacts;
    private final List<ValidationFullScan.Mapping> mappings;
    private final Set<Read> reads=new HashSet<>();
    private final AtomicInteger consumers=new AtomicInteger();
    private final AtomicReference<ValidationLocalBudget.Reservation> working=new AtomicReference<>();
    private final AtomicReference<ValidationOrderedMerge> active=new AtomicReference<>();
    private final ValidationFailureLatch failure=new ValidationFailureLatch();
    private volatile Table current;
    private volatile boolean closed;

    private record Table(int index,PdkValidationReads.Metadata source,PdkValidationReads.Metadata target){}
    NativeValidationPdkRows(NativeValidationOwnerReservations.Reservation original,ValidationSavedDeclarations saved,
            List<ValidationDirectMappings.Declared> declarations,Map<String,ConnectorRef> artifacts,Clock clock){
        this.original=original;this.saved=saved;this.declarations=declarations;this.artifacts=artifacts;this.clock=clock;
        nodeId=original.resources().budget().identity().owner().nodeId();
        var mappings=new ArrayList<ValidationFullScan.Mapping>();
        for(int index=0;index<declarations.size();index++){
            int selected=index;var declared=declarations.get(index);requireStaticScope(saved,declared);
            var keys=declared.mapping().keys();
            for(int component=0;component<keys.source().size();component++){
                var from=keys.source().get(component);var to=keys.target().get(component);
                if((from.logicalTypes().contains("object-id")||to.logicalTypes().contains("object-id"))
                        &&(!from.logicalTypes().equals(Set.of("object-id"))||!to.logicalTypes().equals(Set.of("object-id"))
                            ||from.domain()!=ValidationKey.Kind.TEXT||to.domain()!=ValidationKey.Kind.TEXT))
                    throw unsupported("keys","native-object-id-domain-mismatch");
            }
            mappings.add(new ValidationFullScan.Mapping(declared.mapping().mappingId(),guard->open(selected,guard),
                    (observation,guard)->check(selected,observation,guard)));
        }
        this.mappings=List.copyOf(mappings);
    }
    @Override public List<ValidationFullScan.Mapping> mappings(){return mappings;}
    @Override public Throwable failure(){return failure.get();}

    private ValidationOrderedMerge open(int index,Runnable guard){
        consumers.incrementAndGet();ValidationOrderedGroups source=null,target=null;
        try{
            require();guard.run();var previous=active.getAndSet(null);if(previous!=null)previous.close();awaitClosed(guard);
            if(working.get()==null){
                var lease=original.resources().budget().reserve(new ValidationLocalBudget.Amount(2L*1024*1024,0,0,0));
                if(!working.compareAndSet(null,lease))lease.close();
            }
            var from=metadata(index,true,guard);var to=metadata(index,false,guard);
            require();guard.run();current=new Table(index,from,to);
            source=groups(index,true,guard);target=groups(index,false,guard);
            var merge=new ValidationOrderedMerge(source,target);active.set(merge);
            require();guard.run();return merge;
        }catch(RuntimeException|Error failed){
            record(failed);
            if(source!=null)try{source.close();}catch(RuntimeException|Error cleanup){record(cleanup);}
            if(target!=null)try{target.close();}catch(RuntimeException|Error cleanup){record(cleanup);}
            throwFailure();throw new IllegalStateException("failed table opening has no diagnosis");
        }finally{consumers.decrementAndGet();releaseWorking();}
    }

    private PdkValidationReads.Metadata metadata(int index,boolean source,Runnable guard){
        require();guard.run();awaitClosed(guard);
        var declared=declarations.get(index);var side=source?declared.mapping().source():declared.mapping().target();
        var fields=components(declared.mapping(),source).stream().map(value->value.path().getFirst()).toList();
        var read=admit();PdkValidationReads.MetadataRead handle=null;
        try{
            var resource=resource(side);var connection=connection(side,resource);
            handle=PdkValidationReads.inspect(connection,artifact(resource),CanonicalHash.of(resource),side.table(),
                    full()?List.of():fields,()->{require();guard.run();},original.resources().queryExecutor(),
                    original.resources().decodeExecutor(),ROW);
            read.attach(handle::close,handle.workersExited(),handle.rowsReleased());read.opened();
            require();guard.run();var observed=handle.result();
            if(!observed.binding().equals(binding(side)))throw invalid("savedModelChanged");
            if(source)ValidationDirectMappings.requireSourceTable(declared,observed.model());
            for(var field:observed.model().fields())
                if(field.type()==io.tapstate.core.common.TapstateType.UNKNOWN)throw unsupported(side.nodeId(),side.table()+"/"+field.name(),"field-model-unknown");
            return observed;
        }catch(RuntimeException|Error failed){record(failed);throw failed;}
        finally{
            if(handle==null)read.unopened();
            try{read.close();}finally{read.opened();}
        }
    }

    private ValidationOrderedGroups groups(int index,boolean source,Runnable guard){
        var mapping=declarations.get(index).mapping();var side=source?mapping.source():mapping.target();
        var keys=keyBinding(mapping,source);
        return new ValidationOrderedGroups(side.sourceId(),reader(index,source,null,guard),row->{
            require();guard.run();var key=bound(keys,row);
            return new ValidationOrderedGroups.Key(key,physical(side,keys,key));
        },ROW.bytes());
    }

    private ValidationReadHandle reader(int index,boolean source,ValidationKey root,Runnable guard){
        require();guard.run();awaitClosed(guard);
        var mapping=declarations.get(index).mapping();var side=source?mapping.source():mapping.target();
        var keys=components(mapping,source);var read=admit();ValidationReadHandle raw=null;
        try{
            var resource=resource(side);var connection=connection(side,resource);var artifact=artifact(resource);
            var query=original.resources().queryExecutor();var decode=original.resources().decodeExecutor();
            Runnable active=()->{require();guard.run();};
            raw=root==null?PdkValidationReads.scan(connection,artifact,CanonicalHash.of(resource),side.table(),binding(side),keys,full(),active,query,decode,2,ROW)
                    :PdkValidationReads.lookup(connection,artifact,CanonicalHash.of(resource),side.table(),binding(side),keys,root,full(),active,query,decode,2,ROW);
            var handle=raw;read.attach(handle::close,handle.workersExited(),handle.rowsReleased());read.opened();require();guard.run();
            return new ValidationReadHandle(){
                @Override public Optional<Row> next(){require();guard.run();return handle.next();}
                @Override public void close(){read.close();}
                @Override public CompletionStage<Void> workersExited(){return handle.workersExited();}
                @Override public CompletionStage<Void> rowsReleased(){return handle.rowsReleased();}
            };
        }catch(RuntimeException|Error failed){
            record(failed);if(raw==null)read.unopened();try{read.close();}catch(RuntimeException|Error cleanup){record(cleanup);}
            throwFailure();throw new IllegalStateException("failed read opening has no diagnosis");
        }finally{read.opened();}
    }

    private ValidationRootCheck check(int index,ValidationObservation observation,Runnable guard){
        consumers.incrementAndGet();
        try{
            require();guard.run();var table=current;if(table==null||table.index()!=index)throw invalid("tableSequence");
            var mapping=declarations.get(index).mapping();
            var source=keyBinding(mapping,true);var target=keyBinding(mapping,false);
            var generated=ValidationFieldCheck.generatedId(observation.run(),mapping.mappingId(),mapping.outputId(),
                    artifacts.get(mapping.target().sourceId()).className().equals("io.tapdata.mongodb.MongodbConnector"),
                    table.source().model().fields().stream().anyMatch(field->field.name().equals("_id")),
                    java.util.stream.Stream.concat(mapping.keys().source().stream(),mapping.keys().target().stream())
                            .anyMatch(component->component.path().getFirst().equals("_id")),
                    value->value instanceof ValidationValues.Typed typed&&typed.logicalType().equals("object-id")&&typed.value() instanceof String);
            return ValidationRootReader.readDirect(observation,original.spec().method(),
                    new ValidationRootReader.Query(UUID.randomUUID().toString(),"independent-complete-key-reread-v1",
                            ()->reader(index,true,observation.root().rootKey(),guard)),
                    new ValidationRootReader.Query(UUID.randomUUID().toString(),"independent-complete-key-reread-v1",
                            ()->reader(index,false,observation.root().rootKey(),guard)),
                    new ValidationRelationRows.Limits(65536,ROW),new ValidationRootCheck.Limits(256*1024,256,256,8,2),clock,
                    ()->{require();guard.run();},row->physical(mapping.source(),source,matching(source,row,observation)),
                    row->physical(mapping.target(),target,matching(target,row,observation)),(pair,result)->{
                        if(full())ValidationFieldCheck.check(observation,List.of(),pair.source().working(),pair.target().working(),
                                pair.sourceSample(),pair.targetSample(),generated,String::equals,
                                new ValidationValueComparison.TraversalLimits(ROW,100000),result);
                    });
        }catch(RuntimeException|Error failed){record(failed);throw failed;}
        finally{consumers.decrementAndGet();releaseWorking();}
    }

    private Read admit(){
        var lease=original.resources().budget().reserve(new ValidationLocalBudget.Amount(READ_BYTES,2,1,1));
        var read=new Read(lease);
        synchronized(reads){reads.add(read);}
        return read;
    }
    private final class Read {
        private final ValidationLocalBudget.Reservation lease;
        private final AtomicInteger settled=new AtomicInteger();
        private final AtomicBoolean closing=new AtomicBoolean(),released=new AtomicBoolean();
        private volatile Runnable closer;
        private volatile boolean requested;
        Read(ValidationLocalBudget.Reservation lease){this.lease=lease;}
        void attach(Runnable closer,CompletionStage<Void> workers,CompletionStage<Void> rows){
            this.closer=closer;
            workers.whenComplete((ignored,error)->{stage(error);signal(1);});
            rows.whenComplete((ignored,error)->{stage(error);signal(2);});
            if(requested||closed)close();
        }
        void opened(){signal(8);}
        void unopened(){signal(1|2|4);}
        void stage(Throwable error){
            if(error==null)return;
            while(error instanceof java.util.concurrent.CompletionException&&error.getCause()!=null)error=error.getCause();
            if(requested&&(error instanceof java.util.concurrent.CancellationException||error instanceof TapstateException coded
                    &&(coded.code()==ValidationError.READ_CANCELLED||coded.code()==ValidationError.EXECUTION_CANCELLED)))return;
            record(error);
        }
        void signal(int bits){settled.getAndUpdate(value->value|bits);finish();}
        void close(){
            requested=true;var action=closer;if(action==null||!closing.compareAndSet(false,true))return;
            try{action.run();settled.getAndUpdate(value->value|4);}
            catch(RuntimeException|Error cleanup){record(cleanup);throw cleanup;}
            finally{closing.set(false);finish();}
        }
        void finish(){
            if(settled.get()!=15||closing.get()||!released.compareAndSet(false,true))return;
            try{lease.close();synchronized(reads){reads.remove(this);}}
            catch(RuntimeException|Error failed){released.set(false);record(failed);throw failed;}
            releaseWorking();
        }
    }
    private void awaitClosed(Runnable guard){
        while(true){
            require();guard.run();boolean waiting;
            synchronized(reads){waiting=reads.stream().anyMatch(read->read.requested);}
            if(!waiting)return;
            java.util.concurrent.locks.LockSupport.parkNanos(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(1));
        }
    }
    @Override public void close(){
        closed=true;var cleanup=new ValidationFailureLatch();var merge=active.getAndSet(null);
        if(merge!=null)try{merge.close();}catch(RuntimeException|Error failed){cleanup.record(failed);record(failed);}
        List<Read> closing;synchronized(reads){closing=List.copyOf(reads);}
        for(var read:closing)try{read.close();}catch(RuntimeException|Error failed){cleanup.record(failed);record(failed);}
        releaseWorking();
        if(cleanup.get() instanceof Error fatal)throw fatal;if(cleanup.get() instanceof RuntimeException failed)throw failed;
    }
    @Override public boolean drained(){
        releaseWorking();synchronized(reads){return closed&&consumers.get()==0&&reads.isEmpty()&&working.get()==null;}
    }
    private void releaseWorking(){
        synchronized(reads){if(!closed||consumers.get()!=0||!reads.isEmpty())return;}
        var lease=working.getAndSet(null);if(lease!=null)lease.close();
        saved=null;declarations=List.of();artifacts=Map.of();current=null;
    }
    private boolean full(){return original.spec().method()==ValidationMethod.FULL_COLUMN;}
    private SourceResource resource(ValidationTaskSpec.Table table){return StoredArtifacts.requireSource(saved.artifacts(),table.sourceId());}
    private ConnectorRef artifact(SourceResource resource){var found=artifacts.get(resource.id());if(found==null)throw invalid("artifactScope");return found;}
    private static ConnectionConfig connection(ValidationTaskSpec.Table table,SourceResource resource){
        var config=new ConnectionConfig(resource.id(),resource.connector(),resource.config());
        if(table.database()!=null&&!table.database().equals(config.settings().get("database"))
                ||table.schema()!=null&&!table.schema().equals(config.settings().get("schema")))throw invalid("physicalNamespace");
        return config;
    }
    private static PdkValidationReads.Binding binding(ValidationTaskSpec.Table table){return new PdkValidationReads.Binding(table.modelVersion(),table.codecBinding());}
    private static List<ValidationKeyBinding.Component> components(ValidationTaskSpec.Mapping mapping,boolean source){return source?mapping.keys().source():mapping.keys().target();}
    private static ValidationKeyBinding keyBinding(ValidationTaskSpec.Mapping mapping,boolean source){
        return new ValidationKeyBinding(source?"direct-source-key":"direct-target-key",components(mapping,source),Set.of(),ROW);
    }
    private static ValidationKey bound(ValidationKeyBinding binding,ValidationValues.Row row){
        var result=binding.read(row);if(result instanceof ValidationKeyBinding.Bound bound)return bound.key();
        throw new TapstateException(ValidationError.INCOMPATIBLE_KEY,Map.of("component",((ValidationKeyBinding.Invalid)result).problems().getFirst().component()),null);
    }
    private static ValidationKey matching(ValidationKeyBinding binding,ValidationValues.Row row,ValidationObservation observation){
        var key=bound(binding,row);if(!key.equals(observation.root().rootKey()))throw invalid("rootKeyScope");return key;
    }
    private static ValidationPhysicalIdentity physical(ValidationTaskSpec.Table table,ValidationKeyBinding binding,ValidationKey key){
        return new ValidationPhysicalIdentity(table.sourceId(),table.database(),table.schema(),table.table(),
                binding.components().stream().map(ValidationKeyBinding.Component::path).toList(),key);
    }
    private static void requireStaticScope(ValidationSavedDeclarations saved,ValidationDirectMappings.Declared declared){
        requireStaticScope(saved.mapping(declared.mapping().mappingId()),declared);
    }
    static void requireStaticScope(io.tapstate.core.dsl.ValidationDeclarations.Inspection inspection,ValidationDirectMappings.Declared declared){
        var mapping=declared.mapping();
        // This proves saved selection intent only; native existence and models are still independent reads.
        ValidationDirectMappings.requireSourceTable(declared,new SourceTable(mapping.source().table(),List.of(),List.of(),List.of()));
        if(inspection.routes().stream().noneMatch(route->route.table()==null))return;
        if(declared.source().tables()==null||declared.source().tables().stream().anyMatch(selector->selector instanceof TableRef.Regex))
            throw unsupported("from","direct.table-scope-unknown");
        for(var selector:declared.source().tables()){
            String name=selector instanceof TableRef.Literal literal?literal.name():((TableRef.Spec)selector).name();
            ValidationDirectMappings.requireObservedSourceTable(declared,name);
        }
    }
    private void require(){throwFailure();if(closed)throw cancelled();original.requireReserved();if(closed)throw cancelled();}
    private TapstateException cancelled(){return new TapstateException(ValidationError.EXECUTION_CANCELLED,Map.of("node",nodeId),null);}
    private void record(Throwable error){failure.record(error);ValidationFailureLatch.addContext(failure.get(),error);}
    private void throwFailure(){if(failure.get() instanceof Error fatal)throw fatal;if(failure.get() instanceof RuntimeException failed)throw failed;}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","directRows."+field),null);}
    private static TapstateException unsupported(String field,String rule){return unsupported("direct-full",field,rule);}
    private static TapstateException unsupported(String node,String field,String rule){return new TapstateException(ValidationError.RULE_UNSUPPORTED,
            Map.of("node",node,"field",field,"rule",rule),null);}
}
