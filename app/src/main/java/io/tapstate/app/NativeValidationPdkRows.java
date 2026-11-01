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

/** Serial direct-table ordered pairs, checked and published under the original owner. */
final class NativeValidationPdkRows implements NativeValidationFullJob.Plan {
    private static final System.Logger LOG=System.getLogger(NativeValidationPdkRows.class.getName());
    private static final ValidationValues.Limits ROW=new ValidationValues.Limits(64L*1024,8192,32);
    static final long POSTGRES_RESULT_BYTES=PdkValidationReads.POSTGRES_RESULT_BYTES;
    private static final long METADATA_BYTES=16L*1024*1024+PdkValidationReads.decodingWorkBytes(ROW)+2*ROW.bytes();
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
    // Publishes read metadata or cleanup handles; atomic close and exit receipts own their lifetime.
    @SuppressWarnings("java:S3077")
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
                for(var key:List.of(from,to))if(key.domain()==ValidationKey.Kind.TEXT&&!key.logicalTypes().equals(Set.of("object-id")))
                    throw unsupported("keys",String.join(".",key.path()),"native-text-ordering-unproven");
                if((from.logicalTypes().contains("object-id")||to.logicalTypes().contains("object-id"))
                        &&(!from.logicalTypes().equals(Set.of("object-id"))||!to.logicalTypes().equals(Set.of("object-id"))
                            ||from.domain()!=ValidationKey.Kind.TEXT||to.domain()!=ValidationKey.Kind.TEXT))
                    throw unsupported("keys","native-object-id-domain-mismatch");
            }
            mappings.add(ValidationFullScan.Mapping.paired(declared.mapping().mappingId(),guard->open(selected,guard),
                    (observation,pair,guard)->check(selected,observation,pair,guard)));
        }
        this.mappings=List.copyOf(mappings);
        long pairedBytes=0;
        for(var declared:declarations){
            var mapping=declared.mapping();
            pairedBytes=Math.max(pairedBytes,Math.addExact(rowReadBytes(artifacts.get(mapping.source().sourceId()),0),rowReadBytes(artifacts.get(mapping.target().sourceId()),0)));
        }
        // Admit driver/raw/UTF-16 and metadata windows before discovery. Business map allocation
        // is admitted from both verified models below, before either business query can start.
        growPaired(pairedBytes);
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
            var mapping=declarations.get(index).mapping();
            growPaired(Math.addExact(rowReadBytes(artifacts.get(mapping.source().sourceId()),from.model().fields().size()),
                    rowReadBytes(artifacts.get(mapping.target().sourceId()),to.model().fields().size())));
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
        var read=admit(METADATA_BYTES);PdkValidationReads.MetadataRead handle=null;
        try{
            var resource=resource(side);var connection=connection(side,resource);
            handle=PdkValidationReads.inspect(connection,artifact(resource),CanonicalHash.of(resource),side.table(),
                    full()?List.of():fields,()->{require();guard.run();},original.resources().queryExecutor(),
                    original.resources().decodeExecutor(),ROW);
            read.attach(handle::close,handle.workersExited(),handle.rowsReleased(),handle.cleanupCompleted());read.opened();
            require();guard.run();var observed=handle.result();
            var expected=binding(side);
            if(!observed.binding().equals(expected)){
                // Keep diagnostics bounded to identities/digests; never emit configuration or row values.
                var resolved=artifact(resource);
                LOG.log(System.Logger.Level.WARNING,io.tapstate.core.common.JsonWriter.write(Map.ofEntries(
                        Map.entry("code",ValidationError.INVALID_CONTRACT.code()),Map.entry("event","validation-binding-mismatch"),
                        Map.entry("mapping",declared.mapping().mappingId()),Map.entry("side",source?"source":"destination"),Map.entry("table",side.table()),
                        Map.entry("expectedModel",expected.modelVersion()),Map.entry("observedModel",observed.binding().modelVersion()),
                        Map.entry("expectedCodec",expected.codecBinding()),Map.entry("observedCodec",observed.binding().codecBinding()),
                        Map.entry("artifact",resolved.contentHash()),Map.entry("configuration",CanonicalHash.of(resource)),
                        Map.entry("specification",resolved.spec()==null?"absent":CanonicalHash.ofText(resolved.spec())))));
                throw invalid("savedModelChanged");
            }
            if(source)ValidationDirectMappings.requireSourceTable(declared,observed.model());
            for(var field:observed.model().fields()){
                if(field.type()==io.tapstate.core.common.TapstateType.UNKNOWN)throw unsupported(side.nodeId(),side.table()+"/"+field.name(),"field-model-unknown");
                if(PdkValidationReads.boundedPostgresPages(artifact(resource))&&(field.type()==io.tapstate.core.common.TapstateType.ARRAY||field.type()==io.tapstate.core.common.TapstateType.MAP))
                    throw unsupported(side.nodeId(),side.table()+"/"+field.name(),"native-nested-codec-expansion-unproven");
            }
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
        return new ValidationOrderedGroups(UUID.randomUUID().toString(),reader(index,source,guard),row->{
            require();guard.run();var key=bound(keys,row);
            return new ValidationOrderedGroups.Key(key,physical(side,keys,key));
        },ROW.bytes(),clock);
    }

    private ValidationReadHandle reader(int index,boolean source,Runnable guard){
        require();guard.run();awaitClosed(guard);
        var mapping=declarations.get(index).mapping();var side=source?mapping.source():mapping.target();
        var keys=components(mapping,source);var resource=resource(side);var artifact=artifact(resource);
        var table=current;if(table==null||table.index()!=index)throw invalid("tableSequence");
        int nativeFields=(source?table.source():table.target()).model().fields().size();
        var read=admit(rowReadBytes(artifact,nativeFields));ValidationReadHandle raw=null;
        try{
            var connection=connection(side,resource);
            var query=original.resources().queryExecutor();var decode=original.resources().decodeExecutor();
            Runnable active=()->{require();guard.run();};
            raw=PdkValidationReads.scan(connection,artifact,CanonicalHash.of(resource),side.table(),binding(side),keys,full(),active,query,decode,2,ROW,
                    postgres(artifact)?nativeFields:PdkValidationReads.POSTGRES_NATIVE_FIELDS);
            var handle=raw;read.attach(handle::close,handle.workersExited(),handle.rowsReleased(),PdkValidationReads.cleanupCompleted(handle));read.opened();require();guard.run();
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

    private ValidationRootCheck check(int index,ValidationObservation observation,ValidationOrderedMerge.Pair ordered,Runnable guard){
        consumers.incrementAndGet();
        try{
            require();guard.run();var table=current;if(table==null||table.index()!=index)throw invalid("tableSequence");
            var mapping=declarations.get(index).mapping();
            var generated=ValidationFieldCheck.generatedId(observation.run(),mapping.mappingId(),mapping.outputId(),
                    artifacts.get(mapping.target().sourceId()).className().equals("io.tapdata.mongodb.MongodbConnector"),
                    table.source().model().fields().stream().anyMatch(field->field.name().equals("_id")),
                    java.util.stream.Stream.concat(mapping.keys().source().stream(),mapping.keys().target().stream())
                            .anyMatch(component->component.path().getFirst().equals("_id")),
                    value->value instanceof ValidationValues.Typed typed&&typed.logicalType().equals("object-id")&&typed.value() instanceof String);
            return ValidationRootReader.readOrderedDirect(observation,original.spec().method(),ordered,
                    new ValidationRootCheck.Limits(256L*1024,256,256,8,2),()->{require();guard.run();},(pair,result)->{
                        if(full())ValidationFieldCheck.check(observation,List.of(),pair.source().working(),pair.target().working(),
                                pair.sourceSample(),pair.targetSample(),generated,String::equals,
                                new ValidationValueComparison.TraversalLimits(ROW,100000),result);
                    });
        }catch(RuntimeException|Error failed){record(failed);throw failed;}
        finally{consumers.decrementAndGet();releaseWorking();}
    }

    private void growPaired(long pairedBytes){
        var envelope=original.resources().budget().envelope();long needed=Math.addExact(16L*1024*1024,pairedBytes);
        if(needed>envelope.bytes())original.resources().growTo(new ValidationLocalBudget.Amount(needed,envelope.workers(),envelope.connections(),envelope.reads()),1);
    }
    static long rowReadBytes(ConnectorRef artifact){return rowReadBytes(artifact,PdkValidationReads.POSTGRES_NATIVE_FIELDS);}
    static long rowReadBytes(ConnectorRef artifact,int nativeFields){
        Objects.requireNonNull(artifact);
        if(nativeFields<0||nativeFields>PdkValidationReads.POSTGRES_NATIVE_FIELDS)throw new IllegalArgumentException("invalid native field bound");
        // Native pages bound map cardinality even before decoding rejects an oversized row. The
        // driver caps each query's raw payload; charge its UTF-16 expansion separately. Map entries
        // include field/key/scalar overhead for the admitted model, independently of row validity.
        long mapBytes=Math.multiplyExact((long)PdkValidationReads.POSTGRES_PAGE_ROWS,Math.multiplyExact((long)nativeFields,256L));
        long nativeBytes=postgres(artifact)?Math.addExact(Math.multiplyExact(3L,POSTGRES_RESULT_BYTES),mapBytes):0L;
        return Math.addExact(METADATA_BYTES,nativeBytes);
    }
    private static boolean postgres(ConnectorRef artifact){return PdkValidationReads.boundedPostgresPages(artifact);}
    static ConnectionConfig rowConnection(SourceResource resource,ConnectorRef artifact){
        return PdkValidationReads.boundedConnection(new ConnectionConfig(resource.id(),resource.connector(),resource.config()),artifact);
    }

    private Read admit(long bytes){
        var lease=original.resources().budget().reserve(new ValidationLocalBudget.Amount(bytes,2,1,1));
        var read=new Read(lease);
        synchronized(reads){reads.add(read);}
        return read;
    }
    private final class Read {
        private final ValidationLocalBudget.Reservation lease;
        private final AtomicInteger settled=new AtomicInteger();
        private final AtomicBoolean closing=new AtomicBoolean(),released=new AtomicBoolean();
        // Publishes read metadata or cleanup handles; atomic close and exit receipts own their lifetime.
        @SuppressWarnings("java:S3077")
        private volatile Runnable closer;
        private volatile boolean requested;
        Read(ValidationLocalBudget.Reservation lease){this.lease=lease;}
        void attach(Runnable closer,CompletionStage<Void> workers,CompletionStage<Void> rows,CompletionStage<Void> cleanup){
            this.closer=closer;
            workers.whenComplete((ignored,error)->{stage(error);signal(1);});
            rows.whenComplete((ignored,error)->{stage(error);signal(2);});
            cleanup.whenComplete((ignored,error)->{stage(error);if(error==null)signal(16);});
            if(requested||closed)close();
        }
        void opened(){signal(8);}
        void unopened(){signal(1|2|4|16);}
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
            if(settled.get()!=31||closing.get()||!released.compareAndSet(false,true))return;
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
    private ConnectionConfig connection(ValidationTaskSpec.Table table,SourceResource resource){
        var config=rowConnection(resource,artifact(resource));
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
