package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkValidationCounts;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationCountScan;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
import io.tapstate.spi.store.ConnectionConfig;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Independent PDK plans over the original saved closure; no production connector or state is borrowed. */
final class NativeValidationPdkPlans implements NativeValidationFullJob.Factory {
    private static final long COUNT_WORK_BYTES=16L*1024*1024;
    private final ConnectorProvisioner provisioner;
    private final Clock clock;
    NativeValidationPdkPlans(ConnectorProvisioner provisioner,Clock clock){
        this.provisioner=Objects.requireNonNull(provisioner);this.clock=Objects.requireNonNull(clock);
    }
    @Override public NativeValidationFullJob.Plan prepare(NativeValidationOwnerReservations.Reservation original){
        original.requireReserved();var spec=original.spec();
        if(spec.mode()!=ValidationMode.FULL)throw invalid("mode");
        if(spec.method()!=ValidationMethod.COUNT)throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                Map.of("node",spec.task().pipelineId(),"field","method","rule","independent-row-reader-not-admitted"),null);
        var saved=Objects.requireNonNull(original.declarations,"saved declarations");
        if(saved.original().task().settings().full().maxConcurrentTables()!=1)throw invalid("tableConcurrency");
        var declarations=ValidationDirectMappings.declare(saved);var artifacts=new LinkedHashMap<String,ConnectorRef>();
        var resolved=new HashMap<String,ConnectorRef>();long metadataBytes=0;
        for(var mapping:declarations)for(var source:List.of(mapping.source(),mapping.target())){
            if(artifacts.containsKey(source.id()))continue;
            var known=resolved.get(source.connector());
            if(known!=null){artifacts.put(source.id(),known);continue;}
            original.requireReserved();var selected=Objects.requireNonNull(provisioner.resolve(source.connector()),"connector reference");
            if(selected.contentHash()==null)throw invalid("artifactIdentity");
            var retained=Objects.requireNonNull(provisioner.resolve(source.connector(),selected.contentHash()),"retained connector");
            if(!selected.contentHash().equals(retained.contentHash()))throw invalid("artifactIdentity");
            metadataBytes+=512L+2L*retained.className().length()+(retained.spec()==null?0:2L*retained.spec().length());
            for(var path:retained.classpath())metadataBytes+=128L+2L*path.toString().length();
            if(metadataBytes>4L*1024*1024)throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    Map.of("limit","validation.connectorMetadata"),null);
            original.requireReserved();resolved.put(source.connector(),retained);artifacts.put(source.id(),retained);
        }
        original.requireReserved();return new Counts(original,saved,declarations,Map.copyOf(artifacts));
    }

    private final class Counts implements NativeValidationFullJob.Plan {
        private final NativeValidationOwnerReservations.Reservation original;
        private ValidationSavedDeclarations saved;
        private Map<String,ConnectorRef> artifacts;
        private List<ValidationDirectMappings.Declared> declarations;
        private final List<ValidationCountScan.Mapping> mappings;
        private final String nodeId;
        private final Set<Owned> owned=new HashSet<>();
        private final AtomicBoolean reading=new AtomicBoolean();
        private volatile boolean closed;
        Counts(NativeValidationOwnerReservations.Reservation original,ValidationSavedDeclarations saved,
                List<ValidationDirectMappings.Declared> declarations,Map<String,ConnectorRef> artifacts){
            this.original=original;this.saved=saved;this.artifacts=artifacts;this.declarations=declarations;
            nodeId=original.resources().budget().identity().owner().nodeId();
            var compiled=new ArrayList<ValidationCountScan.Mapping>();
            for(int index=0;index<declarations.size();index++){
                int selected=index;var id=declarations.get(index).mapping().mappingId();
                compiled.add(new ValidationCountScan.Mapping(id,guard->read(selected,true,guard),guard->read(selected,false,guard)));
            }
            mappings=List.copyOf(compiled);
        }
        @Override public List<ValidationCountScan.Mapping> counts(){return mappings;}
        private void require(){
            if(closed)throw cancelled();original.requireReserved();if(closed)throw cancelled();
        }
        private ValidationCountScan.Scalar read(int index,boolean source,Runnable guard){
            require();guard.run();
            if(!reading.compareAndSet(false,true))throw invalid("concurrentRead");
            Owned current=null;
            try{
                require();var declared=declarations.get(index);var table=source?declared.mapping().source():declared.mapping().target();
                // Every decode owns its nested settings; plugin defaults cannot alter a later mapping.
                SourceResource resource=StoredArtifacts.requireSource(saved.artifacts(),table.sourceId());
                var connection=new ConnectionConfig(resource.id(),resource.connector(),resource.config());
                var database=database(table,connection);var artifact=artifacts.get(resource.id());
                if(artifact==null)throw invalid("artifactScope");
                // Charge the independent context and its work before any native initialization.
                var lease=original.resources().budget().reserve(new ValidationLocalBudget.Amount(COUNT_WORK_BYTES,0,1,1));
                PdkValidationCounts reader=null;
                try{
                    reader=new PdkValidationCounts(connection,artifact,new PdkValidationCounts.Request(table.table(),database,
                            PdkValidationCounts.Operation.NATIVE_PROTOCOL),()->{require();guard.run();},actual->{
                        if(source)ValidationDirectMappings.requireObservedSourceTable(declared,actual);
                    });
                    current=new Owned(reader,lease);
                    synchronized(owned){owned.add(current);}
                }catch(RuntimeException|Error failed){
                    if(reader!=null)reader.close();lease.close();throw failed;
                }
                require();guard.run();var started=clock.instant();long count=reader.count();
                require();guard.run();
                return new ValidationCountScan.Scalar(table,count,new ValidationCheckEvent.ReadBoundary(
                        UUID.randomUUID().toString(),"pdk-native-count-v1",started,clock.instant()));
            }finally{
                try{if(current!=null)current.releaseIfDrained();}finally{reading.set(false);clearDrainedMetadata();}
            }
        }
        @Override public void close(){
            closed=true;List<Owned> retiring;synchronized(owned){retiring=List.copyOf(owned);}
            var failed=new ValidationFailureLatch();
            for(var value:retiring){
                try{value.reader.close();}catch(RuntimeException|Error cleanup){failed.record(cleanup);ValidationFailureLatch.addContext(failed.get(),cleanup);}
                finally{value.releaseIfDrained();}
            }
            clearDrainedMetadata();
            if(failed.get() instanceof Error fatal)throw fatal;
            if(failed.get() instanceof RuntimeException failure)throw failure;
        }
        @Override public boolean drained(){synchronized(owned){return closed&&!reading.get()&&owned.isEmpty();}}
        private void clearDrainedMetadata(){
            synchronized(owned){
                if(closed&&!reading.get()&&owned.isEmpty()){
                    saved=null;artifacts=Map.of();declarations=List.of();
                }
            }
        }
        private final class Owned {
            final PdkValidationCounts reader;
            final ValidationLocalBudget.Reservation lease;
            Owned(PdkValidationCounts reader,ValidationLocalBudget.Reservation lease){this.reader=reader;this.lease=lease;}
            void releaseIfDrained(){
                if(reader.drained()){
                    synchronized(owned){owned.remove(this);}
                    lease.close();
                }
            }
        }
        private TapstateException cancelled(){return new TapstateException(ValidationError.EXECUTION_CANCELLED,
                Map.of("node",nodeId),null);}
    }
    private static String database(ValidationTaskSpec.Table table,ConnectionConfig connection){
        var configured=connection.settings().get("database");
        if(configured!=null&&!(configured instanceof String))throw invalid("database");
        if(configured instanceof String text&&text.isBlank())throw invalid("database");
        if(table.database()!=null&&!table.database().equals(configured))throw invalid("databaseScope");
        if(table.schema()!=null&&!table.schema().equals(connection.settings().get("schema")))throw invalid("schemaScope");
        return (String)configured;
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","pdkPlan."+field),null);}
}
