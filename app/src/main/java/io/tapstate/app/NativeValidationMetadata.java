package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Pre-owner independent metadata over selected CONFIG declarations; no task, count result or Job is created. */
final class NativeValidationMetadata {
    private static final ValidationValues.Limits ROW=new ValidationValues.Limits(64*1024,8192,32);
    record Projection(List<String> source,List<String> target){
        Projection{source=fields(source);target=fields(target);if(source.size()!=target.size())throw invalid("keyArity");}
        private static List<String> fields(List<String> fields){
            if(fields==null||fields.size()>ValidationKey.MAX_COMPONENTS)throw invalid("keyFields");
            var unique=new HashSet<String>();for(var field:fields){if(field==null||!field.matches("[A-Za-z_][A-Za-z0-9_]*")||field.length()>512||!unique.add(field))throw invalid("keyField");}
            return List.copyOf(fields);
        }
    }
    record Side(PdkValidationReads.Metadata model,PdkValidationCounts.Operation countOperation,String artifactHash){
        Side{if((model==null)==(countOperation==null)||artifactHash==null||!artifactHash.matches("[a-f0-9]{64}"))throw invalid("side");}
    }
    record Mapping(String id,Side source,Side target){}
    private final MongoConfigurationDomain configuration;
    private final ValidationMemberResources.Member member;
    private final ConnectorProvisioner provisioner;
    private final Clock clock;
    NativeValidationMetadata(MongoConfigurationDomain configuration,ValidationMemberResources.Member member,ConnectorProvisioner provisioner,Clock clock){
        this.configuration=Objects.requireNonNull(configuration);this.member=Objects.requireNonNull(member);
        this.provisioner=Objects.requireNonNull(provisioner);this.clock=Objects.requireNonNull(clock);
    }
    Scope inspect(ValidationTaskStore.Selection selection,List<ValidationSelectedDeclarations.Choice> choices,
            ValidationMethod method,Map<String,Projection> projections,Instant deadline){
        Objects.requireNonNull(selection);Objects.requireNonNull(method);Objects.requireNonNull(projections);
        if(deadline==null||!deadline.isAfter(clock.instant())||deadline.isAfter(clock.instant().plusSeconds(60)))throw invalid("deadline");
        if(!method.supports(ValidationMode.FULL)||method==ValidationMethod.COUNT&&!projections.isEmpty()||projections.size()>ValidationTaskSpec.MAX_MAPPINGS)throw invalid("method");
        var scope=new Scope(member.admitMetadata(),deadline,method);
        try{
            scope.require();
            for(var entry:projections.entrySet())if(entry.getKey()==null||entry.getValue()==null)throw invalid("projectionMapping");
            projections=Map.copyOf(projections);scope.projections=projections;
            var inputs=configuration.validationTasks(scope::require).declarations(selection);
            scope.selected=ValidationSelectedDeclarations.inspect(inputs,choices);
            var known=new HashSet<String>();for(var mapping:scope.selected.mappings())known.add(mapping.mapping().mappingId());
            if(!known.containsAll(projections.keySet()))throw invalid("projectionMapping");
            for(var declared:scope.selected.mappings()){
                scope.require();
                if(method!=ValidationMethod.COUNT)NativeValidationPdkRows.requireStaticScope(scope.selected.output(declared.mapping().outputId()),declared);
                var projection=projections.get(declared.mapping().mappingId());
                var source=scope.read(declared,true,projection==null?List.of():projection.source());
                var target=scope.read(declared,false,projection==null?List.of():projection.target());
                scope.results.add(new Mapping(declared.mapping().mappingId(),source,target));
            }
            scope.require();return scope;
        }catch(RuntimeException|Error failed){scope.record(failed);scope.close();throw failed;}
    }

    /** Borrowed definitions/models stay owned here until the consuming operation explicitly closes the scope. */
    final class Scope implements AutoCloseable {
        private final ValidationMemberResources.Metadata frame;private final Instant deadline;private final ValidationMethod method;
        private final Set<ValidationMemberResources.Probe> active=new HashSet<>();
        private final ValidationFailureLatch failure=new ValidationFailureLatch();private final CompletableFuture<Void> released=new CompletableFuture<>();
        private ValidationSelectedDeclarations selected;
        private Map<String,Projection> projections=Map.of();
        private final Map<String,ConnectorRef> artifacts=new HashMap<>();private final List<Mapping> results=new ArrayList<>();
        private long artifactBytes,modelBytes;private boolean closed,returned;
        private Scope(ValidationMemberResources.Metadata frame,Instant deadline,ValidationMethod method){this.frame=frame;this.deadline=deadline;this.method=method;}
        void require(){
            frame.requireRetained(member);synchronized(this){if(closed)throw invalid("scopeClosed");}
            throwFailure();if(!clock.instant().isBefore(deadline))throw new TapstateException(ValidationError.READ_CANCELLED,Map.of("table","metadata"),null);
        }
        ValidationSelectedDeclarations declarations(){require();return selected;}
        ValidationMethod method(){require();return method;}
        Projection projection(String mapping){require();return projections.get(mapping);}
        boolean nativeBson(String source){
            require();var resource=StoredArtifacts.requireSource(selected.artifacts(),source);var ref=artifacts.get(resource.connector());
            if(ref==null)throw invalid("unobservedArtifact");return ref.className().equals("io.tapdata.mongodb.MongodbConnector");
        }
        List<Mapping> mappings(){require();return List.copyOf(results);}
        CompletionStage<Void> released(){return released.minimalCompletionStage();}
        private ConnectorRef artifact(SourceResource source){
            require();var found=artifacts.get(source.connector());if(found!=null)return found;
            var current=Objects.requireNonNull(provisioner.resolve(source.connector()),"connector reference");
            if(current.contentHash()==null)throw invalid("artifactIdentity");
            var retained=Objects.requireNonNull(provisioner.resolve(source.connector(),current.contentHash()),"retained connector");
            if(!current.contentHash().equals(retained.contentHash()))throw invalid("artifactIdentity");
            artifactBytes+=512L+2L*retained.className().length()+(retained.spec()==null?0:2L*retained.spec().length());
            for(var path:retained.classpath())artifactBytes+=128L+2L*path.toString().length();
            if(artifactBytes>4L*1024*1024)throw limit("connectorMetadata");
            require();artifacts.put(source.connector(),retained);return retained;
        }
        private Side read(ValidationDirectMappings.Declared declared,boolean source,List<String> fields){
            require();var side=source?declared.mapping().source():declared.mapping().target();
            var resource=StoredArtifacts.requireSource(selected.artifacts(),side.sourceId());var artifact=artifact(resource);
            var connection=new ConnectionConfig(resource.id(),resource.connector(),resource.config());
            var probe=method==ValidationMethod.COUNT?member.admitProbe():member.admitProbeWithConsumer();
            synchronized(this){if(closed){probe.close();throw invalid("scopeClosed");}active.add(probe);}
            probe.released().whenComplete((ignored,failed)->{if(failed!=null)record(failed);synchronized(Scope.this){active.remove(probe);}releaseIfDone();});
            try{
                if(method==ValidationMethod.COUNT){
                    var reader=new PdkValidationCounts(connection,artifact,new PdkValidationCounts.Request(side.table(),side.database(),PdkValidationCounts.Operation.NATIVE_PROTOCOL),
                            ()->{require();probe.require();},name->{if(source)ValidationDirectMappings.requireObservedSourceTable(declared,name);});
                    var drained=new CompletableFuture<Void>();
                    try{probe.retain(()->{try{reader.close();}finally{if(reader.drained())drained.complete(null);}},reader.callExited(),drained);}
                    catch(RuntimeException|Error refused){closeUnsubmitted(reader,refused);throw refused;}
                    var future=probe.queryExecutor().submit(()->{try{return reader.inspect();}finally{if(reader.drained())drained.complete(null);}});
                    var operation=await(future);require();return new Side(null,operation,artifact.contentHash());
                }
                var projection=method==ValidationMethod.FULL_COLUMN?List.<String>of():fields;
                var reader=PdkValidationReads.inspect(connection,artifact,CanonicalHash.of(resource),side.table(),projection,
                        ()->{require();probe.require();},probe.queryExecutor(),probe.decodeExecutor(),ROW);
                try{probe.retain(reader,reader.workersExited(),reader.rowsReleased());}
                catch(RuntimeException|Error refused){closeUnsubmitted(reader,refused);throw refused;}
                var model=await(probe.consumerExecutor().submit(reader::result));require();
                if(source)ValidationDirectMappings.requireSourceTable(declared,model.model());
                if(method==ValidationMethod.FULL_COLUMN)for(var field:model.model().fields())
                    if(field.type()==io.tapstate.core.common.TapstateType.UNKNOWN)throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                            Map.of("node",side.nodeId(),"field",field.name(),"rule","field-model-unknown"),null);
                charge(model.model());return new Side(model,null,artifact.contentHash());
            }catch(RuntimeException|Error failed){record(failed);throw failed;}
            finally{
                probe.close();
                // Returning a model transfers its portable ownership to the still-admitted outer frame.
                // An unresolved native worker keeps both frames charged after cancellation or timeout.
                await(probe.released().toCompletableFuture());
            }
        }
        private <T>T await(Future<T> future){
            try{
                while(true){require();try{return future.get(50,TimeUnit.MILLISECONDS);}catch(TimeoutException waiting){/* Recheck this original deadline and member. */}}
            }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new TapstateException(ValidationError.READ_CANCELLED,Map.of("table","metadata"),interrupted);}
            catch(ExecutionException failed){record(failed.getCause());throwFailure();throw new IllegalStateException("failed metadata lifetime has no diagnosis");}
        }
        private void closeUnsubmitted(AutoCloseable reader,Throwable original){
            // Both constructors are deferred: no native invocation can precede retained ownership.
            record(original);try{reader.close();}catch(Throwable cleanup){record(cleanup);}throwFailure();
        }
        private void charge(SourceTable model){
            long bytes=modelBytes(model);if(bytes>4L*1024*1024-modelBytes)throw limit("modelMetadata");modelBytes+=bytes;
        }
        private void record(Throwable reported){
            while(reported instanceof CompletionException&&reported.getCause()!=null)reported=reported.getCause();
            failure.record(reported);
        }
        private void throwFailure(){
            var failed=failure.get();if(failed instanceof Error fatal)throw fatal;if(failed instanceof TapstateException coded)throw coded;
            if(failed!=null)throw new TapstateException(ValidationError.READ_FAILED,Map.of("table","metadata"),failed);
        }
        @Override public void close(){
            List<ValidationMemberResources.Probe> retiring;synchronized(this){if(closed)return;closed=true;retiring=List.copyOf(active);}
            retiring.forEach(ValidationMemberResources.Probe::close);releaseIfDone();
        }
        private void releaseIfDone(){
            boolean release;synchronized(this){release=closed&&!returned&&active.isEmpty();if(release){returned=true;selected=null;projections=Map.of();artifacts.clear();results.clear();}}
            if(release){frame.close();var failed=failure.get();if(failed==null)released.complete(null);else released.completeExceptionally(failed);}
        }
    }
    static long modelBytes(SourceTable model){
        long bytes=256L+2L*model.name().length();
        if(model.fields().size()>4096||model.primaryKey().size()>4096||model.indexes().size()>128)throw limit("modelMetadata");
        for(var field:model.fields()){
            bytes+=256L+2L*field.name().length()+(field.dataType()==null?0:2L*field.dataType().length())+(field.unknownBecause()==null?0:2L*field.unknownBecause().length());
            if(field.numericType()!=null){var numeric=field.numericType();bytes+=512;
                if(numeric.minValue()!=null)bytes+=((long)numeric.minValue().unscaledValue().bitLength()+7)/8;
                if(numeric.maxValue()!=null)bytes+=((long)numeric.maxValue().unscaledValue().bitLength()+7)/8;
            }
            if(field.stringType()!=null)bytes+=256;modelBound(bytes);
        }
        for(var key:model.primaryKey()){bytes+=64L+2L*key.length();modelBound(bytes);}
        for(var index:model.indexes()){
            if(index.fields().size()>4096)throw limit("modelMetadata");bytes+=128L+2L*index.name().length();modelBound(bytes);
            for(var field:index.fields()){bytes+=64L+2L*field.length();modelBound(bytes);}
        }
        modelBound(bytes);return bytes;
    }
    private static void modelBound(long bytes){if(bytes>4L*1024*1024)throw limit("modelMetadata");}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","metadata."+field),null);}
    private static TapstateException limit(String field){return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation."+field),null);}
}
