package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.ValidationReadHandle;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Portable access to independent utility-query sessions. The caller pins the actual saved
 * configuration hash and artifact, admits native ordering and allocation, and retains its budget
 * until close plus both lifetime stages complete. No production context or mutable PDK model escapes.
 */
public final class PdkValidationReads {
    public record Binding(String modelVersion, String codecBinding) {
        public Binding {
            Objects.requireNonNull(modelVersion); Objects.requireNonNull(codecBinding);
        }
    }
    public record Metadata(SourceTable model, Binding binding) {
        public Metadata { Objects.requireNonNull(model); Objects.requireNonNull(binding); }
    }
    public static final class MetadataRead implements AutoCloseable {
        private final ValidationReadHandle reader;
        private final AtomicReference<Metadata> result;
        private MetadataRead(ValidationReadHandle reader, AtomicReference<Metadata> result) {
            this.reader=reader; this.result=result;
        }
        /** Blocking, single-consumer completion of metadata discovery; it never reads business rows. */
        public Metadata result() {
            var unexpected=reader.next();
            if(unexpected.isPresent()) {
                unexpected.orElseThrow().close();
                throw new IllegalStateException("metadata reader returned a business row");
            }
            return Objects.requireNonNull(result.get(),"completed independent metadata");
        }
        @Override public void close(){reader.close();}
        public CompletionStage<Void> workersExited(){return reader.workersExited();}
        public CompletionStage<Void> rowsReleased(){return reader.rowsReleased();}
    }

    private PdkValidationReads(){}

    /** Projection is empty for the complete model, or the exact confirmed keys for key-only reads. */
    public static MetadataRead inspect(ConnectionConfig connection, ConnectorRef artifact, String configurationHash,
            String table, List<String> projection, Runnable requireCurrent, ExecutorService query,
            ExecutorService decode, ValidationValues.Limits rowLimits) {
        inputs(connection,artifact,configurationHash);
        var result=new AtomicReference<Metadata>();
        var reader=deferred(table,query,decode,submit->PdkValidationSession.inspect(connection.connectorId(),artifact,connection.settings(),table,projection,model->{
            String version=PdkValidationModelVersion.of(model);
            var structural=PdkSchemaDiscoverer.structuralModel(List.of(model)).tables().getFirst();
            if(!table.equals(structural.name()))throw invalid("metadata.tableScope");
            var described=new Metadata(structural,new Binding(version,
                    PdkValidationModelVersion.codecBinding(artifact,configurationHash,version)));
            if(!result.compareAndSet(null,described))throw invalid("metadata.repeatedModel");
        },requireCurrent,submit,decode,rowLimits));
        return new MetadataRead(reader,result);
    }

    /**
     * Construction submits no work; first next starts after the caller owns the lifetime handle.
     * The independent native scan must return a complete ascending stream, without offset or limit.
     */
    public static ValidationReadHandle scan(ConnectionConfig connection, ConnectorRef artifact, String configurationHash,
            String table, Binding expected, List<ValidationKeyBinding.Component> keys, boolean fullColumns,
            Runnable requireCurrent, ExecutorService query, ExecutorService decode, int capacity,
            ValidationValues.Limits rowLimits) {
        inputs(connection,artifact,configurationHash); var confirmed=List.copyOf(keys); var fields=fields(confirmed);
        return deferred(table,query,decode,submit->PdkValidationSession.scan(connection.connectorId(),artifact,connection.settings(),expected.codecBinding(),
                new PdkValidationSession.Scan(table,fields,fullColumns?List.of():fields,Math.min(capacity,64)),
                (model,codecs)->decoder(model,codecs,artifact,configurationHash,expected,confirmed,rowLimits),
                requireCurrent,submit,decode,capacity,rowLimits));
    }

    /**
     * Independently reads the complete table and selects this logical key after its own codecs.
     * No generic inverse from logical values to native predicates is assumed. Each root pays for a
     * complete reread, including nonmatching rows and the tail; the caller must admit that workload.
     */
    public static ValidationReadHandle lookup(ConnectionConfig connection, ConnectorRef artifact, String configurationHash,
            String table, Binding expected, List<ValidationKeyBinding.Component> keys, ValidationKey logicalKey,
            boolean fullColumns, Runnable requireCurrent, ExecutorService query, ExecutorService decode, int capacity,
            ValidationValues.Limits rowLimits) {
        inputs(connection,artifact,configurationHash); var confirmed=List.copyOf(keys);
        var binding=new ValidationKeyBinding("pdk-logical-lookup",confirmed,Set.of(),rowLimits);
        Objects.requireNonNull(logicalKey);
        if(confirmed.size()!=logicalKey.parts().size())throw invalid("lookupKeyArity");
        for(int index=0;index<confirmed.size();index++)
            if(confirmed.get(index).domain()!=logicalKey.parts().get(index).kind())throw invalid("lookupKeyDomain");
        var reader=scan(connection,artifact,configurationHash,table,expected,confirmed,fullColumns,
                requireCurrent,query,decode,capacity,rowLimits);
        return new ValidationReadHandle(){
            private final ValidationFailureLatch failure=new ValidationFailureLatch();
            private final AtomicReference<Row> failedRelease=new AtomicReference<>();
            private long delivered;
            @Override public Optional<Row> next(){
                throwFailure();Row held=null;
                try{
                    while(true){
                        requireCurrent.run(); var next=reader.next();
                        if(next.isEmpty()){requireCurrent.run();return next;}
                        held=next.orElseThrow(); var row=held;
                        var actual=binding.read(row.values());
                        if(actual instanceof ValidationKeyBinding.Invalid invalid)
                            throw new TapstateException(ValidationError.INCOMPATIBLE_KEY,
                                    Map.of("component",invalid.problems().getFirst().component()),null);
                        if(logicalKey.equals(((ValidationKeyBinding.Bound)actual).key())){
                            requireCurrent.run();
                            if(delivered==Long.MAX_VALUE)throw invalid("lookupOrdinal");
                            long ordinal=delivered++;
                            var result=Optional.<Row>of(new Row(){
                                @Override public long ordinal(){return ordinal;}
                                @Override public ValidationValues.Row values(){return row.values();}
                                @Override public void close(){row.close();}
                            });
                            held=null;return result;
                        }
                        held=null;
                        try{row.close();}catch(RuntimeException|Error cleanup){failedRelease.set(row);throw cleanup;}
                    }
                }catch(RuntimeException|Error primary){
                    record(primary);
                    if(held!=null)try{held.close();}catch(RuntimeException|Error cleanup){failedRelease.set(held);record(cleanup);}
                    try{reader.close();}catch(RuntimeException|Error cleanup){record(cleanup);}
                    throwFailure();throw new IllegalStateException("failed lookup has no diagnosis");
                }
            }
            @Override public void close(){
                Throwable cleanupFailure=null;
                var pending=failedRelease.get();
                if(pending!=null)try{pending.close();failedRelease.compareAndSet(pending,null);}
                catch(RuntimeException|Error cleanup){record(cleanup);cleanupFailure=cleanup;}
                try{reader.close();}catch(RuntimeException|Error cleanup){record(cleanup);cleanupFailure=cleanup;}
                if(cleanupFailure!=null)throwFailure();
            }
            @Override public CompletionStage<Void> workersExited(){return reader.workersExited();}
            @Override public CompletionStage<Void> rowsReleased(){return reader.rowsReleased();}
            private void record(Throwable reported){failure.record(reported);ValidationFailureLatch.addContext(failure.get(),reported);}
            private void throwFailure(){if(failure.get() instanceof Error fatal)throw fatal;if(failure.get() instanceof RuntimeException failed)throw failed;}
        };
    }

    public static long decodingWorkBytes(ValidationValues.Limits rowLimits){return PdkValidationRows.workReservationBytes(rowLimits);}

    private static PdkValidationCursor.Decoder decoder(TapTable model, PdkValidationCodecs codecs,
            ConnectorRef artifact, String configurationHash, Binding expected,
            List<ValidationKeyBinding.Component> keys, ValidationValues.Limits rowLimits) {
        requireModel(model,artifact,configurationHash,expected);
        for(var component:keys){
            String field=component.path().getFirst();var actual=model.getNameFieldMap().get(field);
            if(actual==null||actual.getTapType()==null||actual.getTapType().getClass()==TapRaw.class)
                throw unsupported(field,"key-model-unknown");
        }
        var decoder=new PdkValidationRows(model,codecs,path->PdkValidationRows.Rule.AUTO,rowLimits);
        return raw->{
            requireModel(model,artifact,configurationHash,expected);
            var row=decoder.decode(raw);
            requireModel(model,artifact,configurationHash,expected);
            return row;
        };
    }

    private static void requireModel(TapTable model, ConnectorRef artifact, String configurationHash, Binding expected){
        String actual=PdkValidationModelVersion.of(model);
        if(!actual.equals(expected.modelVersion())||!PdkValidationModelVersion.codecBinding(artifact,configurationHash,actual)
                .equals(expected.codecBinding()))throw invalid("modelOrCodecChanged");
    }

    private static List<String> fields(List<ValidationKeyBinding.Component> keys){
        Objects.requireNonNull(keys);
        if(keys.isEmpty()||keys.size()>32)throw invalid("keys");
        var fields=new ArrayList<String>();
        for(var component:keys){
            if(component.path().size()!=1||!component.path().getFirst().matches("[A-Za-z_][A-Za-z0-9_]*"))
                throw unsupported("key","native-field-syntax");
            fields.add(component.path().getFirst());
        }
        if(fields.stream().distinct().count()!=fields.size())throw invalid("duplicateKeys");
        return List.copyOf(fields);
    }

    private static void inputs(ConnectionConfig connection,ConnectorRef artifact,String configurationHash){
        Objects.requireNonNull(connection);Objects.requireNonNull(artifact);
        if(artifact.contentHash()==null||!artifact.contentHash().matches("[a-f0-9]{64}")
                ||configurationHash==null||!configurationHash.matches("[a-f0-9]{64}"))throw invalid("artifactOrConfigurationIdentity");
    }

    /** Handoff precedes submission, including an executor that starts work and then throws. */
    private static ValidationReadHandle deferred(String table,ExecutorService query,ExecutorService decode,
            Function<ExecutorService,ValidationReadHandle> construct){
        if(query==decode)throw new IllegalArgumentException("query and decoding require distinct executors");
        Objects.requireNonNull(decode);
        var gate=new DeferredSubmission(query);
        var reader=construct.apply(gate);
        return new ValidationReadHandle(){
            private final ValidationFailureLatch failure=new ValidationFailureLatch();
            @Override public Optional<Row> next(){
                throwFailure();
                try{gate.start();return reader.next();}
                catch(RuntimeException|Error reported){
                    record(reported instanceof RejectedExecutionException
                            ?new TapstateException(ValidationError.READ_FAILED,Map.of("table",table),reported):reported);
                    try{reader.close();}catch(RuntimeException|Error cleanup){record(cleanup);}
                    throwFailure();throw new IllegalStateException("failed submission has no diagnosis");
                }
            }
            @Override public void close(){
                gate.cancel();
                try{reader.close();}catch(RuntimeException|Error cleanup){record(cleanup);throwFailure();}
            }
            @Override public CompletionStage<Void> workersExited(){return reader.workersExited();}
            @Override public CompletionStage<Void> rowsReleased(){return reader.rowsReleased();}
            private void record(Throwable reported){failure.record(reported);ValidationFailureLatch.addContext(failure.get(),reported);}
            private void throwFailure(){if(failure.get() instanceof Error fatal)throw fatal;if(failure.get() instanceof RuntimeException failed)throw failed;}
        };
    }

    private static final class DeferredSubmission extends AbstractExecutorService {
        private final ExecutorService actual;
        private Runnable pending;
        private boolean started,closed;
        DeferredSubmission(ExecutorService actual){this.actual=Objects.requireNonNull(actual);}
        @Override public synchronized void execute(Runnable command){
            if(pending!=null||started||closed)throw new IllegalStateException("independent read submitted more than once");
            pending=Objects.requireNonNull(command);
        }
        void start(){
            Runnable command;
            synchronized(this){
                if(started||closed)return;
                started=true;command=Objects.requireNonNull(pending,"prepared independent submission");pending=null;
            }
            actual.execute(command);
        }
        synchronized void cancel(){closed=true;pending=null;}
        // This view never owns or shuts down the admitted executor.
        @Override public void shutdown(){throw new UnsupportedOperationException();}
        @Override public List<Runnable> shutdownNow(){throw new UnsupportedOperationException();}
        @Override public synchronized boolean isShutdown(){return closed;}
        @Override public boolean isTerminated(){return false;}
        @Override public boolean awaitTermination(long timeout,TimeUnit unit){throw new UnsupportedOperationException();}
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","pdkRead."+field),null);}
    private static TapstateException unsupported(String field,String rule){return new TapstateException(ValidationError.RULE_UNSUPPORTED,
            Map.of("node","independent-pdk-reader","field",field,"rule",rule),null);}
}
