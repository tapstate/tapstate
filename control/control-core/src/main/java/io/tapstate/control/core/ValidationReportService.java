package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.util.*;

/** Bounded FULL report reads from CONFIG only; no preparation, native owner lookup or execution occurs here. */
public final class ValidationReportService {
    public record Reference(String pipeline,String task,String generation,String sequence,String run) {}
    public record Window(String startedAt,String finishedAt) {}
    public record Failure(String code,Map<String,String> params,String exceptionType,String message,String technical,boolean fatal) {}
    public record Summary(Reference reference,String method,boolean closed,String terminal,String terminalKnownAt,String endedAt,Window window,
            String rootCount,String retainedBytes,String countedTables,String mismatchedTables) {}
    public record Page<T>(List<T> items,String total,int offset,int limit) {}
    public record Table(String node,String source,String database,String schema,String table,String modelVersion,String codecBinding) {}
    public record Component(List<String> path,String domain,List<String> logicalTypes,String numericPolicy) {}
    public record Keys(String confirmation,List<Component> source,List<Component> target) {}
    public record Mapping(String id,String outputKind,String output,Table source,Table target,Keys keys) {}
    public record Boundary(String readId,String contractVersion,String startedAt,String finishedAt) {}
    public record Count(String mapping,String output,String source,String target,String difference,Boundary sourceRead,Boundary targetRead) {}
    public record Report(Reference reference,String method,String mappingRevision,String rulesVersion,String fingerprint,List<Mapping> mappings,
            boolean closed,String terminal,String terminalKnownAt,String endedAt,Window window,Failure failure,String rootCount,String retainedBytes,
            List<Count> counts,String result,String premise,boolean premiseVerifiedBySystem,String conclusionScope,String physicalDrain) {}
    public record RootSummary(String id,ValidationWireValues.Root root,ValidationWireValues.Value summary) {}
    public record RootDetail(String id,String revision,ValidationWireValues.Root root,ValidationWireValues.Value observation) {}
    public static final class Response<T> implements AutoCloseable {
        private final ValidationMetadataBudget.Lease allocation;private Object owner;private T value;
        private Response(ValidationMetadataBudget.Lease allocation,Object owner,T value){this.allocation=allocation;this.owner=owner;this.value=value;}
        public T value(){allocation.requireRetained(owner);return value;}
        @Override public void close(){value=null;owner=null;allocation.close();}
    }
    private final ValidationReports reports;private final ValidationMetadataBudget budget;
    public ValidationReportService(ValidationReports reports,ValidationMetadataBudget budget){this.reports=Objects.requireNonNull(reports);this.budget=Objects.requireNonNull(budget);}
    public Response<Page<Summary>> history(String pipeline,String task,String offset,String limit){
        name(pipeline);name(task);var page=paging(offset,limit);
        return read(()->{var found=reports.history(pipeline,task,ValidationMode.FULL,page);
            return new Page<>(found.items().stream().map(value->new Summary(reference(value.reference()),value.method().name(),value.closed(),state(value.terminal()),
                    instant(value.terminalKnownAt()),instant(value.endedAt()),window(value.window()),Long.toString(value.rootCount()),Long.toString(value.retainedBytes()),
                    Integer.toString(value.countedTables()),Integer.toString(value.mismatchedTables()))).toList(),Long.toString(found.total()),page.offset(),page.limit());});
    }
    public Response<Report> report(Reference identity){
        var reference=reference(identity);
        return read(()->reports.find(reference).map(value->{
            var spec=value.specification();var comparison=spec.task();
            var counts=value.counts().stream().map(count->new Count(count.mappingId(),count.outputId(),Long.toString(count.sourceCount()),Long.toString(count.targetCount()),
                    Long.toString(count.difference()),boundary(count.sourceRead()),boundary(count.targetRead()))).toList();
            boolean complete=value.terminal()==ValidationRunState.COMPLETED;
            boolean issues=value.rootCount()!=0||value.counts().stream().anyMatch(count->!count.sameCount());
            String result=complete?(issues?"COMPLETE_WITH_OBSERVATIONS":"COMPLETE_NO_ISSUES_OBSERVED"):(value.closed()?"INCOMPLETE":"IN_PROGRESS");
            return new Report(reference(value.reference()),spec.method().name(),Long.toString(comparison.mappingRevision()),comparison.comparisonRulesVersion(),spec.fingerprint(),
                    comparison.mappings().stream().map(ValidationReportService::mapping).toList(),value.closed(),state(value.terminal()),instant(value.terminalKnownAt()),instant(value.endedAt()),
                    window(value.window()),failure(value.failure()),Long.toString(value.rootCount()),Long.toString(value.retainedBytes()),counts,result,
                    "INITIAL_SYNC_FINISHED_AND_BOTH_SIDES_STABLE",false,"READ_OBSERVATIONS_NO_COMMON_TIME_SNAPSHOT","UNKNOWN");
        }).orElse(null));
    }
    public Response<Page<RootSummary>> roots(Reference identity,String offset,String limit){
        var reference=reference(identity);var page=paging(offset,limit);
        return read(()->{
            if(reports.find(reference).isEmpty())return null;
            var found=reports.roots(reference,page);
            // Original reports cannot be reborn after expiration. Absence during this read stays distinct from zero roots.
            if(reports.find(reference).isEmpty())return null;
            var encoder=new ValidationWireValues.Encoder();
            return new Page<>(found.items().stream().map(value->new RootSummary(value.id(),encoder.root(value.root()),encoder.value(value.summary()))).toList(),
                    Long.toString(found.total()),page.offset(),page.limit());
        });
    }
    public Response<RootDetail> root(Reference identity,String hash){
        var reference=reference(identity);if(hash==null||!hash.matches("[a-f0-9]{64}"))throw invalid("rootHash");
        return read(()->reports.root(reference,hash).map(value->{var encoder=new ValidationWireValues.Encoder();
            return new RootDetail(value.id(),Long.toString(value.revision()),encoder.root(value.root()),encoder.value(value.observation()));}).orElse(null));
    }
    /** The acquired process frame covers actual synchronous I/O, decoded facts and the complete wire projection. */
    private <T>Response<T> read(java.util.function.Supplier<T> query){
        var owner=new Object();var allocation=budget.acquire(owner);
        try{return new Response<>(allocation,owner,query.get());}catch(RuntimeException|Error failed){allocation.close();throw failed;}
    }
    private static Mapping mapping(ValidationTaskSpec.Mapping value){
        var keys=value.keys();return new Mapping(value.mappingId(),value.outputKind().name(),value.outputId(),table(value.source()),table(value.target()),keys==null?null:
                new Keys(keys.confirmationId(),keys.source().stream().map(ValidationReportService::component).toList(),keys.target().stream().map(ValidationReportService::component).toList()));
    }
    private static Table table(ValidationTaskSpec.Table value){return new Table(value.nodeId(),value.sourceId(),value.database(),value.schema(),value.table(),value.modelVersion(),value.codecBinding());}
    private static Component component(ValidationKeyBinding.Component value){return new Component(value.path(),value.domain().name(),value.logicalTypes().stream().sorted().toList(),value.numericPolicy().name());}
    private static Boundary boundary(ValidationCheckEvent.ReadBoundary value){return new Boundary(value.readId(),value.contractVersion(),value.startedAt().toString(),value.finishedAt().toString());}
    private static Failure failure(ValidationReportMetadata.Failure value){return value==null?null:new Failure(value.code(),value.params(),value.exceptionType(),value.message(),value.technical(),value.fatal());}
    private static Window window(ValidationReportMetadata.Window value){return value==null?null:new Window(value.startedAt().toString(),instant(value.finishedAt()));}
    private static String instant(java.time.Instant value){return value==null?null:value.toString();}
    private static String state(ValidationRunState value){return value==null?null:value.name();}
    private static Reference reference(ValidationReports.Reference value){return new Reference(value.pipelineId(),value.taskId(),value.taskGeneration().toString(),Long.toString(value.sequence()),value.runId());}
    private static ValidationReports.Reference reference(Reference value){
        if(value==null)throw invalid("reference");name(value.pipeline());name(value.task());name(value.run());
        return new ValidationReports.Reference(value.pipeline(),value.task(),ValidationMode.FULL,value.run(),uuid(value.generation()),number(value.sequence(),false));
    }
    private static ValidationReports.Paging paging(String offset,String limit){
        long start=number(offset,true),size=number(limit,false);if(start>4096||size>20)throw invalid("page");return new ValidationReports.Paging((int)start,(int)size);
    }
    private static long number(String value,boolean zero){
        if(value==null||!value.matches(zero?"0|[1-9][0-9]{0,18}":"[1-9][0-9]{0,18}"))throw invalid("integer");
        try{long parsed=Long.parseLong(value);if(parsed==Long.MAX_VALUE)throw invalid("integer");return parsed;}catch(NumberFormatException bad){throw invalid("integer");}
    }
    private static UUID uuid(String value){try{var id=UUID.fromString(value);if(!id.toString().equals(value))throw invalid("generation");return id;}catch(IllegalArgumentException|NullPointerException bad){throw invalid("generation");}}
    private static void name(String value){if(value==null||value.isBlank())throw invalid("identity");ValidationError.fieldName(value,"report.identity");}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","report.request."+field),null);}
}
