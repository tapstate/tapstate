package io.tapstate.spi.store;

import io.tapstate.core.validation.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.time.Instant;

/** CONFIG report queries and original terminal mirrors over the existing report/difference resources. */
public interface ValidationReports {
    record Reference(String pipelineId,String taskId,ValidationMode mode,String runId,UUID taskGeneration,long sequence){
        public Reference{pipelineId=identifier(pipelineId,"report.pipeline");taskId=identifier(taskId,"report.task");
            runId=identifier(runId,"report.run");Objects.requireNonNull(mode);Objects.requireNonNull(taskGeneration);
            if(sequence<1||sequence==Long.MAX_VALUE)throw invalid("report.sequence");}
    }
    record Paging(int offset,int limit){
        public Paging{if(offset<0||offset>4096||limit<1||limit>100)throw invalid("report.page");}
    }
    record Page<T>(List<T> items,long total,Paging page){
        public Page{items=List.copyOf(items);Objects.requireNonNull(page);if(total<0||items.size()>page.limit())throw invalid("report.page");}
    }
    record Summary(Reference reference,ValidationMethod method,boolean closed,ValidationRunState terminal,
            Instant terminalKnownAt,Instant endedAt,ValidationReportMetadata.Window window,long rootCount,long retainedBytes,
            int countedTables,int mismatchedTables){
        public Summary{Objects.requireNonNull(reference);Objects.requireNonNull(method);
            if(rootCount<0||retainedBytes<0||countedTables<0||mismatchedTables<0||mismatchedTables>countedTables
                    ||terminal!=null&&(!closed||!terminal.terminal()||terminalKnownAt==null))throw invalid("report.summary");}
    }
    record Snapshot(ValidationReportAdmission.Start admission,ValidationRunSpec specification,boolean closed,
            ValidationRunState terminal,Instant terminalKnownAt,Instant endedAt,ValidationReportMetadata.Window window,ValidationReportMetadata.Failure failure,
            long rootCount,long retainedBytes,List<ValidationCountCheck> counts){
        public Snapshot{
            Objects.requireNonNull(admission);Objects.requireNonNull(specification);counts=List.copyOf(counts);
            if(!admission.run().specHash().equals(specification.fingerprint())||rootCount<0||retainedBytes<0
                    ||terminal!=null&&(!terminal.terminal()||!closed||terminalKnownAt==null)||terminal==null&&terminalKnownAt!=null
                    ||counts.size()>ValidationTaskSpec.MAX_MAPPINGS)
                throw invalid("report.snapshot");
        }
        public ValidationResultStore.Report report(){return admission.report();}
        public Reference reference(){return new Reference(admission.task().pipelineId(),admission.task().taskId(),admission.run().mode(),
                admission.run().runId(),admission.task().generation(),admission.sequence());}
    }
    record RootSummary(String id,ValidationRoot root,Map<String,Object> summary){
        public RootSummary{id=hash(id);Objects.requireNonNull(root);summary=owned(summary,128*1024);}
    }
    record RootDetail(String id,long revision,ValidationRoot root,Map<String,Object> observation){
        public RootDetail{id=hash(id);if(revision<1)throw invalid("report.rootRevision");
            Objects.requireNonNull(root);observation=owned(observation,8*1024*1024);}
    }
    /**
     * Caller supplies a known original CACHE retirement, never a request DTO or a cross-store commit
     * claim. CONFIG verifies its exact sealed report identity; this cannot reopen publication or birth.
     */
    record TerminalMirror(ValidationControlStore.Status known,Instant knownAt,ValidationReportMetadata.Window window,ValidationReportMetadata.Failure failure){
        public TerminalMirror{Objects.requireNonNull(known);Objects.requireNonNull(knownAt);
            if(!known.retired()||!known.state().terminal()||known.run().admission()==null||known.run().snapshot()==null)
                throw invalid("report.terminalMirror");}
    }
    enum Mirror { APPLIED,DUPLICATE,CLOSED_OR_EXPIRED }
    /** Known immutable CACHE terminal copies occupy the same pool while their CONFIG mirror is pending. */
    record Retention(ValidationReportAdmission.Task task,ValidationMode mode,int keep,List<ValidationControlStore.Status> pending){
        public Retention{Objects.requireNonNull(task);Objects.requireNonNull(mode);pending=List.copyOf(pending);
            if(keep<1||keep>100||pending.size()>128)throw invalid("report.retention");
            for(var status:pending)if(!status.retired()||!status.state().terminal()||status.run().admission()==null
                    ||!status.run().admission().task().equals(task)||status.run().identity().mode()!=mode)throw invalid("report.pendingTerminal");}
        public Retention(ValidationReportAdmission.Task task,ValidationMode mode,int keep){this(task,mode,keep,List.of());}
    }
    record Cleanup(int removed,int retained,boolean blocked){
        public Cleanup{if(removed<0||retained<0)throw invalid("report.cleanup");}
    }
    Page<Summary> history(String pipelineId,String taskId,ValidationMode mode,Paging page);
    Optional<Snapshot> find(Reference reference);
    Page<RootSummary> roots(Reference reference,Paging page);
    Optional<RootDetail> root(Reference reference,String rootHash);
    Mirror mirror(TerminalMirror command);
    Cleanup retain(Retention command);

    private static io.tapstate.core.common.TapstateException invalid(String field){return new io.tapstate.core.common.TapstateException(
            ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
    private static String identifier(String value,String field){if(value==null||value.isBlank())throw invalid(field);
        ValidationError.fieldName(value,field);return value;}
    private static String hash(String value){if(value==null||!value.matches("[a-f0-9]{64}"))throw invalid("report.rootHash");return value;}
    private static Map<String,Object> owned(Map<String,Object> value,long bytes){return ValidationValues.ownRow(value,
            new ValidationValues.Limits(bytes,65536,64)).fields();}
}
