package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.probe.ValidationEvaluation;
import io.tapstate.runtime.probe.ValidationEvaluationProbe;
import io.tapstate.spi.store.ValidationReportAdmission;
import io.tapstate.spi.store.ValidationTaskStore;
import java.time.*;
import java.util.*;

/** Direct FULL authoring over independent evaluation and the original CONFIG compare-and-swap. */
public final class ValidationConfigurationService {
    public record Choice(String mappingId,String outputId,String sourceId,String table) {}
    public record Pair(String source,String target) {}
    public record Confirmation(String id,List<Pair> fields) {}
    public record Input(String pipelineId,String taskId,String taskGeneration,String expectedRevision,String method,
            List<Choice> mappings,Map<String,Confirmation> confirmations) {}
    public record Field(String name,String type,String keyDomain,String unavailableReason) {}
    public record Candidate(String origin,List<String> fields,List<String> issues) {}
    public record Side(String nodeId,String sourceId,String database,String schema,String table,String modelVersion,String codecBinding,
            String countOperation,List<Field> fields,List<Candidate> candidates,boolean pipelineWriteKeyUnknown) {}
    public record Mapping(String id,Side source,Side target) {}
    public record View(String method,String configurationRevision,String mappingRevision,String fingerprint,String comparisonFingerprint,List<Mapping> mappings,
            String premise,String conclusionScope,ValidationExecutionService.Context context) {}
    /** Owns all projected metadata until HTTP serialization has completed or failed. */
    public static final class Evaluation implements AutoCloseable {
        private final ValidationEvaluation owner;private View view;
        private Evaluation(ValidationEvaluation owner){this.owner=owner;var value=owner.view();
            view=new View(value.method(),value.configurationRevision(),value.mappingRevision(),value.fingerprint(),owner.comparison().map(ValidationTaskSpec::fingerprint).orElse(null),value.mappings().stream()
                    .map(mapping->new Mapping(mapping.id(),side(mapping.source()),side(mapping.target()))).toList(),value.premise(),value.conclusionScope(),ValidationExecutionService.Context.from(value.context()));}
        public View view(){owner.requireCurrent();return view;}
        @Override public void close(){view=null;owner.close();}
    }
    public record Schedule(String kind,String every,String unit,String anchor,String expression,String zoneId) {}
    public record Save(Input input,String fingerprint,String commandId,int historyLimit,Schedule schedule,boolean scheduleEnabled) {}
    public record Saved(String outcome,String fingerprint,String taskGeneration) {}
    public record SavedPair(List<String> source,List<String> target) {}
    public record Key(String confirmationId,List<SavedPair> fields) {}
    public record SavedMapping(String mappingId,String outputId,String sourceId,String sourceTable,String targetSourceId,String targetTable,Key keys) {}
    public record Task(String pipelineId,String taskId,String taskGeneration,String revision,String nextStartSequence,boolean closed,String method,String comparisonFingerprint,
            int historyLimit,Schedule schedule,boolean scheduleEnabled,List<SavedMapping> mappings,ValidationExecutionService.Context context,Permission permission,ScheduleObservation scheduleObservation) {}
    public record Permission(String phase,String taskGeneration,String revision,String sequence,String command,String run) {}
    public record ScheduleObservation(String node,String boot,String member,String context,String observedAt,String nextDue,String lastDue,String state,String reason,String command,String run,String sequence) {}
    public record Occurrence(String at,String local,String offset,String weekday) {}
    public record Preview(String base,String inputRevision,String zoneId,String rulesVersion,List<Occurrence> occurrences) {}
    private final String cluster;private final ValidationTaskStore tasks;private final ValidationEvaluationProbe evaluations;
    private final ValidationSchedule.Calculator schedules;private final AuditGate audit;private final Clock clock;
    private final io.tapstate.runtime.probe.ValidationOperation.Context context;private final io.tapstate.spi.store.ValidationScheduleObservations observations;
    public ValidationConfigurationService(String cluster,ValidationTaskStore tasks,ValidationEvaluationProbe evaluations,
            ValidationSchedule.Calculator schedules,AuditGate audit,Clock clock){
        this(cluster,tasks,evaluations,schedules,audit,clock,null,io.tapstate.spi.store.ValidationScheduleObservations.EMPTY);
    }
    public ValidationConfigurationService(String cluster,ValidationTaskStore tasks,ValidationEvaluationProbe evaluations,
            ValidationSchedule.Calculator schedules,AuditGate audit,Clock clock,io.tapstate.runtime.probe.ValidationOperation.Context context,
            io.tapstate.spi.store.ValidationScheduleObservations observations){
        this.context=context;this.observations=Objects.requireNonNull(observations);
        this.cluster=Objects.requireNonNull(cluster);this.tasks=Objects.requireNonNull(tasks);this.evaluations=Objects.requireNonNull(evaluations);
        this.schedules=Objects.requireNonNull(schedules);this.audit=Objects.requireNonNull(audit);this.clock=Objects.requireNonNull(clock);
    }
    public Evaluation evaluate(Input input){
        var checked=check(input);var previous=previous(checked);
        var lease=evaluations.evaluate(request(checked,previous));
        try{return new Evaluation(lease);}catch(RuntimeException|Error failed){lease.close();throw failed;}
    }
    public Saved save(String principal,Save command){
        if(command==null||command.input()==null||command.fingerprint()==null||!command.fingerprint().matches("[a-f0-9]{64}"))throw invalid("save");
        var checked=check(command.input());var id=uuid(command.commandId());
        return audit.dispatch(ControlOperations.VALIDATION_SAVE,new AuditContext(principal,checked.task.pipelineId()+"/"+checked.task.taskId()),()->{
            final ValidationTaskStore.Snapshot previous;try{previous=previous(checked,id);}catch(StaleDefinition stale){return new Saved("CONFLICT",null,null);}
            var configured=schedule(command.schedule());
            if(configured!=null)schedules.preview(configured,clock.instant(),Math.max(1,checked.revision),ValidationSchedule.SearchBudget.DEFAULT);
            var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(checked.method,1,configured,command.scheduleEnabled(),command.historyLimit()),
                    previous==null?new ValidationTaskStore.Incremental(0,10):previous.settings().incremental());
            try(var evaluated=evaluations.evaluate(request(checked,previous))){
                if(evaluated.comparison().isEmpty())return new Saved("PRECHECK_REQUIRED",null,null);
                var view=evaluated.view();if(!command.fingerprint().equals(view.fingerprint()))return new Saved("CONFLICT",view.fingerprint(),null);
                evaluated.requireCurrent();
                if(checked.revision==0){
                    var created=tasks.createFresh(new ValidationTaskStore.FreshCreate(checked.task.pipelineId(),checked.task.taskId(),evaluated.parentGeneration(),evaluated.comparison().orElseThrow(),settings,id));
                    return new Saved(created.outcome().name(),view.fingerprint(),created.generation()==null?null:created.generation().toString());
                }
                var result=tasks.update(new ValidationTaskStore.Update(checked.task,checked.revision,evaluated.comparison().orElseThrow(),settings,id));
                return new Saved(result.name(),view.fingerprint(),result==ValidationTaskStore.Write.APPLIED||result==ValidationTaskStore.Write.DUPLICATE?checked.task.generation().toString():null);
            }
        });
    }
    public record Disable(String pipeline,String task,String taskGeneration,String expectedRevision,String command) {}
    public record Disabled(String outcome,String taskGeneration,String receiptRevision,String command,Boolean scheduleEnabled) {}
    public Disabled disable(String principal,Disable input){
        if(input==null)throw invalid("disable");name(input.pipeline());name(input.task());var generation=uuid(input.taskGeneration());
        long revision=number(input.expectedRevision(),false);var command=uuid(input.command());
        var original=new ValidationReportAdmission.Task(cluster,input.pipeline(),input.task(),generation);
        return audit.dispatch(ControlOperations.VALIDATION_SCHEDULE_DISABLE,new AuditContext(principal,input.pipeline()+"/"+input.task()),()->{
            var result=tasks.disableSchedule(new ValidationTaskStore.DisableSchedule(original,revision,command));
            boolean accepted=result==ValidationTaskStore.Write.APPLIED||result==ValidationTaskStore.Write.DUPLICATE;
            return new Disabled(result.name(),accepted?generation.toString():null,accepted?Long.toString(revision+1):null,command.toString(),accepted?false:null);
        });
    }
    public Optional<Task> read(String pipeline,String task){
        name(pipeline);name(task);return tasks.read(pipeline,task).map(value->new Task(pipeline,task,value.task().generation().toString(),Long.toString(value.revision()),
                Long.toString(value.full().nextStartSequence()),value.closed(),value.settings().full().method().name(),value.comparison().fingerprint(),value.settings().full().historyLimit(),
                schedule(value.settings().full().schedule()),value.settings().full().scheduleEnabled(),value.comparison().mappings().stream().map(mapping->new SavedMapping(
                    mapping.mappingId(),mapping.outputId(),mapping.source().sourceId(),mapping.source().table(),mapping.target().sourceId(),mapping.target().table(),
                    mapping.keys()==null?null:new Key(mapping.keys().confirmationId(),java.util.stream.IntStream.range(0,mapping.keys().source().size())
                            .mapToObj(index->new SavedPair(mapping.keys().source().get(index).path(),mapping.keys().target().get(index).path())).toList()))).toList(),ValidationExecutionService.Context.from(context),permission(value.full().permission()),observation(observations.read(value.task(),value.revision()).orElse(null))));
    }
    private static Permission permission(ValidationTaskStore.Permission value){
        if(value==null)return null;var original=value.start();return new Permission(value.phase().name(),original.task().generation().toString(),Long.toString(original.configurationRevision()),Long.toString(original.sequence()),original.commandId(),original.run().runId());
    }
    private static ScheduleObservation observation(io.tapstate.spi.store.ValidationScheduleObservations.Fact value){
        if(value==null)return null;var origin=value.origin();return new ScheduleObservation(origin.node(),origin.boot(),origin.member().toString(),origin.context().toString(),value.observedAt().toString(),
                value.nextDue()==null?null:value.nextDue().toString(),value.lastDue()==null?null:value.lastDue().toString(),value.state().name(),value.reason(),value.command()==null?null:value.command().toString(),value.run(),value.sequence()==0?null:Long.toString(value.sequence()));
    }
    public Preview preview(Schedule schedule,String revision){
        var value=schedules.preview(requiredSchedule(schedule),clock.instant(),number(revision,false),ValidationSchedule.SearchBudget.DEFAULT);
        return new Preview(value.base().toString(),Long.toString(value.inputRevision()),value.zoneId(),value.rulesVersion(),value.occurrences().stream()
                .map(at->new Occurrence(at.at().toString(),at.local().toString(),at.offset().toString(),at.weekday().name())).toList());
    }
    private record Checked(ValidationReportAdmission.Task task,long revision,ValidationMethod method,List<ValidationEvaluation.Choice> choices,Map<String,ValidationEvaluation.Confirmation> confirmations){}
    private Checked check(Input input){
        if(input==null||input.mappings()==null||input.mappings().isEmpty()||input.mappings().size()>ValidationTaskSpec.MAX_MAPPINGS
                ||input.confirmations()==null||input.confirmations().size()>ValidationTaskSpec.MAX_MAPPINGS)throw invalid("input");
        name(input.pipelineId());name(input.taskId());long expected=number(input.expectedRevision(),true);
        if(expected==0&&input.taskGeneration()!=null)uuid(input.taskGeneration());
        var generation=expected==0?UUID.randomUUID():uuid(input.taskGeneration());
        ValidationMethod method;try{method=ValidationMethod.valueOf(input.method());}catch(IllegalArgumentException|NullPointerException bad){throw invalid("method");}
        var choices=input.mappings().stream().map(choice->{if(choice==null)throw invalid("mapping");
            return new ValidationEvaluation.Choice(choice.mappingId(),choice.outputId(),choice.sourceId(),choice.table());}).toList();
        var keys=new LinkedHashMap<String,ValidationEvaluation.Confirmation>();
        input.confirmations().forEach((mapping,confirmation)->{
            name(mapping);if(confirmation==null||confirmation.fields()==null||confirmation.fields().size()>ValidationKey.MAX_COMPONENTS)throw invalid("confirmation");
            keys.put(mapping,new ValidationEvaluation.Confirmation(uuid(confirmation.id()),confirmation.fields().stream().map(pair->{
                if(pair==null)throw invalid("pair");return new ValidationEvaluation.Pair(pair.source(),pair.target());}).toList()));
        });
        return new Checked(new ValidationReportAdmission.Task(cluster,input.pipelineId(),input.taskId(),generation),expected,method,choices,Map.copyOf(keys));
    }
    public static final class StaleDefinition extends TapstateException {
        private StaleDefinition(){super(ValidationError.INVALID_CONTRACT,Map.of("field","configuration.taskRevision"),null);}
    }
    private ValidationTaskStore.Snapshot previous(Checked checked){return previous(checked,null);}
    private ValidationTaskStore.Snapshot previous(Checked checked,UUID command){
        var current=tasks.read(checked.task.pipelineId(),checked.task.taskId()).orElse(null);
        if(checked.revision==0){
            if(current!=null&&(current.closed()||command==null||current.revision()!=1||!command.equals(current.commandId())))throw new StaleDefinition();
            return current;
        }
        if(current==null||!current.task().equals(checked.task)||current.closed()||current.revision()!=checked.revision
                &&!(command!=null&&command.equals(current.commandId())&&current.revision()==checked.revision+1))throw new StaleDefinition();
        return current;
    }
    private ValidationEvaluation.Request request(Checked checked,ValidationTaskStore.Snapshot previous){
        return new ValidationEvaluation.Request(new ValidationTaskStore.Selection(checked.task.pipelineId(),checked.choices.stream().map(ValidationEvaluation.Choice::outputId).distinct().toList()),
                checked.choices,checked.method,checked.confirmations,previous==null?null:previous.comparison(),clock.instant().plusSeconds(45));
    }
    private static Side side(ValidationEvaluation.Side side){return new Side(side.nodeId(),side.sourceId(),side.database(),side.schema(),side.table(),side.modelVersion(),side.codecBinding(),side.countOperation(),
            side.fields().stream().map(field->new Field(field.name(),field.type(),field.keyDomain(),field.unavailableReason())).toList(),
            side.candidates().stream().map(candidate->new Candidate(candidate.origin(),candidate.fields(),candidate.issues())).toList(),side.pipelineWriteKeyUnknown());}
    private static ValidationSchedule requiredSchedule(Schedule value){if(value==null)throw invalid("schedule");return schedule(value);}
    private static ValidationSchedule schedule(Schedule value){
        if(value==null)return null;
        try{
            if("INTERVAL".equals(value.kind())&&value.expression()==null)return new ValidationSchedule.Interval(number(value.every(),false),ValidationSchedule.Unit.valueOf(value.unit()),Instant.parse(value.anchor()),value.zoneId());
            if("CRON".equals(value.kind())&&value.every()==null&&value.unit()==null&&value.anchor()==null)return new ValidationSchedule.Cron(value.expression(),value.zoneId());
        }catch(IllegalArgumentException|NullPointerException|DateTimeException bad){throw invalid("schedule");}
        throw invalid("schedule");
    }
    private static Schedule schedule(ValidationSchedule value){
        if(value==null)return null;if(value instanceof ValidationSchedule.Interval interval)return new Schedule("INTERVAL",Long.toString(interval.every()),interval.unit().name(),interval.anchor().toString(),null,interval.zoneId());
        var cron=(ValidationSchedule.Cron)value;return new Schedule("CRON",null,null,null,cron.expression(),cron.zoneId());
    }
    private static long number(String value,boolean zero){
        if(value==null||!value.matches(zero?"0|[1-9][0-9]{0,18}":"[1-9][0-9]{0,18}"))throw invalid("integer");
        try{long parsed=Long.parseLong(value);if(parsed==Long.MAX_VALUE)throw invalid("integer");return parsed;}catch(NumberFormatException bad){throw invalid("integer");}
    }
    private static UUID uuid(String value){try{var parsed=UUID.fromString(value);if(!parsed.toString().equals(value))throw invalid("uuid");return parsed;}catch(IllegalArgumentException|NullPointerException bad){throw invalid("uuid");}}
    private static void name(String value){if(value==null||value.isBlank())throw invalid("identity");ValidationError.fieldName(value,"configuration.identity");}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","configuration."+field),null);}
}
