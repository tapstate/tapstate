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
    public record View(String method,String configurationRevision,String mappingRevision,String fingerprint,List<Mapping> mappings,
            String premise,String conclusionScope,ValidationExecutionService.Context context) {}
    /** Owns all projected metadata until HTTP serialization has completed or failed. */
    public static final class Evaluation implements AutoCloseable {
        private final ValidationEvaluation owner;private View view;
        private Evaluation(ValidationEvaluation owner){this.owner=owner;var value=owner.view();
            view=new View(value.method(),value.configurationRevision(),value.mappingRevision(),value.fingerprint(),value.mappings().stream()
                    .map(mapping->new Mapping(mapping.id(),side(mapping.source()),side(mapping.target()))).toList(),value.premise(),value.conclusionScope(),ValidationExecutionService.Context.from(value.context()));}
        public View view(){owner.requireCurrent();return view;}
        @Override public void close(){view=null;owner.close();}
    }
    public record Schedule(String kind,String every,String unit,String anchor,String expression,String zoneId) {}
    public record Save(Input input,String fingerprint,String commandId,int historyLimit,Schedule schedule,boolean scheduleEnabled) {}
    public record Saved(String outcome,String fingerprint) {}
    public record SavedPair(List<String> source,List<String> target) {}
    public record Key(String confirmationId,List<SavedPair> fields) {}
    public record SavedMapping(String mappingId,String outputId,String sourceId,String sourceTable,String targetSourceId,String targetTable,Key keys) {}
    public record Task(String pipelineId,String taskId,String taskGeneration,String revision,String nextStartSequence,boolean closed,String method,
            int historyLimit,Schedule schedule,boolean scheduleEnabled,List<SavedMapping> mappings) {}
    public record Occurrence(String at,String local,String offset,String weekday) {}
    public record Preview(String base,String inputRevision,String zoneId,String rulesVersion,List<Occurrence> occurrences) {}
    private final String cluster;private final ValidationTaskStore tasks;private final ValidationEvaluationProbe evaluations;
    private final ValidationSchedule.Calculator schedules;private final AuditGate audit;private final Clock clock;
    public ValidationConfigurationService(String cluster,ValidationTaskStore tasks,ValidationEvaluationProbe evaluations,
            ValidationSchedule.Calculator schedules,AuditGate audit,Clock clock){
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
            var previous=previous(checked,id);var configured=schedule(command.schedule());
            if(configured!=null)schedules.preview(configured,clock.instant(),Math.max(1,checked.revision),ValidationSchedule.SearchBudget.DEFAULT);
            var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(checked.method,1,configured,command.scheduleEnabled(),command.historyLimit()),
                    previous==null?new ValidationTaskStore.Incremental(0,10):previous.settings().incremental());
            try(var evaluated=evaluations.evaluate(request(checked,previous))){
                if(evaluated.comparison().isEmpty())return new Saved("PRECHECK_REQUIRED",null);
                var view=evaluated.view();if(!command.fingerprint().equals(view.fingerprint()))return new Saved("CONFLICT",view.fingerprint());
                evaluated.requireCurrent();
                var result=checked.revision==0?tasks.create(new ValidationTaskStore.Create(checked.task,evaluated.comparison().orElseThrow(),settings,id))
                        :tasks.update(new ValidationTaskStore.Update(checked.task,checked.revision,evaluated.comparison().orElseThrow(),settings,id));
                return new Saved(result.name(),view.fingerprint());
            }
        });
    }
    public Optional<Task> read(String pipeline,String task){
        name(pipeline);name(task);return tasks.read(pipeline,task).map(value->new Task(pipeline,task,value.task().generation().toString(),Long.toString(value.revision()),
                Long.toString(value.full().nextStartSequence()),value.closed(),value.settings().full().method().name(),value.settings().full().historyLimit(),
                schedule(value.settings().full().schedule()),value.settings().full().scheduleEnabled(),value.comparison().mappings().stream().map(mapping->new SavedMapping(
                    mapping.mappingId(),mapping.outputId(),mapping.source().sourceId(),mapping.source().table(),mapping.target().sourceId(),mapping.target().table(),
                    mapping.keys()==null?null:new Key(mapping.keys().confirmationId(),java.util.stream.IntStream.range(0,mapping.keys().source().size())
                            .mapToObj(index->new SavedPair(mapping.keys().source().get(index).path(),mapping.keys().target().get(index).path())).toList()))).toList()));
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
        name(input.pipelineId());name(input.taskId());var generation=uuid(input.taskGeneration());
        ValidationMethod method;try{method=ValidationMethod.valueOf(input.method());}catch(IllegalArgumentException|NullPointerException bad){throw invalid("method");}
        var choices=input.mappings().stream().map(choice->{if(choice==null)throw invalid("mapping");
            return new ValidationEvaluation.Choice(choice.mappingId(),choice.outputId(),choice.sourceId(),choice.table());}).toList();
        var keys=new LinkedHashMap<String,ValidationEvaluation.Confirmation>();
        input.confirmations().forEach((mapping,confirmation)->{
            name(mapping);if(confirmation==null||confirmation.fields()==null||confirmation.fields().size()>ValidationKey.MAX_COMPONENTS)throw invalid("confirmation");
            keys.put(mapping,new ValidationEvaluation.Confirmation(uuid(confirmation.id()),confirmation.fields().stream().map(pair->{
                if(pair==null)throw invalid("pair");return new ValidationEvaluation.Pair(pair.source(),pair.target());}).toList()));
        });
        return new Checked(new ValidationReportAdmission.Task(cluster,input.pipelineId(),input.taskId(),generation),number(input.expectedRevision(),true),method,choices,Map.copyOf(keys));
    }
    public static final class StaleDefinition extends TapstateException {
        private StaleDefinition(){super(ValidationError.INVALID_CONTRACT,Map.of("field","configuration.taskRevision"),null);}
    }
    private ValidationTaskStore.Snapshot previous(Checked checked){return previous(checked,null);}
    private ValidationTaskStore.Snapshot previous(Checked checked,UUID command){
        var current=tasks.read(checked.task.pipelineId(),checked.task.taskId()).orElse(null);
        // A new create replay is left to the original store command/body deduplication.
        if(current!=null&&(!current.task().equals(checked.task)||current.closed()||checked.revision!=0&&current.revision()!=checked.revision
                    &&!(command!=null&&command.equals(current.commandId())&&current.revision()==checked.revision+1))
                ||current==null&&checked.revision!=0)throw new StaleDefinition();
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
