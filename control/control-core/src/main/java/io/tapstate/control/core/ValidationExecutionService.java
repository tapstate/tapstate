package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.runtime.probe.*;
import java.util.*;

/** Audited original FULL control; wire coordinates confer no native execution authority. */
public final class ValidationExecutionService {
    public record Context(String cluster,String node,String boot,String member,String incarnation) {
        static Context from(ValidationOperation.Context value){return value==null?null:new Context(value.cluster(),value.node(),value.boot(),value.member().toString(),value.incarnation().toString());}
    }
    public record Input(Context context,String pipeline,String task,String taskGeneration,String revision,String sequence,String command) {}
    public record Table(String mapping,String phase) {}
    public record View(Input original,String run,String outcome,String state,boolean admitted,boolean waiting,boolean controlUnknown,
            boolean retired,String drain,String reason,List<Table> tables,String premise,String conclusionScope) {}
    public static final class Response implements AutoCloseable {
        private final ValidationOperation.Response owner;private View view;
        private Response(ValidationOperation.Response owner){
            this.owner=owner;var value=owner.value();var original=value.original();
            view=new View(new Input(Context.from(original.context()),original.pipeline(),original.task(),original.taskGeneration().toString(),Long.toString(original.revision()),
                    Long.toString(original.sequence()),original.command().toString()),value.run(),value.outcome().name(),value.state(),value.admitted(),value.waiting(),value.controlUnknown(),
                    value.retired(),value.drain().name(),value.reason(),value.tables().stream().map(table->new Table(table.mapping(),table.phase())).toList(),
                    "INITIAL_SYNC_FINISHED_AND_BOTH_SIDES_STABLE","READ_OBSERVATIONS_NO_COMMON_TIME_SNAPSHOT");
        }
        public View view(){owner.value();return view;}
        @Override public void close(){view=null;owner.close();}
    }
    private final ValidationStartProbe starts;private final ValidationProgressProbe progress;private final ValidationStopProbe stops;private final AuditGate audit;
    public ValidationExecutionService(ValidationStartProbe starts,ValidationProgressProbe progress,ValidationStopProbe stops,AuditGate audit){
        this.starts=Objects.requireNonNull(starts);this.progress=Objects.requireNonNull(progress);this.stops=Objects.requireNonNull(stops);this.audit=Objects.requireNonNull(audit);
    }
    public Response start(String principal,Input input){var original=original(input);return audit.dispatch(ControlOperations.VALIDATION_START,
            new AuditContext(principal,original.pipeline()+"/"+original.task()),()->response(starts.start(original)));}
    public Response read(Input input){return response(progress.read(original(input)));}
    public Response stop(String principal,Input input){var original=original(input);return audit.dispatch(ControlOperations.VALIDATION_STOP,
            new AuditContext(principal,original.pipeline()+"/"+original.task()),()->response(stops.stop(original)));}
    private static Response response(ValidationOperation.Response owner){try{return new Response(owner);}catch(RuntimeException|Error failed){owner.close();throw failed;}}
    public static void validate(Input input){original(input);}
    private static ValidationOperation original(Input input){
        if(input==null||input.context()==null)throw invalid("identity");var context=input.context();
        return new ValidationOperation(new ValidationOperation.Context(context.cluster(),context.node(),context.boot(),uuid(context.member()),uuid(context.incarnation())),
                input.pipeline(),input.task(),uuid(input.taskGeneration()),number(input.revision()),number(input.sequence()),uuid(input.command()));
    }
    private static long number(String value){
        if(value==null||!value.matches("[1-9][0-9]{0,18}"))throw invalid("integer");
        try{long result=Long.parseLong(value);if(result==Long.MAX_VALUE)throw invalid("integer");return result;}catch(NumberFormatException bad){throw invalid("integer");}
    }
    private static UUID uuid(String value){try{var result=UUID.fromString(value);if(!result.toString().equals(value))throw invalid("uuid");return result;}catch(IllegalArgumentException|NullPointerException bad){throw invalid("uuid");}}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","operation."+field),null);}
}
