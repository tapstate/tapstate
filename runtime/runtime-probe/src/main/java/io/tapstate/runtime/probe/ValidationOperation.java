package io.tapstate.runtime.probe;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationTaskSpec;
import java.io.Serializable;
import java.util.*;

/** Exact request coordinates, never execution authority or a reason to reconstruct a lost run. */
public record ValidationOperation(Context context,String pipeline,String task,UUID taskGeneration,long revision,long sequence,UUID command) implements Serializable {
    public ValidationOperation {
        if(context==null||taskGeneration==null||command==null||revision<1||revision==Long.MAX_VALUE||sequence<1||sequence==Long.MAX_VALUE)throw invalid("identity");
        name(pipeline);name(task);
    }
    /** This stamp belongs to one actual entry registry lifetime, independently of the execution owner. */
    public record Context(String cluster,String node,String boot,UUID member,UUID incarnation) implements Serializable {
        public Context {name(cluster);name(node);name(boot);if(member==null||incarnation==null)throw invalid("context");}
    }
    public enum Outcome { PENDING,ACCEPTED,CAPACITY,BUSY,REJECTED,UNKNOWN }
    public enum Drain { UNKNOWN,NOT_DRAINED,DRAINED }
    public record Table(String mapping,String phase) implements Serializable {
        public Table {name(mapping);if(phase==null||!Set.of("PENDING","READING","READ_COMPLETE").contains(phase))throw invalid("table");}
    }
    public record Status(ValidationOperation original,String run,Outcome outcome,String state,boolean admitted,boolean waiting,
            boolean controlUnknown,boolean retired,Drain drain,String reason,List<Table> tables) implements Serializable {
        public Status {
            if(original==null||outcome==null||drain==null||tables==null||tables.size()>ValidationTaskSpec.MAX_MAPPINGS)throw invalid("status");
            if(run!=null)name(run);if(state!=null)io.tapstate.core.validation.ValidationRunState.valueOf(state);
            if(reason!=null&&!reason.matches("[a-z][a-z0-9.-]{0,127}"))throw invalid("reason");tables=List.copyOf(tables);
        }
    }
    /** Request-scoped ownership of the bounded status and logical table projection through serialization. */
    public abstract static class Response implements AutoCloseable {
        public abstract Status value();
        @Override public abstract void close();
    }
    private static void name(String value){if(value==null||value.isBlank())throw invalid("name");ValidationError.fieldName(value,"operation.name");}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","operation."+field),null);}
}
