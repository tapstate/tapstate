package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One complete ordered scan of the fixed mappings, on admitted blocking I/O. The scan selects root
 * identities only: each checker independently rereads its complete necessary source/target scope.
 * Each completed observation is handed to the original publisher before advancing. Neither EOF nor
 * successful publication is strong timing confirmation, and no production operator value is used.
 * The assembly owns native reads, their actual drain and all retained memory until physical exit.
 */
public final class ValidationFullScan {
    @FunctionalInterface public interface Checker {
        ValidationRootCheck read(ValidationObservation observation,Runnable requireCurrent);
    }
    public record Mapping(String id,Function<Runnable,ValidationOrderedMerge> open,Checker checker) {
        public Mapping {
            ValidationError.fieldName(id,"fullScan.mapping");Objects.requireNonNull(open);Objects.requireNonNull(checker);
        }
    }
    private final ValidationRunIdentity identity;
    private final ValidationRunSpec spec;
    private final List<Mapping> mappings;
    private final Runnable requireCurrent;
    private final Consumer<ValidationRootCheck> publish;
    private final AtomicBoolean attempted=new AtomicBoolean(),cancelled=new AtomicBoolean();
    private volatile ValidationOrderedMerge active;
    private volatile boolean complete;
    private final ValidationFailureLatch failure=new ValidationFailureLatch();

    public ValidationFullScan(ValidationRunIdentity identity,ValidationRunSpec spec,List<Mapping> mappings,
            Runnable requireCurrent,Consumer<ValidationRootCheck> publish) {
        this.identity=Objects.requireNonNull(identity);this.spec=Objects.requireNonNull(spec);
        this.mappings=List.copyOf(mappings);this.requireCurrent=Objects.requireNonNull(requireCurrent);this.publish=Objects.requireNonNull(publish);
        if(identity.mode()!=ValidationMode.FULL||spec.mode()!=ValidationMode.FULL||spec.method()==ValidationMethod.COUNT
                ||!identity.specHash().equals(spec.fingerprint()))throw invalid("identity");
        if(this.mappings.size()!=spec.task().mappings().size())throw invalid("mappings");
        for(int index=0;index<this.mappings.size();index++)
            if(!this.mappings.get(index).id().equals(spec.task().mappings().get(index).mappingId()))throw invalid("mappings");
    }
    /** Signal only. Cleanup dispatch and native cancellation belong to the assembly's admitted I/O. */
    public void cancel(){cancelled.set(true);}
    /** May block in the original readers; never invoke on a tasklet or deadline thread. */
    public void closeActive(){var current=active;if(current!=null)current.close();}
    public boolean complete(){return complete;}
    public void run() {
        if(!attempted.compareAndSet(false,true))throw invalid("alreadyAttempted");
        for(int index=0;index<mappings.size();index++) {
            check();var mapping=mappings.get(index);var declared=spec.task().mappings().get(index);
            var merge=Objects.requireNonNull(mapping.open().apply(this::check),"ordered readers");
            try {
                active=merge;check();
                while(true) {
                    check();var next=merge.next();if(next.isEmpty())break;
                    ValidationRoot root;
                    try(var pair=next.orElseThrow()) {
                        var source=pair.source();var target=pair.target();
                        source.ifPresent(group->physical(group.key().physical(),declared.source()));
                        target.ifPresent(group->physical(group.key().physical(),declared.target()));
                        var key=source.isPresent()?source.orElseThrow().key().comparison():target.orElseThrow().key().comparison();
                        root=new ValidationRoot(declared.mappingId(),declared.outputId(),key,List.of());
                    }
                    check();var observation=new ValidationObservation(identity,root,1);
                    var result=Objects.requireNonNull(mapping.checker().read(observation,this::check),"complete root result");
                    if(!observation.equals(result.observation())||result.method()!=spec.method())throw invalid("rootResult");
                    check();publish.accept(result);check();
                }
                check();
            }catch(RuntimeException|Error failed){record(failed);throw failed;}
            finally{
                try{merge.close();}catch(RuntimeException|Error cleanup){record(cleanup);}
                finally{active=null;}
                throwFailure();
            }
        }
        check();complete=true;
    }
    private static void physical(ValidationPhysicalIdentity actual,ValidationTaskSpec.Table expected) {
        if(!actual.sourceId().equals(expected.sourceId())||!Objects.equals(actual.database(),expected.database())
                ||!Objects.equals(actual.schema(),expected.schema())||!actual.table().equals(expected.table()))throw invalid("physicalTable");
    }
    private void record(Throwable failed){failure.record(failed);ValidationFailureLatch.addContext(failure.get(),failed);}
    private void throwFailure(){if(failure.get() instanceof Error fatal)throw fatal;if(failure.get() instanceof RuntimeException failed)throw failed;}
    private void check(){
        throwFailure();
        if(cancelled.get())throw new TapstateException(ValidationError.EXECUTION_CANCELLED,Map.of("node",identity.owner().nodeId()),null);
        requireCurrent.run();
        if(cancelled.get())throw new TapstateException(ValidationError.EXECUTION_CANCELLED,Map.of("node",identity.owner().nodeId()),null);
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","fullScan."+field),null);}
}
