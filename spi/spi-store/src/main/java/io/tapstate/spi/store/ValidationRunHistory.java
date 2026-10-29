package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationMode;
import java.util.*;

/** Immutable original CACHE terminal facts only. A node read never grants run control or query authority. */
public interface ValidationRunHistory {
    record Selection(ValidationReportAdmission.Task task,ValidationMode mode,int recent,int oldest){
        public Selection{Objects.requireNonNull(task);Objects.requireNonNull(mode);
            if(recent<1||recent>100||oldest<1||oldest>128)throw invalid("history.selection");}
    }
    /** Headers omit frozen specs. Unknown terminal time and unsettled controls are protected, not invented or evicted. */
    record Terminals(List<ValidationControlStore.Status> recent,List<ValidationControlStore.Status> oldest,long known,long unknown,long unsettled){
        public Terminals{recent=List.copyOf(recent);oldest=List.copyOf(oldest);
            if(recent.size()>100||oldest.size()>128||known<0||unknown<0||unsettled<0)throw invalid("history.terminals");}
    }
    Terminals terminals(WorkloadClaimFence currentNode,Selection selection);
    Optional<ValidationControlStore.Status> terminal(WorkloadClaimFence currentNode,ValidationControlStore.Run original);
    /** Irreversibly closed CONFIG birth and absent original CACHE row; local disposal only, never a terminal receipt. */
    boolean expired(WorkloadClaimFence currentNode,ValidationControlStore.Run original);
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
}
