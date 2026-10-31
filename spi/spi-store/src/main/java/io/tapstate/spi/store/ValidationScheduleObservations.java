package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Already-published local facts only. Loss or absence confers no scheduling or recovery authority. */
public interface ValidationScheduleObservations {
    ValidationScheduleObservations EMPTY=(task,revision)->Optional.empty();
    enum State { WAITING,OFF,START_REQUESTED,SKIPPED_BUSY,SKIPPED_LATE,FAILED,UNKNOWN }
    record Origin(String node,String boot,UUID member,UUID context){
        public Origin{Objects.requireNonNull(node);Objects.requireNonNull(boot);Objects.requireNonNull(member);Objects.requireNonNull(context);}
    }
    record Fact(ValidationReportAdmission.Task task,long revision,Origin origin,Instant observedAt,Instant nextDue,Instant lastDue,
            State state,String reason,UUID command,String run,long sequence){
        public Fact(ValidationReportAdmission.Task task,long revision,Origin origin,Instant observedAt,Instant nextDue,Instant lastDue,State state,String reason,UUID command,String run){this(task,revision,origin,observedAt,nextDue,lastDue,state,reason,command,run,0);}
        public Fact{Objects.requireNonNull(task);Objects.requireNonNull(origin);Objects.requireNonNull(observedAt);Objects.requireNonNull(state);
            if(revision<1||revision==Long.MAX_VALUE||sequence<0||sequence==Long.MAX_VALUE)throw invalid("revision");
            if(reason!=null&&!reason.matches("[a-z][a-z0-9.-]{0,127}"))throw invalid("reason");
            if(origin.node().length()>512||origin.boot().length()>512||run!=null&&run.length()>512)throw invalid("size");
        }
    }
    Optional<Fact> read(ValidationReportAdmission.Task task,long revision);
    private static io.tapstate.core.common.TapstateException invalid(String field){return new io.tapstate.core.common.TapstateException(
            io.tapstate.core.validation.ValidationError.INVALID_CONTRACT,java.util.Map.of("field","schedule.observation."+field),null);}
}
