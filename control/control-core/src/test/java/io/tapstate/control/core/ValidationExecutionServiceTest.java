package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.probe.ValidationOperation;
import io.tapstate.spi.store.AuditRecord;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationExecutionServiceTest {
    private static ValidationExecutionService.Input input(){return new ValidationExecutionService.Input(new ValidationExecutionService.Context("cluster","node","boot",
            UUID.randomUUID().toString(),UUID.randomUUID().toString()),"flow","task",UUID.randomUUID().toString(),"9007199254740993","9007199254740994",UUID.randomUUID().toString());}
    private static ValidationOperation.Response response(ValidationOperation op,AtomicBoolean closed){
        return new ValidationOperation.Response(){
            @Override public ValidationOperation.Status value(){assertThat(closed).isFalse();return new ValidationOperation.Status(op,"original-run",ValidationOperation.Outcome.ACCEPTED,"RUNNING",true,false,false,false,
                    ValidationOperation.Drain.NOT_DRAINED,null,List.of(new ValidationOperation.Table("orders","READ_COMPLETE")));}
            @Override public void close(){closed.set(true);}
        };
    }
    @Test void auditPrecedesStartAndStopWhileReadIsPassiveAndLeaseRemainsOwned(){
        var audits=new ArrayList<AuditRecord>();var start=new AtomicInteger();var stop=new AtomicInteger();var reads=new AtomicInteger();var closed=new AtomicBoolean();
        var service=new ValidationExecutionService(op->{assertThat(audits).hasSize(1);start.incrementAndGet();return response(op,closed);},
                op->{reads.incrementAndGet();return response(op,closed);},op->{assertThat(audits).hasSize(2);stop.incrementAndGet();return response(op,closed);},
                new AuditGate(audits::add,Clock.systemUTC()));
        var input=input();try(var result=service.start("alice",input)){
            assertThat(result.view().original()).isEqualTo(input);assertThat(closed).isFalse();assertThat(result.view().original().revision()).isEqualTo("9007199254740993");
            assertThat(result.view().state()).isEqualTo("RUNNING");assertThat(result.view().tables().getFirst().phase()).isEqualTo("READ_COMPLETE");
        }
        assertThat(closed).isTrue();closed.set(false);try(var ignored=service.read(input)){assertThat(reads).hasValue(1);}
        closed.set(false);try(var ignored=service.stop("alice",input)){assertThat(stop).hasValue(1);}
        assertThat(start).hasValue(1);assertThat(audits).extracting(AuditRecord::operationId).containsExactly("validation.start","validation.stop");
    }
    @Test void failedAuditOrMalformedCoordinatesNeverReachExecution(){
        var calls=new AtomicInteger();var service=new ValidationExecutionService(op->{calls.incrementAndGet();throw new AssertionError("no start");},
                op->{calls.incrementAndGet();throw new AssertionError("no read");},op->{calls.incrementAndGet();throw new AssertionError("no stop");},
                new AuditGate(ignored->{throw new IllegalStateException("audit failed");},Clock.systemUTC()));
        assertThatThrownBy(()->service.start("alice",input())).isInstanceOf(TapstateException.class).hasMessageContaining("audit-blocked");
        var valid=input();var bad=new ValidationExecutionService.Input(valid.context(),valid.pipeline(),valid.task(),valid.taskGeneration(),"1.0",valid.sequence(),valid.command());
        assertThatThrownBy(()->service.read(bad)).isInstanceOf(TapstateException.class);assertThat(calls).hasValue(0);
    }
}
