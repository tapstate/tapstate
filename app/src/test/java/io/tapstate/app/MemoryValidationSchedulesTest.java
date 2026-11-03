package io.tapstate.app;

import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MemoryValidationSchedulesTest {
    private static ValidationScheduleObservations.Origin origin(){return new ValidationScheduleObservations.Origin("node","boot",UUID.randomUUID(),UUID.randomUUID());}
    private static ValidationReportAdmission.Task task(String id){return new ValidationReportAdmission.Task("cluster","flow",id,UUID.randomUUID());}
    private static ValidationScheduleObservations.Fact fact(ValidationReportAdmission.Task task,long revision,ValidationScheduleObservations.Origin origin){
        return new ValidationScheduleObservations.Fact(task,revision,origin,Instant.EPOCH,Instant.EPOCH.plusSeconds(60),null,ValidationScheduleObservations.State.WAITING,null,null,null);
    }
    @Test void oldPublishAndClearCannotRecreateOrEraseAReplacementWritersFacts(){
        var store=new MemoryValidationSchedules();var first=origin();var second=origin();var task=task("task");
        try(var old=store.open(first)){
            assertThat(old.publish(fact(task,1,first))).isTrue();assertThat(store.view(first).read(task,1)).isPresent();
            try(var next=store.open(second)){
                assertThat(next.publish(fact(task,1,second))).isTrue();assertThat(old.publish(fact(task,1,first))).isFalse();old.close();
                assertThat(store.view(first).read(task,1)).isEmpty();assertThat(store.view(second).read(task,1)).isPresent();
            }
            assertThat(store.view(second).read(task,1)).isEmpty();
        }
    }
    @Test void staleRevisionGenerationAndOtherProcessAbsenceRemainUnknown(){
        var store=new MemoryValidationSchedules();var origin=origin();var task=task("task");
        try(var writer=store.open(origin)){
            writer.publish(fact(task,2,origin));assertThat(store.view(origin).read(task,1)).isEmpty();
            assertThat(store.view(origin).read(new ValidationReportAdmission.Task("cluster","flow","task",UUID.randomUUID()),2)).isEmpty();
            assertThat(new MemoryValidationSchedules().view(origin).read(task,2)).isEmpty();
        }
    }
    @Test void capacityRefusalPreservesExistingFactsAndReplacementAccounting(){
        var store=new MemoryValidationSchedules();var origin=origin();var first=task("first");
        try(var writer=store.open(origin)){
            assertThat(writer.publish(fact(first,1,origin))).isTrue();for(int index=1;index<128;index++)assertThat(writer.publish(fact(task("task_"+index),1,origin))).isTrue();
            assertThat(writer.publish(fact(task("overflow"),1,origin))).isFalse();assertThat(writer.publish(fact(first,2,origin))).isTrue();
            assertThat(store.view(origin).read(first,1)).isEmpty();assertThat(store.view(origin).read(first,2)).isPresent();
        }
        try(var next=store.open(origin)){assertThat(next.publish(fact(task("fresh"),1,origin))).isTrue();}
    }
    @Test void lateRepliesCannotReplaceTheCurrentRevisionOrNewerOccurrence(){
        var store=new MemoryValidationSchedules();var origin=origin();var task=task("task");var due=Instant.EPOCH.plusSeconds(120);
        try(var writer=store.open(origin)){
            var latest=new ValidationScheduleObservations.Fact(task,2,origin,due,due.plusSeconds(60),due,ValidationScheduleObservations.State.START_REQUESTED,null,UUID.randomUUID(),"run",4);
            assertThat(writer.publish(latest)).isTrue();
            assertThat(writer.publish(new ValidationScheduleObservations.Fact(task,2,origin,due.plusSeconds(1),due,due.minusSeconds(60),ValidationScheduleObservations.State.FAILED,null,null,null))).isFalse();
            assertThat(writer.publish(fact(task,1,origin))).isFalse();
            assertThat(writer.publish(new ValidationScheduleObservations.Fact(task,2,origin,due.minusSeconds(1),due.plusSeconds(60),due,ValidationScheduleObservations.State.UNKNOWN,null,null,null))).isFalse();
            assertThat(store.view(origin).read(task,2)).contains(latest);
        }
    }
}
