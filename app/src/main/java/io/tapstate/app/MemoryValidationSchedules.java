package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.*;
import java.util.*;

/** Bounded local observation exchange. Reads never invoke the scheduler or contact another member. */
final class MemoryValidationSchedules {
    private static final int MAX_WRITERS=4,MAX_ENTRIES=128;
    private static final long MAX_BYTES=2L*1024*1024;
    private record Key(Writer writer,ValidationReportAdmission.Task task){}
    private record Entry(ValidationScheduleObservations.Fact fact,long bytes){}
    private final Map<String,Writer> writers=new HashMap<>();private final Map<Key,Entry> entries=new HashMap<>();private long bytes;
    synchronized Writer open(ValidationScheduleObservations.Origin origin){
        var previous=writers.get(origin.node());
        if(previous==null&&writers.size()==MAX_WRITERS)throw capacity("validation.scheduleWriters");
        if(previous!=null)previous.retire();
        var writer=new Writer(origin);writers.put(origin.node(),writer);return writer;
    }
    ValidationScheduleObservations view(ValidationScheduleObservations.Origin origin){
        return (task,revision)->{
            synchronized(MemoryValidationSchedules.this){
                var writer=writers.get(origin.node());if(writer==null||!writer.active||!writer.origin.equals(origin))return Optional.empty();
                var entry=entries.get(new Key(writer,task));return entry==null||entry.fact().revision()!=revision?Optional.empty():Optional.of(entry.fact());
            }
        };
    }
    final class Writer implements AutoCloseable {
        private final ValidationScheduleObservations.Origin origin;private boolean active=true;
        private Writer(ValidationScheduleObservations.Origin origin){this.origin=Objects.requireNonNull(origin);}
        boolean publish(ValidationScheduleObservations.Fact fact){
            Objects.requireNonNull(fact);if(!origin.equals(fact.origin()))throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","schedule.observationOrigin"),null);
            synchronized(MemoryValidationSchedules.this){
                if(!active||writers.get(origin.node())!=this)return false;
                var key=new Key(this,fact.task());var previous=entries.get(key);
                if(previous!=null&&previous.fact().revision()==fact.revision()&&previous.fact().lastDue()!=null&&fact.lastDue()!=null&&fact.lastDue().isBefore(previous.fact().lastDue()))return false;
                if(previous!=null&&(previous.fact().revision()>fact.revision()||previous.fact().revision()==fact.revision()&&previous.fact().observedAt().isAfter(fact.observedAt())))return false;
                long retained=retained(fact);long old=previous==null?0:previous.bytes();
                if(previous==null&&entries.size()==MAX_ENTRIES||retained>MAX_BYTES-bytes+old)return false;
                entries.put(key,new Entry(fact,retained));bytes=bytes-old+retained;return true;
            }
        }
        void forget(ValidationReportAdmission.Task task){synchronized(MemoryValidationSchedules.this){if(!active||writers.get(origin.node())!=this)return;var previous=entries.remove(new Key(this,task));if(previous!=null)bytes-=previous.bytes();}}
        private void retire(){
            active=false;var iterator=entries.entrySet().iterator();while(iterator.hasNext()){var next=iterator.next();if(next.getKey().writer()==this){bytes-=next.getValue().bytes();iterator.remove();}}
        }
        @Override public void close(){synchronized(MemoryValidationSchedules.this){if(!active)return;retire();writers.remove(origin.node(),this);}}
    }
    private static long retained(ValidationScheduleObservations.Fact fact){return 2048L+2L*(fact.task().clusterId().length()+fact.task().pipelineId().length()+fact.task().taskId().length()
            +fact.origin().node().length()+fact.origin().boot().length()+(fact.reason()==null?0:fact.reason().length())+(fact.run()==null?0:fact.run().length()));}
    private static TapstateException capacity(String limit){return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit",limit),null);}
}
