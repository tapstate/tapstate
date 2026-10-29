package io.tapstate.app;

import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AlertMaintenance;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlertMaintenanceRuntimeTest {
    static final Instant T=Instant.parse("2026-10-29T00:00:00Z");
    static final class Port implements AlertMaintenance {
        final List<Integer> visited=new ArrayList<>();boolean fail,full;int samples;Instant time=T;
        @Override public Inventory inventory(){samples++;return new Inventory(time,List.of(new Usage("alert_instances",1,100,64,100,null)),0,null,0,0);}
        @Override public Batch maintain(Cursor cursor,int rows,long bytes,Duration time) {
            if(fail)throw new TapstateException(AlertError.STALE_ACTION,Map.of("action","maintenance-fixture"),null);
            assertThat(rows).isBetween(1,200);assertThat(bytes).isLessThanOrEqualTo(4L*1024*1024);assertThat(time).isLessThanOrEqualTo(Duration.ofSeconds(10));
            visited.add(cursor.collection());return new Batch(full?new Cursor(cursor.collection(),T,"cursor"):null,full?rows:1,1,full?bytes:100,Map.of(),null);
        }
    }
    @Test void firstRoundStartsImmediatelyAndThreeRoundsRotateAllResourcesWithinTheFrozenIoBudget() {
        var time=new AtomicReference<>(T);var clock=clock(time);var port=new Port();port.full=true;
        try(var runtime=new AlertMaintenanceRuntime(port,clock,()->{})) {
            runtime.tick();assertThat(port.visited).containsExactly(0,1,2,3);
            runtime.tick();assertThat(port.samples).isOne();assertThat(port.visited).hasSize(4);
            time.set(T.plusSeconds(300));port.time=time.get();runtime.tick();
            time.set(T.plusSeconds(600));port.time=time.get();runtime.tick();assertThat(port.visited).containsExactly(0,1,2,3,4,5,6,7,8,9,10,11);
            assertThat(runtime.health()).containsEntry("scanned",2400L).containsEntry("ioBytes",48L*1024*1024).containsEntry("changed",12L);
        }
    }
    @Test void failedAndStaleMeasurementsNeverLookHealthyAndSuccessfulRecoveryClearsTheCurrentFailure() {
        var time=new AtomicReference<>(T);var port=new Port();
        try(var runtime=new AlertMaintenanceRuntime(port,clock(time),()->{})) {
            port.fail=true;runtime.tick();assertThat(runtime.health()).containsEntry("degraded",true).containsEntry("failures",1L);
            for(int i=1;i<=2;i++){time.set(T.plusSeconds(300L*i));port.time=time.get();runtime.tick();}
            assertThat(runtime.health()).containsEntry("consecutiveFailures",3);
            port.fail=false;time.set(T.plusSeconds(900));port.time=time.get();runtime.tick();assertThat(runtime.health()).containsEntry("degraded",false).containsEntry("consecutiveFailures",0);
            time.set(T.plusSeconds(1021));assertThat(runtime.health()).containsEntry("degraded",true).containsEntry("capacitySampleStale",true);
            runtime.close();int calls=port.visited.size();runtime.tick();assertThat(port.visited).hasSize(calls);
        }
    }
    @Test void optionalExporterFailureCannotCreateRecursiveInputsOrBreakMaintenance() {
        var port=new Port();var exported=new ArrayList<io.tapstate.core.lifecycle.MetricFact>();
        try(var runtime=new AlertMaintenanceRuntime(port,Clock.fixed(T,ZoneOffset.UTC),()->{})) {
            runtime.reporting(new io.tapstate.spi.metrics.MetricsExport(){
                public void offer(String id,io.tapstate.core.lifecycle.PipelineState state,Instant at,List<io.tapstate.core.lifecycle.MetricFact> facts){throw new AssertionError("maintenance must not fabricate a Pipeline");}
                public void forgetPipelinesOutside(Collection<String> ids){}
                public void offerRuntime(Instant at,List<io.tapstate.core.lifecycle.MetricFact> facts){exported.addAll(facts);throw new IllegalStateException("controlled exporter outage");}
            });
            runtime.tick();assertThat(runtime.health()).containsEntry("degraded",false).containsEntry("changed",12L);
            assertThat(exported).isNotEmpty().allMatch(f->f.name().startsWith("tapstate.alerting.")&&f.points().getFirst().attributes().isEmpty());
        }
    }
    static Clock clock(AtomicReference<Instant> value){return new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return value.get();}};}
    @Test void aResourceAtTheRoundTailEventuallyReceivesItsFullBatchAllowance() {
        var time=new AtomicReference<>(T);var completed=new java.util.concurrent.atomic.AtomicInteger();
        AlertMaintenance port=new AlertMaintenance(){
            public Inventory inventory(){return new Inventory(time.get(),List.of(),0,null,0,0);}
            public Batch maintain(Cursor cursor,int rows,long bytes,Duration duration){
                if(cursor.collection()!=11)return rows>=81?new Batch(null,81,0,100,Map.of(),null):new Batch(cursor,rows,0,100,Map.of("budget",1L),null);
                if(rows<120)return new Batch(cursor,rows,0,100,Map.of("budget",1L),null);
                completed.incrementAndGet();return new Batch(null,120,1,100,Map.of(),null);
            }
        };
        try(var runtime=new AlertMaintenanceRuntime(port,clock(time),()->{})){
            runtime.tick();assertThat(completed.get()).isZero();time.set(T.plusSeconds(300));runtime.tick();assertThat(completed.get()).isPositive();
        }
    }

}
