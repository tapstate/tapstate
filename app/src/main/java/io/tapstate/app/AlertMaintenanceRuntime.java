package io.tapstate.app;

import io.tapstate.core.alerting.AlertMaintenancePolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AlertMaintenance;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** One maintenance worker per admitted process. Health stays local and never feeds the alarm input path. */
final class AlertMaintenanceRuntime implements AutoCloseable {
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(AlertMaintenanceRuntime.class);
    private final AlertMaintenance store;
    private final Clock clock;
    private io.tapstate.spi.metrics.MetricsExport export=io.tapstate.spi.metrics.MetricsExport.none();
    private java.util.function.Supplier<PipelineAlertRuntime.Health> evaluation=()->null;
    void reporting(io.tapstate.spi.metrics.MetricsExport export){this.export=Objects.requireNonNull(export);}
    void evaluation(java.util.function.Supplier<PipelineAlertRuntime.Health> health){this.evaluation=health;}

    private final Runnable active;
    private final ScheduledThreadPoolExecutor worker=new ScheduledThreadPoolExecutor(1,r->{var t=new Thread(r,"alert-maintenance");t.setDaemon(true);return t;});
    private final AtomicBoolean closed=new AtomicBoolean();
    private final AlertMaintenance.Cursor[] cursors=new AlertMaintenance.Cursor[12];
    private final Instant[] oldestCandidates=new Instant[12],oldestProtected=new Instant[12];
    private final boolean[] isolated=new boolean[12],isolatedInScan=new boolean[12];
    private int collection;
    private Instant nextInventory=Instant.MIN,nextCleanup=Instant.MIN,lastSuccess,lastAttempt;
    private long scanned,changed,bytes,failures;
    private int consecutiveFailures;
    private String code;
    private boolean cleanupFailed,inventoryFailed;
    private AlertMaintenance.Inventory inventory;
    private AlertMaintenancePolicy.Capacity capacity=AlertMaintenancePolicy.Capacity.initial();
    private final Map<String,Long> skipped=new TreeMap<>();
    AlertMaintenanceRuntime(AlertMaintenance store,Clock clock,Runnable active){this.store=store;store.enableCapacityProtection();this.clock=clock;this.active=active;worker.setRemoveOnCancelPolicy(true);}
    void start(){worker.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS);}
    synchronized void tick() {
        if(closed.get())return;
        Instant now=clock.instant();
        if(!now.isBefore(nextInventory)) {
            nextInventory=now.plusSeconds(60);
            try {active.run();inventory=store.inventory();capacity=capacity.sample(inventory.logicalBytes());inventoryFailed=false;}
            catch(TapstateException refused){inventoryFailed=true;failure(refused);}
        }
        if(now.isBefore(nextCleanup)){metrics();return;}
        nextCleanup=now.plusSeconds(300);lastAttempt=now;
        int rows=0,tailPriority=-1;long encoded=0;long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();
        try {
            var exhausted=new boolean[12];int remainingCollections=12;
            while(remainingCollections>0&&rows<1000&&encoded<16L*1024*1024&&System.nanoTime()<deadline) {
                int slot=collection;collection=(collection+1)%12;if(exhausted[slot])continue;
                active.run();if(cursors[slot]==null)isolatedInScan[slot]=false;var cursor=cursors[slot]==null?new AlertMaintenance.Cursor(slot,null,null):cursors[slot];
                int allowedRows=Math.min(200,1000-rows);
                var batch=store.maintain(cursor,allowedRows,Math.min(4L*1024*1024,16L*1024*1024-encoded),Duration.ofNanos(Math.max(1,deadline-System.nanoTime())));
                if(batch.next()!=null && allowedRows<200)tailPriority=slot;
                if(batch.next()==null){exhausted[slot]=true;remainingCollections--;}
                cursors[slot]=batch.next();oldestCandidates[slot]=batch.oldestCandidate();oldestProtected[slot]=batch.oldestProtected();
                isolatedInScan[slot]|=batch.skipped().containsKey("unreadable")||batch.skipped().containsKey("unknown-reference");
                if(batch.next()==null)isolated[slot]=isolatedInScan[slot];else if(isolatedInScan[slot])isolated[slot]=true;
                rows+=batch.scanned();encoded+=batch.bytes();scanned+=batch.scanned();changed+=batch.changed();bytes+=batch.bytes();
                batch.skipped().forEach((key,value)->skipped.merge(key,value,Long::sum));
                if(batch.scanned()==0)break;
            }
            if(tailPriority>=0)collection=tailPriority;
            cleanupFailed=false;consecutiveFailures=0;lastSuccess=clock.instant();if(!inventoryFailed)code=null;
        }catch(TapstateException refused){cleanupFailed=true;consecutiveFailures++;failure(refused);}
        metrics();
    }
    private void metrics() {
        var values=health();var now=clock.instant();var facts=new ArrayList<io.tapstate.core.lifecycle.MetricFact>();
        for(String name:List.of("scanned","changed","failures","oldestCandidateAgeSeconds","logicalBytes","admissionReservedBytes","pending","oldestPendingAgeSeconds","failed","exhausted"))
            if(values.get(name) instanceof Number n)facts.add(gauge(name.endsWith("Seconds")?"s":name.endsWith("Bytes")?"By":"1",name,n.longValue(),now));
        facts.add(gauge("1","ownerConflicts",store.diagnostics().getOrDefault("ownerConflicts",0L),now));
        facts.add(gauge("1","degraded",Boolean.TRUE.equals(values.get("degraded"))?1:0,now));
        var input=evaluation.get();if(input!=null){facts.add(gauge("1","intakeRejected",input.rejected(),now));facts.add(gauge("1","queuedInputs",input.queuedInputs(),now));
            if(input.evaluationLagMillis()!=null)facts.add(gauge("ms","evaluationLagMillis",input.evaluationLagMillis(),now));}
        if(inventory!=null)for(var resource:inventory.resources()){
            facts.add(gauge("1",resource.resource()+".rows",resource.rows(),now));facts.add(gauge("By",resource.resource()+".bytes",resource.bytes(),now));
            facts.add(gauge("By",resource.resource()+".reserved",resource.reservedBytes(),now));facts.add(gauge("By",resource.resource()+".protected",resource.protectedBytes(),now));}
        try{export.offerRuntime(now,List.copyOf(facts));}catch(RuntimeException unavailable){LOG.debug("Alert metrics export unavailable");}
    }
    private static io.tapstate.core.lifecycle.MetricFact gauge(String unit,String name,long value,Instant at) {
        return new io.tapstate.core.lifecycle.MetricFact("tapstate.alerting."+name,io.tapstate.core.lifecycle.MetricType.GAUGE,unit,
                List.of(io.tapstate.core.lifecycle.MetricPoint.reading(Map.of(),at,value)));
    }
    private void failure(TapstateException refused){failures++;String previous=code;code=refused.code().code();if(!Objects.equals(previous,code))LOG.warn("Alert maintenance is degraded [{}]",code);}
    synchronized Map<String,Object> health() {
        Instant now=clock.instant();var result=new LinkedHashMap<String,Object>();
        Instant oldest=Arrays.stream(oldestCandidates).filter(Objects::nonNull).min(Instant::compareTo).orElse(null);
        long age=oldest==null?0:Math.max(0,Duration.between(oldest,now).getSeconds());
        boolean stale=inventory==null||inventory.measuredAt().isAfter(now)||Duration.between(inventory.measuredAt(),now).compareTo(Duration.ofSeconds(120))>0;
        boolean unknown=false;for(boolean value:isolated)unknown|=value;
        long pendingGrowth=store.diagnostics().getOrDefault("admissionReservedBytes",0L);
        long estimate=(inventory==null?0:inventory.logicalBytes())+pendingGrowth;
        var instantaneous=estimate>=AlertMaintenancePolicy.CAPACITY_BYTES?AlertMaintenancePolicy.Pressure.FULL:estimate>=AlertMaintenancePolicy.CAPACITY_BYTES*9/10?AlertMaintenancePolicy.Pressure.PAUSED:estimate>=AlertMaintenancePolicy.CAPACITY_BYTES*8/10?AlertMaintenancePolicy.Pressure.WARNING:AlertMaintenancePolicy.Pressure.NORMAL;
        var pressure=instantaneous.ordinal()>capacity.pressure().ordinal()?instantaneous:capacity.pressure();
        boolean degraded=unknown||closed.get()||inventoryFailed||cleanupFailed||stale||consecutiveFailures>=3||age>3600||pressure==AlertMaintenancePolicy.Pressure.FULL;
        result.put("available",true);result.put("degraded",degraded);result.put("warning",age>900||pressure!=AlertMaintenancePolicy.Pressure.NORMAL);
        result.put("code",code);result.put("lastSuccess",text(lastSuccess));result.put("lastAttempt",text(lastAttempt));result.put("cadenceSeconds",300);
        result.put("scanned",scanned);result.put("changed",changed);result.put("ioBytes",bytes);result.put("failures",failures);result.put("consecutiveFailures",consecutiveFailures);
        result.put("oldestCandidateAgeSeconds",age);Instant protectedAt=Arrays.stream(oldestProtected).filter(Objects::nonNull).min(Instant::compareTo).orElse(null);
        long protectedAge=protectedAt==null?0:Math.max(0,Duration.between(protectedAt,now).getSeconds());
        result.put("oldestProtectedAgeSeconds",protectedAge);result.put("referenceBlockedOverDay",protectedAge>86400);
        result.put("isolatedRecords",unknown);
        result.put("skipped",Map.copyOf(skipped));result.put("writeAttempts",store.diagnostics());result.put("ownerConflicts",store.diagnostics().getOrDefault("ownerConflicts",0L));result.put("admissionReservedBytes",pendingGrowth);result.put("pressure",pressure.name());result.put("capacityMeasuredAt",inventory==null?null:text(inventory.measuredAt()));
        result.put("capacitySampleStale",stale);result.put("capacityBytes",AlertMaintenancePolicy.CAPACITY_BYTES);
        result.put("logicalBytes",inventory==null?null:inventory.logicalBytes());result.put("pending",inventory==null?null:inventory.pending());
        result.put("oldestPendingAgeSeconds",inventory==null||inventory.oldestPending()==null?null:Math.max(0,Duration.between(inventory.oldestPending(),now).getSeconds()));
        result.put("failed",inventory==null?null:inventory.failed());result.put("exhausted",inventory==null?null:inventory.exhausted());
        result.put("resources",inventory==null?List.of():inventory.resources().stream().map(r->{var row=new LinkedHashMap<String,Object>();row.put("resource",r.resource());row.put("rows",r.rows());row.put("bytes",r.bytes());row.put("reservedBytes",r.reservedBytes());row.put("protectedBytes",r.protectedBytes());row.put("protectedBytesUpperBound",r.bytes());row.put("protectionAccounting","record-state estimate; references verified during cleanup");row.put("oldestCandidate",text(r.oldestCandidate()));return row;}).toList());
        return Collections.unmodifiableMap(result);
    }
    private static String text(Instant instant){return instant==null?null:instant.toString();}
    @Override public void close(){if(closed.compareAndSet(false,true))worker.shutdownNow();}
}
