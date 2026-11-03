package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** One bounded planner driver in the original business context, independent of convergence and delivery IO. */
final class AlertPlanningRuntime implements AutoCloseable {
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(AlertPlanningRuntime.class);
    private final AlertNotificationPlanner planner;
    private final AtomicBoolean closed=new AtomicBoolean();
    private final ScheduledThreadPoolExecutor timer=new ScheduledThreadPoolExecutor(1,task->{var t=new Thread(task,"alert-planning");t.setDaemon(true);return t;});
    record Failure(String code,Map<String,Object> parameters) { }
    private final AtomicReference<Failure> failure=new AtomicReference<>();
    AlertPlanningRuntime(AlertNotificationPlanner planner){this.planner=planner;timer.setRemoveOnCancelPolicy(true);}
    void start(){timer.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS);}
    void tick(){if(closed.get())return;try{planner.tick();failure.set(null);}catch(TapstateException refused){
        var previous=failure.get();var current=new Failure(refused.code().code(),Map.of("phase","planning"));failure.set(current);
        if(previous==null||!previous.code().equals(current.code()))LOG.warn("Alert planning is degraded [{}]",current.code());
    }}
    Failure failure(){return failure.get();}
    @Override public void close(){if(closed.compareAndSet(false,true))timer.shutdownNow();}
}
