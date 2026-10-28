package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** One bounded planner driver in the original business context, independent of convergence and delivery IO. */
final class AlertPlanningRuntime implements AutoCloseable {
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(AlertPlanningRuntime.class);
    private final AlertNotificationPlanner planner;
    private final AtomicBoolean closed=new AtomicBoolean();
    private final ScheduledThreadPoolExecutor timer=new ScheduledThreadPoolExecutor(1,task->{var t=new Thread(task,"alert-planning");t.setDaemon(true);return t;});
    record Failure(String code,Map<String,Object> parameters) { }
    private volatile Failure failure;
    AlertPlanningRuntime(AlertNotificationPlanner planner){this.planner=planner;timer.setRemoveOnCancelPolicy(true);}
    void start(){timer.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS);}
    void tick(){if(closed.get())return;try{planner.tick();failure=null;}catch(TapstateException refused){
        var previous=failure;failure=new Failure(refused.code().code(),Map.of("phase","planning"));
        if(previous==null||!previous.code().equals(failure.code()))LOG.warn("Alert planning is degraded [{}]",failure.code());
    }}
    Failure failure(){return failure;}
    @Override public void close(){if(closed.compareAndSet(false,true))timer.shutdownNow();}
}
