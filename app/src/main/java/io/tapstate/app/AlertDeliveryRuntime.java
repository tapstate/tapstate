package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import io.tapstate.spi.store.AlertStore;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Bounded dispatch in the admitted business context. Only a newly committed permit can invoke IO. */
final class AlertDeliveryRuntime implements AutoCloseable {
    interface Channels extends AutoCloseable {
        boolean supports(AlertIntent.Frozen frozen);
        Object prepare(AlertIntent.Frozen frozen) throws InterruptedException;
        NotificationChannel open(AlertIntent.Frozen frozen, AlertStore.PrivateMaterial material, Object prepared);
        @Override void close();
    }
    record Failure(String code, Map<String,Object> parameters) { }
    private record Pending(AlertIntent.Identity identity, DeliveryAttempt.Permit permit, NotificationChannel.Result result, int jitter) { }
    private final AlertStore store; private final Clock clock; private final String worker; private final Runnable active;
    private final Channels channels; private final Function<AlertTestAuthority,AlertStore.Access> continuation;
    private final ScheduledThreadPoolExecutor timer=new ScheduledThreadPoolExecutor(1,r->{var t=new Thread(r,"alert-delivery-control");t.setDaemon(true);return t;});
    private final ThreadPoolExecutor pool=new ThreadPoolExecutor(4,4,0,TimeUnit.SECONDS,new SynchronousQueue<>(),r->{var t=new Thread(r,"alert-delivery");t.setDaemon(true);return t;});
    private final Map<String,NotificationChannel> calls=new ConcurrentHashMap<>();
    private final Set<String> running=ConcurrentHashMap.newKeySet();
    private final Map<String,Pending> pending=new ConcurrentHashMap<>();
    private final Map<String,Integer> byChannel=new HashMap<>();
    private final AtomicBoolean closed=new AtomicBoolean();
    private volatile Failure failure; private AlertStore.DueCursor cursor;
    AlertDeliveryRuntime(AlertStore store,Clock clock,String worker,Runnable active,Channels channels,
            Function<AlertTestAuthority,AlertStore.Access> continuation) {
        this.store=store;this.clock=clock;this.worker=worker;this.active=active;this.channels=channels;this.continuation=continuation;timer.setRemoveOnCancelPolicy(true);
    }
    synchronized void start(){if(closed.get())throw new IllegalStateException("closed delivery runtime");timer.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS);}
    /** Capability inspection is local and never opens a channel or resolves private material. */
    boolean supportsForManagement(AlertIntent.Frozen frozen){return !closed.get()&&channels.supports(frozen);}
    synchronized void tick(){
        if(closed.get())return;
        try {
            active.run();
            for(var result:List.copyOf(pending.values())) persist(result);
            renew();
            var page=store.dueIntents(clock.instant(),cursor,32);cursor=page.next();
            boolean paused=page.isolatedRecords();
            for(var row:page.rows()){
                var value=row.value();var id=value.frozen().identity();String key=id.deliveryId();
                if(running.contains(key)||pending.containsKey(key))continue;
                if(settle(row))continue;
                if(!channels.supports(value.frozen())){paused=true;continue;}
                if(value.state().permitsStopped()||value.state().nextAttemptAt()==null)continue;
                if(running.size()+pending.size()>=4||byChannel.getOrDefault(id.channelType(),0)>=2)continue;
                running.add(key);byChannel.merge(id.channelType(),1,Integer::sum);
                try { pool.execute(()->dispatch(id)); }
                catch(RejectedExecutionException full){release(id);paused=true;}
            }
            failure=paused?new Failure(AlertError.CHANNEL_UNAVAILABLE.code(),Map.of("phase","dispatch")):null;
        }catch(TapstateException unavailable){failure=new Failure(unavailable.code().code(),Map.of("phase","dispatch"));}
    }
    private void renew(){
        for(String id:running){
            // A page contains one immutable identity per occupied slot, kept by each in-flight permit.
            var current=inflight.get(id);if(current==null)continue;
            var row=store.intent(current.identity(),null);if(row.isEmpty())continue;
            var value=row.get().value();var now=clock.instant();
            if(worker.equals(value.state().owner().ownerId())&&value.state().owner().activeAt(now)
                    &&!value.state().owner().leaseUntil().isAfter(now.plusSeconds(10))){
                var renewed=AlertIntentControl.claim(value,worker,now,now.plusSeconds(30));commit(row.get(),renewed,AlertStore.CommitKind.PLAN,null);
            }
        }
    }
    private final Map<String,Pending> inflight=new ConcurrentHashMap<>();
    private boolean settle(AlertStore.Stored<AlertIntent.Record> row){
        var value=row.value();var now=clock.instant();
        if(value.state().retained().stoppedAt()!=null)return true;
        for(var attempt:value.state().attempts())if(attempt.endedAt()==null){
            if(now.isBefore(attempt.permit().deadline()))return true;
            var result=new NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,NotificationChannel.Action.RETRY,null,NotificationChannel.Phase.DEADLINE,0,"orphan-attempt");
            persist(new Pending(value.frozen().identity(),attempt.permit(),result,stableJitter(attempt.permit())));return true;
        }
        if(!now.isBefore(value.frozen().deadline())){
            commit(row,AlertIntentControl.stop(value,AlertIntent.StopReason.EXPIRED,now),AlertStore.CommitKind.PLAN,null);return true;
        }
        return value.state().permitsStopped();
    }
    private void dispatch(AlertIntent.Identity identity){
        String key=identity.deliveryId();
        try {
            active.run();var original=store.intent(identity,null);if(original.isEmpty())return;
            var f=original.get().value().frozen();
            Object prepared=channels.prepare(f);
            AlertStore.Access access=identity instanceof AlertIntent.Test?continuation.apply(Objects.requireNonNull(f.testAuthority())):null;
            AlertStore.Stored<AlertIntent.Record> granted=null;
            for(int retry=0;retry<3;retry++){
                active.run();var before=store.intent(identity,null);if(before.isEmpty())return;var v=before.get().value();var now=clock.instant();
                if(v.state().owner().activeAt(now)&&!worker.equals(v.state().owner().ownerId())||v.state().permitsStopped()
                        ||v.state().nextAttemptAt()==null||now.isBefore(v.state().nextAttemptAt())||!now.isBefore(f.deadline())
                        ||v.state().attempts().stream().anyMatch(a->a.endedAt()==null))return;
                var owned=AlertIntentControl.claim(v,worker,now,now.plusSeconds(30));
                var candidate=AlertDeliveryProgress.grant(owned,worker,UUID.randomUUID().toString(),now);
                // UNCHANGED is a replay of an earlier grant. It never grants another external call.
                if(commit(before.get(),candidate,AlertStore.CommitKind.PERMIT,access)==AlertStore.Outcome.STAGED){granted=new AlertStore.Stored<>(before.get().revision()+1,candidate);break;}
            }
            if(granted==null)return;
            var permit=granted.value().state().attempts().getLast().permit();
            var placeholder=new Pending(identity,permit,null,stableJitter(permit));inflight.put(key,placeholder);
            NotificationChannel.Result result;
            if(prepared instanceof NotificationChannel.Result refusal)result=refusal;
            else {
                try(var material=store.resolvePermittedMaterial(identity,permit,access);
                        var channel=channels.open(f,material,prepared)){
                    calls.put(key,channel);active.run();if(closed.get()||!clock.instant().isBefore(permit.deadline()))return;
                    result=channel.send(request(granted.value(),permit));
                }finally{calls.remove(key);}
            }
            var receipt=new Pending(identity,permit,result,placeholder.jitter());
            pending.put(key,receipt);persist(receipt);
        }catch(InterruptedException cancelled){Thread.currentThread().interrupt();}
        catch(TapstateException unavailable){failure=new Failure(unavailable.code().code(),Map.of("phase","dispatch"));}
        finally{inflight.remove(key);release(identity);}
    }
    private void persist(Pending pendingResult){
        var id=pendingResult.identity();
        for(int retry=0;retry<3;retry++){
            var before=store.intent(id,null);if(before.isEmpty()){pending.remove(id.deliveryId(),pendingResult);return;}
            var r=pendingResult.result();
            var protocol=new AlertIntent.Protocol(AlertIntent.Phase.valueOf(r.phase().name()),AlertIntent.Action.valueOf(r.action().name()),r.protocolStatus(),r.implementation(),r.retryAfter());
            var receipt=r.receipt()==null?null:new AlertIntent.Receipt(id.channelType().equals("cloud-sdk")?"sdk-event-center":"channel-protocol",r.receipt().remoteId(),r.receipt().requestId());
            var candidate=AlertDeliveryProgress.result(before.get().value(),pendingResult.permit(),r.outcome(),protocol,receipt,clock.instant(),pendingResult.jitter());
            if(candidate.equals(before.get().value())||commit(before.get(),candidate,AlertStore.CommitKind.RESULT,null)!=AlertStore.Outcome.CONFLICT){pending.remove(id.deliveryId(),pendingResult);return;}
        }
    }
    private AlertStore.Outcome commit(AlertStore.Stored<AlertIntent.Record> before,AlertIntent.Record after,AlertStore.CommitKind kind,AlertStore.Access access){
        var command=new AlertStore.Commit(kind,null,List.of(new AlertStore.IntentChange(before,after)),List.of());
        return access==null?store.commit(command):store.commitAsCaller(command,access);
    }
    static NotificationChannel.Request request(AlertIntent.Record record,DeliveryAttempt.Permit permit){
        var f=record.frozen();
        var destination=destination(f);
        return new NotificationChannel.Request(destination,f.identity() instanceof AlertIntent.Test?NotificationChannel.Purpose.CHANNEL_TEST:NotificationChannel.Purpose.BUSINESS,
                f.identity().deliveryId(),permit.attemptId(),permit.deadline(),f.content().contentType(),Objects.requireNonNull(record.state().payload()).bytes());
    }
    static NotificationChannel.Destination destination(AlertIntent.Frozen f){
        String receiver=f.identity() instanceof AlertIntent.Business b?b.key().receiverIncarnation():"candidate-"+((AlertIntent.Test)f.identity()).command().parameters().candidate().id();
        return new NotificationChannel.Destination(f.identity().scope(),f.identity().channelType(),receiver,f.identity().targetId(),
                AlertIntent.hash(io.tapstate.core.common.JsonWriter.write(List.of("alert-target-binding-v1",f.identity().scope().authorityId(),f.identity().scope().clusterId(),receiver,f.identity().targetId(),f.channel().typeId())).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }
    private static int stableJitter(DeliveryAttempt.Permit permit){return Math.floorMod(permit.attemptId().hashCode(),2001);}
    private synchronized void release(AlertIntent.Identity id){running.remove(id.deliveryId());byChannel.computeIfPresent(id.channelType(),(k,v)->v==1?null:v-1);}
    Failure failure(){return failure;}
    int occupied(){return running.size()+pending.size();}
    @Override public synchronized void close(){if(closed.compareAndSet(false,true)){timer.shutdownNow();calls.values().forEach(NotificationChannel::close);pool.shutdownNow();channels.close();}}
}
