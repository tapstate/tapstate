package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAckFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** One volatile bounded-run frontier per Pipeline; closing this runtime discards every pending notification. */
final class SnapshotLoadReceipts implements AutoCloseable,ScopedPipelineCaptureCoordinator.BoundedLoads {
    private static final String CONTEXT="tapstate.snapshot-load-receipts";
    private record Entry(SnapshotLoadAcks.Identity identity,SnapshotRecoveryFrontier frontier) {}
    private final HazelcastInstance member;
    private final Runnable active;
    private final Consumer<ExecutionFence> requireRun;
    private final Consumer<ExecutionFence> requireLocal;
    private final Map<String,Entry> entries=new HashMap<>();
    private record Notice(SnapshotLoadAcks.Identity identity,String stream,String writer,SourceOrder through) {}
    private final Set<Notice> pending=ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService notifications=Executors.newSingleThreadScheduledExecutor(task -> {
        var thread=new Thread(task,"tapstate-snapshot-load-notifications");thread.setDaemon(true);return thread;
    });
    private volatile boolean closed;

    SnapshotLoadReceipts(HazelcastInstance member,Runnable active,ExecutionAuthorization authorization) {
        this(member,active,authorization::require,authorization::requireLocal);
    }
    SnapshotLoadReceipts(HazelcastInstance member,Runnable active,Consumer<ExecutionFence> requireRun,Consumer<ExecutionFence> requireLocal) {
        this.member=Objects.requireNonNull(member);this.active=Objects.requireNonNull(active);
        this.requireRun=Objects.requireNonNull(requireRun);this.requireLocal=Objects.requireNonNull(requireLocal);
        if(member.getUserContext().putIfAbsent(CONTEXT,this)!=null)throw new IllegalStateException("bounded-load receipts already bound");
    }
    static SnapshotLoadReceipts of(HazelcastInstance member) {
        Object found=member.getUserContext().get(CONTEXT);
        if(!(found instanceof SnapshotLoadReceipts receiver))throw SnapshotLoadAcks.refused();return receiver;
    }
    SinkAckFactory forRun(ExecutionFence fence,Map<String,Long> epochs) {
        requireActive();requireRun.accept(fence);
        var local=member.getCluster().getLocalMember();
        var owner=new SnapshotLoadAcks.Owner(local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE),local.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE),local.getUuid());
        return new SnapshotLoadAcks(new SnapshotLoadAcks.Identity(fence,owner,epochs));
    }
    synchronized boolean begin(SnapshotLoadAcks.Identity identity,Map<String,List<String>> writers) {
        requireOwner(identity);var previous=entries.get(identity.fence().pipelineId());
        if(previous!=null && previous.identity().fence().equals(identity.fence())) {
            if(!previous.identity().equals(identity) || previous.frontier()==null)throw SnapshotLoadAcks.refused();previous.frontier().begin(writers);return true;
        }
        var frontier=new SnapshotRecoveryFrontier(identity.epochs());frontier.begin(writers);
        entries.put(identity.fence().pipelineId(),new Entry(identity,frontier));return true;
    }
    synchronized boolean bounded(SnapshotLoadAcks.Identity identity,String stream,String writer,SourceOrder through) {
        requireOwner(identity);var current=entries.get(identity.fence().pipelineId());
        if(current==null || !current.identity().equals(identity))return false;
        // A late reply for this exact retired run only releases its sender; it cannot restore completion evidence.
        if(current.frontier()!=null)current.frontier().bounded(stream,writer,through);return true;
    }
    @Override public synchronized boolean complete(ExecutionFence fence) {
        requireActive();requireLocal.accept(fence);var current=entries.get(fence.pipelineId());
        return current!=null && current.identity().fence().equals(fence) && current.frontier()!=null && current.frontier().complete();
    }
    @Override public synchronized void forget(ExecutionFence fence) {
        var current=entries.get(fence.pipelineId());
        if(current!=null && current.identity().fence().equals(fence))entries.put(fence.pipelineId(),new Entry(current.identity(),null));
        pending.removeIf(notice -> notice.identity().fence().equals(fence));
    }
    void enqueue(SnapshotLoadAcks.Identity identity,String stream,String writer,SourceOrder through) {
        requireActive();requireLocal.accept(identity.fence());
        var notice=new Notice(identity,stream,writer,through);
        if(pending.add(notice))schedule(notice,0);
    }
    private void schedule(Notice notice,long delay) {
        try {notifications.schedule(() -> deliver(notice),delay,TimeUnit.MILLISECONDS);}
        catch(RejectedExecutionException closing) {pending.remove(notice);if(!closed)throw closing;}
    }
    private void deliver(Notice notice) {
        if(closed || !pending.contains(notice))return;
        try {requireActive();requireLocal.accept(notice.identity().fence());}
        catch(TapstateException expired) {pending.remove(notice);return;}
        boolean delivered=false;
        try {delivered=SnapshotLoadAcks.notify(member,notice.identity(),notice.stream(),notice.writer(),notice.through());}
        catch(TapstateException unavailable) { /* The same authorized run retains this fact through a transport interruption. */ }
        if(delivered)pending.remove(notice);
        else if(!closed)schedule(notice,100);
    }
    private void requireOwner(SnapshotLoadAcks.Identity identity) {
        requireActive();
        if(!identity.owner().matches(member.getCluster().getLocalMember()))throw SnapshotLoadAcks.refused();
        requireRun.accept(identity.fence());requireActive();
    }
    private void requireActive(){if(closed)throw SnapshotLoadAcks.refused();active.run();}
    @Override public synchronized void close(){if(closed)return;closed=true;entries.clear();pending.clear();member.getUserContext().remove(CONTEXT,this);notifications.shutdownNow();}
}
