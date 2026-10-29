package io.tapstate.app;

import com.hazelcast.cluster.*;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;

/** Combines CONFIG expectation with the original failure detector, never an unexplained leave callback. */
final class ClusterNodeAlertSource implements MembershipListener, AutoCloseable {
    private record Snapshot(Map<AlarmKey.Resource,Row> rows,Map<String,Row> latest,Map<AlarmKey.Resource,UUID> live,
            ClusterMembership membership,long epoch,long deadline,Instant observedAt) { }
    private final HazelcastInstance member;
    private final ClusterMembershipGate membership;
    private final PipelineAlertRuntime runtime;
    private final Supplier<AlertNodeLifecycleStore> stores;
    private final Supplier<Registration> registration;
    private final Function<Registration,Runnable> proof;
    private final Runnable active;
    private final Clock clock;
    private final AtomicBoolean refreshing=new AtomicBoolean();
    private final UUID listener;
    private volatile boolean closed;
    private volatile AlertNodeLifecycleStore store;
    private volatile Row own;
    private volatile Snapshot snapshot;
    private long epoch;
    private final String retirementCommand=UUID.randomUUID().toString();
    private Instant retirementAt;
    ClusterNodeAlertSource(HazelcastInstance member,ClusterMembershipGate membership,PipelineAlertRuntime runtime,
            BuiltinResourceSignals signals,Runnable active,Clock clock,Supplier<AlertNodeLifecycleStore> stores,
            Supplier<Registration> registration,Function<Registration,Runnable> proof) {
        this.member=member;this.membership=membership;this.runtime=runtime;this.active=active;this.clock=clock;
        this.stores=stores;this.registration=registration;this.proof=proof;
        signals.nodeProofs(this::evidence);listener=member.getCluster().addMembershipListener(this);
    }
    /** Listener callbacks perform no CONFIG IO; the bounded host independently reads the authoritative member snapshot. */
    @Override public void memberAdded(MembershipEvent event) { }
    @Override public void memberRemoved(MembershipEvent event) { }
    void admitted(){tick();}
    void tick() {
        if(closed || !refreshing.compareAndSet(false,true))return;
        try {
            active.run();var committed=membership.committed();
            if(committed==null || membership.visibleNodeIds().isEmpty())throw ThreeLayerStorePort.refused();
            if(store==null)store=stores.get();
            if(own==null) {
                var original=registration.get();own=store.register(original,clock.instant(),proof.apply(original));
            }
            proof.apply(own.registration()).run();
            var rows=new HashMap<AlarmKey.Resource,Row>();var newest=new HashMap<String,Row>();
            for(var row:store.list()) {
                var r=row.registration();
                if(!r.setup().equals(own.registration().setup()) || !r.cache().equals(own.registration().cache()))continue;
                if(row.registeredAt().isAfter(clock.instant()))throw ThreeLayerStorePort.refused();
                rows.put(r.key().resource(),row);
                newest.merge(r.key().nodeId(),row,(a,b)->{
                    long first=a.registration().session().claimGeneration(),second=b.registration().session().claimGeneration();
                    if(first==second && !a.registration().key().bootId().equals(b.registration().key().bootId()))throw ThreeLayerStorePort.refused();
                    return first>second?a:b;
                });
            }
            if(!rows.containsKey(own.registration().key().resource()))throw ThreeLayerStorePort.refused();
            var live=new HashMap<AlarmKey.Resource,UUID>();
            for(Member peer:member.getCluster().getMembers()) {
                var resource=resource(peer);if(resource!=null)live.put(resource,peer.getUuid());
            }
            var previous=snapshot;
            if(previous==null || !previous.rows().equals(rows) || !previous.live().equals(live) || !previous.membership().equals(committed))epoch=Math.incrementExact(epoch);
            var captured=new Snapshot(Map.copyOf(rows),Map.copyOf(newest),Map.copyOf(live),committed,epoch,
                    System.nanoTime()+Duration.ofSeconds(10).toNanos(),clock.instant());
            snapshot=captured;
            for(var entry:captured.rows().entrySet()) {
                var evidence=evidence(entry.getKey());if(evidence.isEmpty() || !evidence.orElseThrow().expected())continue;
                var row=entry.getValue();if(row.phase()!=Phase.EXPECTED)continue;
                boolean present=row.registration().memberId().equals(
                        Optional.ofNullable(captured.live().get(entry.getKey())).map(UUID::toString).orElse(null));
                runtime.resourceInput(entry.getKey(),"cluster.node-unavailable",0,row.registration().producerIncarnation(),
                        evidence.orElseThrow().requireCurrent(),new AlertInput.Conditions(captured.observedAt(),
                                present?AlertSignal.Condition.GOOD:AlertSignal.Condition.BAD,null));
            }
        }catch(TapstateException unavailable){snapshot=null;runtime.collectionUnavailable("node-lifecycle-unavailable");}
        finally{refreshing.set(false);}
    }
    private Optional<AlertStore.NodeEvidence> evidence(AlarmKey.Resource resource) {
        var captured=snapshot;if(closed || captured==null)return Optional.empty();
        var row=captured.rows().get(resource);if(row==null)return Optional.empty();
        var r=row.registration();var latest=captured.latest().get(r.key().nodeId());
        boolean superseded=latest!=null && !latest.registration().key().bootId().equals(r.key().bootId())
                && latest.registration().session().claimGeneration()>r.session().claimGeneration();
        boolean present=r.memberId().equals(Optional.ofNullable(captured.live().get(resource)).map(UUID::toString).orElse(null));
        boolean expected=!superseded && (row.phase()==Phase.EXPECTED || present);
        Runnable guard=()->{
            active.run();
            var current=snapshot;
            if(closed || current==null || current.epoch()!=captured.epoch() || System.nanoTime()-captured.deadline()>=0
                    || !captured.membership().equals(membership.committed()) || membership.visibleNodeIds().isEmpty())throw ThreeLayerStorePort.refused();
        };
        return Optional.of(new AlertStore.NodeEvidence(resource,resource.id(),resource.incarnation(),captured.membership().revision(),
                r.producerIncarnation(),guard,expected,row.revision(),superseded?latest.registration().key().bootId():null));
    }
    /** Only the main application close event calls this before invalidating its original runtime owner. */
    synchronized void orderlyShutdown() {
        if(closed || own==null || store==null)return;
        if(retirementAt==null)retirementAt=clock.instant();
        var original=own.registration();
        own=store.retire(original,own.revision(),retirementCommand,retirementAt,proof.apply(original));
    }
    private static AlarmKey.Resource resource(Member peer) {
        String node=peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE),boot=peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE);
        return node==null || node.isBlank() || boot==null || boot.isBlank() || peer.getUuid()==null ? null
                : new AlarmKey.Resource("cluster-node",node,boot);
    }
    @Override public void close(){closed=true;member.getCluster().removeMembershipListener(listener);snapshot=null;}
}
