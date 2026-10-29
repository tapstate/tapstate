package io.tapstate.app;

import com.hazelcast.cluster.*;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;

/** Only a failure-detector removal of a previously committed original member is a failure input. */
final class ClusterNodeAlertSource implements MembershipListener, AutoCloseable {
    private record MemberState(AlarmKey.Resource resource,UUID memberId,boolean removed) { }
    private final HazelcastInstance member;
    private final ClusterMembershipGate membership;
    private final PipelineAlertRuntime runtime;
    private final BuiltinResourceSignals signals;
    private final Runnable active;
    private final Clock clock;
    private final Map<AlarmKey.Resource,MemberState> monitored=new LinkedHashMap<>();
    private final UUID registration;
    private boolean closed;
    ClusterNodeAlertSource(HazelcastInstance member,ClusterMembershipGate membership,PipelineAlertRuntime runtime,
            BuiltinResourceSignals signals,Runnable active,Clock clock){
        this.member=member;this.membership=membership;this.runtime=runtime;this.signals=signals;this.active=active;this.clock=clock;
        registration=member.getCluster().addMembershipListener(this);
    }
    @Override public synchronized void memberAdded(MembershipEvent event){ }
    @Override public synchronized void memberRemoved(MembershipEvent event){
        Member peer=event.getMember();var key=resource(peer);if(key==null)return;
        var previous=monitored.get(key);
        if(previous!=null&&previous.memberId().equals(peer.getUuid())){
            monitored.put(key,new MemberState(key,peer.getUuid(),true));publish(monitored.get(key));
        }
    }
    synchronized void tick(){
        if(closed)return;
        var committed=membership.committed();if(committed==null)return;
        for(Member peer:member.getCluster().getMembers()){
            var key=resource(peer);if(key==null||!committed.activeNodeIds().contains(key.id()))continue;
            if(!monitored.containsKey(key)&&monitored.size()>=256){runtime.collectionUnavailable("member-budget");return;}
            var old=monitored.get(key);
            if(old==null||old.memberId().equals(peer.getUuid()))monitored.put(key,new MemberState(key,peer.getUuid(),false));
        }
        for(var state:List.copyOf(monitored.values()))publish(state);
    }
    private void publish(MemberState state){
        var original=membership.committed();if(original==null)return;
        boolean expected=original.activeNodeIds().contains(state.resource().id());
        String producer=BuiltinResourceSignals.digest(state.resource().id(),state.resource().incarnation());
        Runnable guard=()->{active.run();var current=membership.committed();
            if(closed||current==null||!current.equals(original)||membership.visibleNodeIds().isEmpty())throw ThreeLayerStorePort.refused();};
        signals.nodeEvidence(new AlertStore.NodeEvidence(state.resource(),state.resource().id(),state.resource().incarnation(),original.revision(),producer,guard,expected));
        if(expected)runtime.resourceInput(state.resource(),"cluster.node-unavailable",0,producer,guard,
                new AlertInput.Conditions(clock.instant(),state.removed()?AlertSignal.Condition.BAD:AlertSignal.Condition.GOOD,null));
    }
    private static AlarmKey.Resource resource(Member peer){
        String node=peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE),boot=peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE);
        return node==null||node.isBlank()||boot==null||boot.isBlank()||peer.getUuid()==null?null:new AlarmKey.Resource("cluster-node",node,boot);
    }
    @Override public synchronized void close(){closed=true;member.getCluster().removeMembershipListener(registration);monitored.clear();}
}
