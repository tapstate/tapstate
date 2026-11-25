package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.*;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.store.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

/** Bounded sink facts travel to the exact submitting member; no durable source position is manufactured. */
final class SnapshotLoadAcks implements SinkAckFactory {
    private static final long serialVersionUID=1L;
    static final String EXECUTOR="tapstate.snapshot-load-receipts";
    record Owner(String node,String boot,UUID member) implements Serializable {
        Owner {Objects.requireNonNull(node);Objects.requireNonNull(boot);Objects.requireNonNull(member);}
        boolean matches(Member actual) {
            return member.equals(actual.getUuid()) && node.equals(actual.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE))
                    && boot.equals(actual.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE));
        }
    }
    record Identity(ExecutionFence fence,Owner owner,Map<String,Long> epochs) implements Serializable {
        Identity {
            Objects.requireNonNull(fence);Objects.requireNonNull(owner);epochs=Map.copyOf(epochs);
            if(fence.scope()==null || !owner.node().equals(fence.ownerNodeId()) || !owner.boot().equals(fence.ownerBootId()))throw refused();
            new SnapshotRecoveryFrontier(epochs);
        }
    }
    private final Identity identity;
    SnapshotLoadAcks(Identity identity){this.identity=Objects.requireNonNull(identity);}
    @Override public void beginRun(HazelcastInstance coordinator,Map<String,List<String>> roster,WorkloadClaimFence claim) {
        if(!new ExecutionFence(identity.fence().scope(),claim).equals(identity.fence()))throw refused();
        var copy=new TreeMap<String,List<String>>();roster.forEach((stream,writers) -> copy.put(stream,List.copyOf(writers)));
        if(!send(coordinator,new Delivery(identity,Map.copyOf(copy),null,null,null)))throw refused();
    }
    @Override public SinkAck resolve(HazelcastInstance member) {
        var receipts=SnapshotLoadReceipts.of(member);
        return new Writer(receipts,identity,null);
    }
    private static final class Writer implements SinkAck {
        private static final long serialVersionUID=1L;
        private final transient SnapshotLoadReceipts receipts;
        private final Identity identity;
        private final String writer;
        Writer(SnapshotLoadReceipts receipts,Identity identity,String writer){this.receipts=receipts;this.identity=identity;this.writer=writer;}
        @Override public void advance(String stream,ChainPosition position) { }
        @Override public SinkAck forWriter(String id){if(id==null || id.isBlank())throw refused();return new Writer(receipts,identity,id);}
        @Override public void bounded(String stream,SourceOrder through,WorkloadClaimFence claim) {
            if(writer==null || !new ExecutionFence(identity.fence().scope(),claim).equals(identity.fence()))throw refused();
            receipts.enqueue(identity,stream,writer,through);
        }
    }
    static boolean notify(HazelcastInstance member,Identity identity,String stream,String writer,SourceOrder through) {
        return send(member,new Delivery(identity,null,stream,writer,through));
    }
    private static boolean send(HazelcastInstance member,Delivery delivery) {
        try {
            var targets=member.getCluster().getMembers().stream().filter(delivery.identity.owner()::matches).toList();
            if(targets.size()!=1)throw refused();
            var target=targets.getFirst();
            var answers=member.getExecutorService(EXECUTOR).submitToMembers(delivery,List.of(target));
            var answer=answers.get(target);if(answer==null)throw refused();
            return Boolean.TRUE.equals(answer.get(5,TimeUnit.SECONDS));
        } catch(InterruptedException interrupted) {
            Thread.currentThread().interrupt();throw refused();
        } catch(TimeoutException | CancellationException | RejectedExecutionException | HazelcastInstanceNotActiveException
                | MemberLeftException | TargetDisconnectedException | TargetNotMemberException | OperationTimeoutException unavailable) {
            throw refused();
        } catch(ExecutionException failed) {
            var cause=failed.getCause();
            if(cause instanceof MemberLeftException || cause instanceof TargetDisconnectedException || cause instanceof TargetNotMemberException
                    || cause instanceof OperationTimeoutException || cause instanceof HazelcastInstanceNotActiveException)throw refused();
            if(cause instanceof Error error)throw error;
            throw new IllegalStateException("bounded-load receiver failed",cause);
        }
    }
    private static final class Delivery implements Callable<Boolean>,Serializable,HazelcastInstanceAware {
        private static final long serialVersionUID=1L;
        private final Identity identity;
        private final Map<String,List<String>> roster;
        private final String stream,writer;
        private final SourceOrder through;
        private transient HazelcastInstance member;
        Delivery(Identity identity,Map<String,List<String>> roster,String stream,String writer,SourceOrder through) {
            this.identity=identity;this.roster=roster;this.stream=stream;this.writer=writer;this.through=through;
        }
        @Override public void setHazelcastInstance(HazelcastInstance member){this.member=member;}
        @Override public Boolean call() {
            try {
                if(!identity.owner().matches(member.getCluster().getLocalMember()))return false;
                var receiver=SnapshotLoadReceipts.of(member);
                return roster!=null?receiver.begin(identity,roster):receiver.bounded(identity,stream,writer,through);
            } catch(TapstateException unavailable) {return false;}
        }
    }
    static TapstateException refused(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
}
