package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.OperatorMapCapacity;
import io.tapstate.runtime.engine.OperatorMapConfigurations;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.IoError;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Prepares the same original operator run on the exact data members the candidate was planned for. */
final class HazelcastOperatorReadiness {
    static final String EXECUTOR = "tapstate.operator-readiness";
    private final HazelcastInstance member;
    private final Duration timeout;

    HazelcastOperatorReadiness(HazelcastInstance member,Duration timeout) {
        this.member = Objects.requireNonNull(member,"member"); this.timeout = Objects.requireNonNull(timeout,"timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("operator preparation needs a positive timeout");
        timeout.toNanos();
    }

    void prepare(OperatorRunManifest original,Collection<OperatorMapCapacity> capacities,Collection<String> plannedMembers,Runnable requireActive) {
        Objects.requireNonNull(original,"original"); Objects.requireNonNull(requireActive,"requireActive");
        var captured = List.copyOf(capacities); var planned = List.copyOf(plannedMembers);
        if (!(original.executionIdentity() instanceof ExecutionFence fence) || fence.scope() == null
                || !fence.pipelineId().equals(original.pipelineId())
                || original.bindings().stream().anyMatch(binding -> !fence.scope().equals(binding.placement().scope())
                        || !fence.operatorExecutionKey().equals(binding.executionKey()))) throw fenced();
        requireActive.run();
        try {
            Snapshot before = snapshot(original.pipelineId());
            if (planned.isEmpty() || Set.copyOf(planned).size() != planned.size() || !before.identities().keySet().equals(Set.copyOf(planned)))
                throw refused(original.pipelineId(),"cluster","membership-changed");
            long deadline = System.nanoTime()+timeout.toNanos();
            Map<Member,Future<Result>> answers = member.getExecutorService(EXECUTOR).submitToMembers(
                    new Install(original,captured,before.identities()),before.members().values());
            for (var target : before.members().entrySet()) {
                requireActive.run(); Result result = answer(original.pipelineId(),target.getKey(),answers.get(target.getValue()),deadline);
                if (result == null || !before.identities().get(target.getKey()).equals(result.identity()))
                    throw refused(original.pipelineId(),target.getKey(),"identity-changed");
                if (result.refusalCode() != null) throw refused(original.pipelineId(),target.getKey(),result.refusalCode());
            }
            requireActive.run();
            if (!before.identities().equals(snapshot(original.pipelineId()).identities()))
                throw refused(original.pipelineId(),"cluster","membership-changed");
            requireActive.run();
        } catch (HazelcastInstanceNotActiveException | TargetDisconnectedException | TargetNotMemberException | OperationTimeoutException unavailable) {
            throw refused(original.pipelineId(),"cluster","member-unavailable");
        }
    }

    /** The caller has already stopped the original writer. Only its exact hot names are released. */
    void releaseHot(OperatorRunManifest original) {
        requireOriginal(original);
        try {
            Snapshot before = snapshot(original.pipelineId());
            var names = original.bindings().stream().map(io.tapstate.runtime.engine.OperatorMapBinding::mapName)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            // Enumerate existing objects: getMap would recreate a missing old namespace during cleanup.
            for (var object : member.getDistributedObjects()) {
                if (object instanceof com.hazelcast.map.IMap<?,?> map && names.contains(object.getName())) {
                    map.flush(); map.destroy();
                }
            }
            long deadline = System.nanoTime()+timeout.toNanos();
            Map<Member,Future<Result>> answers = member.getExecutorService(EXECUTOR).submitToMembers(
                    new Release(original,before.identities()),before.members().values());
            for (var target : before.members().entrySet()) {
                Result result = answer(original.pipelineId(),target.getKey(),answers.get(target.getValue()),deadline);
                if (result == null || !before.identities().get(target.getKey()).equals(result.identity()))
                    throw refused(original.pipelineId(),target.getKey(),"identity-changed");
                if (result.refusalCode() != null) throw refused(original.pipelineId(),target.getKey(),result.refusalCode());
            }
            if (!before.identities().equals(snapshot(original.pipelineId()).identities()))
                throw refused(original.pipelineId(),"cluster","membership-changed");
        } catch (HazelcastInstanceNotActiveException | TargetDisconnectedException | TargetNotMemberException | OperationTimeoutException unavailable) {
            throw refused(original.pipelineId(),"cluster","member-unavailable");
        }
    }
    private static void requireOriginal(OperatorRunManifest original) {
        Objects.requireNonNull(original,"original");
        if (!(original.executionIdentity() instanceof ExecutionFence fence) || fence.scope() == null
                || !fence.pipelineId().equals(original.pipelineId())
                || original.bindings().stream().anyMatch(binding -> !fence.scope().equals(binding.placement().scope())
                        || !fence.operatorExecutionKey().equals(binding.executionKey()))) throw fenced();
    }

    private Snapshot snapshot(String pipeline) {
        var members = new LinkedHashMap<String,Member>(); var identities = new LinkedHashMap<String,Identity>();
        for (var candidate : member.getCluster().getMembers()) {
            if (candidate.isLiteMember()) continue;
            Identity identity;
            try { identity = identity(candidate); }
            catch (TapstateException missing) { throw refused(pipeline,ClusterMembershipGate.stableIdOf(candidate),"identity-missing"); }
            if (members.putIfAbsent(identity.nodeId(),candidate) != null) throw refused(pipeline,identity.nodeId(),"identity-duplicate");
            identities.put(identity.nodeId(),identity);
        }
        return new Snapshot(Map.copyOf(members),Map.copyOf(identities));
    }
    private Result answer(String pipeline,String target,Future<Result> future,long deadline) {
        if (future == null) throw refused(pipeline,target,"no-answer");
        try { return future.get(Math.max(0,deadline-System.nanoTime()),TimeUnit.NANOSECONDS); }
        catch (TimeoutException slow) { throw refused(pipeline,target,"timeout"); }
        catch (CancellationException cancelled) { throw refused(pipeline,target,"cancelled"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw refused(pipeline,target,"interrupted"); }
        catch (ExecutionException failed) {
            Throwable cause = failed.getCause() == null ? failed : failed.getCause();
            if (cause instanceof MemberLeftException || cause instanceof HazelcastInstanceNotActiveException || cause instanceof TargetDisconnectedException
                    || cause instanceof TargetNotMemberException || cause instanceof OperationTimeoutException) throw refused(pipeline,target,"member-unavailable");
            if (cause instanceof RuntimeException bug) throw bug;
            if (cause instanceof Error bug) throw bug;
            throw new IllegalStateException("unexpected member preparation failure",cause);
        }
    }
    record Identity(String nodeId,String bootId,UUID engineId) implements Serializable {}
    private record Snapshot(Map<String,Member> members,Map<String,Identity> identities) {}
    record Result(Identity identity,String refusalCode) implements Serializable {}
    private static Identity identity(Member member) {
        String node = member.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE), boot = member.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE);
        if (node == null || node.isBlank() || boot == null || boot.isBlank() || member.getUuid() == null) throw fenced();
        return new Identity(node,boot,member.getUuid());
    }
    static final class Install implements Callable<Result>,Serializable,HazelcastInstanceAware {
        private final OperatorRunManifest original;
        private final List<OperatorMapCapacity> capacities;
        private final Map<String,Identity> expected;
        private transient HazelcastInstance member;
        Install(OperatorRunManifest original,List<OperatorMapCapacity> capacities,Map<String,Identity> expected) {
            this.original = Objects.requireNonNull(original,"original"); this.capacities = List.copyOf(capacities); this.expected = Map.copyOf(expected);
        }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member = member; }
        @Override public Result call() {
            Identity actual = null;
            try {
                actual = identity(member.getCluster().getLocalMember());
                if (!actual.equals(expected.get(actual.nodeId()))) throw fenced();
                OperatorMapConfigurations.install(member,original,capacities);
                return new Result(actual,null);
            } catch (TapstateException refused) {
                // Carry only the canonical code, never the enum, credentials or storage exception arguments.
                return new Result(actual,refused.code().code());
            }
        }
    }
    static final class Release implements Callable<Result>,Serializable,HazelcastInstanceAware {
        private final OperatorRunManifest original;
        private final Map<String,Identity> expected;
        private transient HazelcastInstance member;
        Release(OperatorRunManifest original,Map<String,Identity> expected) {
            this.original = Objects.requireNonNull(original,"original"); this.expected = Map.copyOf(expected);
        }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member = member; }
        @Override public Result call() {
            Identity actual = null;
            try {
                actual = identity(member.getCluster().getLocalMember());
                if (!actual.equals(expected.get(actual.nodeId()))) throw fenced();
                requireOriginal(original);
                var registry = io.tapstate.runtime.engine.OperatorMapBindings.resolve(member,true);
                // A later slot is untouched; an already released original is an idempotent result.
                for (var binding : original.bindings()) registry.release(binding);
                return new Result(actual,null);
            } catch (TapstateException refused) { return new Result(actual,refused.code().code()); }
        }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
    private static TapstateException refused(String pipeline,String target,String reason) {
        return new TapstateException(ActuationError.OPERATOR_STATE_UNAVAILABLE_ON_MEMBER,Map.of("pipeline",pipeline,"member",target,"reason",reason),null);
    }
}
