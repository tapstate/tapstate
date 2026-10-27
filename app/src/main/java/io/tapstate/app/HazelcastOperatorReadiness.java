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
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageRecoveryCommands;
import io.tapstate.runtime.engine.OperatorMapBinding;
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
import java.util.function.BiFunction;
import java.util.function.Function;

/** Prepares the same original operator run on the exact data members the candidate was planned for. */
final class HazelcastOperatorReadiness {
    static final String EXECUTOR = "tapstate.operator-readiness";
    private final HazelcastInstance member;
    private final Duration timeout;
    private static final String RETIREMENT_CONTEXT = "tapstate.pipeline-retirement";
    private static final String SHARED_PREVIEW_CONTEXT = "tapstate.shared-srs-preview";
    private record LocalRetirement(ScopedPipelineCaptureCoordinator captures, Runnable active) {}
    private record LocalSharedPreview(SharedSrsPreviewMember preview,ScopedPipelineCaptureCoordinator captures) {}
    static void bindRetirement(HazelcastInstance member, ScopedPipelineCaptureCoordinator captures, Runnable active) {
        if (member.getUserContext().putIfAbsent(RETIREMENT_CONTEXT, new LocalRetirement(captures, active)) != null) throw fenced();
    }
    static void bindSharedPreview(HazelcastInstance member,SharedSrsPreviewMember preview,ScopedPipelineCaptureCoordinator captures) {
        if(member.getUserContext().putIfAbsent(SHARED_PREVIEW_CONTEXT,new LocalSharedPreview(preview,captures))!=null)throw fenced();
    }

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
        releaseHot(original, true);
    }
    void releaseRetiredHot(OperatorRunManifest original) { releaseHot(original, false); }
    private void releaseHot(OperatorRunManifest original, boolean flush) {
        requireOriginal(original);
        try {
            Snapshot before = snapshot(original.pipelineId());
            var names = original.bindings().stream().map(io.tapstate.runtime.engine.OperatorMapBinding::mapName)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            // Enumerate existing objects: getMap would recreate a missing old namespace during cleanup.
            for (var object : member.getDistributedObjects()) {
                if (object instanceof com.hazelcast.map.IMap<?,?> map && names.contains(object.getName())) {
                    if (flush) map.flush(); map.destroy();
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
    /** Stops only the fixed physical scope on each original data member and returns its retained hot descriptors. */
    List<OperatorRunManifest> isolate(PipelineIntentScope scope, WorkloadClaimFence ceiling, Runnable active) {
        var values = collect(scope, ceiling, true, active); var groups = new LinkedHashMap<ExecutionFence, java.util.Set<OperatorMapBinding>>();
        for (var binding : values) {
            if (ceiling == null) throw fenced();
            if (!binding.placement().scope().equals(scope) || !(binding.executionIdentity() instanceof ExecutionFence original)
                    || !scope.equals(original.scope()) || !original.operatorExecutionKey().equals(binding.executionKey())) throw fenced();
            ScopedPipelineCaptureCoordinator.requireNotLater(original, ceiling);
            groups.computeIfAbsent(original, ignored -> new java.util.LinkedHashSet<>()).add(binding);
        }
        return groups.entrySet().stream().map(entry -> new OperatorRunManifest(scope.pipelineId(), entry.getKey(), List.copyOf(entry.getValue()))).toList();
    }
    void requireRetired(PipelineIntentScope scope, WorkloadClaimFence ceiling, Runnable active) {
        if (!collect(scope, ceiling, false, active).isEmpty()) throw fenced();
    }
    /** All current data members must acknowledge this exact attempt; it is not business execution authority. */
    SharedTicket freezeShared(StorageRecoveryCommands.Plan original,Runnable active) {
        if(original==null || original.manifest().selection().extent()!=StorageRecoveryCommands.Extent.SHARED_SRS)throw fenced();
        var ticket=new SharedTicket(original,sharedSnapshot(original.manifest().selection().canonicalChain()).identities());
        ticket.call("FREEZE",active);ticket.requireHeld(original,active);return ticket;
    }
    final class SharedTicket {
        private final StorageRecoveryCommands.Plan original;
        private final SharedAttempt attempt;
        private final Map<String,Identity> expected;
        private volatile boolean captureStopped,tenuresEnded,endedVerified;
        private SharedTicket(StorageRecoveryCommands.Plan original,Map<String,Identity> expected) {
            if(original.manifest().selection().extent()!=StorageRecoveryCommands.Extent.SHARED_SRS || expected.isEmpty())throw fenced();
            this.original=original;this.attempt=SharedAttempt.from(original);this.expected=Map.copyOf(expected);
        }
        void stopCapture(Runnable active){captureStopped=false;tenuresEnded=false;endedVerified=false;call("STOP_CAPTURE",active);captureStopped=true;}
        void endOwnedTenures(Runnable active){if(!captureStopped)throw fenced();tenuresEnded=false;endedVerified=false;call("END_TENURES",active);tenuresEnded=true;}
        void verifyEnded(Runnable active){requireStaged(original,active);endedVerified=false;call("VERIFY",active);endedVerified=true;}
        void requireStaged(StorageRecoveryCommands.Plan selected,Runnable active){if(!captureStopped || !tenuresEnded)throw fenced();requireHeld(selected,active);}
        void requireVerified(StorageRecoveryCommands.Plan selected,Runnable active){if(!endedVerified)throw fenced();requireStaged(selected,active);}
        /** Local roster and receiver checks only, suitable for the original CONFIG guard. */
        void requireHeld(StorageRecoveryCommands.Plan selected,Runnable active) {
            active.run();
            if(!original.operationId().equals(selected.operationId()) || !original.originalScope().equals(selected.originalScope())
                    || !original.planHash().equals(selected.planHash()) || original.controlRevision()!=selected.controlRevision()
                    || !original.manifest().equals(selected.manifest()))throw fenced();
            if(!expected.equals(sharedSnapshot(capture()).identities()))throw sharedRefused(capture(),"cluster","membership-changed");
            if(!(member.getUserContext().get(SHARED_PREVIEW_CONTEXT) instanceof LocalSharedPreview local))throw fenced();
            local.preview().requireHeld(selected);active.run();
            if(!expected.equals(sharedSnapshot(capture()).identities()))throw sharedRefused(capture(),"cluster","membership-changed");
        }
        private String capture(){return original.manifest().selection().canonicalChain();}
        private void call(String action,Runnable active) {
            try {
                active.run();var before=sharedSnapshot(capture());
                if(!expected.equals(before.identities()))throw sharedRefused(capture(),"cluster","membership-changed");
                long deadline=System.nanoTime()+timeout.toNanos();
                var replies=member.getExecutorService(EXECUTOR).submitToMembers(new SharedCall(attempt,action,expected),before.members().values());
                for(var target:before.members().entrySet()) {
                    active.run();var result=answer(replies.get(target.getValue()),deadline,reason -> sharedRefused(capture(),target.getKey(),reason));
                    if(result==null || !expected.get(target.getKey()).equals(result.identity()))throw sharedRefused(capture(),target.getKey(),"identity-changed");
                    if(result.refusalCode()!=null)throw sharedRefused(capture(),target.getKey(),result.refusalCode());
                }
                active.run();if(!expected.equals(sharedSnapshot(capture()).identities()))throw sharedRefused(capture(),"cluster","membership-changed");
            } catch(HazelcastInstanceNotActiveException|TargetDisconnectedException|TargetNotMemberException|OperationTimeoutException unavailable) {
                throw sharedRefused(capture(),"cluster","member-unavailable");
            }
        }
    }
    private List<OperatorMapBinding> collect(PipelineIntentScope scope, WorkloadClaimFence ceiling, boolean stop, Runnable active) {
        try { return collectAvailable(scope,ceiling,stop,active); }
        catch (HazelcastInstanceNotActiveException | TargetDisconnectedException | TargetNotMemberException | OperationTimeoutException unavailable) {
            throw refused(scope.pipelineId(),"cluster","member-unavailable");
        }
    }
    private List<OperatorMapBinding> collectAvailable(PipelineIntentScope scope, WorkloadClaimFence ceiling, boolean stop, Runnable active) {
        active.run(); var before = snapshot(scope.pipelineId()); long deadline = System.nanoTime() + timeout.toNanos();
        var replies = member.getExecutorService(EXECUTOR).submitToMembers(new Collect(scope, ceiling, stop, before.identities()), before.members().values());
        var result = new java.util.ArrayList<OperatorMapBinding>();
        for (var target : before.members().entrySet()) {
            active.run(); var reply = answer(scope.pipelineId(), target.getKey(), replies.get(target.getValue()), deadline);
            if (reply == null || !before.identities().get(target.getKey()).equals(reply.identity())) throw fenced();
            if (reply.refusalCode() != null) throw refused(scope.pipelineId(), target.getKey(), reply.refusalCode());
            result.addAll(reply.bindings());
        }
        active.run(); if (!before.identities().equals(snapshot(scope.pipelineId()).identities())) throw fenced(); return List.copyOf(result);
    }
    private static void requireOriginal(OperatorRunManifest original) {
        Objects.requireNonNull(original,"original");
        if (!(original.executionIdentity() instanceof ExecutionFence fence) || fence.scope() == null
                || !fence.pipelineId().equals(original.pipelineId())
                || original.bindings().stream().anyMatch(binding -> !fence.scope().equals(binding.placement().scope())
                        || !fence.operatorExecutionKey().equals(binding.executionKey()))) throw fenced();
    }

    private Snapshot snapshot(String pipeline) {return snapshot((node,reason) -> refused(pipeline,node,reason));}
    private Snapshot sharedSnapshot(String capture) {
        try{return snapshot((node,reason) -> sharedRefused(capture,node,reason));}
        catch(HazelcastInstanceNotActiveException|TargetDisconnectedException|TargetNotMemberException|OperationTimeoutException unavailable) {
            throw sharedRefused(capture,"cluster","member-unavailable");
        }
    }
    private Snapshot snapshot(BiFunction<String,String,TapstateException> failure) {
        var members = new LinkedHashMap<String,Member>(); var identities = new LinkedHashMap<String,Identity>();
        for (var candidate : member.getCluster().getMembers()) {
            if (candidate.isLiteMember()) continue;
            Identity identity;
            try { identity = identity(candidate); }
            catch (TapstateException missing) { throw failure.apply(ClusterMembershipGate.stableIdOf(candidate),"identity-missing"); }
            if (members.putIfAbsent(identity.nodeId(),candidate) != null) throw failure.apply(identity.nodeId(),"identity-duplicate");
            identities.put(identity.nodeId(),identity);
        }
        return new Snapshot(Map.copyOf(members),Map.copyOf(identities));
    }
    private <T> T answer(String pipeline,String target,Future<T> future,long deadline) {
        return answer(future,deadline,reason -> refused(pipeline,target,reason));
    }
    private <T> T answer(Future<T> future,long deadline,Function<String,TapstateException> failure) {
        if (future == null) throw failure.apply("no-answer");
        try { return future.get(Math.max(0,deadline-System.nanoTime()),TimeUnit.NANOSECONDS); }
        catch (TimeoutException slow) { throw failure.apply("timeout"); }
        catch (CancellationException cancelled) { throw failure.apply("cancelled"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw failure.apply("interrupted"); }
        catch (ExecutionException failed) {
            Throwable cause = failed.getCause() == null ? failed : failed.getCause();
            if (cause instanceof MemberLeftException || cause instanceof HazelcastInstanceNotActiveException || cause instanceof TargetDisconnectedException
                    || cause instanceof TargetNotMemberException || cause instanceof OperationTimeoutException) throw failure.apply("member-unavailable");
            if (cause instanceof RuntimeException bug) throw bug;
            if (cause instanceof Error bug) throw bug;
            throw new IllegalStateException("unexpected member preparation failure",cause);
        }
    }
    record Identity(String nodeId,String bootId,UUID engineId) implements Serializable {}
    private record Snapshot(Map<String,Member> members,Map<String,Identity> identities) {}
    record Result(Identity identity,String refusalCode) implements Serializable {}
    record Collected(Identity identity, List<OperatorMapBinding> bindings, String refusalCode) implements Serializable {}
    /** Original lookup coordinates only; no credentials, model payloads, grants or serialized enums. */
    private record SharedAttempt(String cluster,long binding,String mode,String incarnation,long authGeneration,
            String operation,String hash,long revision) implements Serializable {
        static SharedAttempt from(StorageRecoveryCommands.Plan original) {
            var scope=original.originalScope();return new SharedAttempt(scope.clusterId(),scope.bindingVersion(),scope.mode().name(),
                    scope.cacheIncarnation(),scope.authGeneration(),original.operationId(),original.planHash(),original.controlRevision());
        }
        CacheAuthScope scope(){return new CacheAuthScope(cluster,binding,StorageBinding.Mode.valueOf(mode),incarnation,authGeneration);}
    }
    static final class SharedCall implements Callable<Result>,Serializable,HazelcastInstanceAware {
        private final SharedAttempt original;private final String action;private final Map<String,Identity> expected;
        private transient HazelcastInstance member;
        private SharedCall(SharedAttempt original,String action,Map<String,Identity> expected){this.original=original;this.action=action;this.expected=Map.copyOf(expected);}
        @Override public void setHazelcastInstance(HazelcastInstance member){this.member=member;}
        @Override public Result call() {
            Identity actual=null;
            try {
                actual=identity(member.getCluster().getLocalMember());
                if(!actual.equals(expected.get(actual.nodeId())) || !(member.getUserContext().get(SHARED_PREVIEW_CONTEXT) instanceof LocalSharedPreview local))throw fenced();
                var plan=local.preview().original(original.scope(),original.operation(),original.hash(),original.revision());
                var shared=plan.manifest().sharedSrs();
                switch(action) {
                    case "FREEZE" -> local.preview().freeze(plan);
                    case "STOP_CAPTURE" -> {local.preview().requireHeld(plan);local.captures().stopSharedLocally(shared.previous(),shared.captureFence());}
                    case "END_TENURES" -> local.preview().endOwnedTenures(plan);
                    case "VERIFY" -> {local.preview().requireEnded(plan);local.captures().requireSharedRetired(shared.previous(),shared.captureFence());}
                    default -> throw new IllegalStateException("unknown shared preview action");
                }
                local.preview().original(original.scope(),original.operation(),original.hash(),original.revision());
                return new Result(actual,null);
            } catch(TapstateException refused){
                org.slf4j.LoggerFactory.getLogger(HazelcastOperatorReadiness.class).debug(
                        "Shared recovery {} member action {} refused with {} at {}",original.operation(),action,
                        refused.code().code(),java.util.Arrays.toString(refused.getStackTrace()));
                return new Result(actual,refused.code().code());
            }
        }
    }
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
    static final class Collect implements Callable<Collected>, Serializable, HazelcastInstanceAware {
        private final PipelineIntentScope scope; private final Ceiling ceiling; private final boolean stop;
        private final Map<String,Identity> expected; private transient HazelcastInstance member;
        Collect(PipelineIntentScope scope, WorkloadClaimFence ceiling, boolean stop, Map<String,Identity> expected) {
            this.scope=scope; this.ceiling=ceiling==null?null:Ceiling.from(ceiling); this.stop=stop; this.expected=Map.copyOf(expected);
        }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member=member; }
        @Override public Collected call() {
            Identity actual=null;
            try {
                actual=identity(member.getCluster().getLocalMember());
                if (!actual.equals(expected.get(actual.nodeId())) || !(member.getUserContext().get(RETIREMENT_CONTEXT) instanceof LocalRetirement local)) throw fenced();
                local.active().run(); if (stop) local.captures().retireScope(scope,ceiling==null?null:ceiling.local());
                if (local.captures().hasScope(scope)) throw fenced();
                var bindings=io.tapstate.runtime.engine.OperatorMapBindings.resolve(member,true).originals(scope); local.active().run();
                return new Collected(actual,bindings,null);
            } catch (TapstateException refused) { return new Collected(actual,List.of(),refused.code().code()); }
        }
    }
    /** Carries coordinates as data; it is not a transported grant or an enum serialization. */
    private record Ceiling(String cluster,String pipeline,String node,String boot,long claim,long execution,long topology) implements Serializable {
        static Ceiling from(WorkloadClaimFence original) {
            if (original.key().type()!=io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION) throw fenced();
            return new Ceiling(original.key().clusterId(),original.key().resourceId(),original.owner().nodeId(),original.owner().bootId(),
                    original.claimGeneration(),original.executionGeneration(),original.topologyRevision());
        }
        WorkloadClaimFence local() {
            return new WorkloadClaimFence(new io.tapstate.spi.store.WorkloadClaimKey(cluster,io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION,pipeline),
                    new io.tapstate.spi.store.WorkloadOwner(node,boot),claim,execution,topology);
        }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
    private static TapstateException refused(String pipeline,String target,String reason) {
        return new TapstateException(ActuationError.OPERATOR_STATE_UNAVAILABLE_ON_MEMBER,Map.of("pipeline",pipeline,"member",target,"reason",reason),null);
    }
    private static TapstateException sharedRefused(String capture,String target,String reason) {
        return new TapstateException(ActuationError.SHARED_RECOVERY_MEMBER_UNAVAILABLE,Map.of("capture",capture,"member",target,"reason",reason),null);
    }
}
