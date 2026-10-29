package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.*;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationOwnerRanking;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.ValidationClaims;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Bounded owner-reservation routing to one actual committed member. A visible member alone is not eligible.
 * Existing ownership is always reused; only an explicit, side-effect-free capacity refusal permits
 * another ranked candidate. Unknown replies retain the original request and never start a second
 * attempt. Receivers load saved configuration and obtain native authority themselves; these wire
 * coordinates grant no claim, business-read permission or right to reconstruct an old local run.
 * ACCEPTED means only that an owner/envelope is reserved. This phase never creates a CONFIG launch
 * ticket or starts a Job, business query or key subscription. Activation requires the separate exact
 * saved ticket and original owner-local reservation; an unknown reservation cannot activate itself.
 */
final class ValidationOwnerRouter implements AutoCloseable {
    static final String EXECUTOR = "tapstate.validation-control";
    private static final String CONTEXT = "tapstate.validation-control.receiver";

    record Request(String pipeline, String task, UUID taskGeneration, long configurationRevision, long startSequence,
                   String mode, UUID command, String run) implements Serializable {
        Request {
            id(pipeline); id(task); id(run); Objects.requireNonNull(taskGeneration); Objects.requireNonNull(command);
            if (configurationRevision < 1 || configurationRevision == Long.MAX_VALUE || startSequence < 1 || startSequence == Long.MAX_VALUE
                    || mode == null || !Set.of("FULL", "INCREMENTAL").contains(mode)) throw invalid("routing.request");
        }
    }
    enum Outcome { ACCEPTED, CAPACITY, BUSY, REDIRECT, REJECTED, UNKNOWN }
    record Identity(String node, String boot, UUID member) implements Serializable {
        Identity { id(node); id(boot); Objects.requireNonNull(member); }
        boolean matches(Member value) {
            return !value.isLiteMember() && member.equals(value.getUuid())
                    && node.equals(value.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE))
                    && boot.equals(value.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE));
        }
    }
    record Reply(Outcome outcome, UUID command, Identity receiver, long claimGeneration,
                 long executionGeneration, long topologyRevision, String reasonCode) implements Serializable {
        Reply {
            Objects.requireNonNull(outcome); Objects.requireNonNull(command);
            if (outcome == Outcome.ACCEPTED && (receiver == null || claimGeneration < 1 || executionGeneration < 1 || topologyRevision < 0)
                    || outcome != Outcome.ACCEPTED && (claimGeneration != 0 || executionGeneration != 0 || topologyRevision != 0)
                    || reasonCode != null && !reasonCode.matches("[a-z][a-z0-9.-]{0,127}")) throw invalid("routing.reply");
        }
    }
    record Result(Outcome outcome, ValidationOwner owner, String reasonCode) {
        Result {
            Objects.requireNonNull(outcome);
            if (outcome == Outcome.ACCEPTED != (owner != null)) throw invalid("routing.localResult");
        }
        static Result capacity() { return new Result(Outcome.CAPACITY, null, null); }
    }
    record Receiver(UUID incarnation, Identity identity, long topologyRevision) implements Serializable {
        Receiver { Objects.requireNonNull(incarnation); Objects.requireNonNull(identity); if (topologyRevision < 1) throw invalid("routing.receiver"); }
    }
    @FunctionalInterface interface Local {
        /** CAPACITY/REDIRECT must prove no claim, ticket, Job or unknown allocation was retained. */
        Result reserve(Request request);
    }
    enum Action { ACTIVATE,STATUS,STOP }
    /** Immutable original coordinates only. No spec, executable object, model or connection crosses the wire. */
    record Command(Action action,Request request,String cluster,String node,String boot,long claim,long execution,long topology) implements Serializable {
        Command{Objects.requireNonNull(action);Objects.requireNonNull(request);id(cluster);id(node);id(boot);
            if(claim<1||execution<1||topology<0)throw invalid("routing.commandOwner");}
        ValidationOwner owner(){return new ValidationOwner(cluster,request.pipeline(),node,boot,claim,execution,topology);}
        static Command of(Action action,Request request,ValidationOwner owner){return new Command(action,request,owner.clusterId(),owner.nodeId(),owner.bootId(),owner.claimGeneration(),owner.executionGeneration(),owner.topologyRevision());}
    }
    record CommandReply(Outcome outcome,Command original,String state,boolean admitted,boolean waiting,boolean unknown,boolean retired,boolean drained,String reasonCode,List<ValidationRunLifecycle.Table> tables) implements Serializable {
        CommandReply{Objects.requireNonNull(outcome);Objects.requireNonNull(original);
            tables=List.copyOf(tables);if(tables.size()>io.tapstate.core.validation.ValidationTaskSpec.MAX_MAPPINGS)throw invalid("routing.tableProgress");
            if(outcome==Outcome.ACCEPTED){if(state==null)throw invalid("routing.commandState");io.tapstate.core.validation.ValidationRunState.valueOf(state);}
            else if(state!=null||admitted||waiting||unknown||retired||drained||!tables.isEmpty())throw invalid("routing.commandReply");
            if(reasonCode!=null&&!reasonCode.matches("[a-z][a-z0-9.-]{0,127}"))throw invalid("routing.commandReason");}
        CommandReply(Outcome outcome,Command original,String state,boolean admitted,boolean waiting,boolean unknown,boolean retired,boolean drained,String reasonCode){
            this(outcome,original,state,admitted,waiting,unknown,retired,drained,reasonCode,List.of());}
    }
    @FunctionalInterface interface Commands {CommandReply execute(Command command);}
    private record Roster(String cluster, long revision, Map<String, Identity> identities, Map<String, Member> members) {
        boolean same(Roster other) { return cluster.equals(other.cluster) && revision == other.revision && identities.equals(other.identities); }
    }

    private final HazelcastInstance member;
    private final ClusterMembershipGate membership;
    private final Supplier<WorkloadClaimFence> node;
    private final Function<String, Optional<WorkloadClaimReading>> readOwner;
    private final Local local;
    private final Commands commands;
    private final Runnable active;
    private final long timeoutNanos;
    private final UUID incarnation = UUID.randomUUID();
    private volatile boolean closed;

    ValidationOwnerRouter(HazelcastInstance member, ClusterMembershipGate membership,
            Supplier<WorkloadClaimFence> node, Function<String, Optional<WorkloadClaimReading>> readOwner,
            Local local, Runnable active, Duration timeout) {
        this(member,membership,node,readOwner,local,command->commandUnavailable(command,Outcome.REJECTED,null),active,timeout);
    }
    ValidationOwnerRouter(HazelcastInstance member,ClusterMembershipGate membership,Supplier<WorkloadClaimFence> node,
            Function<String,Optional<WorkloadClaimReading>> readOwner,Local local,Commands commands,Runnable active,Duration timeout){
        this.member = Objects.requireNonNull(member); this.membership = Objects.requireNonNull(membership);
        this.node = Objects.requireNonNull(node); this.readOwner = Objects.requireNonNull(readOwner);
        this.local = Objects.requireNonNull(local); this.active = Objects.requireNonNull(active);
        this.commands=Objects.requireNonNull(commands);
        if (timeout == null || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofSeconds(30)) > 0) throw invalid("routing.timeout");
        timeoutNanos = timeout.toNanos();
        var executor = member.getConfig().findExecutorConfig(EXECUTOR);
        if (executor.getPoolSize() < 1 || executor.getPoolSize() > 4 || executor.getQueueCapacity() < 1 || executor.getQueueCapacity() > 64)
            throw invalid("routing.executorBudget");
        if (member.getUserContext().putIfAbsent(CONTEXT, this) != null) throw invalid("routing.alreadyBound");
    }

    /** One exact original owner only; missing/unknown status never causes a reservation or replacement start. */
    CommandReply command(Command command){
        Objects.requireNonNull(command);var before=roster();
        var target=before.identities.get(command.node());
        if(!before.cluster.equals(command.cluster())||target==null||!target.boot().equals(command.boot())||command.topology()>before.revision)
            return commandUnavailable(command,Outcome.REJECTED,null);
        try{
            var executor=member.getExecutorService(EXECUTOR);long deadline=System.nanoTime()+timeoutNanos;
            var destination=before.members.get(target.node());
            var receiver=executor.submitToMember(new Probe(target,before.revision),destination).get(timeoutNanos,TimeUnit.NANOSECONDS);
            if(receiver==null||!target.equals(receiver.identity())||before.revision!=receiver.topologyRevision())return commandUnavailable(command,Outcome.REJECTED,null);
            long remaining=deadline-System.nanoTime();if(remaining<=0)return commandUnavailable(command,Outcome.UNKNOWN,null);
            var answer=executor.submitToMember(new CommandCall(command,target,before.revision,receiver.incarnation()),destination).get(remaining,TimeUnit.NANOSECONDS);
            requireActive();if(!before.same(roster())||answer==null||!command.equals(answer.original()))return commandUnavailable(command,Outcome.UNKNOWN,null);
            return answer;
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();return commandUnavailable(command,Outcome.UNKNOWN,null);}
        catch(TimeoutException|CancellationException|RejectedExecutionException|HazelcastInstanceNotActiveException|MemberLeftException
                |TargetDisconnectedException|TargetNotMemberException|OperationTimeoutException unavailable){return commandUnavailable(command,Outcome.UNKNOWN,null);}
        catch(ExecutionException failed){
            var cause=failed.getCause();if(cause instanceof TapstateException coded)return commandUnavailable(command,Outcome.UNKNOWN,coded.code().code());
            if(cause instanceof MemberLeftException||cause instanceof TargetDisconnectedException||cause instanceof TargetNotMemberException
                    ||cause instanceof OperationTimeoutException||cause instanceof HazelcastInstanceNotActiveException||cause instanceof RejectedExecutionException)
                return commandUnavailable(command,Outcome.UNKNOWN,null);
            if(cause instanceof Error fatal)throw fatal;throw new IllegalStateException("validation original command failed",cause);
        }catch(TapstateException unavailable){return commandUnavailable(command,Outcome.UNKNOWN,unavailable.code().code());}
    }
    private CommandReply receive(Command command,Identity expected,long revision){
        var current=roster();
        if(current.revision!=revision||!expected.matches(member.getCluster().getLocalMember())||!expected.equals(current.identities.get(expected.node))
                ||!command.cluster().equals(current.cluster)||!command.node().equals(expected.node)||!command.boot().equals(expected.boot)
                ||command.topology()>current.revision)return commandUnavailable(command,Outcome.REJECTED,null);
        var answer=Objects.requireNonNull(commands.execute(command));requireActive();
        return current.same(roster())&&command.equals(answer.original())?answer:commandUnavailable(command,Outcome.UNKNOWN,null);
    }
    static CommandReply commandUnavailable(Command command,Outcome outcome,String reason){return new CommandReply(outcome,command,null,false,false,false,false,false,reason);}

    Reply reserve(Request request) {
        Objects.requireNonNull(request); var before = roster(); long deadline = System.nanoTime() + timeoutNanos;
        var tried = new HashSet<Identity>();
        for (int attempt = 0; attempt <= before.members.size(); attempt++) {
            requireActive(); if (!before.same(roster()) || System.nanoTime() - deadline >= 0) return unknown(request, null, null);
            var reading = Objects.requireNonNull(readOwner.apply(request.pipeline()));
            reading.ifPresent(value -> {
                var key = value.claim().key();
                if (!key.equals(ValidationClaims.key(before.cluster, request.pipeline())))
                    throw invalid("routing.claim");
            });
            boolean owned = reading.filter(WorkloadClaimReading::leased).isPresent();
            Identity selected;
            if (owned) {
                var claim = reading.orElseThrow().claim();
                selected = before.identities.get(claim.owner().nodeId());
                if (selected == null || !selected.boot.equals(claim.owner().bootId()) || tried.contains(selected)) return unknown(request, selected, null);
            } else {
                selected = ValidationOwnerRanking.rank(before.cluster, request.pipeline(), before.identities.keySet()).stream()
                        .map(before.identities::get).filter(value -> !tried.contains(value)).findFirst().orElse(null);
                if (selected == null) return new Reply(Outcome.CAPACITY, request.command(), null, 0, 0, 0, null);
            }
            tried.add(selected);
            var reply = invoke(request, selected, before.revision, before.members.get(selected.node), deadline);
            try { requireActive(); if (!before.same(roster())) return unknown(request, selected, reply.reasonCode()); }
            catch (TapstateException unavailable) { return unknown(request, selected, unavailable.code().code()); }
            if (reply.outcome() == Outcome.CAPACITY && !owned || reply.outcome() == Outcome.REDIRECT) continue;
            return reply;
        }
        return unknown(request, null, null);
    }

    private Reply invoke(Request request, Identity target, long revision, Member destination, long deadline) {
        try {
            long remaining = deadline - System.nanoTime(); if (remaining <= 0) return unknown(request, target, null);
            var executor = member.getExecutorService(EXECUTOR);
            var receiver = executor.submitToMember(new Probe(target, revision), destination).get(remaining, TimeUnit.NANOSECONDS);
            if (receiver == null) return rejected(request, target, null);
            if (!target.equals(receiver.identity()) || revision != receiver.topologyRevision()) return unknown(request, target, null);
            remaining = deadline - System.nanoTime(); if (remaining <= 0) return unknown(request, target, null);
            var future = executor.submitToMember(new Call(request, target, revision, receiver.incarnation()), destination);
            var reply = future.get(remaining, TimeUnit.NANOSECONDS);
            if (reply == null || !request.command().equals(reply.command()) || !target.equals(reply.receiver())) return unknown(request, target, null);
            return reply;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); return unknown(request, target, null);
        } catch (TimeoutException | CancellationException | RejectedExecutionException | HazelcastInstanceNotActiveException
                | MemberLeftException | TargetDisconnectedException | TargetNotMemberException | OperationTimeoutException unavailable) {
            return unknown(request, target, null);
        } catch (ExecutionException failed) {
            var cause = failed.getCause();
            if (cause instanceof TapstateException coded) return unknown(request, target, coded.code().code());
            if (cause instanceof MemberLeftException || cause instanceof TargetDisconnectedException || cause instanceof TargetNotMemberException
                    || cause instanceof OperationTimeoutException || cause instanceof HazelcastInstanceNotActiveException
                    || cause instanceof RejectedExecutionException) return unknown(request, target, null);
            if (cause instanceof Error fatal) throw fatal;
            throw new IllegalStateException("validation control receiver failed", cause);
        }
    }

    private Reply receive(Request request, Identity expected, long revision) {
        boolean entered = false;
        try {
            var current = roster();
            if (current.revision != revision || !expected.matches(member.getCluster().getLocalMember())
                    || !expected.equals(current.identities.get(expected.node))) return rejected(request, expected, null);
            entered = true; var result = Objects.requireNonNull(local.reserve(request));
            requireActive(); if (!current.same(roster())) return unknown(request, expected, result.reasonCode());
            var owner = result.owner();
            if (owner != null && (!owner.clusterId().equals(current.cluster) || !owner.pipelineId().equals(request.pipeline())
                    || !owner.nodeId().equals(expected.node) || !owner.bootId().equals(expected.boot)
                    || owner.topologyRevision() > current.revision)) throw invalid("routing.resultOwner");
            return new Reply(result.outcome(), request.command(), expected, owner == null ? 0 : owner.claimGeneration(),
                    owner == null ? 0 : owner.executionGeneration(), owner == null ? 0 : owner.topologyRevision(), result.reasonCode());
        } catch (TapstateException unavailable) {
            return entered ? unknown(request, expected, unavailable.code().code()) : rejected(request, expected, unavailable.code().code());
        }
    }

    private Receiver probe(Identity expected, long revision) {
        try {
            var current = roster();
            if (current.revision != revision || !expected.matches(member.getCluster().getLocalMember())
                    || !expected.equals(current.identities.get(expected.node))) return null;
            return new Receiver(incarnation, expected, revision);
        } catch (TapstateException unavailable) { return null; }
    }

    private Roster roster() {
        requireActive(); var committed = membership.committed();
        if (committed == null || !membership.businessEligible()) throw unauthorized();
        var identities = new TreeMap<String, Identity>(); var members = new TreeMap<String, Member>();
        for (var candidate : member.getCluster().getMembers()) {
            if (candidate.isLiteMember()) continue;
            var stable = candidate.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE);
            if (stable == null || !committed.activeNodeIds().contains(stable)) continue;
            var boot = candidate.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE);
            if (boot == null || boot.isBlank()) continue;
            var identity = new Identity(stable, boot, candidate.getUuid());
            if (identities.put(stable, identity) != null) throw unauthorized();
            members.put(stable, candidate);
        }
        if (members.isEmpty() || members.size() > 1024) throw unauthorized();
        return new Roster(committed.clusterId(), committed.revision(), Map.copyOf(identities), Map.copyOf(members));
    }
    private void requireActive() {
        if (closed) throw unauthorized(); active.run();
        var proof = Objects.requireNonNull(node.get()); var actual = member.getCluster().getLocalMember();
        if (proof.key().type() != WorkloadClaimType.NODE_SESSION || !proof.key().resourceId().equals(proof.owner().nodeId())
                || !proof.owner().nodeId().equals(actual.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE))
                || !proof.owner().bootId().equals(actual.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE))) throw unauthorized();
        var committed = membership.committed();
        if (committed == null || !committed.clusterId().equals(proof.key().clusterId()) || !committed.activeNodeIds().contains(proof.owner().nodeId())) throw unauthorized();
    }
    @Override public void close() { closed = true; member.getUserContext().remove(CONTEXT, this); }
    private static Reply unknown(Request request, Identity target, String reason) { return new Reply(Outcome.UNKNOWN, request.command(), target, 0, 0, 0, reason); }
    private static Reply rejected(Request request, Identity target, String reason) { return new Reply(Outcome.REJECTED, request.command(), target, 0, 0, 0, reason); }
    private static void id(String value) { if (value == null || value.isBlank()) throw invalid("routing.identity"); ValidationError.fieldName(value, "routing.identity"); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException unauthorized() { return new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "validation-control"), null); }

    static final class Call implements Callable<Reply>, Serializable, HazelcastInstanceAware {
        private static final long serialVersionUID = 1L;
        private final Request request; private final Identity expected; private final long revision;
        private final UUID receiver;
        private transient HazelcastInstance member;
        Call(Request request, Identity expected, long revision, UUID receiver) {
            this.request = request; this.expected = expected; this.revision = revision; this.receiver = Objects.requireNonNull(receiver);
        }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member = member; }
        @Override public Reply call() {
            Object receiver = member.getUserContext().get(CONTEXT);
            return receiver instanceof ValidationOwnerRouter router && this.receiver.equals(router.incarnation)
                    ? router.receive(request, expected, revision) : rejected(request, expected, null);
        }
    }
    static final class Probe implements Callable<Receiver>, Serializable, HazelcastInstanceAware {
        private static final long serialVersionUID = 1L;
        private final Identity expected; private final long revision;
        private transient HazelcastInstance member;
        Probe(Identity expected, long revision) { this.expected = expected; this.revision = revision; }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member = member; }
        @Override public Receiver call() {
            Object receiver = member.getUserContext().get(CONTEXT);
            return receiver instanceof ValidationOwnerRouter router ? router.probe(expected, revision) : null;
        }
    }
    static final class CommandCall implements Callable<CommandReply>,Serializable,HazelcastInstanceAware{
        private static final long serialVersionUID=1L;
        private final Command command;private final Identity expected;private final long revision;private final UUID receiver;
        private transient HazelcastInstance member;
        CommandCall(Command command,Identity expected,long revision,UUID receiver){this.command=Objects.requireNonNull(command);this.expected=Objects.requireNonNull(expected);this.revision=revision;this.receiver=Objects.requireNonNull(receiver);}
        @Override public void setHazelcastInstance(HazelcastInstance member){this.member=member;}
        @Override public CommandReply call(){var context=member.getUserContext().get(CONTEXT);
            return context instanceof ValidationOwnerRouter router&&receiver.equals(router.incarnation)?router.receive(command,expected,revision):commandUnavailable(command,Outcome.REJECTED,null);}
    }
}
