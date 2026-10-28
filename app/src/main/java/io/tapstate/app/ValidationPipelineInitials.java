package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.*;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.*;
import io.tapstate.runtime.srs.SourceInitialLoadObserver;
import io.tapstate.spi.store.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.function.Function;

/** Bounded volatile initial-output witnesses; normal source/writer threads never perform transport. */
final class ValidationPipelineInitials implements SourceInitialLoadObserver, AutoCloseable {
    static final String CONTEXT = "tapstate.pipeline-initials", EXECUTOR = "tapstate.pipeline-initials", LOOKUP_EXECUTOR="tapstate.pipeline-initial-lookups";
    private static final long MAX_PENDING_BYTES = 16L * 1024 * 1024, MAX_STATE_BYTES = 32L * 1024 * 1024;
    private record Node(String node, String boot, UUID member) implements Serializable {
        boolean matches(Member value) { return member.equals(value.getUuid()) && node.equals(value.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)) && boot.equals(value.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)); }
    }
    private record Original(ExecutionFence fence, Node coordinator, UUID token) implements Serializable {}
    record Attempt(long job, long execution) implements Serializable {}
    private record Key(UUID token, Attempt attempt, String kind, String stream, String writer, int index, UUID producer) {}
    private record Packet(Original original, Attempt attempt, String kind, String stream, String writer,
                          String mode, long epoch, Node producer, int index, SinkAckFactory.RunWriters roster) implements Serializable {
        Key key() { return new Key(original.token(), attempt, kind, stream, writer, index, producer == null ? null : producer.member()); }
        long bytes() {
            long size = 4096L + 2L * (text(stream).length() + text(writer).length() + text(mode).length() + original.fence().pipelineId().length());
            if (roster != null) {
                for (var value : roster.byStream().entrySet()) { size += 256L + 2L * value.getKey().length(); for (String id : value.getValue()) size += 128L + 2L * id.length(); }
                for (var value : roster.byOutput().entrySet()) { size += 256L + 2L * value.getKey().length(); for (var route : value.getValue().entrySet()) { size += 128L + 2L * route.getKey().length(); for (String id : route.getValue()) size += 64L + 2L * id.length(); } }
            }
            return size;
        }
    }
    private final HazelcastInstance member;
    private final CacheBusinessScope scope;
    private final Runnable active;
    private final Consumer<ExecutionFence> requireRun, requireLocal;
    private final Node local;
    private final Function<ExecutionFence, Optional<Attempt>> currentExecution;
    private final ConcurrentMap<ExecutionFence, Entry> entries = new ConcurrentHashMap<>();
    private final ConcurrentMap<Key, Packet> pending = new ConcurrentHashMap<>();
    private final Set<UUID> failedSenders = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<UUID, Packet> failedDeliveries = new ConcurrentHashMap<>();
    private final AtomicLong pendingBytes = new AtomicLong(), stateBytes = new AtomicLong(), sequence = new AtomicLong();
    private final ScheduledExecutorService transport;
    private volatile boolean closed;

    ValidationPipelineInitials(HazelcastInstance member, CacheBusinessScope scope, Runnable active, ExecutionAuthorization authorization) {
        this(member, scope, active, authorization::require, authorization::requireLocal);
    }
    ValidationPipelineInitials(HazelcastInstance member, CacheBusinessScope scope, Runnable active,
            Consumer<ExecutionFence> requireRun, Consumer<ExecutionFence> requireLocal) {
        this(member,scope,active,requireRun,requireLocal,null);
    }
    ValidationPipelineInitials(HazelcastInstance member, CacheBusinessScope scope, Runnable active,
            Consumer<ExecutionFence> requireRun, Consumer<ExecutionFence> requireLocal, Function<ExecutionFence,Optional<Attempt>> currentExecution) {
        this.member = Objects.requireNonNull(member); this.scope = Objects.requireNonNull(scope); this.active = Objects.requireNonNull(active);
        this.requireRun = Objects.requireNonNull(requireRun); this.requireLocal = Objects.requireNonNull(requireLocal); local = node(member);
        this.currentExecution=currentExecution;
        if (member.getUserContext().putIfAbsent(CONTEXT, this) != null || member.getUserContext().putIfAbsent(SourceInitialLoadObserver.USER_CONTEXT_KEY, this) != null) throw invalid("initial.alreadyBound");
        transport = Executors.newSingleThreadScheduledExecutor(task -> { var thread = new Thread(task, "validation-initial-receipts"); thread.setDaemon(true); return thread; });
        transport.scheduleWithFixedDelay(this::flush, 10, 50, TimeUnit.MILLISECONDS);
    }
    record Usage(int entries, int attempts, long stateBytes, long pendingBytes) {}
    Usage usage(ExecutionFence fence) { var entry=entries.get(fence); return new Usage(entries.size(),entry==null?0:entry.attempts.size(),stateBytes.get(),pendingBytes.get()); }
    ValidationInitialLoadState.Status read(ExecutionFence fence,Set<String> selected) {
        requireActive();requireRun.accept(fence);
        if(local.node().equals(fence.ownerNodeId())&&local.boot().equals(fence.ownerBootId()))return status(fence,selected);
        var targets=member.getCluster().getMembers().stream().filter(value->!value.isLiteMember()
                &&fence.ownerNodeId().equals(value.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE))
                &&fence.ownerBootId().equals(value.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE))).toList();
        if(targets.size()!=1)return unavailable("initial.coordinator-unavailable");
        try {
            var reply=member.getExecutorService(EXECUTOR).submitToMember(new Query(fence,Set.copyOf(selected)),targets.getFirst()).get(3,TimeUnit.SECONDS);
            requireRun.accept(fence);
            return new ValidationInitialLoadState.Status(switch(reply.state()){case "READY"->ValidationInitialLoadState.State.READY;case "WAITING"->ValidationInitialLoadState.State.WAITING;default->ValidationInitialLoadState.State.UNAVAILABLE;},reply.reason());
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();return unavailable("initial.lookup-interrupted");}
        catch(ExecutionException|TimeoutException unresolved){return unavailable("initial.lookup-unknown");}
    }

    SinkAckFactory decorate(ExecutionFence fence, SinkAckFactory original) {
        Objects.requireNonNull(original); if (fence == null || fence.scope() == null) return original;
        requireActive(); if (!scope.equals(scopeOf(fence)) || !local.node().equals(fence.ownerNodeId()) || !local.boot().equals(fence.ownerBootId())) throw invalid("initial.originalCoordinator");
        requireRun.accept(fence); pruneSuperseded(fence); Entry existing = entries.get(fence);
        if (existing == null) synchronized (entries) {
            existing = entries.get(fence);
            if (existing == null) {
                if (entries.size() == 64) pruneKnownTerminal();
                if (entries.size() == 64) return original;
                existing = new Entry(new Original(fence, local, UUID.randomUUID())); entries.put(fence, existing);
            }
        }
        return new ObservedFactory(original, existing.original);
    }

    @Override public void started(HazelcastInstance actual, Serializable identity, long job, long execution, String stream, int index, String mode, long epoch) {
        if (closed || !(identity instanceof ExecutionFence fence)) return;
        var entry = entries.get(fence); if (entry == null) return;
        var attempt = new Attempt(job, execution); if (!acceptOrigin(entry,attempt)) return; long order = sequence.incrementAndGet();
        if (!entry.sources.containsKey(attempt) && entry.sources.size() >= 4) { fail(entry.original, "initial.attempt-capacity"); return; }
        var sources = entry.sources.computeIfAbsent(attempt, ignored -> new ConcurrentHashMap<>());
        if (!sources.containsKey(stream)) {
            long cost = 512L + 2L * stream.length();
            if (sources.size() >= 4096 || !reserveState(entry,attempt,cost)) { if(attempt.equals(entry.latestOrigin))fail(entry.original, "initial.evidence-capacity"); return; }
        }
        sources.put(stream, new Life(order, true, false));
        enqueue(new Packet(entry.original, attempt, "SOURCE", stream, null, mode, epoch, node(actual), index, null));
    }
    @Override public void ended(long job, long execution, String stream, int index, boolean completedInput) {
        var attempt = new Attempt(job, execution);
        for (var entry : entries.values()) {
            var sources = entry.sources.get(attempt); if (sources == null) continue;
            sources.computeIfPresent(stream, (key, life) -> new Life(life.order(), false, completedInput));
        }
    }

    ValidationInitialLoadState.Status status(ExecutionFence fence, Set<String> selected) {
        requireActive(); requireRun.accept(fence); var entry = entries.get(fence);
        if (entry == null) return unavailable("initial.original-unavailable");
        if (entry.failed != null || failedSenders.contains(entry.original.token())) return unavailable(entry.failed == null ? "initial.receipt-capacity" : entry.failed);
        var live = currentExecution == null ? findCurrent(fence) : currentExecution.apply(fence);
        if (live.isEmpty()) return new ValidationInitialLoadState.Status(ValidationInitialLoadState.State.WAITING,"initial.execution-unavailable");
        Attempt chosen=live.orElseThrow();
        var witness = entry.attempts.get(chosen);
        if(witness==null)return new ValidationInitialLoadState.Status(ValidationInitialLoadState.State.WAITING,"initial.roster-starting");
        var result=witness.status(selected);
        if(result.state()==ValidationInitialLoadState.State.READY) {
            var checked=currentExecution==null?findCurrent(fence):currentExecution.apply(fence);requireRun.accept(fence);
            if(checked.isEmpty()||!checked.orElseThrow().equals(chosen))return new ValidationInitialLoadState.Status(ValidationInitialLoadState.State.WAITING,"initial.execution-changed");
        }
        return result;
    }
    private Optional<Attempt> findCurrent(ExecutionFence fence) {
        var job=member.getJet().getJob(fence.pipelineId());
        if(job==null || !fence.equals(job.getConfig().getArgument(Engine.RUN_IDENTITY_ARGUMENT)))return Optional.empty();
        var ids=new HashSet<Long>();
        var localId=JetLiveExecution.current(member,job.getId());if(localId.isPresent())ids.add(localId.getAsLong());
        var localMember=member.getCluster().getLocalMember().getUuid();
        var targets=member.getCluster().getMembers().stream().filter(value->!value.isLiteMember()&&!value.getUuid().equals(localMember)).toList();
        var answers=member.getExecutorService(LOOKUP_EXECUTOR).submitToMembers(new CurrentExecution(job.getId()),targets);
        for(var answer:answers.values())try {Long value=answer.get(2,TimeUnit.SECONDS);if(value!=null)ids.add(value);}
        catch(InterruptedException interrupted){Thread.currentThread().interrupt();return Optional.empty();}
        catch(ExecutionException|TimeoutException unavailable){return Optional.empty();}
        if(ids.size()==1)return Optional.of(new Attempt(job.getId(),ids.iterator().next()));
        if(ids.isEmpty()&&job.getStatus()==com.hazelcast.jet.core.JobStatus.COMPLETED) {
            var entry=entries.get(fence);var last=entry==null?null:entry.latestOrigin;
            if(last!=null&&last.job()==job.getId())return Optional.of(last);
        }
        return Optional.empty();
    }
    private boolean acceptOrigin(Entry entry,Attempt attempt) {
        synchronized(entry.originLock) {
            if(entry.closed)return false;
            Optional<Attempt> live;
            if(currentExecution!=null)live=currentExecution.apply(entry.original.fence());
            else {var value=JetLiveExecution.current(member,attempt.job());live=value.isPresent()?Optional.of(new Attempt(attempt.job(),value.getAsLong())):Optional.empty();}
            if(live.isPresent()&&!live.orElseThrow().equals(attempt))return false;
            if(live.isEmpty()&&!attempt.equals(entry.latestOrigin))return false;
            entry.latestOrigin=attempt;pruneAttempts(entry,attempt);return true;
        }
    }
    private boolean reserveState(Entry entry,Attempt attempt,long cost) {
        synchronized(entry.budgetLock) {
            if(entry.closed||!attempt.equals(entry.latestOrigin)||!reserve(stateBytes,cost,MAX_STATE_BYTES))return false;
            entry.charges.merge(attempt,cost,Long::sum);return true;
        }
    }
    private void pruneAttempts(Entry entry,Attempt current) {
        synchronized(entry.budgetLock) {
            for(var old:Set.copyOf(entry.charges.keySet()))if(!old.equals(current)) {var cost=entry.charges.remove(old);stateBytes.addAndGet(-cost);entry.attempts.remove(old);entry.sources.remove(old);}
        }
    }
    private void pruneSuperseded(ExecutionFence current) {
        // A refusal can mean transport failure. Only a positively authorized strictly newer
        // generation of this same Pipeline supersedes these readonly witnesses.
        for(var entry:entries.values()) {
            var old=entry.original.fence();
            if(!old.pipelineId().equals(current.pipelineId())||!scopeOf(old).equals(scopeOf(current)))continue;
            boolean newer=current.claimGeneration()>old.claimGeneration()
                    ||current.claimGeneration()==old.claimGeneration()&&current.executionGeneration()>old.executionGeneration();
            if(!newer)continue;
            discard(entry);
        }
    }
    private void pruneKnownTerminal() {
        for(var entry:entries.values()) {
            var job=member.getJet().getJob(entry.original.fence().pipelineId());
            if(job!=null&&entry.original.fence().equals(job.getConfig().getArgument(Engine.RUN_IDENTITY_ARGUMENT))&&job.getStatus().isTerminal())discard(entry);
        }
    }
    private void discard(Entry entry) {
        synchronized(entry.budgetLock) {
            entry.closed=true;
            if(entries.remove(entry.original.fence(),entry)) {
                for(long cost:entry.charges.values())stateBytes.addAndGet(-cost);stateBytes.addAndGet(-entry.rosterBytes);entry.rosterBytes=0;entry.roster=null;entry.charges.clear();entry.attempts.clear();entry.sources.clear();
                failedSenders.remove(entry.original.token());failedDeliveries.remove(entry.original.token());
                for(var packet:pending.values())if(packet.original().equals(entry.original)&&pending.remove(packet.key(),packet))pendingBytes.addAndGet(-packet.bytes());
            }
        }
    }

    private void enqueue(Packet packet) {
        if (closed || failedSenders.contains(packet.original().token())) return;
        var key = packet.key(); var previous = pending.get(key);
        if (previous != null) { if (!previous.equals(packet)) fail(packet.original(), "initial.receipt-changed"); return; }
        long cost = packet.bytes();
        if (!reserve(pendingBytes, cost, MAX_PENDING_BYTES)) { fail(packet.original(), "initial.receipt-capacity"); return; }
        if (pending.putIfAbsent(key, packet) != null) pendingBytes.addAndGet(-cost);
    }
    private void fail(Original original, String reason) {
        if (failedSenders.size() < 128) {
            failedSenders.add(original.token());
            failedDeliveries.putIfAbsent(original.token(), new Packet(original, new Attempt(0, 0), "FAILED", null, null, reason, 0, null, 0, null));
        }
        var entry = entries.get(original.fence()); if (entry != null && entry.original.equals(original)) entry.failed = reason;
    }
    private void flush() {
        if (closed) return;
        for (var failure : failedDeliveries.values()) try { if (deliver(failure)) failedDeliveries.remove(failure.original().token(), failure); }
        catch (InterruptedException stopped) { Thread.currentThread().interrupt(); return; }
        catch (RuntimeException | ExecutionException | TimeoutException unavailable) { /* The bounded original failure remains pending. */ }
        int sent = 0;
        for (var packet : pending.values()) {
            if (++sent > 64) break;
            try {
                boolean delivered = deliver(packet);
                if (delivered && pending.remove(packet.key(), packet)) pendingBytes.addAndGet(-packet.bytes());
            } catch (InterruptedException stopped) { Thread.currentThread().interrupt(); return; }
            catch (RuntimeException | ExecutionException | TimeoutException unavailable) { /* Retry only this original bounded fact. */ }
        }
    }
    private boolean deliver(Packet packet) throws InterruptedException, ExecutionException, TimeoutException {
        if (packet.original().coordinator().matches(member.getCluster().getLocalMember())) return receive(packet);
        var targets = member.getCluster().getMembers().stream().filter(packet.original().coordinator()::matches).toList();
        if (targets.size() != 1) return false;
        return Boolean.TRUE.equals(member.getExecutorService(EXECUTOR).submitToMember(new Delivery(packet), targets.getFirst()).get(2, TimeUnit.SECONDS));
    }
    private boolean receive(Packet packet) {
        requireActive(); var original = packet.original();
        if (!original.coordinator().matches(member.getCluster().getLocalMember()) || !scope.equals(scopeOf(original.fence()))) return true;
        var entry = entries.get(original.fence());
        if (entry == null || !entry.original.equals(original)) return true;
        requireLocal.accept(original.fence());
        if (packet.kind().equals("FAILED")) { entry.failed = packet.mode(); return true; }
        if (failedSenders.contains(original.token())) { entry.failed = "initial.receipt-capacity"; return true; }
        var witness = entry.attempts.get(packet.attempt());
        if (packet.kind().equals("BEGIN")) {
            synchronized(entry.budgetLock) {
                if(entry.closed)return true;
                if(entry.roster==null) {
                    long cost=packet.bytes();if(!reserve(stateBytes,cost,MAX_STATE_BYTES)){entry.failed="initial.evidence-capacity";return true;}
                    entry.rosterBytes=cost;entry.roster=packet.roster();
                } else if(!entry.roster.equals(packet.roster())){entry.failed="initial.roster-changed";return true;}
            }
            if(!acceptOrigin(entry,packet.attempt()))return true;
            if (witness != null) return true;
            synchronized (entry) {
                if (entry.attempts.containsKey(packet.attempt())) return true;
                if (entry.attempts.size() == 4) { entry.failed = "initial.attempt-capacity"; return true; }
                long charge = packet.bytes() * 3 + packet.roster().byStream().size() * 4096L;
                if (!reserveState(entry,packet.attempt(),charge)) { entry.failed = "initial.evidence-capacity"; return true; }
                try {
                    var counts = new HashMap<String, Integer>(); packet.roster().byStream().keySet().forEach(stream -> counts.put(stream, 1));
                    witness = new ValidationInitialLoadState(new SinkAckFactory.ExecutionWriters(packet.attempt().job(), packet.attempt().execution(), packet.roster()), counts,
                            8L * 1024 * 1024, () -> { requireActive(); requireLocal.accept(original.fence()); });
                    entry.attempts.put(packet.attempt(), witness);
                } catch (RuntimeException failed) { entry.failed = "initial.evidence-unavailable"; }
            }
            return true;
        }
        if(!acceptOrigin(entry,packet.attempt()))return true;
        if(witness==null&&entry.roster!=null) {
            var begin=new Packet(original,packet.attempt(),"BEGIN",null,null,null,0,null,0,entry.roster);
            receive(begin);witness=entry.attempts.get(packet.attempt());
        }
        if (witness == null) return false;
        var attempt = new ValidationInitialLoadState.Attempt(packet.attempt().job(), packet.attempt().execution());
        switch (packet.kind()) {
            case "SOURCE" -> witness.source(attempt, packet.stream(), new ValidationInitialLoadState.Producer(packet.producer().node(), packet.producer().boot(), packet.producer().member(), packet.index()),
                    new ValidationInitialLoadState.Source(switch (packet.mode()) { case "SNAPSHOT" -> ValidationInitialLoadState.Mode.SNAPSHOT; case "NO_INITIAL_LOAD" -> ValidationInitialLoadState.Mode.NO_INITIAL_LOAD; default -> ValidationInitialLoadState.Mode.UNKNOWN; }, packet.epoch()));
            case "STARTED" -> witness.writerStarted(attempt, packet.writer());
            case "BOUND" -> witness.bound(attempt, packet.stream(), packet.writer(), SourceOrder.snapshotRow(packet.epoch()));
            default -> entry.failed = "initial.unknown-receipt";
        }
        return true;
    }
    private record Life(long order, boolean active, boolean completed) {}
    private static final class Entry {
        final Original original;
        final ConcurrentMap<Attempt, ValidationInitialLoadState> attempts = new ConcurrentHashMap<>();
        final ConcurrentMap<Attempt, ConcurrentMap<String, Life>> sources = new ConcurrentHashMap<>();
        final Object originLock=new Object(),budgetLock=new Object();
        final Map<Attempt,Long> charges=new HashMap<>();
        volatile Attempt latestOrigin;
        volatile SinkAckFactory.RunWriters roster;
        long rosterBytes;
        volatile boolean closed;
        volatile String failed;
        Entry(Original original) { this.original = original; }
    }
    private static final class ObservedFactory implements SinkAckFactory {
        private static final long serialVersionUID = 1L;
        private final SinkAckFactory actual; private final Original original;
        ObservedFactory(SinkAckFactory actual, Original original) { this.actual = actual; this.original = original; }
        @Override public void beginRun(HazelcastInstance member, ExecutionWriters writers, WorkloadClaimFence fence) {
            actual.beginRun(member, writers, fence);
            of(member).enqueue(new Packet(original, new Attempt(writers.jobId(), writers.executionId()), "BEGIN", null, null, null, 0, null, 0, writers.writers()));
        }
        @Override public SinkAck resolve(HazelcastInstance member) { return new ObservedAck(actual.resolve(member), of(member), original, null); }
        @Override public io.tapstate.core.lifecycle.LoadLandings loadLandings(HazelcastInstance member) { return actual.loadLandings(member); }
    }
    private static final class ObservedAck implements SinkAck {
        private static final long serialVersionUID = 1L;
        private final SinkAck actual; private final transient ValidationPipelineInitials service; private final Original original; private final String writer;
        private transient Attempt attempt;
        ObservedAck(SinkAck actual, ValidationPipelineInitials service, Original original, String writer) { this.actual = actual; this.service = service; this.original = original; this.writer = writer; }
        @Override public SinkAck forWriter(String id) { return new ObservedAck(actual.forWriter(id), service, original, id); }
        @Override public void advance(String stream, ChainPosition position) { actual.advance(stream, position); }
        @Override public void advance(String stream, ChainPosition position, WorkloadClaimFence fence) { actual.advance(stream, position, fence); }
        @Override public void started(long job, long execution, WorkloadClaimFence fence) {
            actual.started(job, execution, fence); attempt = new Attempt(job, execution);
            service.enqueue(new Packet(original, attempt, "STARTED", null, writer, null, 0, null, 0, null));
        }
        @Override public void started(long job,long execution) {
            actual.started(job,execution);attempt=new Attempt(job,execution);
            service.enqueue(new Packet(original,attempt,"STARTED",null,writer,null,0,null,0,null));
        }
        @Override public void bounded(String stream, SourceOrder through, WorkloadClaimFence fence) {
            actual.bounded(stream, through, fence);
            if (attempt != null && through.seq() == SourceOrder.SNAPSHOT_SEQ) service.enqueue(new Packet(original, attempt, "BOUND", stream, writer, null, through.epoch(), null, 0, null));
        }
    }
    private static final class Delivery implements Callable<Boolean>, Serializable, HazelcastInstanceAware {
        private static final long serialVersionUID = 1L; private final Packet packet; private transient HazelcastInstance member;
        Delivery(Packet packet) { this.packet = packet; }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member = member; }
        @Override public Boolean call() { return of(member).receive(packet); }
    }
    private static final class CurrentExecution implements Callable<Long>,Serializable,HazelcastInstanceAware {
        private static final long serialVersionUID=1L;private final long job;private transient HazelcastInstance member;
        CurrentExecution(long job){this.job=job;}
        @Override public void setHazelcastInstance(HazelcastInstance member){this.member=member;}
        @Override public Long call(){var current=JetLiveExecution.current(member,job);return current.isPresent()?current.getAsLong():null;}
    }
    private record Reply(String state,String reason) implements Serializable {}
    private static final class Query implements Callable<Reply>,Serializable,HazelcastInstanceAware {
        private static final long serialVersionUID=1L;private final ExecutionFence fence;private final Set<String> selected;private transient HazelcastInstance member;
        Query(ExecutionFence fence,Set<String> selected){this.fence=fence;this.selected=selected;}
        @Override public void setHazelcastInstance(HazelcastInstance member){this.member=member;}
        @Override public Reply call(){var state=of(member).status(fence,selected);return new Reply(state.state().name(),state.reason());}
    }
    private static ValidationPipelineInitials of(HazelcastInstance member) { if (member.getUserContext().get(CONTEXT) instanceof ValidationPipelineInitials found) return found; throw invalid("initial.contextUnavailable"); }
    private static Node node(HazelcastInstance member) { var value = member.getCluster().getLocalMember(); return new Node(Objects.requireNonNull(value.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)), Objects.requireNonNull(value.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)), value.getUuid()); }
    private void requireActive() { if (closed) throw invalid("initial.contextClosed"); active.run(); }
    private static String text(String value) { return value == null ? "" : value; }
    private static CacheBusinessScope scopeOf(ExecutionFence fence) { var scope = fence.scope(); return new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()); }
    private static boolean reserve(AtomicLong counter, long bytes, long maximum) { for (;;) { long current = counter.get(); if (bytes > maximum - current) return false; if (counter.compareAndSet(current, current + bytes)) return true; } }
    private static ValidationInitialLoadState.Status unavailable(String reason) { return new ValidationInitialLoadState.Status(ValidationInitialLoadState.State.UNAVAILABLE, reason); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    @Override public void close() { if (closed) return; closed = true; member.getUserContext().remove(CONTEXT, this); member.getUserContext().remove(SourceInitialLoadObserver.USER_CONTEXT_KEY, this); transport.shutdownNow(); pending.clear(); entries.clear(); failedSenders.clear(); }
}
