package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.function.LongSupplier;
import static io.tapstate.app.ValidationOwnerRouter.*;

/**
 * Native owner/envelope reservation, before CONFIG launch-ticket creation and Job activation. It
 * never adopts an owner lease without its original live local context. Unknown claims retain their
 * original operation for cleanup; a new execution uses a new tenure. No business reader or hint
 * subscription is created by reservation. Resource capacity is held locally before owner acquire.
 */
final class NativeValidationOwnerReservations implements ValidationOwnerRouter.Local, AutoCloseable {
    private static final System.Logger LOG = System.getLogger(NativeValidationOwnerReservations.class.getName());
    private static final String CONTEXT = "tapstate.validation-owner-reservations";
    private static final int MAX_PIPELINES = 64, MAX_OPERATIONS = 128;
    private static final long HANDOFF_NANOS = Duration.ofSeconds(30).toNanos();
    private static final ValidationLocalBudget.Amount CONTROL_ENVELOPE=new ValidationLocalBudget.Amount(64*1024,1,0,0);
    @FunctionalInterface interface Startup {StartupGate inspect(ValidationSavedDeclarations saved,ValidationRunSpec spec);}
    record StartupGate(boolean controlOnly,Supplier<ValidationRunLifecycle.Readiness> current){
        StartupGate{Objects.requireNonNull(current);}
        static StartupGate ready(){return new StartupGate(false,()->ValidationRunLifecycle.Readiness.READY);}
    }
    private record Operation(String task, UUID generation, String mode, UUID command) {}
    private final HazelcastInstance member;
    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final CacheBusinessScope scope;
    private final Supplier<WorkloadClaimFence> node;
    private final ClusterMembershipGate membership;
    private final ValidationMemberResources.Member resources;
    private final ValidationMemberResources process;
    private final ValidationOwnerTimers timers;
    private final Runnable active;
    private final Duration ttl, renewal;
    private final int maxPipelines, maxOperations;
    private final LongSupplier handoffClock;
    private final Startup startup;
    private final Map<String, Pipeline> pipelines = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> expiry;
    private volatile boolean closed;

    NativeValidationOwnerReservations(HazelcastInstance member, MongoConfigurationDomain configuration, MongoCacheDomain cache,
            CacheBusinessScope scope, Supplier<WorkloadClaimFence> node, ClusterMembershipGate membership,
            ValidationMemberResources.Member resources, ValidationMemberResources process, ValidationOwnerTimers timers,
            Runnable active, Duration ttl, Duration renewal) {
        this(member, configuration, cache, scope, node, membership, resources, process, timers, active, ttl, renewal, MAX_PIPELINES, MAX_OPERATIONS);
    }
    NativeValidationOwnerReservations(HazelcastInstance member, MongoConfigurationDomain configuration, MongoCacheDomain cache,
            CacheBusinessScope scope, Supplier<WorkloadClaimFence> node, ClusterMembershipGate membership,
            ValidationMemberResources.Member resources, ValidationMemberResources process, ValidationOwnerTimers timers,
            Runnable active, Duration ttl, Duration renewal, int maxPipelines, int maxOperations) {
        this(member, configuration, cache, scope, node, membership, resources, process, timers, active, ttl, renewal, maxPipelines, maxOperations, System::nanoTime);
    }
    NativeValidationOwnerReservations(HazelcastInstance member, MongoConfigurationDomain configuration, MongoCacheDomain cache,
            CacheBusinessScope scope, Supplier<WorkloadClaimFence> node, ClusterMembershipGate membership,
            ValidationMemberResources.Member resources, ValidationMemberResources process, ValidationOwnerTimers timers,
            Runnable active, Duration ttl, Duration renewal,Startup startup){
        this(member,configuration,cache,scope,node,membership,resources,process,timers,active,ttl,renewal,MAX_PIPELINES,MAX_OPERATIONS,System::nanoTime,startup);
    }
    NativeValidationOwnerReservations(HazelcastInstance member, MongoConfigurationDomain configuration, MongoCacheDomain cache,
            CacheBusinessScope scope, Supplier<WorkloadClaimFence> node, ClusterMembershipGate membership,
            ValidationMemberResources.Member resources, ValidationMemberResources process, ValidationOwnerTimers timers,
            Runnable active, Duration ttl, Duration renewal, int maxPipelines, int maxOperations, LongSupplier handoffClock) {
        this(member,configuration,cache,scope,node,membership,resources,process,timers,active,ttl,renewal,maxPipelines,maxOperations,handoffClock,(saved,spec)->StartupGate.ready());
    }
    NativeValidationOwnerReservations(HazelcastInstance member, MongoConfigurationDomain configuration, MongoCacheDomain cache,
            CacheBusinessScope scope, Supplier<WorkloadClaimFence> node, ClusterMembershipGate membership,
            ValidationMemberResources.Member resources, ValidationMemberResources process, ValidationOwnerTimers timers,
            Runnable active, Duration ttl, Duration renewal, int maxPipelines, int maxOperations, LongSupplier handoffClock,Startup startup) {
        this.member = Objects.requireNonNull(member); this.configuration = Objects.requireNonNull(configuration); this.cache = Objects.requireNonNull(cache);
        this.scope = Objects.requireNonNull(scope); this.node = Objects.requireNonNull(node); this.membership = Objects.requireNonNull(membership);
        this.resources = Objects.requireNonNull(resources); this.process = Objects.requireNonNull(process); this.timers = Objects.requireNonNull(timers);
        this.active = Objects.requireNonNull(active); this.ttl = Objects.requireNonNull(ttl); this.renewal = Objects.requireNonNull(renewal);
        if (maxPipelines < 1 || maxPipelines > MAX_PIPELINES || maxOperations < 1 || maxOperations > MAX_OPERATIONS) throw invalid("owner.reservationLimits");
        this.maxPipelines = maxPipelines; this.maxOperations = maxOperations;
        this.handoffClock = Objects.requireNonNull(handoffClock);
        this.startup=Objects.requireNonNull(startup);
        if (ttl.isNegative() || ttl.isZero() || renewal.isNegative() || renewal.isZero() || renewal.compareTo(ttl) >= 0) throw invalid("owner.leaseIntervals");
        if (member.getUserContext().putIfAbsent(CONTEXT, this) != null) throw invalid("owner.alreadyBound");
        expiry = timers.deadlines.scheduleWithFixedDelay(this::expire, 1, 1, TimeUnit.SECONDS);
    }

    Optional<WorkloadClaimReading> readOwner(String pipeline) {
        requireActive();
        // Selection reads data, not an execution grant. A remote Pipeline considered here must not
        // consume this member's local owner registry. Native preparation still proves both domains.
        var prepared = configuration.prepareValidationAccess(pipeline, scope, cache, node.get(), this::requireActive);
        var reading = prepared.claims().read(ValidationClaims.key(scope.clusterId(), pipeline)); requireActive(); return reading;
    }

    @Override public Result reserve(Request request) {
        requireActive(); var local = pipeline(request.pipeline());
        if (!local.lock.tryLock()) { unpin(local); return new Result(Outcome.BUSY, null, null); }
        Reservation operation = null;
        try {
            refreshLifetime(local);
            reap(local);
            var key = new Operation(request.task(), request.taskGeneration(), request.mode(), request.command());
            operation = local.operations.get(key);
            if (operation != null) {
                if (!operation.request.equals(request)) return new Result(Outcome.REJECTED, null, ValidationError.INVALID_CONTRACT.code());
                return operation.reply();
            }
            if (local.operations.size() == maxOperations) reapSupersededHandoffs(local);
            if (local.operations.size() == maxOperations) return Result.capacity();
            operation = new Reservation(local, request); local.operations.put(key, operation);
            operation.preparing = true; operation.metadata = resources.admitMetadata();
            var task = task(request); var definitions = configuration.validationTasks(this::requireActive);
            var inputs = definitions.inputs(task, request.configurationRevision());
            if (inputs.isEmpty()) return operation.reject();
            operation.declarations = ValidationSavedDeclarations.inspect(inputs.orElseThrow(), configuration.bindings().load().orElseThrow(() -> unauthorized(request.pipeline())).defaultSourceId());
            var saved = operation.declarations.original().task();
            operation.spec = new ValidationRunSpec(saved.comparison(), ValidationMode.valueOf(request.mode()),
                    request.mode().equals("FULL") ? saved.settings().full().method() : ValidationMethod.FULL_COLUMN);
            var selected = saved.mode(operation.spec.mode());
            if (selected.permission() != null && selected.permission().start().sequence() == request.startSequence()) return operation.reject();
            if (selected.nextStartSequence() != request.startSequence()) return operation.reject();
            operation.startup=startup.inspect(operation.declarations,operation.spec);operation.dataEnvelope=envelope(saved,operation.spec);
            operation.unbound = resources.reserve(operation.startup.controlOnly()?CONTROL_ENVELOPE:operation.dataEnvelope, operation.startup.controlOnly()?0:1);
            operation.claimUncertain = true;
            var owner = ensureOwner(local); operation.claimUncertain = false; if (owner == null) return operation.redirect();
            operation.owner = owner; operation.access = local.access; owner.lease.require();
            if (!closeObsolete(local, owner)) throw invalid("owner.obsoletePermissions");
            NativeValidationReportHistory.reconcile(local.access,node.get(),saved,java.time.Clock.systemUTC(),
                    known->local.access.control().expire(new ValidationControlStore.Expiration(owner.lease.owner(),known)));
            var current = definitions.read(request.pipeline(), request.task()).orElse(null);
            if (current == null || !current.task().equals(task) || current.revision() != request.configurationRevision() || current.closed()
                    || current.mode(operation.spec.mode()).nextStartSequence() != request.startSequence()) return operation.reject();
            if (current.mode(operation.spec.mode()).permission() != null && current.mode(operation.spec.mode()).permission().phase() != ValidationTaskStore.Phase.CLOSED)
                return operation.busy();
            if (operation.cancelled.get()) return operation.reject();
            var identity = new ValidationRunIdentity(owner.lease.owner(), request.task(), request.run(), operation.spec.mode(), operation.spec.fingerprint(),
                    operation.spec.task().mappingRevision(), operation.spec.task().comparisonRulesVersion());
            operation.bound = operation.unbound.bind(identity, owner.lease::require);
            operation.requireReserved(); return operation.reply();
        } catch (TapstateException refusal) {
            if (operation == null) throw refusal;
            operation.reason = refusal.code().code(); operation.cancelled.set(true);
            if (operation.claimUncertain) { operation.unknown = true; return new Result(Outcome.UNKNOWN, null, operation.reason); }
            return new Result(refusal.code() == ValidationError.VALUE_LIMIT_EXCEEDED ? Outcome.CAPACITY : Outcome.REJECTED, null, operation.reason);
        } catch (RuntimeException | Error defect) {
            if (operation != null) operation.cancelled.set(true); throw defect;
        } finally {
            if (operation != null) { operation.preparing = false; if (operation.bound == null || operation.cancelled.get()) operation.disposeUnused(); }
            local.lock.unlock();
            unpin(local);
        }
    }

    ValidationReportAdmission.Start original(Request request,ValidationOwner expected){
        requireActive();var local=pipeline(request.pipeline());
        if(!local.lock.tryLock()){unpin(local);throw unauthorized(local.pipeline);}
        try{
            var reservation=local.operations.get(new Operation(request.task(),request.taskGeneration(),request.mode(),request.command()));
            if(reservation==null||!reservation.request.equals(request))throw invalid("owner.originalReservation");
            reservation.requireReserved();var identity=reservation.bound.budget().identity();
            if(!identity.owner().equals(expected))throw invalid("owner.originalReservationOwner");
            return new ValidationReportAdmission.Start(task(request),request.configurationRevision(),request.startSequence(),request.command().toString(),identity);
        }finally{local.lock.unlock();unpin(local);}
    }
    record Pending(boolean stopped,boolean drained) {}
    /** Exact existing handoff only; missing status never creates a Pipeline registry or native authority. */
    Optional<Pending> pending(Request request,ValidationOwner expected,boolean stop){
        requireActive();final Pipeline local;
        synchronized(pipelines){local=pipelines.get(request.pipeline());if(local==null)return Optional.empty();local.references++;}
        if(!local.lock.tryLock()){unpin(local);throw unauthorized(local.pipeline);}
        try{
            var original=local.operations.get(new Operation(request.task(),request.taskGeneration(),request.mode(),request.command()));
            if(original==null||!original.request.equals(request)||original.bound==null||original.owner==null
                    ||!original.bound.budget().identity().owner().equals(expected))return Optional.empty();
            original.requireOwner();if(stop&&!original.cancelIfUnused()||!stop&&original.isActivated())return Optional.empty();
            return Optional.of(new Pending(original.cancelled.get(),original.cancelled.get()&&original.bound.fullyReleased()));
        }finally{local.lock.unlock();unpin(local);}
    }
    Reservation take(ValidationReportAdmission.Start start) {
        requireActive(); var local = pipeline(start.task().pipelineId());
        if (!local.lock.tryLock()) { unpin(local); throw unauthorized(local.pipeline); }
        try {
        UUID command; try { command = UUID.fromString(start.commandId()); } catch (IllegalArgumentException invalid) { throw invalid("owner.command"); }
        var key = new Operation(start.task().taskId(), start.task().generation(), start.run().mode().name(), command);
        var reservation = local.operations.get(key); if (reservation == null) throw invalid("owner.originalReservation");
        reservation.requireReserved();
        if (!reservation.request.command().toString().equals(start.commandId()) || !reservation.request.run().equals(start.run().runId()) || reservation.request.startSequence() != start.sequence()
                || reservation.request.configurationRevision() != start.configurationRevision() || !reservation.bound.budget().identity().equals(start.run()))
            throw invalid("owner.originalStart");
        return reservation;
        } finally { local.lock.unlock(); unpin(local); }
    }
    /** Serializes only this Pipeline's validation control, independently of normal processing. */
    boolean control(String pipeline,Runnable action) {
        var local=pipeline(pipeline);if(!local.lock.tryLock()){unpin(local);return false;}
        try{action.run();return true;}finally{local.lock.unlock();unpin(local);}
    }

    void expireRetained(ValidationControlStore.Status known){
        requireActive();final Pipeline local;
        synchronized(pipelines){local=pipelines.get(known.run().identity().owner().pipelineId());if(local==null)return;local.references++;}
        if(!local.lock.tryLock()){unpin(local);return;}
        try{
            var current=local.owner;if(current==null||current.retired.get()||!current.lease.authorized())return;
            current.lease.require();access(local).control().expire(new ValidationControlStore.Expiration(current.lease.owner(),known));
        }finally{local.lock.unlock();unpin(local);}
    }

    private Owner ensureOwner(Pipeline local) {
        var current = local.owner;
        if (current != null && !current.retired.get() && current.lease.authorized()) {
            if (current.countingGeneration == 0) return initializeOwner(local, current);
            var snapshot = access(local).control().snapshot(current.lease.owner());
            if (!(snapshot.closed() && snapshot.active().isEmpty() && current.lease.owner().equals(snapshot.ledgerOwner())
                    && snapshot.countingGeneration() == current.countingGeneration)) return current;
            current.retire(); current.lease.close(); local.owner = null;
        } else if (current != null) { current.retire(); local.owner = null; }
        var access = access(local); var claims = new ClusterWorkloadClaims(access.claims(), membership);
        var key = ValidationClaims.key(scope.clusterId(), local.pipeline); var actualNode = node.get();
        var previous = claims.read(key);
        if (previous.filter(WorkloadClaimReading::leased).isPresent()) {
            var held = previous.orElseThrow().claim();
            if (!held.owner().equals(actualNode.owner())) return null;
            // Registry absence is never permission to adopt an old execution, even on this boot.
            if (!claims.release(held)) throw unauthorized(local.pipeline);
        }
        long askedAt = System.nanoTime(); var topology = membership.committed();
        if (topology == null) throw unauthorized(local.pipeline);
        var acquired = claims.acquire(key, actualNode.owner(), topology.revision(), ttl).orElseThrow(() -> unauthorized(local.pipeline));
        if (!acquired.acquired()) return null;
        var advanced = claims.advanceExecution(acquired.claim(), topology.revision(), Set.of(actualNode.owner().nodeId()));
        if (advanced.isEmpty()) { claims.release(acquired.claim()); throw unauthorized(local.pipeline); }
        var owner = new Owner(local, advanced.orElseThrow(), claims, askedAt); local.owner = owner;
        owner.lease.maintain(timers.renewals, timers.deadlines, renewal, Duration.ofMillis(250)); owner.lease.require();
        return initializeOwner(local, owner);
    }

    private Owner initializeOwner(Pipeline local, Owner owner) {
        var access = access(local);
        if (!closeObsolete(local, owner)) throw invalid("owner.obsoletePermissions");
        var snapshot = access.control().snapshot(owner.lease.owner());
        for (var old : snapshot.active()) {
            if (old.run().identity().owner().claimGeneration() >= owner.lease.owner().claimGeneration()) throw invalid("owner.unexpectedActiveRun");
            var outcome = access.control().terminateLostOwner(new ValidationControlStore.LostOwner(owner.lease.owner(), old.run(), old.run().report()));
            if (outcome == ValidationControlStore.Mutation.STALE) throw invalid("owner.obsoleteRun");
        }
        snapshot = access.control().snapshot(owner.lease.owner());
        if (!snapshot.active().isEmpty() || !snapshot.closed() || snapshot.countingGeneration() == Long.MAX_VALUE) throw invalid("owner.countingGeneration");
        owner.countingGeneration = snapshot.countingGeneration() + 1; return owner;
    }

    private boolean closeObsolete(Pipeline pipeline, Owner owner) {
        for (int pass = 0; pass < 104; pass++) { owner.lease.require(); if (access(pipeline).closeObsoletePermissions(owner.lease.owner(), 20)) return true; }
        return false;
    }
    private MongoConfigurationDomain.ValidationAccess access(Pipeline pipeline) {
        requireActive();
        if (pipeline.access == null) pipeline.access = configuration.prepareValidationAccess(pipeline.pipeline, scope, cache, node.get(), this::requireActive);
        return pipeline.access;
    }
    private void refreshLifetime(Pipeline pipeline) {
        var old = pipeline.access;
        if (old == null) return;
        // Only a proven different CONFIG lifecycle permits a fresh preparation. Transport failure,
        // stale owner proof or a retry of the same lifetime never causes rebinding or adoption.
        var current = configuration.currentValidationGeneration(pipeline.pipeline, scope, this::requireActive);
        if (old.generation().equals(current)) return;
        if (pipeline.owner != null) {
            var retired = pipeline.owner; retired.retire(); process.cleanup(retired.lease::close); pipeline.owner = null;
        }
        for (var entry : pipeline.operations.values()) entry.close();
        var replacement = configuration.prepareValidationAccess(pipeline.pipeline, scope, cache, node.get(), this::requireActive);
        if (!replacement.generation().equals(current)) throw unauthorized(pipeline.pipeline);
        pipeline.access = replacement;
    }
    private Pipeline pipeline(String id) {
        if (id == null || id.isBlank()) throw invalid("owner.pipeline"); ValidationError.fieldName(id, "owner.pipeline");
        synchronized (pipelines) {
            var existing = pipelines.get(id); if (existing != null) { existing.references++; return existing; }
            if (pipelines.size() == maxPipelines) evictQuiescent();
            if (pipelines.size() == maxPipelines) throw invalid("owner.pipelineLimit");
            var created = new Pipeline(id); created.references++; pipelines.put(id, created); return created;
        }
    }
    private void unpin(Pipeline pipeline) { synchronized (pipelines) { if (--pipeline.references < 0) throw new IllegalStateException("negative Pipeline reservation reference count"); } }
    private void reap(Pipeline pipeline) { pipeline.operations.values().removeIf(Reservation::reapable); }
    private void reapSupersededHandoffs(Pipeline pipeline) {
        // A locally returned ACCEPTED may have been lost in transport. Expiry alone cannot erase
        // that original. Only monotonic CONFIG sequence/revision/lifetime evidence supersedes it.
        try (var frame = resources.admitMetadata()) {
            var definitions = configuration.validationTasks(this::requireActive);
            record Version(UUID generation, boolean closed, long revision, long full, long incremental) {}
            var saved = new HashMap<String, Optional<Version>>();
            for (var entry : pipeline.operations.values()) {
                if (!entry.closedHandoff()) continue;
                var current = saved.computeIfAbsent(entry.request.task(), task -> definitions.read(pipeline.pipeline, task)
                        .map(value -> new Version(value.task().generation(), value.closed(), value.revision(), value.full().nextStartSequence(), value.incremental().nextStartSequence())));
                if (current.isEmpty()) continue;
                var value = current.orElseThrow();
                if (!value.generation().equals(entry.request.taskGeneration()) || value.closed()
                        || value.revision() > entry.request.configurationRevision()
                        || (entry.request.mode().equals("FULL") ? value.full() : value.incremental()) > entry.request.startSequence()) entry.superseded = true;
            }
            reap(pipeline);
        }
    }
    private void evictQuiescent() {
        var entries = pipelines.values().iterator();
        while (entries.hasNext()) {
            var pipeline = entries.next();
            if (pipeline.references != 0 || !pipeline.lock.tryLock()) continue;
            try {
                reap(pipeline);
                if (!pipeline.operations.isEmpty()) continue;
                var owner = pipeline.owner;
                if (owner != null) { owner.retire(); process.cleanup(owner.lease::close); }
                entries.remove();
            } finally { pipeline.lock.unlock(); }
        }
    }
    private ValidationReportAdmission.Task task(Request request) { return new ValidationReportAdmission.Task(scope.clusterId(), request.pipeline(), request.task(), request.taskGeneration()); }
    private ValidationLocalBudget.Amount envelope(ValidationTaskStore.Snapshot saved, ValidationRunSpec spec) {
        int tables = spec.mode() == ValidationMode.FULL ? Math.min(saved.settings().full().maxConcurrentTables(), spec.task().mappings().size()) : 1;
        boolean count = spec.method() == ValidationMethod.COUNT;
        // Scalar plans still retain their fixed metadata and independently admitted connector buffers.
        return new ValidationLocalBudget.Amount(Math.multiplyExact(count ? 32L * 1024 * 1024 : 64L * 1024 * 1024, tables),
                Math.multiplyExact(count ? 3 : 20, tables), Math.multiplyExact(count ? 2 : 8, tables), Math.multiplyExact(count ? 2 : 8, tables));
    }
    void expire() {
        long now = handoffClock.getAsLong();
        var failures = new ValidationFailureLatch();
        for (var pipeline : pipelines.values()) {
            for (var entry : pipeline.operations.values()) if (!entry.activated && now - entry.created >= HANDOFF_NANOS) attempt(failures, entry::close);
            var owner = pipeline.owner;
            if (owner != null && !owner.retired.get() && now - owner.created >= HANDOFF_NANOS && pipeline.operations.values().stream().allMatch(value -> value.cancelled.get())) {
                attempt(failures, () -> { try { owner.retire(); } finally { process.cleanup(owner.lease::close); } });
            }
        }
        if (failures.get() instanceof Error fatal) throw fatal;
        if (failures.get() != null) LOG.log(System.Logger.Level.WARNING, "validation owner expiry cleanup failed", failures.get());
    }
    private void requireActive() {
        if (closed) throw invalid("owner.contextClosed"); active.run();
        if (!membership.businessEligible() || node.get() == null) throw invalid("owner.nodeNotAuthorized");
    }

    final class Reservation implements AutoCloseable {
        final Pipeline pipeline; final Request request; final long created = handoffClock.getAsLong();
        final AtomicBoolean cancelled = new AtomicBoolean(), stopSignalled = new AtomicBoolean();
        volatile boolean preparing, activated, accepted, superseded, completed, locallyDisposed, claimUncertain, unknown, reportPending; volatile String reason;
        volatile Owner owner; volatile ValidationSavedDeclarations declarations; volatile ValidationRunSpec spec;
        volatile StartupGate startup;volatile ValidationLocalBudget.Amount dataEnvelope;
        volatile MongoConfigurationDomain.ValidationAccess access;
        volatile ValidationMemberResources.Metadata metadata; volatile ValidationMemberResources.Unbound unbound; volatile ValidationMemberResources.Run bound;
        private volatile Runnable stop;
        Reservation(Pipeline pipeline, Request request) { this.pipeline = pipeline; this.request = request; }
        ValidationRunSpec spec() { requireReserved(); return spec; }
        ValidationMemberResources.Run resources() { requireReserved(); return bound; }
        MongoConfigurationDomain.ValidationAccess access() { return access; }
        long countingGeneration() { requireReserved(); return owner.countingGeneration; }
        ValidationRunLifecycle.Readiness readiness(){requireReserved();var answer=Objects.requireNonNull(startup.current().get());requireReserved();return answer;}
        ValidationRunLifecycle.Readiness beginExecution(){
            var ready=readiness();if(ready!=ValidationRunLifecycle.Readiness.READY)return ready;
            bound.growTo(dataEnvelope,1);requireReserved();return ready;
        }
        ValidationTaskStore.Snapshot savedTask() {
            requireReserved();
            var saved=configuration.validationTasks(this::requireReserved).read(request.pipeline(),request.task()).orElseThrow(()->invalid("owner.savedTaskUnavailable"));
            requireReserved();return saved;
        }
        void requireReserved() {
            requireOwner(); if (cancelled.get() || bound == null) throw invalid("owner.reservationClosed");
        }
        void requireOwner() {
            requireActive(); if (owner == null || owner.retired.get()) throw invalid("owner.reservationClosed");
            owner.lease.require();
        }
        synchronized boolean isActivated(){return activated;}
        synchronized boolean cancelIfUnused(){
            requireOwner();if(activated)return false;cancelled.set(true);disposeUnused();return true;
        }
        synchronized void activate(Runnable stop) {
            requireReserved(); if (activated) throw invalid("owner.alreadyActivated"); this.stop = Objects.requireNonNull(stop); activated = true;
        }
        synchronized void retainReportMetadata(){requireReserved();reportPending=true;}
        synchronized void reportSettled(){
            reportPending=false;
            if(completed&&bound!=null&&bound.fullyReleased())clearMetadata();
        }
        void releaseAfterKnownTerminal() {
            if (bound == null) throw invalid("owner.boundResources");
            completed = true; cancelled.set(true);
            bound.close(); bound.allReleased().whenComplete((ignored, failed) -> {
                if(failed==null)synchronized(this){stop=null;if(!reportPending)clearMetadata();}
            });
        }
        boolean originalOwnerAvailable(){
            var value=owner;return value!=null&&!value.retired.get()&&value.lease.authorized();
        }
        Optional<ValidationControlStore.Status> retainedTerminal(ValidationControlStore.Run original){
            requireActive();var known=access().history().terminal(node.get(),original);requireActive();return known;
        }
        boolean expired(ValidationControlStore.Run original){requireActive();return access().history().expired(node.get(),original);}
        void releaseClosedContextMetadata(){
            // Only local ownership is relinquished. Durable unknown/terminal records remain for fenced cleanup.
            if(bound==null)throw invalid("owner.boundResources");
            cancelled.set(true);locallyDisposed=true;
            bound.allReleased().whenComplete((ignored,failed)->{
                if(failed==null)synchronized(this){reportPending=false;stop=null;clearMetadata();}
            });
        }
        Optional<ValidationTaskStore.Snapshot> retainedTask(){
            requireActive();return configuration.validationTasks(NativeValidationOwnerReservations.this::requireActive).read(request.pipeline(),request.task());
        }
        private boolean reapable() {
            return !preparing && !unknown && cancelled.get() && (!activated || completed || locallyDisposed) && metadata == null
                    && (!accepted || completed || locallyDisposed || superseded) && (bound == null || bound.fullyReleased());
        }
        private boolean closedHandoff() { return accepted && !activated && !unknown && cancelled.get() && metadata == null && bound != null && bound.fullyReleased(); }
        private Result reply() {
            if (unknown) return new Result(Outcome.UNKNOWN, null, reason);
            if (cancelled.get()) return new Result(Outcome.REJECTED, null, reason);
            if (bound == null) return new Result(Outcome.UNKNOWN, null, reason);
            requireReserved(); accepted = true; return new Result(Outcome.ACCEPTED, owner.lease.owner(), null);
        }
        private Result reject() { cancelled.set(true); return new Result(Outcome.REJECTED, null, reason); }
        private Result redirect() { cancelled.set(true); return new Result(Outcome.REDIRECT, null, reason); }
        private Result busy() { cancelled.set(true); return new Result(Outcome.BUSY, null, reason); }
        private synchronized void disposeUnused() {
            if (preparing || activated) return;
            if (bound != null) { bound.close(); bound.processorsExited(); }
            if (unbound != null) unbound.close(); clearMetadata();
        }
        private synchronized void clearMetadata() { declarations = null; spec = null;startup=null;dataEnvelope=null; if (metadata != null) { metadata.close(); metadata = null; } }
        @Override public void close() {
            Runnable callback;
            synchronized (this) {
                cancelled.set(true); callback = activated && stopSignalled.compareAndSet(false, true) ? stop : null;
            }
            if (callback != null) callback.run(); else disposeUnused();
        }
    }
    private static final class Pipeline {
        final String pipeline; final ReentrantLock lock = new ReentrantLock();
        final Map<Operation, Reservation> operations = new ConcurrentHashMap<>();
        volatile MongoConfigurationDomain.ValidationAccess access; volatile Owner owner;
        int references;
        Pipeline(String pipeline) { this.pipeline = pipeline; }
    }
    private final class Owner {
        final Pipeline pipeline; final AtomicBoolean retired = new AtomicBoolean(); final ValidationOwnerLease lease; final long created = handoffClock.getAsLong();
        volatile long countingGeneration;
        Owner(Pipeline pipeline, WorkloadClaim claim, ClusterWorkloadClaims claims, long askedAt) {
            this.pipeline = pipeline;
            lease = new ValidationOwnerLease(pipeline.pipeline, claim, askedAt, ttl, System::nanoTime, () -> {
                try { requireActive(); return true; } catch (TapstateException unavailable) { return false; }
            }, value -> claims.renew(value, ttl), claims::release, ignored -> retire());
        }
        void retire() {
            if (!retired.compareAndSet(false, true)) return;
            var failures = new ValidationFailureLatch();
            attempt(failures, lease::revoke);
            for (var entry : pipeline.operations.values()) if (entry.owner == this) attempt(failures, entry::close);
            throwFailure(failures);
        }
    }
    boolean closed(){return closed;}
    @Override public void close() {
        if (closed) return; closed = true; expiry.cancel(false); member.getUserContext().remove(CONTEXT, this);
        var failures = new ValidationFailureLatch();
        for (var pipeline : pipelines.values()) {
            for (var entry : pipeline.operations.values()) attempt(failures, entry::close);
            var owner = pipeline.owner; if (owner != null) { attempt(failures, owner::retire); process.cleanup(owner.lease::close); }
        }
        throwFailure(failures);
    }
    private static void attempt(ValidationFailureLatch failures, Runnable action) {
        try { action.run(); } catch (RuntimeException | Error failure) { failures.record(failure); ValidationFailureLatch.addContext(failures.get(), failure); }
    }
    private static void throwFailure(ValidationFailureLatch failures) {
        if (failures.get() instanceof Error fatal) throw fatal;
        if (failures.get() instanceof RuntimeException failed) throw failed;
    }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException unauthorized(String pipeline) { return new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", pipeline), null); }
}
