package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.adapters.mongostore.MongoStructureInspection;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import java.util.function.*;

/** Bounded production feed. The two evaluation workers own no transport and never run convergence. */
final class PipelineAlertRuntime implements PipelineAlertFeed, AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PipelineAlertRuntime.class);
    static final int MAX_OBJECTS = 256, MAX_MATCHES = 32, MAX_INPUT_BYTES = 128 * 1024;
    static final int QUEUE_SIZE = 64, MAX_QUEUED_BYTES = 8 * 1024 * 1024;
    private static final int TIMER_TASK_LIMIT = QUEUE_SIZE - 8;
    private record TimerTurn(Object identity, BooleanSupplier offer) {}
    private Object nextTimer;
    record Health(boolean closed, boolean degraded, long rejected, String code,Map<String,Object> parameters,int queuedInputs,int queuedBytes,Long evaluationLagMillis,Instant lastEvaluatedAt,Map<String,Object> maintenance) {
        Health(boolean closed,boolean degraded,long rejected,String code,Map<String,Object> parameters,int inputs,int bytes){this(closed,degraded,rejected,code,parameters,inputs,bytes,null,null,Map.of());}
    }
    record Services(AlertStore store, AlertEvaluationHost evaluator, AlarmTypeCatalog catalog, AlertPlanningRuntime planner, AlertDeliveryRuntime delivery, Set<String> providers,io.tapstate.adapters.alerting.AlarmTypeText descriptions,AlertMaintenanceRuntime maintenance,AlertChannelCatalog channels) {
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog,AlertPlanningRuntime planner,AlertDeliveryRuntime delivery,Set<String> providers,io.tapstate.adapters.alerting.AlarmTypeText descriptions,AlertMaintenanceRuntime maintenance){this(store,evaluator,catalog,planner,delivery,providers,descriptions,maintenance,AlertChannelRegistry.defaults().catalog());}
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog,AlertPlanningRuntime planner,AlertDeliveryRuntime delivery,Set<String> providers,io.tapstate.adapters.alerting.AlarmTypeText descriptions){this(store,evaluator,catalog,planner,delivery,providers,descriptions,null);}
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog,AlertPlanningRuntime planner,AlertDeliveryRuntime delivery,Set<String> providers){this(store,evaluator,catalog,planner,delivery,providers,io.tapstate.adapters.alerting.AlarmTypeText.english());}
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog,AlertPlanningRuntime planner,AlertDeliveryRuntime delivery){this(store,evaluator,catalog,planner,delivery,Set.of("pipeline-lifecycle","pipeline-observations"));}
        Services { providers=Set.copyOf(providers);Objects.requireNonNull(descriptions);Objects.requireNonNull(channels); }
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog,AlertPlanningRuntime planner){this(store,evaluator,catalog,planner,null);}
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog){this(store,evaluator,catalog,null,null);}
        Services(AlertStore store,AlertEvaluationHost evaluator){this(store,evaluator,BuiltinAlarmTypes.catalog());}
    }
    private record Producer(PipelineIntentScope scope, WorkloadClaimFence claim, PipelineIntentSnapshot intent) {
        String incarnation() {
            return fingerprint(List.of(scope.clusterId(), Long.toString(scope.bindingVersion()), scope.cacheIncarnation(),
                    scope.pipelineId(), Long.toString(scope.stateGeneration()), claim.owner().nodeId(), claim.owner().bootId(),
                    Long.toString(claim.claimGeneration()), Long.toString(claim.executionGeneration()), Long.toString(claim.topologyRevision())));
        }
    }
    private record AdmissionKey(PipelineIntentScope scope, WorkloadClaimFence claim) {
        AdmissionKey(ExecutionFence run) {
            this(run.scope(),new WorkloadClaimFence(new WorkloadClaimKey(run.scope().clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,run.pipelineId()),
                    new WorkloadOwner(run.ownerNodeId(),run.ownerBootId()),run.claimGeneration(),run.executionGeneration(),run.topologyRevision()));
        }
    }
    private static final class Slot {
        final Producer producer;
        final Instant expectedSince;
        final AtomicLong sequence, freshnessSequence;
        final AtomicLong interruptions = new AtomicLong();
        volatile long recoveredInterruption;
        final AtomicBoolean tickQueued = new AtomicBoolean();
        volatile Instant publication;
        volatile PipelineState actual;
        final ArrayDeque<Work> pending = new ArrayDeque<>();
        boolean scheduled;
        volatile TerminalAlerts terminal;
        final TerminalPublication.Admission admission;
        Slot(Producer producer, Instant now, Slot previous, TerminalPublication.Admission admission) {
            this.producer = producer;this.admission = admission;
            boolean same = previous != null && previous.producer.incarnation().equals(producer.incarnation());
            this.sequence = same ? previous.sequence : new AtomicLong();
            this.freshnessSequence = same ? previous.freshnessSequence : new AtomicLong();
            this.expectedSince = same && previous.producer.intent().desired().targetState() == PipelineState.RUNNING
                    && producer.intent().desired().targetState() == PipelineState.RUNNING ? previous.expectedSince : now;
            if (same) interruptions.incrementAndGet();
        }
        boolean discontinuous() { return interruptions.get() != recoveredInterruption; }
    }
    private final Clock clock;
    private final Supplier<Services> open;
    private final Runnable active;
    private final BiFunction<PipelineIntentScope, WorkloadClaimFence, Optional<Runnable>> claimProof;
    private final PipelineIntentStore intents;
    private final ThreadPoolExecutor workers;
    private final ScheduledThreadPoolExecutor freshness = new ScheduledThreadPoolExecutor(1, task -> {
        return ShutdownAwait.daemon(task,"alert-freshness");
    });
    private final AtomicBoolean started=new AtomicBoolean();
    private final Object objectAdmission=new Object();
    private final ConcurrentMap<String, Slot> slots = new ConcurrentHashMap<>();
    private final ConcurrentMap<AdmissionKey, TerminalPublication.Admission> pipelineAdmissions = new ConcurrentHashMap<>();
    private final AtomicInteger retainedResourceBindings=new AtomicInteger();
    private final AtomicInteger queuedBytes = new AtomicInteger();
    private final AtomicInteger queuedTasks = new AtomicInteger();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean(), refreshing = new AtomicBoolean();
    private final AtomicLong lastLogged = new AtomicLong(Long.MIN_VALUE);
    private static final class ResourceSlot {
        final ArrayDeque<Work> pending=new ArrayDeque<>();
        final EnumMap<OperationalEvidence.Capability,Work> pendingHealthyCache=new EnumMap<>(OperationalEvidence.Capability.class);
        final EnumSet<OperationalEvidence.Capability> recoveringCache=EnumSet.noneOf(OperationalEvidence.Capability.class);
        final ConcurrentMap<AlarmKey,AlertEvaluationHost.Binding> bindings=new ConcurrentHashMap<>();
        final AtomicBoolean tickQueued=new AtomicBoolean();
        boolean scheduled;
    }
    private final ConcurrentMap<AlarmKey.Resource,ResourceSlot> resourceSlots = new ConcurrentHashMap<>();
    private final AtomicReference<java.util.function.Consumer<AlarmKey.Resource>> resourceRetired=new AtomicReference<>(r->{});
    void resourceRetired(java.util.function.Consumer<AlarmKey.Resource> listener){resourceRetired.set(listener);}
    private final AtomicReference<Services> services=new AtomicReference<>();
    private final CountDownLatch servicesReady = new CountDownLatch(1);
    private final Set<Services> ownedServices=ConcurrentHashMap.newKeySet();
    private final Set<Services> stoppedServices=ConcurrentHashMap.newKeySet();
    private final ShutdownAwait shutdown=new ShutdownAwait();
    private final AtomicReference<CompletableFuture<Void>> stopping=new AtomicReference<>();
    private final AdmissionDrain inputAdmission=new AdmissionDrain();
    private final ExecutorService settlements=Executors.newSingleThreadExecutor(task->ShutdownAwait.daemon(task,"alert-settlement"));
    private final java.util.concurrent.ConcurrentLinkedQueue<Throwable> settlementFailures=new java.util.concurrent.ConcurrentLinkedQueue<>();
    private volatile Instant lastEvaluatedAt;
    private final ConcurrentMap<Work,Instant> inputTimes=new ConcurrentHashMap<>();
    private final AlertDiagnosticMessages diagnostics;
    private record RuleSnapshot(List<AlertConfiguration.Rule> definitions, boolean complete) {}
    private final AtomicReference<RuleSnapshot> rules = new AtomicReference<>(new RuleSnapshot(List.of(), false));
    private record Failure(String code,Map<String,Object> parameters){}
    private final AtomicReference<Failure> failure = new AtomicReference<>(new Failure(null,Map.of()));
    private final AtomicReference<AlarmKey> reconciliationCursor=new AtomicReference<>();
    private boolean legacyBindingsInScan,legacyBindingsUnavailable,legacyScanComplete;
    private record EvaluationRead(long round,long deadline,boolean complete,Map<AlarmKey,AlertStore.Stored<AlertStateRecord.Head>> rows) { }
    private final AtomicLong evaluationRound=new AtomicLong();
    private final AtomicReference<EvaluationRead> evaluationRead = new AtomicReference<>();
    private record CachedState(Optional<AlertStore.Stored<AlertStateRecord.Head>> row) { }

    private Optional<CachedState> loaded(long round, AlarmKey key) {
        var read = evaluationRead.get();
        if (read == null || read.round() != round || evaluationRound.get() != round
                || System.nanoTime() - read.deadline() >= 0) return Optional.empty();
        var row = read.rows().get(key);
        return row != null || read.complete() ? Optional.of(new CachedState(Optional.ofNullable(row))) : Optional.empty();
    }

    private void tickBinding(long round, AlertEvaluationHost.Binding binding) {
        tickBinding(round, binding, false);
    }

    private void tickBinding(long round, AlertEvaluationHost.Binding binding, boolean stoppedLifecycle) {
        var evaluator = services.get().evaluator();
        var cached = loaded(round, binding.key());
        AlertEvaluationHost.requireResult(stoppedLifecycle
                ? cached.isPresent() ? evaluator.tickStoppedPipeline(binding, cached.orElseThrow().row()) : evaluator.tickStoppedPipeline(binding)
                : cached.isPresent() ? evaluator.tick(binding, cached.orElseThrow().row()) : evaluator.tick(binding));
    }
    private final String runtimeId = UUID.randomUUID().toString();

    PipelineAlertRuntime(Supplier<Services> open, PipelineIntentStore intents,
            BiFunction<PipelineIntentScope, WorkloadClaimFence, Optional<Runnable>> claimProof, Runnable active, Clock clock) {
        this(open,intents,claimProof,active,clock,null);
    }
    PipelineAlertRuntime(Supplier<Services> open, PipelineIntentStore intents,
            BiFunction<PipelineIntentScope, WorkloadClaimFence, Optional<Runnable>> claimProof, Runnable active, Clock clock,
            AlertDiagnosticMessages diagnostics) {
        this.diagnostics = diagnostics;
        this.open = Objects.requireNonNull(open); this.intents = Objects.requireNonNull(intents);
        this.claimProof = Objects.requireNonNull(claimProof); this.active = Objects.requireNonNull(active);
        this.clock = Objects.requireNonNull(clock);
        var serial = new AtomicInteger();
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE), task -> {
            return ShutdownAwait.daemon(task, "alert-evaluation-" + serial.incrementAndGet());
        }, new ThreadPoolExecutor.AbortPolicy());
    }
    @Override public boolean needsFrequentTerminalSamples(PipelineIntentScope scope) {
        return needsFrequentTerminalSamples(scope, null);
    }

    @Override public boolean needsFrequentTerminalSamples(PipelineIntentScope scope, PipelineState actual) {
        var snapshot = rules.get();
        if (closed.get() || !snapshot.complete()) return true;
        var resource = new AlarmKey.Resource("pipeline", scope.pipelineId(), Long.toString(scope.stateGeneration()));
        return snapshot.definitions().stream().anyMatch(rule -> rule.header().active() && rule.enabled()
                && rule.resources().matches(resource) && (rule.metric() != null
                        || actual != PipelineState.STOPPED && rule.typeId().equals("pipeline.failed")));
    }

    @Override public boolean needsFreshTerminalMetrics(PipelineIntentScope scope) {
        var snapshot = rules.get();
        if (closed.get() || !snapshot.complete()) return true;
        var resource = new AlarmKey.Resource("pipeline", scope.pipelineId(), Long.toString(scope.stateGeneration()));
        return snapshot.definitions().stream().anyMatch(rule -> rule.header().active() && rule.enabled()
                && rule.resources().matches(resource) && rule.metric() != null);
    }

    @Override public void expecting(PipelineConvergencePass pass) {
        if (pass == null || closed.get()) return;
        synchronized (objectAdmission) {
            var key=new AdmissionKey(pass.scope(),pass.claim());
            pipelineAdmissions.entrySet().removeIf(entry -> entry.getKey().scope().equals(key.scope())
                    && (entry.getKey().claim().claimGeneration() < key.claim().claimGeneration()
                        || entry.getKey().claim().claimGeneration() == key.claim().claimGeneration()
                            && entry.getKey().claim().executionGeneration() < key.claim().executionGeneration())
                    && entry.getValue().idle());
            var slot = slot(pass);
            if (slot != null) {
                if (slot.terminal != null) slot.terminal.invalid = true;
                slot.terminal = null;slot.admission.reopen();
            }
        }
    }
    private Slot slot(PipelineConvergencePass pass) {
        var producer = new Producer(pass.scope(), pass.claim(), pass.intent());
        String id = pass.scope().pipelineId();
        synchronized (objectAdmission) {
            var existing = slots.get(id);
            if (existing != null && existing.producer.equals(producer)) return existing;
            if (existing == null && slots.size() + resourceSlots.size() >= MAX_OBJECTS) { reject(null, "object-budget"); return null; }
            var key = new AdmissionKey(producer.scope(), producer.claim());
            var admission = pipelineAdmissions.computeIfAbsent(key, ignored -> new TerminalPublication.Admission());
            var created = new Slot(producer, clock.instant(), existing, admission); slots.put(id, created); return created;
        }
    }
    final class PipelineGuard implements Runnable, AutoCloseable {
        final ExecutionFence original;
        final Runnable guard;
        final TerminalPublication.Admission admission;
        final AtomicBoolean retained = new AtomicBoolean(true);
        PipelineGuard(ExecutionFence original, Runnable guard) {
            this.original=original;this.guard=guard;
            admission=pipelineAdmissions.computeIfAbsent(new AdmissionKey(original), ignored -> new TerminalPublication.Admission());
            if(!admission.enter())throw ThreeLayerStorePort.refused();
        }
        boolean retained() { return retained.get(); }
        public void run() { guard.run(); }
        public void close() { if(retained.compareAndSet(true,false))admission.leave(); }
        void failed() { admission.failed=true; }
    }
    PipelineGuard pipelineGuard(ExecutionFence original, Runnable guard) { return new PipelineGuard(original, guard); }

    private final class TerminalAlerts implements TerminalPublication.Receipt {
        final Slot slot;
        final TerminalPublication publication;
        final RuleSnapshot snapshot;
        final PipelineState actual;
        final long interruptions, freshnessSequence;
        volatile boolean completed, invalid, admitted;
        TerminalAlerts(Slot slot, TerminalPublication publication, PipelineState actual) {
            this.slot = slot; this.publication = publication; this.actual = actual;
            snapshot = rules.get(); interruptions = slot.interruptions.get();
            freshnessSequence = slot.freshnessSequence.getAndIncrement();
        }
        public void reopen() {
            if(slots.get(publication.scope().pipelineId())==slot && slot.terminal==this)slot.admission.reopen();
        }
        public boolean seal() {
            return status() == TerminalPublication.Status.COMPLETE && slot.admission.seal()
                    && status() == TerminalPublication.Status.COMPLETE;
        }
        public TerminalPublication.Status status() {
            if (closed.get() || invalid || slot.admission.failed || slots.get(publication.scope().pipelineId()) != slot || slot.terminal != this
                    || slot.interruptions.get() != interruptions || !snapshot.equals(rules.get())
                    || needsFrequentTerminalSamples(publication.scope(), actual)) return TerminalPublication.Status.FAILED;
            synchronized (slot.pending) {
                if (!slot.admission.idle() || slot.scheduled || !slot.pending.isEmpty()) return TerminalPublication.Status.PENDING;
            }
            return completed ? TerminalPublication.Status.COMPLETE : TerminalPublication.Status.FAILED;
        }
    }

    @Override public TerminalPublication.Receipt terminalPublished(PipelineConvergencePass pass, TerminalPublication terminal,
            Observation observation, Throwable cause, boolean measured) {
        if (pass == null || closed.get() || !terminal.equals(TerminalPublication.from(pass, terminal.checkpointEpoch(), observation))
                || observation.state() != PipelineState.STOPPED && observation.state() != PipelineState.FAILED
                || needsFrequentTerminalSamples(terminal.scope(), observation.state())) return TerminalPublication.failed();
        var slot = slot(pass);
        if (slot == null) return TerminalPublication.failed();
        TerminalAlerts receipt;
        synchronized (slot.pending) {
            var previous = slot.terminal;
            if (previous != null && previous.publication.equals(terminal)
                    && previous.status() != TerminalPublication.Status.FAILED) return previous;
            receipt = new TerminalAlerts(slot, terminal, observation.state());
            slot.terminal = receipt;
        }
        publish(slot, slot.sequence.getAndIncrement(), observation, cause,
                () -> expectedSlot(pass) == slot && slot.terminal == receipt, measured, receipt);
        return receipt;
    }

    @Override public void published(PipelineConvergencePass pass, Observation observation, Throwable cause) {
        published(pass, observation, cause, true);
    }
    @Override public void published(PipelineConvergencePass pass, Observation observation, Throwable cause, boolean measured) {
        if (pass == null || closed.get()) return;
        Slot slot = slot(pass); if (slot == null) return;
        publish(slot, slot.sequence.getAndIncrement(), observation, cause, () -> true, measured);
    }
    @Override public void publishedIfExpected(PipelineConvergencePass pass, Observation observation, Throwable cause) {
        publishIfExpected(pass, observation, cause, () -> true);
    }

    @Override public void publishedPeriodic(PipelineConvergencePass pass, Observation observation, BooleanSupplier current) {
        publishIfExpected(pass, observation, null, current);
    }

    private void publishIfExpected(PipelineConvergencePass pass, Observation observation, Throwable cause, BooleanSupplier current) {
        if (closed.get() || !current.getAsBoolean()) return;
        Slot slot;
        long sequence;
        synchronized (objectAdmission) {
            slot = expectedSlot(pass);
            if (slot == null) return;
            // Consecutive intents for one incarnation share the sequence counter.
            sequence = slot.sequence.getAndIncrement();
        }
        publish(slot, sequence, observation, cause, () -> current.getAsBoolean()
                && slots.get(slot.producer.scope().pipelineId()) == slot);
    }

    private void publish(Slot slot, long sequence, Observation observation, Throwable cause, BooleanSupplier current) {
        publish(slot, sequence, observation, cause, current, true);
    }

    private void publish(Slot slot, long sequence, Observation observation, Throwable cause, BooleanSupplier current, boolean measured) {
        publish(slot, sequence, observation, cause, current, measured, null);
    }

    private void publish(Slot slot, long sequence, Observation observation, Throwable cause, BooleanSupplier current,
            boolean measured, TerminalAlerts terminal) {
        if (terminal == null) {
            if (slot.admission.sealed()) return;
            synchronized (slot.pending) { if (slot.terminal != null) slot.terminal.invalid = true; }
        }
        int bytes = inputBytes(observation);
        if (bytes < 0 || observation.observedAt() == null || observation.observedAt().isAfter(clock.instant())) {
            reject(slot, "input-budget-or-clock"); return;
        }
        var original = new Observation(observation.pipelineId(), observation.state(), Map.of(), Map.of(), Map.of(),
                null, observation.observedAt(), observation.facts());
        var trace = (diagnostics == null ? AlarmDiagnosticCapture.capture(cause, List.of(), failure -> failure instanceof TapstateException coded
                ? Optional.of(coded.code().code()) : Optional.empty()) : diagnostics.pipeline(cause,observation.failure())).orElse(null);
        var capturedRules = (terminal == null ? rules.get() : terminal.snapshot).definitions();
        submit(slot, bytes + 16 * 1024, () -> {
            if (!current.getAsBoolean()) return;
            try {
                publishCurrent(slot, original, trace, capturedRules, current, sequence, measured, terminal);
                if (terminal != null && current.getAsBoolean()) terminal.completed = true;
            } catch (TapstateException refused) {
                if (current.getAsBoolean()) throw refused;
            }
        },()->{if(terminal!=null)terminal.invalid=true;},terminal);
    }

    private void publishCurrent(Slot slot, Observation original, AlarmDiagnostics.SafeTrace trace,
            List<AlertConfiguration.Rule> capturedRules, BooleanSupplier current, long sequence, boolean measured, TerminalAlerts terminal) {
        var proved = currentProof(slot);
        if (!current.getAsBoolean()) return;
        long interruption = slot.interruptions.get();
        slot.actual = original.state();
        if (slot.publication == null || original.observedAt().isAfter(slot.publication)) slot.publication = original.observedAt();
        for (var rule : matching(slot, capturedRules)) {
            var binding = binding(slot, rule,proved);
            if (rule.typeId().equals("pipeline.observation-stale") || !measured && rule.metric() != null) continue;
            if (slot.discontinuous()) accept(binding, sequence, new AlertInput.Discontinuity(clock.instant()));
            AlertInput input = rule.metric() != null ? new AlertInput.Metric(original)
                    : new AlertInput.Conditions(original.observedAt(), slot.producer.intent().desired().targetState() != PipelineState.RUNNING
                            ? AlertSignal.Condition.UNKNOWN : original.state() == PipelineState.FAILED
                            ? AlertSignal.Condition.BAD : original.state() == PipelineState.RUNNING
                            && slot.producer.intent().desired().targetState() == PipelineState.RUNNING
                            ? AlertSignal.Condition.GOOD : AlertSignal.Condition.UNKNOWN, trace);
            requireTerminalAcceptance(accept(binding, sequence, input), terminal != null && rule.header().active() && rule.enabled());
        }
        if (terminal != null) pipelineTimer(slot, capturedRules, -1, terminal.freshnessSequence, proved, true);
        requireCurrent(slot,proved);
        slot.recoveredInterruption = interruption;
    }
    @Override public void unavailable(PipelineConvergencePass pass) {
        if (pass != null) reject(slots.get(pass.scope().pipelineId()), "publication-unavailable");
    }
    @Override public void unavailableIfExpected(PipelineConvergencePass pass) {
        synchronized (objectAdmission) {
            var slot = expectedSlot(pass);
            if (slot != null) reject(slot, "publication-unavailable");
        }
    }
    private Slot expectedSlot(PipelineConvergencePass pass) {
        if (pass == null || closed.get()) return null;
        var producer = new Producer(pass.scope(), pass.claim(), pass.intent());
        var current = slots.get(producer.scope().pipelineId());
        return current != null && current.producer.equals(producer) ? current : null;
    }
    @Override public void retain(Collection<String> ids) {
        slots.keySet().retainAll(Set.copyOf(ids));
    }
    /** Independent of convergence and of exporter callbacks; ordinary startup never prepares old storage. */
    void start(){
        if(closed.get()||!started.compareAndSet(false,true))throw new IllegalStateException("evaluation host already started or closed");
        freshness.setRemoveOnCancelPolicy(true);freshness.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS);
    }
    private final AtomicReference<Runnable> observerTick=new AtomicReference<>(()->{});
    void observerTick(Runnable observer){observerTick.set(Objects.requireNonNull(observer));}
    void tick() {
        if (closed.get()) return;
        long round=evaluationRound.incrementAndGet();
        if (refreshing.compareAndSet(false, true)) {
            var previousRules = rules.get();
            submit(null, 0, () -> {
                try { refresh(round); }
                catch (RuntimeException | Error failed) {
                    rules.compareAndSet(previousRules, new RuleSnapshot(previousRules.definitions(), false));
                    throw failed;
                } finally { refreshing.set(false); }
            }, () -> {
                rules.compareAndSet(previousRules, new RuleSnapshot(previousRules.definitions(), false));
                refreshing.set(false);
            });
        }
        var turns = new ArrayList<TimerTurn>();
        long currentRound = round;
        resourceSlots.forEach((resource, slot) -> turns.add(new TimerTurn(slot, () -> resourceTimerOffer(resource, slot, currentRound))));
        var capturedRules = rules.get().definitions();
        slots.values().forEach(slot -> turns.add(new TimerTurn(slot, () -> pipelineTimerOffer(slot, capturedRules, currentRound))));
        int start = 0;
        for (int index = 0; index < turns.size(); index++) if (turns.get(index).identity() == nextTimer) { start = index;break; }
        for (int offset = 0; offset < turns.size(); offset++) {
            var turn = turns.get((start + offset) % turns.size());
            if (!turn.offer().getAsBoolean()) { nextTimer = turn.identity();return; }
        }
        nextTimer = null;
    }

    private boolean resourceTimerOffer(AlarmKey.Resource resource, ResourceSlot slot, long round) {
        if (!slot.tickQueued.compareAndSet(false, true)) return true;
        if (!inputAdmission.enter()) { slot.tickQueued.set(false);return false; }
        try {
            return submitResourceReserved(slot, 0, () -> { try {
                if (resourceSlots.get(resource) != slot) return;
                for (var entry : slot.bindings.entrySet()) {
                    var previous = entry.getValue();
                    var current = rules.get().definitions().stream().filter(rule -> rule.header().identity().equals(previous.definition().header().identity())
                            && rule.matches(entry.getKey())).findFirst();
                    if (current.isEmpty()) continue;
                    var binding = new AlertEvaluationHost.Binding(current.orElseThrow(), previous.resource(), previous.producer(),
                            previous.sourceBindingRevision(), active, () -> null, previous.domainToken());
                    entry.setValue(binding);tickBinding(round, binding);
                }
            } finally { slot.tickQueued.set(false); } }, () -> slot.tickQueued.set(false), null, TIMER_TASK_LIMIT);
        } finally { inputAdmission.leave(); }
    }

    private boolean pipelineTimerOffer(Slot slot, List<AlertConfiguration.Rule> capturedRules, long round) {
        var terminal = slot.terminal;
        if (slot.admission.sealed() || terminal != null && !terminal.invalid
                && (terminal.completed || terminal.admitted && terminal.status() == TerminalPublication.Status.PENDING)
                && terminal.actual == PipelineState.STOPPED && terminal.snapshot.equals(rules.get())
                && !needsFrequentTerminalSamples(terminal.publication.scope(), PipelineState.STOPPED)) return true;
        if (!slot.tickQueued.compareAndSet(false, true)) return true;
        if (!inputAdmission.enter()) { slot.tickQueued.set(false);return false; }
        try {
            return submitReserved(slot, 0, () -> {
                long sequence = slot.freshnessSequence.getAndIncrement();
                return () -> { try {
                    var proved = currentProof(slot);long interruption = slot.interruptions.get();
                    pipelineTimer(slot, capturedRules, round, sequence, proved, false);
                    requireCurrent(slot, proved);slot.recoveredInterruption = interruption;
                } finally { slot.tickQueued.set(false); } };
            }, () -> slot.tickQueued.set(false), TIMER_TASK_LIMIT);
        } finally { inputAdmission.leave(); }
    }

    private void pipelineTimer(Slot slot, List<AlertConfiguration.Rule> capturedRules, long round,
            long sequence, Runnable proved, boolean terminal) {
        for (var rule : matching(slot, capturedRules)) {
            var binding = binding(slot, rule, proved);
            if (!rule.enabled() || !rule.header().active()) {
                AlertEvaluationHost.requireResult(services.get().evaluator().end(binding.key(), AlertSnapshot.EndReason.DISABLED, binding.requireCurrent()));
            } else if (rule.typeId().equals("pipeline.observation-stale")) {
                Instant now = clock.instant();
                boolean expected = slot.producer.intent().desired().targetState() == PipelineState.RUNNING
                        && slot.actual != PipelineState.COMPLETED && slot.actual != PipelineState.STOPPED
                        && slot.actual != PipelineState.PAUSED && slot.actual != PipelineState.FAILED;
                var retained = terminal ? Optional.<CachedState>empty() : loaded(round, binding.key());
                var outcome = accept(binding, sequence, new AlertInput.Freshness(now, expected, slot.expectedSince, slot.publication,
                        AlertEvaluationHost.seconds(rule, "startup-grace", 120), AlertEvaluationHost.seconds(rule, "max-age", 120)),
                        false, retained.isPresent(), retained.flatMap(CachedState::row));
                requireTerminalAcceptance(outcome, terminal);
            } else {
                if (!terminal && slot.discontinuous()) accept(binding, slot.sequence.get(), new AlertInput.Discontinuity(clock.instant()));
                boolean stopped = rule.typeId().equals("pipeline.failed") && rule.metric() == null
                        && slot.actual == PipelineState.STOPPED && slot.producer.intent().desired().targetState() == PipelineState.STOPPED;
                tickBinding(round, binding, stopped);
            }
        }
    }
    private void refresh(long round) {
        Failure previousFailure=failure.get();
        active.run();
        Services resolved = services.get() == null ? Objects.requireNonNull(open.get()) : services.get();
        ownedServices.add(resolved);
        if(closed.get()){stop(resolved);return;}
        var all = new ArrayList<AlertConfiguration.Rule>();
        var unavailableRules=new HashSet<String>();
        var after=reconciliationCursor.get();long readStarted=System.nanoTime();
        var snapshot=resolved.store().evaluationSnapshot(after,50);
        var page=snapshot.rules();
        unavailableRules.addAll(page.unavailableRules().keySet());
        for (var definition : page.rows()) {
            if (!(definition instanceof AlertConfiguration.Rule rule)) throw refused("rule-kind");
            all.add(rule.validated(resolved.catalog()));
        }
        if (all.size()+unavailableRules.size() > 1000 || all.size()+unavailableRules.size() == 1000 && page.nextId() != null) throw refused("rule-snapshot-budget");
        active.run(); synchronized(this){
            if(closed.get()){stop(resolved);return;}
            if(services.get()==null&&resolved.planner()!=null)resolved.planner().start(); if(services.get()==null&&resolved.delivery()!=null)resolved.delivery().start(); if(services.get()==null&&resolved.maintenance()!=null){resolved.maintenance().evaluation(this::health);resolved.maintenance().start();} services.set(resolved); rules.set(new RuleSnapshot(List.copyOf(all), unavailableRules.isEmpty()));
            servicesReady.countDown();
        }
        observerTick.get().run();
        if(reconciliationCursor.get()==null)legacyBindingsInScan=false;
        var retained = snapshot.states();
        // Node lifecycle evidence is published by the observer after the current rules are installed.
        if (retained.rows().stream().anyMatch(row -> row.value().snapshot().rule().key().resource().kind().equals("cluster-node")))
            retained = resolved.store().evaluationStates(after,50);
        for(var row:retained.rows())if((row.value().binding()==null||!unavailableRules.contains(row.value().binding().ruleIdentity().id()))&&retained.removedResources().contains(row.value().snapshot().rule().key().resource())){
            if(row.value().retiredAt()==null)AlertEvaluationHost.requireResult(resolved.evaluator().end(row.value().snapshot().rule().key(),AlertSnapshot.EndReason.RESOURCE_REMOVED,active));
            var resource=row.value().snapshot().rule().key().resource();ResourceSlot obsolete;
            synchronized(objectAdmission){obsolete=resourceSlots.remove(resource);if(obsolete!=null){retainedResourceBindings.addAndGet(-obsolete.bindings.size());obsolete.bindings.clear();}}
            if(obsolete!=null)cancelResource(obsolete);resourceRetired.get().accept(resource);
        }
        boolean legacyBindingUnavailable=false;
        for(var key:retained.obsoleteBindings()) {
            var original=retained.rows().stream().filter(row->row.value().snapshot().rule().key().equals(key)).findFirst().orElseThrow().value();
            if(original.binding()!=null&&unavailableRules.contains(original.binding().ruleIdentity().id()))continue;
            if(original.retiredAt()==null && original.snapshot().enabled()) {
                if(original.operationalEvidence()!=null && original.operationalEvidence().version()==2 && AlertOperationDomain.supported(key))
                    AlertEvaluationHost.requireResult(resolved.evaluator().supersede(key,active));
                else legacyBindingUnavailable=true;
            }
            synchronized(objectAdmission){var slot=resourceSlots.get(key.resource());
                if(slot!=null && slot.bindings.remove(key)!=null)retainedResourceBindings.decrementAndGet();}
        }
        resolved.evaluator().reconcileRules(all,retained.rows().stream().filter(row->row.value().binding()==null||!unavailableRules.contains(row.value().binding().ruleIdentity().id())).toList(),active);
        for(var row:retained.rows()) {
            var head=row.value();var key=head.snapshot().rule().key();
            if(key.resource().kind().equals("pipeline") || key.resource().kind().equals("cluster-node") || head.retiredAt()!=null
                    || retained.removedResources().contains(key.resource()) || retained.obsoleteBindings().contains(key) || head.binding()==null)continue;
            var slot=resourceSlots.get(key.resource());if(slot!=null && slot.bindings.containsKey(key))continue;
            var definition=all.stream().filter(rule->rule.header().identity().equals(head.binding().ruleIdentity()) && rule.matches(key)).findFirst();
            if(definition.isEmpty())continue;
            var binding=new AlertEvaluationHost.Binding(definition.orElseThrow(),key.resource(),head.snapshot().rule().producerIncarnation(),
                    head.binding().sourceBindingRevision(),active,()->null,AlertOperationDomain.token(head.binding().ruleIdentity().incarnation(),key));
            var result=resolved.evaluator().tick(binding,Optional.of(row));
            if(result==AlertEvaluationHost.Result.CONFLICT)throw refused("retained-resource-contention");
        }
        synchronized(objectAdmission) {
            for(var slot:resourceSlots.values())for(var entry:slot.bindings.entrySet()) {
                if(all.stream().noneMatch(rule->rule.header().identity().equals(entry.getValue().definition().header().identity()) && rule.matches(entry.getKey()))
                        && slot.bindings.remove(entry.getKey(),entry.getValue()))retainedResourceBindings.decrementAndGet();
            }
        }
        legacyBindingsInScan|=legacyBindingUnavailable;
        if(legacyBindingUnavailable)legacyBindingsUnavailable=true;
        if(retained.next()==null){legacyBindingsUnavailable=legacyBindingsInScan;legacyScanComplete=true;}
        reconciliationCursor.set(retained.next());
        boolean missing=all.stream().anyMatch(r->r.header().active()&&r.enabled()&&r.metric()==null
                &&!resolved.providers().contains(resolved.catalog().types().stream().filter(t->t.id().equals(r.typeId())).findFirst().orElseThrow().providerId()));
        if(!unavailableRules.isEmpty())failure.compareAndSet(previousFailure,new Failure(AlertError.INVALID_CONFIGURATION.code(),Map.of("field","evaluationHost","reason","rule-capability-unavailable")));
        else if(missing || legacyBindingsUnavailable || !legacyScanComplete)failure.compareAndSet(previousFailure,new Failure(AlertError.INVALID_CONFIGURATION.code(),Map.of("field","evaluationHost","reason","input-unavailable")));
        else if (slots.values().stream().noneMatch(Slot::discontinuous)) failure.compareAndSet(previousFailure,null);
        var rows=new HashMap<AlarmKey,AlertStore.Stored<AlertStateRecord.Head>>();
        retained.rows().forEach(row->rows.put(row.value().snapshot().rule().key(),row));
        evaluationRead.set(new EvaluationRead(round, readStarted + TimeUnit.SECONDS.toNanos(1),
                after == null && retained.next() == null, Map.copyOf(rows)));
    }
    /** The original operation has already occurred; enqueueing alone is never durable acceptance. */
    void resourceInput(AlarmKey.Resource resource, String typeId, long sourceRevision, String producer,
            Runnable originalGuard, AlertInput input) {
        resourceInput(resource,typeId,sourceRevision,producer,originalGuard,active,input);
    }
    void resourceInput(AlarmKey.Resource resource,String typeId,long sourceRevision,String producer,
            Runnable originalGuard,Runnable localProof,AlertInput input) {resourceInput(resource,typeId,sourceRevision,producer,originalGuard,localProof,input,()->{});}
    void resourceInput(AlarmKey.Resource resource,String typeId,long sourceRevision,String producer,
            Runnable originalGuard,Runnable localProof,AlertInput input,Runnable settled) {
        resourceInput(resource,typeId,sourceRevision,producer,originalGuard,localProof,input,settled,false);
    }
    /** Every admitted member repeats this same authoritative snapshot; only the current owner writes it. */
    void sharedResourceInput(AlarmKey.Resource resource,String typeId,long sourceRevision,String producer,
            Runnable originalGuard,AlertInput input) {
        resourceInput(resource,typeId,sourceRevision,producer,originalGuard,active,input,()->{},true);
    }
    /** Read-only inspection evidence uses the original cluster binding and never an external delivery route. */
    void structureInspection(AlarmKey.Resource resource, Runnable originalGuard, Instant observedAt,
            SystemCollections.Database domain, List<MongoStructureInspection.Difference> differences, boolean complete) {
        String type = switch (domain) {
            case CONFIG -> "cluster.config-structure-drift";
            case CACHE -> "cluster.cache-structure-drift";
            default -> throw new IllegalArgumentException("unsupported structure inspection domain");
        };
        var input = structureInspectionInput(observedAt, domain, differences, complete);
        resourceInput(resource, type, 0, "cluster-structure-inspection", originalGuard, input);
    }
    static AlertInput.Conditions structureInspectionInput(Instant observedAt, SystemCollections.Database domain,
            List<MongoStructureInspection.Difference> differences, boolean complete) {
        var findings = List.copyOf(differences);
        if(findings.stream().anyMatch(d -> d.domain()!=domain))throw new IllegalArgumentException("foreign inspection domain");
        // Partial observations cannot establish recovery, but confirmed differences remain actionable.
        var condition = !findings.isEmpty() ? AlertSignal.Condition.BAD
                : complete ? AlertSignal.Condition.GOOD : AlertSignal.Condition.UNKNOWN;
        AlarmDiagnostics.SafeTrace trace = null;
        if(!findings.isEmpty()) {
            StringBuilder text = new StringBuilder("Managed ").append(domain).append(" structure inspection:\n");
            String command = complete && findings.size()==1 ? findings.getFirst().repairCommand() : null;
            // Put the only eligible command before bounded details so trace truncation cannot cut it.
            if(command!=null && command.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=512
                    && command.codePoints().noneMatch(Character::isISOControl))
                text.append("Manual repair: ").append(command).append('\n');
            else text.append("No safe manual repair command is available. Contact support.\n");
            int count = Math.min(findings.size(), 16);
            for(int i=0;i<count;i++)text.append(findings.get(i).description()).append('\n');
            if(findings.size()>count)text.append("Additional differences omitted.\n");
            String safe = text.toString().codePoints().collect(StringBuilder::new,
                    (out, cp) -> out.appendCodePoint(Character.isISOControl(cp) && cp!='\n' ? ' ' : cp),
                    StringBuilder::append).toString();
            trace = new AlarmDiagnostics.SafeTrace(safe, 1, false, findings.size()>count);
        }
        return new AlertInput.Conditions(observedAt, condition, trace);
    }
    void cacheInput(AlarmKey.Resource resource, String producer, Runnable guard, AlertInput.Operational input) {
        if(!inputAdmission.enter())return;
        try {
            resourceInputAdmitted(resource,"cluster.storage-unavailable",0,producer,guard,active,input,()->{},false,input);
        } finally {inputAdmission.leave();}
    }
    private void resourceInput(AlarmKey.Resource resource,String typeId,long sourceRevision,String producer,
            Runnable originalGuard,Runnable localProof,AlertInput input,Runnable settled,boolean shared) {
        if(!inputAdmission.enter()){if(originalGuard instanceof PipelineGuard owner)owner.failed();settled.run();return;}
        try {resourceInputAdmitted(resource,typeId,sourceRevision,producer,originalGuard,localProof,input,settled,shared,null);}
        finally{inputAdmission.leave();}
    }
    private void resourceInputAdmitted(AlarmKey.Resource resource,String typeId,long sourceRevision,String producer,
            Runnable originalGuard,Runnable localProof,AlertInput input,Runnable settled,boolean shared,AlertInput.Operational cacheSample) {
        var owner=originalGuard instanceof PipelineGuard owned ? owned : null;
        var successful=new AtomicBoolean();var released=new AtomicBoolean();
        if(owner!=null && !owner.admission.enter()){settled.run();return;}
        Runnable completion=()->{
            if(!released.compareAndSet(false,true))return;
            if(owner!=null){if(!successful.get())owner.failed();owner.admission.leave();}
            settled.run();
        };
        if(closed.get()){completion.run();return;}
        ResourceSlot slot;
        synchronized(objectAdmission){
            if(!resourceSlots.containsKey(resource)&&resourceSlots.size()+slots.size()>=MAX_OBJECTS){reject(null,"object-budget");completion.run();return;}
            slot=resourceSlots.computeIfAbsent(resource,ignored->new ResourceSlot());
        }
        var snapshot=rules.get();
        if(owner!=null && !snapshot.complete())owner.failed();
        var captured=snapshot.definitions();
        var trace = input instanceof AlertInput.Operational op ? op.trace()
                : input instanceof AlertInput.Conditions conditions ? conditions.trace() : null;
        int inputSize=512+(trace==null?0:trace.text().getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        submitResourceAdmitted(slot,inputSize,()->{try{
            if(resourceSlots.get(resource)!=slot)throw refused("retired-resource");
            active.run();originalGuard.run();var resolved=services.get();
            if(resolved==null)throw refused("storage-not-ready");
            var matching=captured.stream().filter(r->r.typeId().equals(typeId)&&r.resources().matches(resource)).toList();
            if(matching.size()>MAX_MATCHES)throw refused("matching-rule-budget");
            for(var rule:matching){
                String domain=input instanceof AlertInput.Operational op?op.domainToken():null;
                Runnable current=()->{active.run();localProof.run();};
                java.util.function.Supplier<AlertStore.EvaluationProof> proof=domain==null?()->null:()->AlertStore.EvaluationProof.operation(current);
                if(owner!=null&&domain!=null)proof=()->AlertStore.EvaluationProof.operation(
                        (AlertStore.EvaluationProof.PipelineOperationGuard)()->{active.run();originalGuard.run();localProof.run();});
                var binding=new AlertEvaluationHost.Binding(rule,resource,producer,sourceRevision,current,proof,domain);
                var before=resolved.store().state(binding.key(),new AlertStore.ReadAccess(true,Set.of(),active));
                if(before.isEmpty() && input instanceof AlertInput.Operational op && op.domainToken()!=null && op.succeeded())continue;
                if (before.isEmpty() && AlertEvaluationHost.structureInspection(typeId)
                        && input instanceof AlertInput.Conditions conditions
                        && AlertConditionGroup.evaluate(conditions.operator(), conditions.values()) != AlertSignal.Condition.BAD) continue;
                synchronized(objectAdmission) {
                    if(closed.get() || resourceSlots.get(resource)!=slot)throw refused("retired-resource");
                    if(!slot.bindings.containsKey(binding.key())) {
                        if(retainedResourceBindings.get()>=MAX_OBJECTS*MAX_MATCHES)throw refused("resource-binding-budget");
                        retainedResourceBindings.incrementAndGet();
                    }
                    slot.bindings.put(binding.key(),binding);
                }
                long next=before.map(v->Math.incrementExact(v.value().snapshot().lastSequence())).orElse(0L);
                if(input instanceof AlertInput.Operational op && before.isPresent()){
                    var head=before.orElseThrow().value();var evidence=head.operationalEvidence();
                    if(head.snapshot().rule().revision()==rule.evaluationRevision()&&head.snapshot().rule().producerIncarnation().equals(producer)
                            &&head.binding().sourceBindingRevision()==sourceRevision&&head.snapshot().enabled()
                            &&evidence!=null&&evidence.bindingDigest().equals(op.bindingDigest())
                            &&evidence.equals(evidence.observe(op.capability(),op.succeeded()))
                            &&head.snapshot().status()!=AlertSnapshot.Status.PENDING
                            &&head.snapshot().recoverySince()==null
                            &&head.snapshot().quality()==AlertSnapshot.Quality.VALID
                            &&head.snapshot().lastObservedAt()!=null
                            &&(op.trace()==null||head.episode()!=null&&head.episode().diagnostics().status()!=AlertStateRecord.DiagnosticStatus.NOT_CAPTURED
                                &&head.episode().diagnostics().status()!=AlertStateRecord.DiagnosticStatus.NOT_APPLICABLE)
                            &&op.observedAt().isBefore(head.snapshot().lastObservedAt().plus(healthyCoalescingWindow(head.snapshot()))))continue;
                }
                var outcome=accept(binding, next, input, shared, true, before);
                if(owner!=null && rule.header().active() && rule.enabled() && outcome!=AlertEvaluationHost.Result.ACCEPTED)owner.failed();
            }
            successful.set(true);
        }finally{completion.run();}},completion,cacheSample);
    }
    private static Duration healthyCoalescingWindow(AlertSnapshot snapshot) {
        // Leave room for the producer's sampling cadence inside the original continuity bound.
        var halfGap=snapshot.rule().maxGap().dividedBy(2);var ceiling=Duration.ofSeconds(5);
        return halfGap.compareTo(ceiling)<0?halfGap:ceiling;
    }
    private void submitResource(ResourceSlot slot,int bytes,Runnable action){submitResource(slot,bytes,action,()->{});}
    private void submitResource(ResourceSlot slot,int bytes,Runnable action,Runnable refused){
        if(!inputAdmission.enter()){refused.run();return;}
        try {submitResourceAdmitted(slot,bytes,action,refused,null);}finally{inputAdmission.leave();}
    }
    private void submitResourceAdmitted(ResourceSlot slot,int bytes,Runnable action,Runnable refused,AlertInput.Operational cacheSample){
        submitResourceReserved(slot,bytes,action,refused,cacheSample,QUEUE_SIZE);
    }
    private boolean submitResourceReserved(ResourceSlot slot,int bytes,Runnable action,Runnable refused,AlertInput.Operational cacheSample,int taskLimit){
        if(closed.get()){refused.run();return false;}
        boolean start=false;
        String refusal=null;
        synchronized(slot.pending){
            boolean coalesce=false;
            if(cacheSample!=null){
                var capability=cacheSample.capability();
                if(!cacheSample.succeeded()){
                    // No healthy sample may move past a failure, even across capabilities.
                    slot.pendingHealthyCache.clear();
                    slot.recoveringCache.add(capability);
                }else if(slot.recoveringCache.contains(capability)){
                    slot.pendingHealthyCache.clear();
                }else{
                    coalesce=true;
                    var previous=slot.pendingHealthyCache.get(capability);
                    if(previous!=null){
                        // The resource's durable sequence is shared by read and write.
                        previous.action=action;
                        inputTimes.put(previous,clock.instant());
                        slot.pending.remove(previous);
                        slot.pending.addLast(previous);
                        return true;
                    }
                }
            }
            if(queuedTasks.incrementAndGet()>taskLimit){
                queuedTasks.decrementAndGet();refusal="queue-capacity";
            }else if(queuedBytes.addAndGet(bytes)>MAX_QUEUED_BYTES){
                queuedTasks.decrementAndGet();queuedBytes.addAndGet(-bytes);refusal="queue-byte-budget";
            }else{
                var work=new Work(null,bytes,action,refused);
                if(coalesce)slot.pendingHealthyCache.put(cacheSample.capability(),work);
                if(cacheSample!=null&&cacheSample.succeeded())slot.recoveringCache.remove(cacheSample.capability());
                slot.pending.addLast(work);start=!slot.scheduled;slot.scheduled=true;
            }
        }
        if(refusal!=null){if(taskLimit==QUEUE_SIZE || !refusal.equals("queue-capacity"))reject(null,refusal);refused.run();return false;}
        if(start)dispatchResource(slot);
        return true;
    }
    private void dispatchResource(ResourceSlot slot){
        try{workers.execute(new ResourceDispatcher(slot));}catch(RejectedExecutionException unavailable){cancelResource(slot);}
    }
    private final class ResourceDispatcher implements Runnable {
        final ResourceSlot slot;ResourceDispatcher(ResourceSlot slot){this.slot=slot;}
        public void run(){Work work;synchronized(slot.pending){
                work=slot.pending.pollFirst();
                slot.pendingHealthyCache.values().remove(work);
            }
            try{if(work!=null)work.run();}finally{boolean more;synchronized(slot.pending){more=!slot.pending.isEmpty();if(!more)slot.scheduled=false;}if(more)dispatchResource(slot);}
        }
    }
    private void cancelResource(ResourceSlot slot){List<Work> pending;synchronized(slot.pending){pending=List.copyOf(slot.pending);slot.pending.clear();slot.pendingHealthyCache.clear();slot.scheduled=false;}ShutdownAwait.all(pending.stream().<Runnable>map(work->work::cancel).toList());}
    void collectionUnavailable(String reason){reject(null,reason);}

    private List<AlertConfiguration.Rule> matching(Slot slot, List<AlertConfiguration.Rule> snapshot) {
        var resource = resource(slot);
        var matches = snapshot.stream().filter(r -> r.resources().matches(resource)
                && (r.typeId().equals("pipeline.failed") || r.typeId().equals("pipeline.observation-stale") || r.metric() != null)).toList();
        if (matches.size() > MAX_MATCHES) throw refused("matching-rule-budget");
        if (services.get() == null) throw refused("storage-not-ready");
        return matches;
    }
    private AlertEvaluationHost.Binding binding(Slot slot, AlertConfiguration.Rule rule,Runnable proved) {
        String producer = fingerprint(List.of(slot.producer.incarnation(),
                rule.typeId().equals("pipeline.observation-stale") ? "freshness" : "publication", runtimeId));
        // Active state updates carry the exact intent into the CONFIG transaction. The work
        // shares one original bounded lease within this queued input; every commit still checks
        // its exact intent and resource generation. Intermediate guards keep local identity.
        // Administrative END carries no producer proof, so it retains the full remote guard.
        Runnable current=rule.header().active()&&rule.enabled()?()->requireLocal(slot):()->requireCurrent(slot);
        return new AlertEvaluationHost.Binding(rule, resource(slot), producer, 0, current, () -> {
            Runnable lease=proved;
            return new AlertStore.EvaluationProof(slot.producer.intent(),()->{
                active.run();lease.run();if(closed.get()||slots.get(slot.producer.scope().pipelineId())!=slot)throw refused("original-producer");
            });
        });
    }
    private AlarmKey.Resource resource(Slot slot) {
        return new AlarmKey.Resource("pipeline", slot.producer.scope().pipelineId(), Long.toString(slot.producer.scope().stateGeneration()));
    }
    private void requireLocal(Slot slot) {
        active.run();
        if(closed.get()||slots.get(slot.producer.scope().pipelineId())!=slot)throw refused("original-producer");
    }
    private Runnable currentProof(Slot slot){
        requireLocal(slot);
        var proof=claimProof.apply(slot.producer.scope(),slot.producer.claim());
        if(proof.isEmpty()||intents.loadForActuation(slot.producer.scope().pipelineId()).filter(slot.producer.intent()::equals).isEmpty())throw refused("original-producer");
        var fixed=proof.orElseThrow();requireProof(slot,fixed);return fixed;
    }
    private void requireCurrent(Slot slot){currentProof(slot);}
    private void requireProof(Slot slot,Runnable proved){
        requireLocal(slot);
        try{proved.run();}catch(TapstateException expired){throw refused("original-producer");}
        active.run();
    }
    private void requireCurrent(Slot slot,Runnable proved){
        requireProof(slot,proved);
        if(intents.loadForActuation(slot.producer.scope().pipelineId()).filter(slot.producer.intent()::equals).isEmpty())throw refused("original-producer");
        requireProof(slot,proved);
    }
    private static void requireTerminalAcceptance(AlertEvaluationHost.Result result, boolean terminal) {
        if (terminal && result != AlertEvaluationHost.Result.ACCEPTED) throw refused("terminal-input-not-accepted");
    }
    private AlertEvaluationHost.Result accept(AlertEvaluationHost.Binding binding, long sequence, AlertInput input) {
        return accept(binding, sequence, input, false, false, Optional.empty());
    }
    private AlertEvaluationHost.Result accept(AlertEvaluationHost.Binding binding, long sequence, AlertInput input, boolean shared,
            boolean stateLoaded, Optional<AlertStore.Stored<AlertStateRecord.Head>> cachedState) {
        var outcome = services.get().evaluator().accept(binding, sequence, input, stateLoaded, cachedState);
        String deferral = outcome==AlertEvaluationHost.Result.BUSY ? (shared ? null : cacheHeartbeatDeferralRefusal(binding,input)) : "not-busy";
        if(outcome==AlertEvaluationHost.Result.BUSY && deferral==null)return outcome;
        if (outcome == AlertEvaluationHost.Result.CONFLICT || outcome == AlertEvaluationHost.Result.BUSY) {
            var refusal=refused("state-contention");
            long second=clock.instant().getEpochSecond(),previous=lastLogged.get();
            if((previous==Long.MIN_VALUE||second-previous>=60)&&lastLogged.compareAndSet(previous,second))
                LOG.warn("Alert collection is degraded [{}] [resourceKind={},type={},outcome={},capability={},succeeded={},deferralRefusal={}]",
                        refusal.code().code(),binding.resource().kind(),binding.definition().typeId(),outcome,
                        input instanceof AlertInput.Operational op?op.capability():null,
                        input instanceof AlertInput.Operational op?op.succeeded():null,deferral);
            throw refusal;
        }
        return outcome;
    }
    /** Deferral is not acceptance: no timestamp, sequence, capability or ownership is changed. */
    private String cacheHeartbeatDeferralRefusal(AlertEvaluationHost.Binding binding,AlertInput input) {
        if(!(input instanceof AlertInput.Operational op)||!op.succeeded()||op.domainToken()!=null||binding.domainToken()!=null
                ||!Set.of(OperationalEvidence.Capability.CACHE_READ,OperationalEvidence.Capability.CACHE_WRITE).contains(op.capability())
                ||!binding.resource().kind().equals("cluster-storage")||!binding.resource().id().equals("cache")
                ||!binding.definition().typeId().equals("cluster.storage-unavailable")||binding.sourceBindingRevision()!=0)return "ineligible-input";
        binding.requireCurrent().run();
        var current=services.get().store().definition(AlertConfiguration.Kind.RULE,binding.definition().header().identity().id(),null);
        if(current.isEmpty()||!current.orElseThrow().equals(binding.definition()))return "rule-changed";
        var retained=services.get().store().state(binding.key(),new AlertStore.ReadAccess(true,Set.of(),active));
        binding.requireCurrent().run();if(retained.isEmpty())return "head-missing";
        var head=retained.orElseThrow().value();var snapshot=head.snapshot();var evidence=head.operationalEvidence();var now=clock.instant();
        if(head.retiredAt()!=null||!snapshot.enabled()||!binding.definition().header().active()||!binding.definition().enabled())return "inactive";
        if(!Set.of(AlertSnapshot.Status.CLEAR,AlertSnapshot.Status.RESOLVED).contains(snapshot.status())
                ||snapshot.quality()!=AlertSnapshot.Quality.VALID||snapshot.pendingSince()!=null||snapshot.recoverySince()!=null)return "unhealthy-state";
        if(!head.owner().activeAt(now))return "owner-expired";
        if(head.binding()==null||!head.binding().ruleIdentity().equals(binding.definition().header().identity())
                ||head.binding().sourceBindingRevision()!=binding.sourceBindingRevision()
                ||snapshot.rule().revision()!=binding.definition().evaluationRevision()
                ||!snapshot.rule().producerIncarnation().equals(binding.producer()))return "binding-changed";
        if(evidence==null||evidence.version()!=1||evidence.failedCapabilities()!=0
                ||!evidence.bindingDigest().equals(op.bindingDigest()))return "evidence-changed";
        var observed=snapshot.lastObservedAt();var rule=snapshot.rule();
        if(observed==null)return "observation-missing";
        if(observed.isAfter(now)||op.observedAt().isAfter(now))return "future-observation";
        if(observed.plus(rule.maxAge()).isBefore(now))return "retained-observation-too-old";
        if(op.observedAt().plus(rule.maxAge()).isBefore(now))return "input-too-old";
        if(observed.plus(rule.maxGap()).isBefore(op.observedAt()))return "continuity-gap";
        return null;
    }
    private void submit(Slot slot, int bytes, Runnable action, Runnable refused) {
        submit(slot,bytes,action,refused,null);
    }
    private void submit(Slot slot,int bytes,Runnable action,Runnable refused,TerminalAlerts terminal) {
        if(!inputAdmission.enter()){refused.run();return;}
        try {submitReserved(slot,bytes,()->action,refused,QUEUE_SIZE,terminal);}finally{inputAdmission.leave();}
    }
    private boolean submitReserved(Slot slot, int bytes, Supplier<Runnable> action, Runnable refused, int taskLimit) {
        return submitReserved(slot,bytes,action,refused,taskLimit,null);
    }
    private boolean submitReserved(Slot slot,int bytes,Supplier<Runnable> action,Runnable refused,int taskLimit,TerminalAlerts terminal) {
        if(slot!=null && !slot.admission.enter()){refused.run();return false;}
        if (closed.get()) {if(slot!=null)slot.admission.leave();refused.run(); return false; }
        if (queuedTasks.incrementAndGet() > taskLimit) { queuedTasks.decrementAndGet();if(slot!=null)slot.admission.leave(); if(taskLimit==QUEUE_SIZE)reject(slot, "queue-capacity"); refused.run(); return false; }
        int retained = queuedBytes.addAndGet(bytes);
        if (retained > MAX_QUEUED_BYTES) { queuedTasks.decrementAndGet(); queuedBytes.addAndGet(-bytes);if(slot!=null)slot.admission.leave(); reject(slot, "queue-byte-budget"); refused.run(); return false; }
        Work work = new Work(slot, bytes, action.get(), refused);
        if (slot == null) {
            try { workers.execute(work); } catch (RejectedExecutionException full) { work.cancel(); }
        } else {
            boolean start;
            synchronized (slot.pending) {
                slot.pending.addLast(work);
                if(terminal!=null)terminal.admitted=true;
                start = !slot.scheduled; slot.scheduled = true;
            }
            if (start) dispatch(slot);
        }
        return true;
    }
    private final class Work implements Runnable {
        final Slot slot; final int bytes; Runnable action; final Runnable refused; final AtomicInteger state = new AtomicInteger();
        Work(Slot slot, int bytes, Runnable action, Runnable refused) {
            this.slot=slot;this.bytes=bytes;this.action=action;this.refused=refused;
            inputTimes.put(this,clock.instant());
        }
        public void run() {
            if(!state.compareAndSet(0,1))return;
            if (closed.get()) { try{refused.run();}finally{release();} return; }
            try { action.run(); lastEvaluatedAt=clock.instant(); }
            catch (RuntimeException unavailable) {
                if(unavailable instanceof TapstateException coded)reject(slot,coded);else reject(slot,"evaluation-failed");
                if(!(unavailable instanceof TapstateException))throw unavailable;
            }
            catch (Error fatal) {
                if(slot!=null){var current=slots.get(slot.producer.scope().pipelineId());
                    if(current!=null && current.admission==slot.admission && current.terminal!=null)current.terminal.invalid=true;}
                throw fatal;
            }
            finally { release(); }
        }
        void release() { if(state.getAndSet(2)!=2)settle(); }
        void settle(){queuedBytes.addAndGet(-bytes);queuedTasks.decrementAndGet();inputTimes.remove(this);if(slot!=null)slot.admission.leave();}
        void cancel() {
            if(state.compareAndSet(0,2))settlements.execute(()->{
                try{reject(slot,"closed-or-capacity");refused.run();}
                catch(RuntimeException|Error failure){settlementFailures.add(failure);}
                finally{settle();}
            });
        }
    }
    private void dispatch(Slot slot) {
        try { workers.execute(new Dispatcher(slot)); }
        catch(RejectedExecutionException stopped){cancelPending(slot);}
    }
    private final class Dispatcher implements Runnable {
        final Slot slot;
        Dispatcher(Slot slot){this.slot=slot;}
        public void run(){
            Work work;
            synchronized(slot.pending){work=slot.pending.pollFirst();}
            try { if(work!=null)work.run(); }
            finally {
                boolean more;
                synchronized(slot.pending){more=!slot.pending.isEmpty();if(!more)slot.scheduled=false;}
                if(more)dispatch(slot);
            }
        }
    }
    private void cancelPending(Slot slot) {
        List<Work> pending;
        synchronized(slot.pending){pending=List.copyOf(slot.pending);slot.pending.clear();slot.scheduled=false;}
        ShutdownAwait.all(pending.stream().<Runnable>map(work->work::cancel).toList());
    }
    private void reject(Slot slot, String reason) {
        reject(slot,refused(reason));
    }
    private void reject(Slot slot, TapstateException refusal) {
        if (slot != null) {
            slot.interruptions.incrementAndGet();
            var current=slots.get(slot.producer.scope().pipelineId());
            if(current!=null && current.admission==slot.admission && current.terminal!=null)current.terminal.invalid=true;
        }
        var parameters=new HashMap<String,Object>();
        refusal.args().forEach((key,value)->parameters.put(key,value instanceof String text&&text.matches("[a-zA-Z0-9._:-]{1,128}")?text:"redacted"));
        rejected.incrementAndGet(); failure.set(new Failure(refusal.code().code(),Map.copyOf(parameters)));
        long second = clock.instant().getEpochSecond(), previous = lastLogged.get();
        if ((previous == Long.MIN_VALUE || second - previous >= 60) && lastLogged.compareAndSet(previous, second)) {
            LOG.warn("Alert collection is degraded [{}]", refusal.code().code());
        }
    }
    Services managementServices() {
        active.run();
        // Management can arrive before the background rule snapshot publishes its services.
        if (!closed.get() && services.get() == null) {
            try {
                if (!servicesReady.await(5, TimeUnit.SECONDS)) {
                    throw new TapstateException(io.tapstate.control.core.ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
                }
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new TapstateException(io.tapstate.control.core.ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
            }
        }
        active.run();
        if (closed.get() || services.get() == null) throw new TapstateException(io.tapstate.control.core.ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        return services.get();
    }
    Health health() {
        Failure current=failure.get();var resolved=services.get();
        var planning=resolved==null||resolved.planner()==null?null:resolved.planner().failure();
        var delivery=resolved==null||resolved.delivery()==null?null:resolved.delivery().failure();
        if(current==null&&delivery!=null)current=new Failure(delivery.code(),delivery.parameters());
        if(current==null&&planning!=null)current=new Failure(planning.code(),planning.parameters());
        var maintenance=resolved==null||resolved.maintenance()==null?Map.<String,Object>of():resolved.maintenance().health();
        Long lag=lastEvaluatedAt==null?null:inputTimes.values().stream().min(Instant::compareTo).map(at->Math.max(0,Duration.between(at,clock.instant()).toMillis())).orElse(0L);
        return new Health(closed.get(),current!=null||Boolean.TRUE.equals(maintenance.get("degraded")),rejected.get(),current==null?null:current.code(),current==null?Map.of():current.parameters(),queuedTasks.get(),queuedBytes.get(),lag,lastEvaluatedAt,maintenance);
    }
    static int inputBytes(Observation observation) {
        long bytes = 128; int points = 0;
        if (observation.facts().size() > ObservationMetricInput.MAX_FACTS) return -1;
        for (MetricFact fact : observation.facts()) {
            bytes += 2L * (fact.name().length() + fact.unit().length()) + 64;
            for (MetricPoint point : fact.points()) {
                if (++points > ObservationMetricInput.MAX_POINTS) return -1;
                bytes += 128;
                for (var a : point.attributes().entrySet()) bytes += 2L * (a.getKey().length() + a.getValue().length()) + 64;
                if (bytes > MAX_INPUT_BYTES) return -1;
            }
            if (bytes > MAX_INPUT_BYTES) return -1;
        }
        return (int) bytes;
    }
    private static TapstateException refused(String field) {
        return new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", "evaluationHost", "reason", field), null);
    }
    private static String fingerprint(List<String> parts) {
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            for (String part : parts) {
                byte[] encoded = part.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(encoded.length).array()); digest.update(encoded);
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    @Override public void close() { requestStop(); }
    private CompletableFuture<Void> requestStop() {
        closed.set(true);inputAdmission.stop();
        servicesReady.countDown();
        var proposed=new CompletableFuture<Void>();
        var existing=stopping.compareAndExchange(null,proposed);
        if(existing!=null)return existing;
        try {initiateStop();proposed.complete(null);}
        catch(RuntimeException|Error failure){proposed.completeExceptionally(failure);stopping.compareAndSet(proposed,null);throw failure;}
        return proposed;
    }
    private void initiateStop() {
        closed.set(true);
        inputAdmission.stop();
        freshness.shutdown();workers.shutdown();
        var actions=new ArrayList<Runnable>();
        var cancelled=new ArrayList<Runnable>();workers.getQueue().drainTo(cancelled);
        for(Runnable task:cancelled){actions.add(()->{
                if(task instanceof Work work)work.cancel();
                else if(task instanceof Dispatcher dispatcher)cancelPending(dispatcher.slot);
                else if(task instanceof ResourceDispatcher dispatcher)cancelResource(dispatcher.slot);
        });}
        slots.values().forEach(slot->actions.add(()->cancelPending(slot)));
        resourceSlots.values().forEach(slot->actions.add(()->cancelResource(slot)));
        ownedServices.forEach(owned->actions.add(()->stop(owned)));
        ShutdownAwait.all(actions);
    }
    private void stop(Services resolved){
        if(!stoppedServices.add(resolved))return;
        try {
            ShutdownAwait.all(List.of(
                    ()->{if(resolved.planner()!=null)resolved.planner().close();},
                    ()->{if(resolved.delivery()!=null)resolved.delivery().close();},
                    ()->{if(resolved.maintenance()!=null)resolved.maintenance().close();}));
        }catch(RuntimeException|Error failure){stoppedServices.remove(resolved);throw failure;}
    }
    void awaitStopped(long deadline){
        var stopped=requestStop();
        shutdown.complete(deadline,()->{
            ShutdownAwait.future(stopped,deadline);
            inputAdmission.await(deadline);
            ShutdownAwait.executor(freshness,deadline);ShutdownAwait.executor(workers,deadline);
            // A creating evaluation may register services after stop's first snapshot.
            for(var owned:ownedServices){
                stop(owned);
                if(owned.planner()!=null)owned.planner().awaitStopped(deadline);
                if(owned.delivery()!=null)owned.delivery().awaitStopped(deadline);
                if(owned.maintenance()!=null)owned.maintenance().awaitStopped(deadline);
            }
            settlements.shutdown();ShutdownAwait.executor(settlements,deadline);
            var failure=settlementFailures.poll();
            if(failure!=null){
                var failures=new ArrayList<Runnable>();
                do{
                    var captured=failure;
                    failures.add(()->{if(captured instanceof Error error)throw error;
                        if(captured instanceof RuntimeException runtime)throw runtime;});
                }while((failure=settlementFailures.poll())!=null);
                ShutdownAwait.all(failures);
            }
            slots.clear();resourceSlots.clear();retainedResourceBindings.set(0);rules.set(new RuleSnapshot(List.of(), false));
        });
    }
}
