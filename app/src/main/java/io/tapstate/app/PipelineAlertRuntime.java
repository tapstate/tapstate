package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** Bounded production feed. The two evaluation workers own no transport and never run convergence. */
final class PipelineAlertRuntime implements PipelineAlertFeed, AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PipelineAlertRuntime.class);
    static final int MAX_OBJECTS = 256, MAX_MATCHES = 32, MAX_INPUT_BYTES = 128 * 1024;
    static final int QUEUE_SIZE = 64, MAX_QUEUED_BYTES = 8 * 1024 * 1024;
    record Health(boolean closed, boolean degraded, long rejected, String code,Map<String,Object> parameters,int queuedInputs,int queuedBytes) { }
    record Services(AlertStore store, AlertEvaluationHost evaluator, AlarmTypeCatalog catalog, AlertPlanningRuntime planner, AlertDeliveryRuntime delivery, Set<String> providers,io.tapstate.adapters.alerting.AlarmTypeText descriptions) {
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog,AlertPlanningRuntime planner,AlertDeliveryRuntime delivery,Set<String> providers){this(store,evaluator,catalog,planner,delivery,providers,io.tapstate.adapters.alerting.AlarmTypeText.english());}
        Services(AlertStore store,AlertEvaluationHost evaluator,AlarmTypeCatalog catalog,AlertPlanningRuntime planner,AlertDeliveryRuntime delivery){this(store,evaluator,catalog,planner,delivery,Set.of("pipeline-lifecycle","pipeline-observations"));}
        Services { providers=Set.copyOf(providers);Objects.requireNonNull(descriptions); }
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
        Slot(Producer producer, Instant now, Slot previous) {
            this.producer = producer;
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
        var thread=new Thread(task,"alert-freshness");thread.setDaemon(true);return thread;
    });
    private final AtomicBoolean started=new AtomicBoolean();
    private final Object objectAdmission=new Object();
    private final ConcurrentMap<String, Slot> slots = new ConcurrentHashMap<>();
    private final AtomicInteger retainedResourceBindings=new AtomicInteger();
    private final AtomicInteger queuedBytes = new AtomicInteger();
    private final AtomicInteger queuedTasks = new AtomicInteger();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean(), refreshing = new AtomicBoolean();
    private final AtomicLong lastLogged = new AtomicLong(Long.MIN_VALUE);
    private static final class ResourceSlot {
        final ArrayDeque<Work> pending=new ArrayDeque<>();
        final ConcurrentMap<AlarmKey,AlertEvaluationHost.Binding> bindings=new ConcurrentHashMap<>();
        final AtomicBoolean tickQueued=new AtomicBoolean();
        boolean scheduled;
    }
    private final ConcurrentMap<AlarmKey.Resource,ResourceSlot> resourceSlots = new ConcurrentHashMap<>();
    private volatile java.util.function.Consumer<AlarmKey.Resource> resourceRetired=r->{};
    void resourceRetired(java.util.function.Consumer<AlarmKey.Resource> listener){resourceRetired=listener;}
    private volatile Services services;
    private volatile List<AlertConfiguration.Rule> rules = List.of();
    private record Failure(String code,Map<String,Object> parameters){}
    private final AtomicReference<Failure> failure = new AtomicReference<>(new Failure(null,Map.of()));
    private volatile AlarmKey reconciliationCursor;
    private boolean legacyBindingsInScan,legacyBindingsUnavailable,legacyScanComplete;
    private final String runtimeId = UUID.randomUUID().toString();

    PipelineAlertRuntime(Supplier<Services> open, PipelineIntentStore intents,
            BiFunction<PipelineIntentScope, WorkloadClaimFence, Optional<Runnable>> claimProof, Runnable active, Clock clock) {
        this.open = Objects.requireNonNull(open); this.intents = Objects.requireNonNull(intents);
        this.claimProof = Objects.requireNonNull(claimProof); this.active = Objects.requireNonNull(active);
        this.clock = Objects.requireNonNull(clock);
        var serial = new AtomicInteger();
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE), task -> {
            var thread = new Thread(task, "alert-evaluation-" + serial.incrementAndGet()); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }
    @Override public void expecting(PipelineConvergencePass pass) { if (pass != null && !closed.get()) slot(pass); }
    private Slot slot(PipelineConvergencePass pass) {
        var producer = new Producer(pass.scope(), pass.claim(), pass.intent());
        String id = pass.scope().pipelineId();
        synchronized (objectAdmission) {
            var existing = slots.get(id);
            if (existing != null && existing.producer.equals(producer)) return existing;
            if (existing == null && slots.size() + resourceSlots.size() >= MAX_OBJECTS) { reject(null, "object-budget"); return null; }
            var created = new Slot(producer, clock.instant(), existing); slots.put(id, created); return created;
        }
    }
    @Override public void published(PipelineConvergencePass pass, Observation observation, Throwable cause) {
        if (pass == null || closed.get()) return;
        Slot slot = slot(pass); if (slot == null) return;
        long sequence = slot.sequence.getAndIncrement();
        int bytes = inputBytes(observation);
        if (bytes < 0 || observation.observedAt() == null || observation.observedAt().isAfter(clock.instant())) {
            reject(slot, "input-budget-or-clock"); return;
        }
        var original = new Observation(observation.pipelineId(), observation.state(), Map.of(), Map.of(), Map.of(),
                null, observation.observedAt(), observation.facts());
        var trace = AlarmDiagnosticCapture.capture(cause, List.of()).orElse(null);
        var capturedRules = rules;
        submit(slot, bytes + 16 * 1024, () -> {
            requireCurrent(slot);
            long interruption = slot.interruptions.get();
            slot.actual = original.state();
            if (slot.publication == null || original.observedAt().isAfter(slot.publication)) slot.publication = original.observedAt();
            for (var rule : matching(slot, capturedRules)) {
                var binding = binding(slot, rule);
                if (rule.typeId().equals("pipeline.observation-stale")) continue;
                if (slot.discontinuous()) accept(binding, sequence, new AlertInput.Discontinuity(clock.instant()));
                AlertInput input = rule.metric() != null ? new AlertInput.Metric(original)
                        : new AlertInput.Conditions(original.observedAt(), slot.producer.intent().desired().targetState() != PipelineState.RUNNING
                                ? AlertSignal.Condition.UNKNOWN : original.state() == PipelineState.FAILED
                                ? AlertSignal.Condition.BAD : original.state() == PipelineState.RUNNING
                                && slot.producer.intent().desired().targetState() == PipelineState.RUNNING
                                ? AlertSignal.Condition.GOOD : AlertSignal.Condition.UNKNOWN, trace);
                accept(binding, sequence, input);
            }
            slot.recoveredInterruption = interruption;
        });
    }
    @Override public void unavailable(PipelineConvergencePass pass) {
        if (pass != null) reject(slots.get(pass.scope().pipelineId()), "publication-unavailable");
    }
    @Override public void retain(Collection<String> ids) {
        slots.keySet().retainAll(Set.copyOf(ids));
    }
    /** Independent of convergence and of exporter callbacks; ordinary startup never prepares old storage. */
    void start(){
        if(closed.get()||!started.compareAndSet(false,true))throw new IllegalStateException("evaluation host already started or closed");
        freshness.setRemoveOnCancelPolicy(true);freshness.scheduleWithFixedDelay(this::tick,0,1,TimeUnit.SECONDS);
    }
    private volatile Runnable observerTick=()->{};
    void observerTick(Runnable observer){observerTick=Objects.requireNonNull(observer);}
    void tick() {
        if (closed.get()) return;
        if (refreshing.compareAndSet(false, true)) {
            submit(null, 0, () -> {
                try { refresh(); } finally { refreshing.set(false); }
            }, () -> refreshing.set(false));
        }
        for(var entry:resourceSlots.entrySet()){
            var slot=entry.getValue();if(!slot.tickQueued.compareAndSet(false,true))continue;
            submitResource(slot,0,()->{try{
                if(resourceSlots.get(entry.getKey())!=slot)return;
                for(var entryBinding:slot.bindings.entrySet()) {
                    var previous=entryBinding.getValue();
                    var current=rules.stream().filter(rule->rule.header().identity().equals(previous.definition().header().identity())
                            &&rule.matches(entryBinding.getKey())).findFirst();
                    if(current.isEmpty())continue;
                    var binding=new AlertEvaluationHost.Binding(current.orElseThrow(),previous.resource(),previous.producer(),
                            previous.sourceBindingRevision(),active,()->null,previous.domainToken());
                    entryBinding.setValue(binding);
                    AlertEvaluationHost.requireResult(services.evaluator().tick(binding));
                }
            }finally{slot.tickQueued.set(false);}},()->slot.tickQueued.set(false));
        }
        var capturedRules = rules;
        for (Slot slot : slots.values()) {
            if (!slot.tickQueued.compareAndSet(false, true)) continue;
            long sequence = slot.freshnessSequence.getAndIncrement();
            submit(slot, 0, () -> {
                try {
                    requireCurrent(slot);
                    long interruption=slot.interruptions.get();
                    for (var rule : matching(slot, capturedRules)) {
                        var binding = binding(slot, rule);
                        if (!rule.enabled() || !rule.header().active()) {
                            AlertEvaluationHost.requireResult(services.evaluator().end(binding.key(), AlertSnapshot.EndReason.DISABLED, binding.requireCurrent()));
                        } else if (rule.typeId().equals("pipeline.observation-stale")) {
                            Instant now = clock.instant();
                            boolean expected = slot.producer.intent().desired().targetState() == PipelineState.RUNNING
                                    && slot.actual != PipelineState.COMPLETED && slot.actual != PipelineState.STOPPED
                                    && slot.actual != PipelineState.PAUSED && slot.actual != PipelineState.FAILED;
                            accept(binding, sequence, new AlertInput.Freshness(now, expected, slot.expectedSince, slot.publication,
                                    AlertEvaluationHost.seconds(rule, "startup-grace", 120), AlertEvaluationHost.seconds(rule, "max-age", 120)));
                        } else {
                            if (slot.discontinuous()) accept(binding, slot.sequence.get(), new AlertInput.Discontinuity(clock.instant()));
                            AlertEvaluationHost.requireResult(services.evaluator().tick(binding));
                        }
                    }
                    slot.recoveredInterruption=interruption;
                } finally { slot.tickQueued.set(false); }
            }, () -> slot.tickQueued.set(false));
        }
    }
    private void refresh() {
        Failure previousFailure=failure.get();
        active.run();
        Services resolved = services == null ? Objects.requireNonNull(open.get()) : services;
        var all = new ArrayList<AlertConfiguration.Rule>();
        String cursor = null;
        do {
            var page = resolved.store().evaluationRules(cursor, 100);
            for (var definition : page.rows()) {
                if (!(definition instanceof AlertConfiguration.Rule rule)) throw refused("rule-kind");
                all.add(rule.validated(resolved.catalog()));
            }
            cursor = page.nextId();
            if (all.size() > 1000 || all.size() == 1000 && cursor != null) throw refused("rule-snapshot-budget");
        } while (cursor != null);
        active.run(); synchronized(this){
            if(closed.get()){if(resolved.planner()!=null)resolved.planner().close();if(resolved.delivery()!=null)resolved.delivery().close();return;}
            if(services==null&&resolved.planner()!=null)resolved.planner().start(); if(services==null&&resolved.delivery()!=null)resolved.delivery().start(); services = resolved; rules = List.copyOf(all);
        }
        observerTick.run();
        if(reconciliationCursor==null)legacyBindingsInScan=false;
        var retained = resolved.store().evaluationStates(reconciliationCursor, 50);
        for(var row:retained.rows())if(retained.removedResources().contains(row.value().snapshot().rule().key().resource())){
            AlertEvaluationHost.requireResult(resolved.evaluator().end(row.value().snapshot().rule().key(),AlertSnapshot.EndReason.RESOURCE_REMOVED,active));
            var resource=row.value().snapshot().rule().key().resource();ResourceSlot obsolete;
            synchronized(objectAdmission){obsolete=resourceSlots.remove(resource);if(obsolete!=null){retainedResourceBindings.addAndGet(-obsolete.bindings.size());obsolete.bindings.clear();}}
            if(obsolete!=null)cancelResource(obsolete);resourceRetired.accept(resource);
        }
        boolean legacyBindingUnavailable=false;
        for(var key:retained.obsoleteBindings()) {
            var original=retained.rows().stream().filter(row->row.value().snapshot().rule().key().equals(key)).findFirst().orElseThrow().value();
            if(original.retiredAt()==null && original.snapshot().enabled()) {
                if(original.operationalEvidence()!=null && original.operationalEvidence().version()==2 && AlertOperationDomain.supported(key))
                    AlertEvaluationHost.requireResult(resolved.evaluator().supersede(key,active));
                else legacyBindingUnavailable=true;
            }
            synchronized(objectAdmission){var slot=resourceSlots.get(key.resource());
                if(slot!=null && slot.bindings.remove(key)!=null)retainedResourceBindings.decrementAndGet();}
        }
        resolved.evaluator().reconcileRules(all, retained.rows(), active);
        for(var row:retained.rows()) {
            var head=row.value();var key=head.snapshot().rule().key();
            if(key.resource().kind().equals("pipeline") || key.resource().kind().equals("cluster-node") || head.retiredAt()!=null
                    || retained.removedResources().contains(key.resource()) || retained.obsoleteBindings().contains(key) || head.binding()==null)continue;
            var slot=resourceSlots.get(key.resource());if(slot!=null && slot.bindings.containsKey(key))continue;
            var definition=all.stream().filter(rule->rule.header().identity().equals(head.binding().ruleIdentity()) && rule.matches(key)).findFirst();
            if(definition.isEmpty())continue;
            var binding=new AlertEvaluationHost.Binding(definition.orElseThrow(),key.resource(),head.snapshot().rule().producerIncarnation(),
                    head.binding().sourceBindingRevision(),active,()->null,AlertOperationDomain.token(head.binding().ruleIdentity().incarnation(),key));
            var result=resolved.evaluator().tick(binding);
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
        reconciliationCursor = retained.next();
        boolean missing=all.stream().anyMatch(r->r.header().active()&&r.enabled()&&r.metric()==null
                &&!resolved.providers().contains(resolved.catalog().types().stream().filter(t->t.id().equals(r.typeId())).findFirst().orElseThrow().providerId()));
        if(missing || legacyBindingsUnavailable || !legacyScanComplete)failure.compareAndSet(previousFailure,new Failure(AlertError.INVALID_CONFIGURATION.code(),Map.of("field","evaluationHost","reason","input-unavailable")));
        else if (slots.values().stream().noneMatch(Slot::discontinuous)) failure.compareAndSet(previousFailure,null);
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
        if(closed.get()){settled.run();return;}
        ResourceSlot slot;
        synchronized(objectAdmission){
            if(!resourceSlots.containsKey(resource)&&resourceSlots.size()+slots.size()>=MAX_OBJECTS){reject(null,"object-budget");settled.run();return;}
            slot=resourceSlots.computeIfAbsent(resource,ignored->new ResourceSlot());
        }
        var captured=rules;
        submitResource(slot,512,()->{try{
            if(resourceSlots.get(resource)!=slot)throw refused("retired-resource");
            active.run();originalGuard.run();var resolved=services;
            if(resolved==null)throw refused("storage-not-ready");
            var matching=captured.stream().filter(r->r.typeId().equals(typeId)&&r.resources().matches(resource)).toList();
            if(matching.size()>MAX_MATCHES)throw refused("matching-rule-budget");
            for(var rule:matching){
                String domain=input instanceof AlertInput.Operational op?op.domainToken():null;
                var binding=new AlertEvaluationHost.Binding(rule,resource,producer,sourceRevision,()->{active.run();localProof.run();},domain);
                var before=resolved.store().state(binding.key(),new AlertStore.ReadAccess(true,Set.of(),active));
                if(before.isEmpty() && input instanceof AlertInput.Operational op && op.domainToken()!=null && op.succeeded())continue;
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
                    if(head.snapshot().rule().revision()==rule.header().revision()&&head.snapshot().rule().producerIncarnation().equals(producer)
                            &&head.binding().sourceBindingRevision()==sourceRevision&&head.snapshot().enabled()
                            &&evidence!=null&&evidence.bindingDigest().equals(op.bindingDigest())
                            &&evidence.equals(evidence.observe(op.capability(),op.succeeded()))
                            &&head.snapshot().status()!=AlertSnapshot.Status.PENDING
                            &&head.snapshot().recoverySince()==null
                            &&head.snapshot().quality()==AlertSnapshot.Quality.VALID
                            &&head.snapshot().lastObservedAt()!=null
                            &&op.observedAt().isBefore(head.snapshot().lastObservedAt().plusSeconds(5)))continue;
                }
                accept(binding,next,input);
            }
        }finally{settled.run();}},settled);
    }
    private void submitResource(ResourceSlot slot,int bytes,Runnable action){submitResource(slot,bytes,action,()->{});}
    private void submitResource(ResourceSlot slot,int bytes,Runnable action,Runnable refused){
        if(closed.get()){refused.run();return;}
        if(queuedTasks.incrementAndGet()>QUEUE_SIZE){queuedTasks.decrementAndGet();reject(null,"queue-capacity");refused.run();return;}
        if(queuedBytes.addAndGet(bytes)>MAX_QUEUED_BYTES){queuedTasks.decrementAndGet();queuedBytes.addAndGet(-bytes);reject(null,"queue-byte-budget");refused.run();return;}
        var work=new Work(null,bytes,action,refused);boolean start;
        synchronized(slot.pending){slot.pending.addLast(work);start=!slot.scheduled;slot.scheduled=true;}
        if(start)dispatchResource(slot);
    }
    private void dispatchResource(ResourceSlot slot){
        try{workers.execute(new ResourceDispatcher(slot));}catch(RejectedExecutionException unavailable){cancelResource(slot);}
    }
    private final class ResourceDispatcher implements Runnable {
        final ResourceSlot slot;ResourceDispatcher(ResourceSlot slot){this.slot=slot;}
        public void run(){Work work;synchronized(slot.pending){work=slot.pending.pollFirst();}
            try{if(work!=null)work.run();}finally{boolean more;synchronized(slot.pending){more=!slot.pending.isEmpty();if(!more)slot.scheduled=false;}if(more)dispatchResource(slot);}
        }
    }
    private void cancelResource(ResourceSlot slot){List<Work> pending;synchronized(slot.pending){pending=List.copyOf(slot.pending);slot.pending.clear();slot.scheduled=false;}pending.forEach(Work::cancel);}
    void collectionUnavailable(String reason){reject(null,reason);}

    private List<AlertConfiguration.Rule> matching(Slot slot, List<AlertConfiguration.Rule> snapshot) {
        var resource = resource(slot);
        var matches = snapshot.stream().filter(r -> r.resources().matches(resource)
                && (r.typeId().equals("pipeline.failed") || r.typeId().equals("pipeline.observation-stale") || r.metric() != null)).toList();
        if (matches.size() > MAX_MATCHES) throw refused("matching-rule-budget");
        if (services == null) throw refused("storage-not-ready");
        return matches;
    }
    private AlertEvaluationHost.Binding binding(Slot slot, AlertConfiguration.Rule rule) {
        String producer = fingerprint(List.of(slot.producer.incarnation(),
                rule.typeId().equals("pipeline.observation-stale") ? "freshness" : "publication", runtimeId));
        return new AlertEvaluationHost.Binding(rule, resource(slot), producer, 0, () -> requireCurrent(slot), () -> {
            Runnable lease=claimProof.apply(slot.producer.scope(),slot.producer.claim()).orElseThrow(()->refused("original-lease"));
            return new AlertStore.EvaluationProof(slot.producer.intent(),()->{
                active.run();lease.run();if(closed.get()||slots.get(slot.producer.scope().pipelineId())!=slot)throw refused("original-producer");
            });
        });
    }
    private AlarmKey.Resource resource(Slot slot) {
        return new AlarmKey.Resource("pipeline", slot.producer.scope().pipelineId(), Long.toString(slot.producer.scope().stateGeneration()));
    }
    private void requireCurrent(Slot slot) {
        active.run();
        if (closed.get() || slots.get(slot.producer.scope().pipelineId()) != slot
                || claimProof.apply(slot.producer.scope(), slot.producer.claim()).isEmpty()
                || intents.loadForActuation(slot.producer.scope().pipelineId()).filter(slot.producer.intent()::equals).isEmpty()) {
            throw refused("original-producer");
        }
        active.run();
    }
    private void accept(AlertEvaluationHost.Binding binding, long sequence, AlertInput input) {
        var outcome = services.evaluator().accept(binding, sequence, input);
        if (outcome == AlertEvaluationHost.Result.CONFLICT || outcome == AlertEvaluationHost.Result.BUSY) throw refused("state-contention");
    }
    private void submit(Slot slot, int bytes, Runnable action) { submit(slot, bytes, action, () -> { }); }
    private void submit(Slot slot, int bytes, Runnable action, Runnable refused) {
        if (closed.get()) { refused.run(); return; }
        if (queuedTasks.incrementAndGet() > QUEUE_SIZE) { queuedTasks.decrementAndGet(); reject(slot, "queue-capacity"); refused.run(); return; }
        int retained = queuedBytes.addAndGet(bytes);
        if (retained > MAX_QUEUED_BYTES) { queuedTasks.decrementAndGet(); queuedBytes.addAndGet(-bytes); reject(slot, "queue-byte-budget"); refused.run(); return; }
        Work work = new Work(slot, bytes, action, refused);
        if (slot == null) {
            try { workers.execute(work); } catch (RejectedExecutionException full) { work.cancel(); }
        } else {
            boolean start;
            synchronized (slot.pending) {
                slot.pending.addLast(work); start = !slot.scheduled; slot.scheduled = true;
            }
            if (start) dispatch(slot);
        }
    }
    private final class Work implements Runnable {
        final Slot slot; final int bytes; final Runnable action, refused; final AtomicBoolean finished = new AtomicBoolean();
        Work(Slot slot, int bytes, Runnable action, Runnable refused) { this.slot=slot;this.bytes=bytes;this.action=action;this.refused=refused; }
        public void run() {
            if (closed.get()) { cancel(); return; }
            try { action.run(); }
            catch (RuntimeException unavailable) {
                if(unavailable instanceof TapstateException coded)reject(slot,coded);else reject(slot,"evaluation-failed");
                if(!(unavailable instanceof TapstateException))throw unavailable;
            }
            finally { release(); }
        }
        void release() { if(finished.compareAndSet(false,true)){queuedBytes.addAndGet(-bytes);queuedTasks.decrementAndGet();} }
        void cancel() { if(!finished.get()){reject(slot,"closed-or-capacity");refused.run();release();} }
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
        pending.forEach(Work::cancel);
    }
    private void reject(Slot slot, String reason) {
        reject(slot,refused(reason));
    }
    private void reject(Slot slot, TapstateException refusal) {
        if (slot != null) slot.interruptions.incrementAndGet();
        var parameters=new HashMap<String,Object>();
        refusal.args().forEach((key,value)->parameters.put(key,value instanceof String text&&text.matches("[a-zA-Z0-9._:-]{1,128}")?text:"redacted"));
        rejected.incrementAndGet(); failure.set(new Failure(refusal.code().code(),Map.copyOf(parameters)));
        long second = clock.instant().getEpochSecond(), previous = lastLogged.get();
        if ((previous == Long.MIN_VALUE || second - previous >= 60) && lastLogged.compareAndSet(previous, second)) {
            LOG.warn("Alert collection is degraded [{}]", refusal.code().code());
        }
    }
    synchronized Services managementServices() {
        active.run();
        if (closed.get() || services == null) throw new TapstateException(io.tapstate.control.core.ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        return services;
    }
    Health health() {
        Failure current=failure.get();var resolved=services;
        var planning=resolved==null||resolved.planner()==null?null:resolved.planner().failure();
        var delivery=resolved==null||resolved.delivery()==null?null:resolved.delivery().failure();
        if(current==null&&delivery!=null)current=new Failure(delivery.code(),delivery.parameters());
        if(current==null&&planning!=null)current=new Failure(planning.code(),planning.parameters());
        return new Health(closed.get(),current!=null,rejected.get(),current==null?null:current.code(),current==null?Map.of():current.parameters(),queuedTasks.get(),queuedBytes.get());
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
    @Override public void close() {
        if(closed.compareAndSet(false,true)){
            freshness.shutdownNow();
            synchronized(this){if(services!=null&&services.planner()!=null)services.planner().close();if(services!=null&&services.delivery()!=null)services.delivery().close();}
            for(Runnable task:workers.shutdownNow()){
                if(task instanceof Work work)work.cancel();
                else if(task instanceof Dispatcher dispatcher)cancelPending(dispatcher.slot);
                else if(task instanceof ResourceDispatcher dispatcher)cancelResource(dispatcher.slot);
            }
            slots.values().forEach(this::cancelPending);slots.clear();resourceSlots.values().forEach(this::cancelResource);resourceSlots.clear();retainedResourceBindings.set(0);rules=List.of();
        }
    }
}
