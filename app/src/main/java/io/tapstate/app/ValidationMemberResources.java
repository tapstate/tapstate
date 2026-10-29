package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
import io.tapstate.spi.store.ValidationTaskStore;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-owned validation allocations survive replacement of a CACHE/business context. A closed
 * context loses admission but does not return another scope's queued/running/native allocations.
 * The configured validation slice leaves half of the cooperative processor budget for normal work;
 * this is bounded admission, not physical CPU isolation or a performance acceptance claim.
 */
final class ValidationMemberResources implements AutoCloseable {

    private static final ValidationLocalBudget.Amount CAPACITY = new ValidationLocalBudget.Amount(256L * 1024 * 1024, 64, 16, 16);
    private record Key(String cluster, String node, String boot) {}
    record Snapshot(ValidationLocalBudget.Snapshot data, ValidationLocalBudget.Amount processReserved, int processors, int processorCapacity, int metadataFrames) {}
    private final io.tapstate.core.validation.ValidationMetadataBudget metadata;
    ValidationMemberResources(){this(new io.tapstate.core.validation.ValidationMetadataBudget());}
    ValidationMemberResources(io.tapstate.core.validation.ValidationMetadataBudget metadata){this.metadata=Objects.requireNonNull(metadata); }
    private final Map<Key, State> members = new HashMap<>();
    private final Set<Probe> probes=new HashSet<>();
    private ValidationLocalBudget.Amount reserved = new ValidationLocalBudget.Amount(0, 0, 0, 0);
    private int processors, processorCapacity = Integer.MAX_VALUE;
    private final ExecutorService queries = pool("validation-query", 16, 64);
    private final ExecutorService decoders = pool("validation-decode", 16, 64);
    private final ExecutorService checks = pool("validation-check", 4, 32);
    private final ExecutorService controls = pool("validation-control-io", 4, 64);
    private final ExecutorService cleanup = pool("validation-cleanup", 4, 32);
    private final ScheduledThreadPoolExecutor controlTicks = ticks();
    /** Dispatch only; actual control/cleanup I/O stays on admitted executors and may outlive a business context. */
    ScheduledFuture<?> controlTicks(Runnable dispatch){
        return controlTicks.scheduleWithFixedDelay(dispatch,10,100,TimeUnit.MILLISECONDS);
    }
    private static ScheduledThreadPoolExecutor ticks(){
        var executor=new ScheduledThreadPoolExecutor(1,task->{var thread=new Thread(task,"validation-control-tick");thread.setDaemon(true);return thread;});
        executor.setRemoveOnCancelPolicy(true);executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);return executor;
    }
    private volatile boolean closed;

    synchronized Member bind(String cluster, String node, String boot, int cooperativeThreads, Runnable active) {
        Objects.requireNonNull(active); identifier(cluster); identifier(node); identifier(boot);
        if (closed || cooperativeThreads < 1) throw invalid("resources.member");
        var key = new Key(cluster, node, boot); var state = members.get(key);
        if (state == null) {
            if (members.size() == 4) throw limit("validation.processMembers");
            state = new State(key, cooperativeThreads); members.put(key, state);
        }
        processorCapacity = Math.min(processorCapacity, cooperativeThreads / 2);
        return new Member(state, active, state.cooperativeThreads == cooperativeThreads);
    }

    final class Member implements AutoCloseable {
        private final State state; private final Runnable active; private final boolean compatible;
        private volatile boolean retired;
        private Member(State state, Runnable active, boolean compatible) { this.state = state; this.active = active; this.compatible = compatible; }
        Metadata admitMetadata() {
            require();var lease=metadata.acquire(this);
            try { require(); return new Metadata(lease); } catch (RuntimeException | Error failed) { lease.close(); throw failed; }
        }
        /** Independent metadata discovery has no fictitious validation owner, run or cooperative processor. */
        Probe admitProbe(){return admitProbe(false);}
        /** A blocking metadata consumer is a third paid worker; native query and decode retain their own slots. */
        Probe admitProbeWithConsumer(){return admitProbe(true);}
        private Probe admitProbe(boolean consumer){
            var frame=admitMetadata();Unbound allocation=null;
            try{
                int workers=consumer?3:2;
                allocation=reserve(new ValidationLocalBudget.Amount(16L*1024*1024,workers,1,1),0);var probe=new Probe(this,frame,allocation,workers);
                synchronized(ValidationMemberResources.this){if(closed||retired)throw invalid("resources.contextClosed");probes.add(probe);}
                return probe;
            }
            catch(RuntimeException|Error refused){if(allocation!=null)allocation.close();frame.close();throw refused;}
        }
        /** One pre-owner control body, with no fictitious run or cooperative processor. */
        Control admitControl(){
            var frame=admitMetadata();try{return control(frame,true);}catch(RuntimeException|Error failed){frame.close();throw failed;}
        }
        /** The request scope, not this worker, retains and eventually returns this exact member's frame. */
        Control admitControl(Metadata frame){frame.requireRetained(this);return control(frame,false);}
        private Control control(Metadata frame,boolean ownsFrame){
            Unbound allocation=null;
            try{allocation=reserve(new ValidationLocalBudget.Amount(4L*1024*1024,1,1,1),0);return new Control(this,frame,allocation,ownsFrame);}
            catch(RuntimeException|Error failed){if(allocation!=null)allocation.close();throw failed;}
        }
        Run admit(ValidationRunIdentity identity, ValidationLocalBudget.Amount envelope, int processors, Runnable owner) {
            try (var reserved = reserve(envelope, processors)) { return reserved.bind(identity, owner); }
        }
        Unbound reserve(ValidationLocalBudget.Amount envelope, int processors) {
            Objects.requireNonNull(envelope); require();
            if (envelope.isEmpty()) throw invalid("resources.envelope");
            if (processors < 0 || processors > 1024) throw invalid("resources.processors");
            var unbound = new Unbound(this, envelope, processors);
            synchronized (ValidationMemberResources.this) {
                if (closed) throw invalid("resources.processClosed");
                if (!compatible || processors > processorCapacity - ValidationMemberResources.this.processors) throw limit("validation.cooperativeHeadroom");
                requireSpace(envelope); reserved = add(reserved, envelope);
                ValidationMemberResources.this.processors += processors; state.processors += processors;
            }
            return unbound;
        }
        Snapshot snapshot() { synchronized (ValidationMemberResources.this) { return new Snapshot(state.budget.snapshot(), reserved, processors, processorCapacity, metadata.inUse()); } }
        private void require() { if (closed || retired) throw invalid("resources.contextClosed"); active.run(); }
        private ValidationMemberResources process(){return ValidationMemberResources.this;}
        @Override public void close() { retired = true; }
    }

    /** Local envelope comes before durable owner acquisition; no invented owner identity is needed. */
    final class Unbound implements AutoCloseable {
        private final Member member; private ValidationLocalBudget.Amount envelope; private int processors;
        private final AtomicBoolean dataReturned = new AtomicBoolean(), processorsReturned = new AtomicBoolean();
        private int phase; private boolean closeRequested;
        private Unbound(Member member, ValidationLocalBudget.Amount envelope, int processors) { this.member = member; this.envelope = envelope; this.processors = processors; }
        synchronized Run bind(ValidationRunIdentity identity, Runnable owner) {
            Objects.requireNonNull(identity); Objects.requireNonNull(owner);
            if (phase != 0) throw invalid("resources.reservationConsumed"); phase = 1;
            ValidationLocalBudget.Run allocated = null;
            try {
                member.require(); allocated = member.state.budget.admit(identity, envelope, () -> { member.require(); owner.run(); });
                var run = new Run(allocated, this); phase = 2;
                if (closeRequested) { run.close(); run.processorsExited(); }
                return run;
            } catch (RuntimeException | Error failed) {
                if (allocated != null) allocated.close(); phase = 3; returnData(); returnProcessors(); throw failed;
            }
        }
        private synchronized void returnData() {
            if (dataReturned.compareAndSet(false, true)) synchronized (ValidationMemberResources.this) { reserved = subtract(reserved, envelope); }
        }
        private synchronized void returnProcessors() {
            synchronized (ValidationMemberResources.this) {
                if (processorsReturned.get()) return;
                member.state.processors -= processors; ValidationMemberResources.this.processors -= processors; processorsReturned.set(true);
            }
        }
        @Override public synchronized void close() {
            closeRequested = true;
            if (phase == 0) { phase = 3; returnData(); returnProcessors(); }
        }
    }

    final class Metadata implements AutoCloseable {
        private final io.tapstate.core.validation.ValidationMetadataBudget.Lease lease;
        private Metadata(io.tapstate.core.validation.ValidationMetadataBudget.Lease lease){this.lease=lease;}
        long reservedBytes() { return ValidationTaskStore.Inputs.WORK_RESERVATION_BYTES; }
        void requireRetained(Member member){
            if(member.process()!=ValidationMemberResources.this)throw invalid("resources.metadataOwner");lease.requireRetained(member);member.require();
        }
        /** Return after the actual read/planner and every retained definition have relinquished ownership. */
        @Override public void close(){lease.close();}
    }

    /** A queued or running original control body retains its allocation until actual exit. */
    final class Control implements AutoCloseable {
        private final Member member;private final Metadata frame;private final Unbound allocation;private final boolean ownsFrame;
        private final CompletableFuture<Void> released=new CompletableFuture<>();
        private boolean submitted,retiring,exited,returned;private Throwable bodyFailure;
        private Control(Member member,Metadata frame,Unbound allocation,boolean ownsFrame){this.member=member;this.frame=frame;this.allocation=allocation;this.ownsFrame=ownsFrame;}
        synchronized void require(){frame.requireRetained(member);if(retiring)throw invalid("resources.controlClosed");}
        CompletionStage<Void> released(){return released.minimalCompletionStage();}
        void execute(Runnable command){
            Objects.requireNonNull(command);
            synchronized(this){require();if(submitted)throw invalid("resources.controlSubmitted");submitted=true;}
            Runnable original=()->{
                Throwable failure=null;
                try{require();command.run();}catch(RuntimeException|Error failed){failure=failed;throw failed;}
                finally{finish(failure);}
            };
            try{controls.execute(original);}
            catch(RejectedExecutionException refused){finish(refused);throw refused;}
            catch(RuntimeException|Error unknown){if(((ThreadPoolExecutor)controls).remove(original))finish(unknown);throw unknown;}
        }
        private void finish(Throwable failure){
            synchronized(this){bodyFailure=failure;exited=true;retiring=true;}
            release();
        }
        private void release(){
            final Throwable failure;
            synchronized(this){if(returned||!retiring||submitted&&!exited)return;returned=true;failure=bodyFailure;}
            allocation.close();if(ownsFrame)frame.close();if(failure==null)released.complete(null);else released.completeExceptionally(failure);
        }
        @Override public void close(){synchronized(this){retiring=true;}release();}
    }

    /** One metadata-query lifetime plus its complete retained request/response frame, until explicitly relinquished. */
    final class Probe implements AutoCloseable {
        private final Member member;private final Metadata frame;private final Unbound allocation;
        private final Set<Handle> handles=new HashSet<>();
        private final CompletableFuture<Void> released=new CompletableFuture<>();
        private final io.tapstate.core.validation.ValidationFailureLatch failure=new io.tapstate.core.validation.ValidationFailureLatch();
        private final ExecutorService query=new ProbeExecutor(queries),decode=new ProbeExecutor(decoders),consumer=new ProbeExecutor(checks);
        private final int workerLimit;
        private int workers;private boolean retiring,returned;private ScheduledFuture<?> retry;
        Probe(Member member,Metadata frame,Unbound allocation,int workerLimit){this.member=member;this.frame=frame;this.allocation=allocation;this.workerLimit=workerLimit;}
        CompletionStage<Void> released(){return released.minimalCompletionStage();}
        ExecutorService queryExecutor(){require();return query;}
        ExecutorService decodeExecutor(){require();return decode;}
        ExecutorService consumerExecutor(){require();if(workerLimit!=3)throw invalid("resources.probeConsumer");return consumer;}
        void require(){member.require();synchronized(this){if(retiring)throw invalid("resources.probeClosed");}}
        /** Construction must remain deferred until this handle is remembered; one independent connection at a time. */
        void retain(AutoCloseable original,CompletionStage<Void> workersExited,CompletionStage<Void> rowsReleased){
            Objects.requireNonNull(original);Objects.requireNonNull(workersExited);Objects.requireNonNull(rowsReleased);require();
            var handle=new Handle(original);
            synchronized(this){if(retiring)throw invalid("resources.probeClosed");if(!handles.isEmpty())throw limit("validation.metadataReaders");handles.add(handle);}
            // Exceptional completion still proves physical exit; the reader retains its diagnosis.
            workersExited.whenComplete((ignored,failed)->{if(failed!=null)recordFailure(failed);receipt(handle);});
            rowsReleased.whenComplete((ignored,failed)->{if(failed==null)receipt(handle);});
        }
        private void recordFailure(Throwable reported){
            while(reported instanceof CompletionException&&reported.getCause()!=null)reported=reported.getCause();
            failure.record(reported);
        }
        private void receipt(Handle handle){synchronized(this){handle.receipts++;settle(handle);}releaseIfDone();}
        private void settle(Handle handle){if(handle.receipts==2&&(!handle.closing||handle.closeExited))handles.remove(handle);}
        private void releaseIfDone(){
            boolean release;ScheduledFuture<?> cancel=null;
            synchronized(this){
                release=retiring&&!returned&&workers==0&&handles.isEmpty();if(release)returned=true;
                if(retry!=null&&handles.stream().noneMatch(handle->handle.closing&&!handle.closeSucceeded)){
                    cancel=retry;retry=null;
                }
            }
            if(cancel!=null)cancel.cancel(false);
            if(release){
                allocation.close();probeReleased(this);frame.close();var diagnosis=failure.get();
                if(diagnosis==null)released.complete(null);else released.completeExceptionally(diagnosis);
            }
        }
        @Override public void close(){
            synchronized(this){
                if(!retiring){retiring=true;for(var handle:handles)handle.closing=true;}
                // Dispatch only on the process timer; rejected or failed original closes stay retryable.
                if(retry==null&&handles.stream().anyMatch(handle->!handle.closeSucceeded))retry=controlTicks(this::retryCleanup);
            }
            query.shutdown();decode.shutdown();consumer.shutdown();retryCleanup();releaseIfDone();
        }
        private void retryCleanup(){
            List<Handle> originals;
            synchronized(this){
                originals=handles.stream().filter(handle->handle.closing&&!handle.submitted&&!handle.closeSucceeded).toList();
                for(var handle:originals){handle.submitted=true;handle.closeExited=false;}
            }
            for(var handle:originals)cleanup(()->{
                try{handle.original.close();synchronized(Probe.this){handle.closeSucceeded=true;}}
                catch(RuntimeException|Error failed){failure.record(failed);throw failed;}
                catch(Exception failed){var coded=new TapstateException(ValidationError.READ_FAILED,Map.of("table","metadata-cleanup"),failed);failure.record(coded);throw coded;}
                finally{synchronized(Probe.this){handle.closeExited=true;settle(handle);}releaseIfDone();}
            }).whenComplete((ignored,failed)->{
                if(failed!=null)recordFailure(failed);
                synchronized(Probe.this){handle.submitted=false;}
                releaseIfDone();
            });
        }
        private final class Handle {
            final AutoCloseable original;int receipts;boolean closing,submitted,closeExited,closeSucceeded;
            Handle(AutoCloseable original){this.original=original;}
        }
        private final class ProbeExecutor extends AbstractExecutorService {
            private final ExecutorService delegate;private boolean closed;private int outstanding;
            ProbeExecutor(ExecutorService delegate){this.delegate=delegate;}
            @Override public void execute(Runnable command){
                Objects.requireNonNull(command);require();
                synchronized(Probe.this){if(retiring||closed)throw new RejectedExecutionException("metadata executor is closed");
                    if(workers==workerLimit)throw limit("validation.metadataWorkers");workers++;outstanding++;}
                Runnable retained=()->{try{command.run();}finally{exited();}};
                try{delegate.execute(retained);}
                catch(RejectedExecutionException refused){exited();throw refused;}
                catch(RuntimeException|Error unknown){if(((ThreadPoolExecutor)delegate).remove(retained))exited();throw unknown;}
            }
            private void exited(){synchronized(Probe.this){workers--;outstanding--;Probe.this.notifyAll();}releaseIfDone();}
            @Override public void shutdown(){synchronized(Probe.this){closed=true;Probe.this.notifyAll();}}
            @Override public List<Runnable> shutdownNow(){shutdown();return List.of();}
            @Override public boolean isShutdown(){synchronized(Probe.this){return closed;}}
            @Override public boolean isTerminated(){synchronized(Probe.this){return closed&&outstanding==0;}}
            @Override public boolean awaitTermination(long timeout,TimeUnit unit)throws InterruptedException{
                long until=System.nanoTime()+unit.toNanos(timeout);synchronized(Probe.this){long left;
                    while(!isTerminated()&&(left=until-System.nanoTime())>0)TimeUnit.NANOSECONDS.timedWait(Probe.this,left);return isTerminated();}
            }
        }
    }

    final class Run implements AutoCloseable {
        private final ValidationLocalBudget.Run budget; private final Unbound reservation;
        private final CompletableFuture<Void> released = new CompletableFuture<>();
        private final CompletableFuture<Void> fullyReleased = new CompletableFuture<>();
        private final List<View> views = new ArrayList<>();
        private boolean retired;
        private Run(ValidationLocalBudget.Run budget, Unbound reservation) {
            this.budget = budget; this.reservation = reservation;
            budget.released().whenComplete((ignored, failure) -> {
                reservation.returnData();
                if (failure == null) released.complete(null); else released.completeExceptionally(failure);
                completeRelease();
            });
        }
        ValidationLocalBudget.Run budget() { return budget; }
        /** Same original allocation; process capacity is charged before member growth and any work. */
        void growTo(ValidationLocalBudget.Amount target,int desiredProcessors){
            Objects.requireNonNull(target);
            reservation.member.require();var growth=reservation.member.state.budget.prepareGrowth(budget,target);
            synchronized(reservation){
                if(reservation.phase!=2||reservation.dataReturned.get()||reservation.processorsReturned.get())throw invalid("resources.executionClosed");
                var old=reservation.envelope;int oldProcessors=reservation.processors;
                if(!old.equals(growth.originalEnvelope()))throw invalid("resources.executionGrowthChanged");
                if(target.bytes()<old.bytes()||target.workers()<old.workers()||target.connections()<old.connections()||target.reads()<old.reads()
                        ||desiredProcessors<oldProcessors||desiredProcessors>1024)throw invalid("resources.executionGrowth");
                var extra=subtract(target,old);int extraProcessors=desiredProcessors-oldProcessors;
                synchronized(ValidationMemberResources.this){
                    if(closed||reservation.member.retired)throw invalid("resources.executionClosed");
                    if(!reservation.member.compatible||extraProcessors>processorCapacity-processors)throw limit("validation.cooperativeHeadroom");
                    requireSpace(extra);reserved=add(reserved,extra);processors+=extraProcessors;reservation.member.state.processors+=extraProcessors;
                }
                try{growth.commit();reservation.envelope=target;reservation.processors=desiredProcessors;}
                catch(RuntimeException|Error failed){synchronized(ValidationMemberResources.this){reserved=subtract(reserved,extra);processors-=extraProcessors;reservation.member.state.processors-=extraProcessors;}throw failed;}
            }
        }
        synchronized ExecutorService queryExecutor() { return view(queries); }
        synchronized ExecutorService decodeExecutor() { return view(decoders); }
        synchronized ExecutorService checkExecutor() { return view(checks); }
        /** Reports a proven unsubmitted check outside resource locks; unknown execution stays owned. */
        void executeCheck(Runnable command,Runnable notSubmitted) {
            final View executor;
            try{synchronized(this){executor=view(checks);}}
            catch(RuntimeException|Error refused){notSubmitted.run();throw refused;}
            executor.execute(command,notSubmitted);
        }
        synchronized ExecutorService controlExecutor() { return view(controls); }
        void executeControl(Runnable command, Runnable notSubmitted) {
            final View executor;
            try { synchronized (this) { executor = view(controls); } }
            catch (RuntimeException | Error refused) { notSubmitted.run(); throw refused; }
            executor.execute(command, notSubmitted);
        }
        private View view(ExecutorService executor) {
            if (retired) throw invalid("resources.executorView");
            for (var existing : views) if (existing.executor == executor) return existing;
            if (views.size() == 8) throw invalid("resources.executorView");
            var view = new View(executor, budget); views.add(view); return view;
        }
        /** Actual tasklet exit or a definitively absent submission, never just cancellation/Jet state. */
        void processorsExited() {
            reservation.returnProcessors();
            completeRelease();
        }
        CompletionStage<Void> released() { return released.minimalCompletionStage(); }
        boolean fullyReleased() { return released.isDone() && reservation.dataReturned.get() && reservation.processorsReturned.get(); }
        CompletionStage<Void> allReleased() { return fullyReleased.minimalCompletionStage(); }
        private void completeRelease() { if (fullyReleased()) fullyReleased.complete(null); }
        @Override public void close() {
            List<View> retiring;
            synchronized (this) { if (retired) return; retired = true; retiring = List.copyOf(views); }
            retiring.forEach(View::shutdown); budget.close();
        }
    }

    /** Cleanup is admitted independently and operates only on already-owned original handles. */
    CompletionStage<Void> cleanup(Runnable originalClose) {
        Objects.requireNonNull(originalClose); var done = new CompletableFuture<Void>();
        try { cleanup.execute(() -> { try { originalClose.run(); done.complete(null); } catch (Throwable failed) { done.completeExceptionally(failed); } }); }
        catch (RejectedExecutionException unavailable) { done.completeExceptionally(unavailable); }
        return done.minimalCompletionStage();
    }

    private final class State {
        final int cooperativeThreads; final ValidationLocalBudget budget; int processors;
        State(Key key, int cooperativeThreads) {
            this.cooperativeThreads = cooperativeThreads;
            budget = new ValidationLocalBudget(key.cluster, key.node, key.boot, CAPACITY, 128, () -> {
                if (closed) throw invalid("resources.processClosed");
            });
        }
    }

    /** A Future may cancel before dequeue. The retained wrapper still owns its slot until removal/exit. */
    private static final class View extends AbstractExecutorService {
        private final ExecutorService executor; private final ValidationLocalBudget.Run budget;
        private boolean closed; private int outstanding;
        private View(ExecutorService executor, ValidationLocalBudget.Run budget) { this.executor = executor; this.budget = budget; }
        @Override public void execute(Runnable command) {
            execute(command,()->{});
        }
        private void execute(Runnable command,Runnable notSubmitted) {
            Objects.requireNonNull(command);
            final ValidationLocalBudget.Reservation worker;
            try{worker=budget.reserve(new ValidationLocalBudget.Amount(1024,1,0,0));}
            catch(RuntimeException|Error refused){notSubmitted.run();throw refused;}
            boolean admitted;
            synchronized (this) { admitted = !closed; if (admitted) outstanding++; }
            if (!admitted) { worker.close();notSubmitted.run();throw new RejectedExecutionException("validation executor view is closed"); }
            Runnable retained = () -> { try { command.run(); } finally { exited(worker); } };
            try { executor.execute(retained); }
            catch (RejectedExecutionException refused) { exited(worker);notSubmitted.run();throw refused; }
            catch (RuntimeException | Error unknown) {
                if (((ThreadPoolExecutor) executor).remove(retained)) {exited(worker);notSubmitted.run();}
                throw unknown;
            }
        }
        private void exited(ValidationLocalBudget.Reservation worker) {
            synchronized (this) { outstanding--; notifyAll(); } worker.close();
        }
        @Override public synchronized void shutdown() { closed = true; notifyAll(); }
        @Override public List<Runnable> shutdownNow() { shutdown(); return List.of(); }
        @Override public synchronized boolean isShutdown() { return closed; }
        @Override public synchronized boolean isTerminated() { return closed && outstanding == 0; }
        @Override public synchronized boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            long nanos = unit.toNanos(timeout); long deadline = System.nanoTime() + nanos;
            while (!isTerminated() && nanos > 0) { TimeUnit.NANOSECONDS.timedWait(this, nanos); nanos = deadline - System.nanoTime(); }
            return isTerminated();
        }
    }

    private void probeReleased(Probe original){
        boolean stopCleanup;
        synchronized(this){probes.remove(original);stopCleanup=closed&&probes.isEmpty();}
        if(stopCleanup){controlTicks.shutdown();cleanup.shutdown();}
    }
    @Override public void close() {
        List<State> retiring;List<Probe> originals;
        synchronized (this) { if (closed) return; closed = true; retiring = List.copyOf(members.values());originals=List.copyOf(probes); }
        retiring.forEach(state -> state.budget.close());
        // Original metadata handles still need bounded retry dispatch after native pool shutdown.
        originals.forEach(Probe::close);
        // Queued wrappers still run/exit their own original bodies. Removing them would lose ownership.
        queries.shutdown(); decoders.shutdown(); checks.shutdown(); controls.shutdown();
        boolean stopCleanup;synchronized(this){stopCleanup=probes.isEmpty();}
        if(stopCleanup){controlTicks.shutdown();cleanup.shutdown();}
    }
    private static ExecutorService pool(String name, int workers, int queue) {
        return new ThreadPoolExecutor(workers, workers, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queue), task -> {
            var thread = new Thread(task, name); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }
    private void requireSpace(ValidationLocalBudget.Amount request) {
        if (request.bytes() > CAPACITY.bytes() - reserved.bytes() || request.workers() > CAPACITY.workers() - reserved.workers()
                || request.connections() > CAPACITY.connections() - reserved.connections() || request.reads() > CAPACITY.reads() - reserved.reads())
            throw limit("validation.processEnvelope");
    }
    private static ValidationLocalBudget.Amount add(ValidationLocalBudget.Amount one, ValidationLocalBudget.Amount two) {
        return new ValidationLocalBudget.Amount(Math.addExact(one.bytes(), two.bytes()), Math.addExact(one.workers(), two.workers()),
                Math.addExact(one.connections(), two.connections()), Math.addExact(one.reads(), two.reads()));
    }
    private static ValidationLocalBudget.Amount subtract(ValidationLocalBudget.Amount one, ValidationLocalBudget.Amount two) {
        return new ValidationLocalBudget.Amount(one.bytes() - two.bytes(), one.workers() - two.workers(), one.connections() - two.connections(), one.reads() - two.reads());
    }
    private static void identifier(String value) { if (value == null || value.isBlank()) throw invalid("resources.identity"); ValidationError.fieldName(value, "resources.identity"); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit(String limit) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", limit), null); }
}
