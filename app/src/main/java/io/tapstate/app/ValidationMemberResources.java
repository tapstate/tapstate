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
    private static final int METADATA_FRAMES = 4;
    private static final ValidationLocalBudget.Amount CAPACITY = new ValidationLocalBudget.Amount(256L * 1024 * 1024, 64, 16, 16);
    private record Key(String cluster, String node, String boot) {}
    record Snapshot(ValidationLocalBudget.Snapshot data, ValidationLocalBudget.Amount processReserved, int processors, int processorCapacity, int metadataFrames) {}
    private final Semaphore metadata = new Semaphore(METADATA_FRAMES);
    private final Map<Key, State> members = new HashMap<>();
    private ValidationLocalBudget.Amount reserved = new ValidationLocalBudget.Amount(0, 0, 0, 0);
    private int processors, processorCapacity = Integer.MAX_VALUE;
    private final ExecutorService queries = pool("validation-query", 16, 64);
    private final ExecutorService decoders = pool("validation-decode", 16, 64);
    private final ExecutorService checks = pool("validation-check", 4, 32);
    private final ExecutorService controls = pool("validation-control-io", 4, 64);
    private final ExecutorService cleanup = pool("validation-cleanup", 4, 32);
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
            require(); if (!metadata.tryAcquire()) throw limit("validation.metadataFrames");
            try { require(); return new Metadata(); } catch (RuntimeException | Error failed) { metadata.release(); throw failed; }
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
        Snapshot snapshot() { synchronized (ValidationMemberResources.this) { return new Snapshot(state.budget.snapshot(), reserved, processors, processorCapacity, METADATA_FRAMES - metadata.availablePermits()); } }
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
        private final AtomicBoolean returned = new AtomicBoolean();
        long reservedBytes() { return ValidationTaskStore.Inputs.WORK_RESERVATION_BYTES; }
        void requireRetained(Member member){if(member.process()!=ValidationMemberResources.this||returned.get())throw invalid("resources.metadataOwner");member.require();}
        /** Return after the actual read/planner and every retained definition have relinquished ownership. */
        @Override public void close() { if (returned.compareAndSet(false, true)) metadata.release(); }
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

    @Override public void close() {
        List<State> retiring;
        synchronized (this) { if (closed) return; closed = true; retiring = List.copyOf(members.values()); }
        retiring.forEach(state -> state.budget.close());
        // Queued wrappers still run/exit their own original bodies. Removing them would lose ownership.
        queries.shutdown(); decoders.shutdown(); checks.shutdown(); controls.shutdown(); cleanup.shutdown();
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
