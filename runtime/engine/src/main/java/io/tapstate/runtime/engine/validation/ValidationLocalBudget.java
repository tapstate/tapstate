package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * One process-local validation allocation for a fixed cluster/node/boot. The controller supplies this
 * member's already admitted cluster allocation and shares this instance across all validation runs.
 * This is not a distributed quota, workload claim, driver memory estimator or execution scheduler.
 * Each run reserves its entire envelope before job submission; retained resources keep that envelope
 * charged after retirement. The envelope includes bounded allocation metadata as well as payloads. No waiting queue, expiry, cancellation-based release or disk spill exists.
 */
public final class ValidationLocalBudget implements AutoCloseable {
    public record Amount(long bytes, int workers, int connections, int reads) {
        public Amount {
            if (bytes < 0 || workers < 0 || connections < 0 || reads < 0) throw invalid("budget.amount");
        }
        public boolean isEmpty() { return bytes == 0 && workers == 0 && connections == 0 && reads == 0; }
        private Amount plus(Amount other) {
            return new Amount(Math.addExact(bytes, other.bytes), Math.addExact(workers, other.workers),
                    Math.addExact(connections, other.connections), Math.addExact(reads, other.reads));
        }
        private Amount minus(Amount other) {
            return new Amount(bytes - other.bytes, workers - other.workers, connections - other.connections, reads - other.reads);
        }
    }
    public record Snapshot(Amount capacity, Amount reserved, int runs, boolean closed) {}
    private static final Amount ZERO = new Amount(0, 0, 0, 0);
    private final Object lock = new Object();
    private final String cluster;
    private final String node;
    private final String boot;
    private final Amount capacity;
    private final int maxRuns;
    private final Runnable requireSynchronizationHeadroom;
    private final Map<String, Run> runs = new HashMap<>();
    private Amount reserved = ZERO;
    private boolean closed;

    /** Headroom is a bounded, nonblocking local gate; cleanup never depends on that gate. */
    public ValidationLocalBudget(String cluster, String node, String boot, Amount capacity, int maxRuns,
            Runnable requireSynchronizationHeadroom) {
        this.cluster = identifier(cluster); this.node = identifier(node); this.boot = identifier(boot);
        this.capacity = Objects.requireNonNull(capacity, "capacity");
        if (capacity.isEmpty() || maxRuns < 1 || maxRuns > 1024) throw invalid("budget.capacity");
        this.maxRuns = maxRuns;
        this.requireSynchronizationHeadroom = Objects.requireNonNull(requireSynchronizationHeadroom, "headroom");
    }

    /** The caller must reconcile unknown job submission before retiring or reusing its run slot. */
    public Run admit(ValidationRunIdentity identity, Amount envelope, Runnable requireAuthorized) {
        Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        if (envelope.isEmpty()) throw invalid("budget.envelope");
        if (!cluster.equals(identity.owner().clusterId()) || !node.equals(identity.owner().nodeId())
                || !boot.equals(identity.owner().bootId())) throw invalid("budget.owner");
        requireAuthorized.run(); requireSynchronizationHeadroom.run();
        synchronized (lock) {
            if (closed) throw cancelled();
            if (runs.containsKey(identity.runId())) throw invalid("budget.duplicateRun");
            if (runs.size() == maxRuns) throw limit("validation.nodeRuns");
            fits(capacity, reserved, envelope, "validation.node");
            var run = new Run(identity, envelope, requireAuthorized);
            reserved = reserved.plus(envelope); runs.put(identity.runId(), run);
            return run;
        }
    }

    public Snapshot snapshot() { synchronized (lock) { return new Snapshot(capacity, reserved, runs.size(), closed); } }
    /** Only the admission controller holding this budget may enlarge one of its original runs. */
    public Run.Growth prepareGrowth(Run run,Amount target){
        Objects.requireNonNull(run);if(!run.belongsTo(this))throw invalid("budget.growthOwner");return run.prepareGrowth(target);
    }

    /** Prevents new allocations without reclaiming another owner's still-live resources. */
    @Override public void close() {
        Run[] active;
        synchronized (lock) { closed = true; active = runs.values().toArray(Run[]::new); }
        for (var run : active) run.close();
    }

    public final class Run implements AutoCloseable {
        private final ValidationRunIdentity identity;
        private Amount envelope;
        private final Runnable requireAuthorized;
        private final CompletableFuture<Void> released = new CompletableFuture<>();
        private Amount used = ZERO;
        private int allocations;
        private boolean retiring;
        private boolean returned;
        private Run(ValidationRunIdentity identity, Amount envelope, Runnable requireAuthorized) {
            this.identity = identity; this.envelope = envelope; this.requireAuthorized = requireAuthorized;
        }
        public ValidationRunIdentity identity() { return identity; }
        public Amount envelope() { synchronized(lock){return envelope;} }
        /** Invokes guards without resource monitors. This is memory accounting, never execution authority. */
        private Growth prepareGrowth(Amount target){
            Objects.requireNonNull(target);requireAuthorized.run();requireSynchronizationHeadroom.run();
            synchronized(lock){
                if(closed||retiring||returned)throw cancelled();
                if(target.bytes()<envelope.bytes()||target.workers()<envelope.workers()||target.connections()<envelope.connections()||target.reads()<envelope.reads())throw invalid("budget.growth");
                return new Growth(envelope,target);
            }
        }
        private boolean belongsTo(ValidationLocalBudget original){return ValidationLocalBudget.this==original;}
        /** Original native accounting attempt; reserve() still checks live authority for every resource. */
        public final class Growth {
            private final Amount original,target;private boolean applied;
            private Growth(Amount original,Amount target){this.original=original;this.target=target;}
            public Amount originalEnvelope(){return original;}
            /** No injected callback or future completion runs during this atomic counter update. */
            public void commit(){synchronized(lock){
                if(closed||retiring||returned)throw cancelled();if(applied)return;
                if(!envelope.equals(original))throw invalid("budget.growthChanged");
                var extra=target.minus(original);fits(capacity,reserved,extra,"validation.node");reserved=reserved.plus(extra);envelope=target;applied=true;
            }}
        }
        public Amount used() { synchronized (lock) { return used; } }
        public CompletionStage<Void> released() { return released.minimalCompletionStage(); }

        /** Reserve worst-case live ownership before opening, dispatching or allocating payloads. */
        public Reservation reserve(Amount amount) {
            Objects.requireNonNull(amount, "amount");
            if (amount.isEmpty()) throw invalid("budget.reservation");
            requireAuthorized.run(); requireSynchronizationHeadroom.run();
            synchronized (lock) {
                if (closed || retiring) throw cancelled();
                fits(envelope, used, amount, "validation.run");
                if (allocations == 4096) throw limit("validation.runAllocations");
                used = used.plus(amount); allocations++;
                return new Reservation(this, amount);
            }
        }
        @Override public void close() {
            boolean complete;
            synchronized (lock) { retiring = true; complete = returnEnvelope(); }
            if (complete) released.complete(null);
        }
        private boolean returnEnvelope() {
            if (!retiring || allocations != 0 || returned) return false;
            if (!used.isEmpty()) throw new IllegalStateException("validation allocations disagree with retained usage");
            returned = true;
            if (!runs.remove(identity.runId(), this)) throw new IllegalStateException("validation run lost its allocation");
            reserved = reserved.minus(envelope);
            return true;
        }
    }

    /**
     * An exact owner's reservation. Close only after the represented resources really relinquish
     * ownership, including exceptional exit; a Future cancellation or run stop is not such proof.
     * Working copies, pending results and edge queues require their own retained reservations.
     */
    public final class Reservation implements AutoCloseable {
        private final Run run;
        private final Amount amount;
        private boolean returned;
        private Reservation(Run run, Amount amount) { this.run = run; this.amount = amount; }
        public Amount amount() { return amount; }
        @Override public void close() {
            boolean complete;
            synchronized (lock) {
                if (returned) return;
                returned = true; run.used = run.used.minus(amount); run.allocations--;
                complete = run.returnEnvelope();
            }
            if (complete) run.released.complete(null);
        }
    }

    private static void fits(Amount capacity, Amount used, Amount requested, String scope) {
        if (requested.bytes > capacity.bytes - used.bytes) throw limit(scope + ".bytes");
        if (requested.workers > capacity.workers - used.workers) throw limit(scope + ".workers");
        if (requested.connections > capacity.connections - used.connections) throw limit(scope + ".connections");
        if (requested.reads > capacity.reads - used.reads) throw limit(scope + ".reads");
    }
    private TapstateException cancelled() {
        return new TapstateException(ValidationError.EXECUTION_CANCELLED, Map.of("node", node), null);
    }
    private static String identifier(String value) {
        ValidationError.fieldName(value, "budget.identity");
        if (value.isBlank()) throw invalid("budget.identity");
        return value;
    }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit(String dimension) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null); }
}
