package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local registrations behind a listener installed once on one physical capture opening. Registering
 * and retiring bindings never opens or closes the original source subscription. The assembly must
 * install this listener at capture creation and close the registry when that incarnation ends.
 *
 * <p>Only the control/background path registers, retires, reaps and checks drain state. Capture only
 * reads a bounded immutable entry snapshot behind one nonwaiting callback permit. It never runs
 * registry locks, transport, control I/O or future callbacks. Concurrent callbacks cannot reorder one
 * sender's keys: overlap fails that binding instead of waiting or silently dropping a batch.
 *
 * <p>The caller authenticates current run, Pipeline execution, routing/key revisions and capture authority before registration,
 * supplies a fresh registration ID per run/opening, and invalidates bindings on their loss. No ID may
 * be reused after reaping. Scope equality is not authorization, source readiness or a coverage barrier.
 * A registration covers callbacks that read the newly published entry snapshot; already-entered
 * callbacks are outside that boundary. Joint coverage needs a separate explicit activation protocol.
 */
public final class ValidationCaptureHintRegistry implements AutoCloseable {
    public record Scope(String clusterId, String captureId, String openingId, String sourceId, String nodeId, String bootId,
                        long epoch, WorkloadClaimFence fence, Set<String> tables) {
        public Scope {
            clusterId = id(clusterId, "clusterId");
            captureId = id(captureId, "captureId"); openingId = id(openingId, "openingId");
            sourceId = id(sourceId, "sourceId"); nodeId = id(nodeId, "nodeId"); bootId = id(bootId, "bootId");
            if (epoch < 0 || tables == null || tables.isEmpty() || tables.size() > 4096) throw invalid("hint.captureScope");
            for (String table : tables) id(table, "table");
            tables = Set.copyOf(tables);
            if (fence != null && (fence.key().type() != WorkloadClaimType.CAPTURE || !fence.key().clusterId().equals(clusterId)
                    || !fence.owner().nodeId().equals(nodeId) || !fence.owner().bootId().equals(bootId))) {
                throw invalid("hint.captureFence");
            }
            if (fence != null) id(fence.key().resourceId(), "captureClaimResource");
        }
        private long bytes() {
            long bytes = 2048L + 2L * (clusterId.length() + captureId.length() + openingId.length() + sourceId.length()
                    + nodeId.length() + bootId.length());
            for (String table : tables) bytes += 64L + 2L * table.length();
            if (fence != null) bytes += 512L + 2L * (fence.key().clusterId().length() + fence.key().resourceId().length());
            return bytes;
        }
    }
    public record Limits(int registrations, long bytes, ValidationCaptureHintTap.Limits perBinding) {
        public Limits {
            Objects.requireNonNull(perBinding, "perBinding");
            if (registrations < 1 || registrations > 16 || bytes < 1 || bytes > 8L * 1024 * 1024
                    || perBinding.subscriptions() != 1 || registrations * perBinding.keyBytes() > 64L * 1024 * 1024) throw invalid("hint.registryLimits");
        }
    }
    public record Snapshot(int registrations, int retiring, long bytes, boolean closed, Throwable failure) {}

    public final class Registration implements AutoCloseable {
        private final ValidationCaptureHintTap.Binding binding;
        private final CaptureListener tap;
        private final long charge;
        private final AtomicInteger active = new AtomicInteger();
        private final CompletableFuture<Void> packetsDrained;
        private volatile boolean accepting = true;
        private volatile long retiredAfter = Long.MAX_VALUE;
        private Registration(ValidationCaptureHintTap.Binding binding, long charge) {
            this.binding = binding; this.charge = charge;
            tap = ValidationCaptureHintTap.wrap((events, position) -> {}, List.of(binding), limits.perBinding());
            packetsDrained = binding.outbox().drained().toCompletableFuture();
        }
        public Scope scope() { return scope; }
        public boolean accepting() { return accepting && !closed && binding.outbox().isOpen(); }
        /** Passive check only: no callback is completed by the source thread. */
        public boolean drained() {
            return !accepting && callbackCompleted.get() >= retiredAfter && broadcasting.get() == 0
                    && active.get() == 0 && packetsDrained.isDone();
        }
        public int activeCallbacks() { return active.get(); }
        public Optional<Throwable> failure() {
            Throwable source = failure.get();
            Throwable local = binding.outbox().failure().orElse(null);
            return Optional.ofNullable(local instanceof Error || source == null ? local : source);
        }
        @Override public void close() { retire(this); }
        private boolean enter() {
            if (!accepting()) return false;
            if (!active.compareAndSet(0, 1)) {
                binding.outbox().reportFailure(invalid("hint.concurrentCaptureCallback"));
                return false;
            }
            if (!accepting()) { active.set(0); return false; }
            return true;
        }
    }

    private final Scope scope;
    private final Limits limits;
    private final Object control = new Object();
    private final Map<String, Registration> retained = new LinkedHashMap<>();
    private final AtomicBoolean installed = new AtomicBoolean();
    private final AtomicBoolean callbackActive = new AtomicBoolean();
    private final AtomicLong callbackStarted = new AtomicLong();
    private final AtomicLong callbackCompleted = new AtomicLong();
    private final AtomicInteger broadcasting = new AtomicInteger();
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private volatile List<Registration> current = List.of();
    private volatile boolean closed;
    private long bytes;

    /** Scope/binding metadata, decoder scratch and outbox envelopes are admitted before installation. */
    public ValidationCaptureHintRegistry(Scope scope, Limits limits) {
        this.scope = Objects.requireNonNull(scope, "scope"); this.limits = Objects.requireNonNull(limits, "limits");
        bytes = scope.bytes();
        if (bytes > limits.bytes()) throw invalid("hint.registryBytes");
    }
    public Scope scope() { return scope; }

    /** Called exactly once before the physical port receives its listener; this does not open the port. */
    public CaptureListener install(CaptureListener original) {
        Objects.requireNonNull(original, "original");
        if (!installed.compareAndSet(false, true)) throw invalid("hint.registryAlreadyInstalled");
        return original instanceof CaptureStartedListener started ? new Started(started) : new Forward(original);
    }

    /**
     * Control-only. Idempotent for the identical binding object; an ID collision with another binding
     * is refused. This does not accept an old opening merely because its source/epoch numbers match.
     */
    public Registration register(Scope expected, ValidationCaptureHintTap.Binding binding) {
        Objects.requireNonNull(binding, "binding");
        if (!scope.equals(expected)) throw invalid("hint.captureScopeChanged");
        var sender = binding.outbox().sender();
        if (!binding.outbox().run().owner().clusterId().equals(scope.clusterId())
                || !sender.sourceId().equals(scope.sourceId()) || !sender.nodeId().equals(scope.nodeId())
                || !sender.bootId().equals(scope.bootId()) || sender.captureEpoch() != scope.epoch()
                || sender.captureFence() != (scope.fence() == null ? 0 : scope.fence().claimGeneration())
                || !scope.tables().contains(sender.tableId())) {
            throw invalid("hint.senderCaptureScope");
        }
        long charge = 1024L + sender.retainedBytes();
        Registration added;
        synchronized (control) {
            if (!installed.get() || closed) throw invalid("hint.captureNotOpen");
            Registration previous = retained.get(sender.registrationId());
            if (previous != null) {
                if (previous.binding != binding) throw invalid("hint.registrationCollision");
                return previous;
            }
            reapLocked();
            if (retained.size() == limits.registrations() || charge > limits.bytes() - bytes) throw invalid("hint.registrationCapacity");
            added = new Registration(binding, charge);
            retained.put(sender.registrationId(), added); bytes += charge;
            publish();
        }
        if (closed) {
            retire(added);
            throw invalid("hint.captureNotOpen");
        }
        return added;
    }

    /**
     * Source-side retirement signal: closes admission and records the original reason without registry
     * or outbox locks, transport, or completing drain callbacks. It never stops the original source.
     * The control path must still call close/reap and await actual callbacks/packet requests before
     * releasing reservations. This is safe to invoke from a physical capture's locked lifecycle path.
     */
    public void invalidate(Throwable reason) {
        failure.record(Objects.requireNonNull(reason, "reason"));
        closed = true;
        broadcast(reason);
    }

    /** Reclaims metadata only after both actual key callbacks and physical packet requests have exited. */
    public int reap() { synchronized (control) { return reapLocked(); } }
    public Snapshot snapshot() {
        synchronized (control) {
            return new Snapshot(retained.size(), (int) retained.values().stream().filter(entry -> !entry.accepting).count(), bytes, closed, failure.get());
        }
    }
    private void retire(Registration registration) {
        synchronized (control) {
            if (registration.accepting) {
                registration.accepting = false;
                publish();
                // A callback takes its generation before reading the entry snapshot. Later snapshots
                // cannot retain this entry, so future unrelated callbacks do not extend this cutoff.
                registration.retiredAfter = callbackStarted.get();
            }
        }
        // Outbox drain callbacks run only on this control/background path, outside the registry lock.
        registration.binding.outbox().close();
    }
    @Override public void close() {
        List<Registration> ending;
        synchronized (control) {
            closed = true;
            ending = new ArrayList<>(retained.values());
            var newlyRetired = new ArrayList<Registration>();
            for (Registration entry : ending) {
                if (entry.accepting) { entry.accepting = false; newlyRetired.add(entry); }
            }
            current = List.of();
            long cutoff = callbackStarted.get();
            for (Registration entry : newlyRetired) entry.retiredAfter = cutoff;
        }
        for (Registration entry : ending) entry.binding.outbox().close();
    }
    private int reapLocked() {
        int removed = 0;
        var iterator = retained.values().iterator();
        while (iterator.hasNext()) {
            Registration entry = iterator.next();
            if (entry.drained()) { bytes -= entry.charge; iterator.remove(); removed++; }
        }
        return removed;
    }
    private void publish() { current = retained.values().stream().filter(entry -> entry.accepting).toList(); }

    private class Forward implements CaptureListener {
        private final CaptureListener original;
        private Forward(CaptureListener original) { this.original = original; }
        @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
            deliverHints(events, position);
            original.onBatch(events, position);
        }
        private void deliverHints(List<Envelope> events, Optional<SourcePosition> position) {
            if (closed || current.stream().noneMatch(Registration::accepting)) return;
            if (!callbackActive.compareAndSet(false, true)) {
                broadcast(invalid("hint.concurrentCaptureCallback"));
                return;
            }
            long generation = 0;
            try {
                if (closed) return;
                long previous = callbackStarted.get();
                if (previous == Long.MAX_VALUE) {
                    Throwable exhausted = invalid("hint.callbackGeneration");
                    failure.record(exhausted); closed = true; broadcast(exhausted);
                    return;
                }
                generation = previous + 1;
                callbackStarted.set(generation);
                for (Registration entry : current) {
                    if (!entry.enter()) continue;
                    try { entry.tap.onBatch(events, position); }
                    finally { entry.active.set(0); }
                }
            } finally {
                if (generation != 0) callbackCompleted.set(generation);
                callbackActive.set(false);
            }
        }
        @Override public void onError(Throwable problem) {
            invalidate(problem);
            original.onError(problem);
        }
        @Override public void onAcknowledged(SourcePosition position) { original.onAcknowledged(position); }
        @Override public void onAcknowledgeFailed(Throwable problem) { original.onAcknowledgeFailed(problem); }
    }
    private void broadcast(Throwable problem) {
        broadcasting.incrementAndGet();
        try {
            for (Registration entry : current) entry.binding.outbox().reportFailure(problem);
        } finally { broadcasting.decrementAndGet(); }
    }

    private final class Started extends Forward implements CaptureStartedListener {
        private final CaptureStartedListener original;
        private Started(CaptureStartedListener original) { super(original); this.original = original; }
        @Override public void onStart(SourcePosition position) { original.onStart(position); }
    }
    private static String id(String value, String field) {
        if (value == null || value.isBlank()) throw invalid("hint." + field);
        return ValidationError.fieldName(value, "hint." + field);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
