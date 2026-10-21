package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRootHintBatch.Change;
import io.tapstate.core.validation.ValidationRootHintBatch.Sender;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Bounded source-local delivery of already owned key facts. The capture path only calls tryOffer;
 * it never waits, invokes a transport or completes a user callback. Failure is latched locally.
 * A separate validation controller must observe this latch and sender liveness even if failure
 * notification is lost. No method changes normal source delivery, offsets or acknowledgements.
 *
 * <p>The background transport polls one physical attempt and settles it only after its request and
 * decoder have actually exited. An unknown result retries the same immutable logical packet. No new
 * attempt can overlap that request, and neither timeout nor close releases its memory early. This is
 * not a transport implementation, authentication, current authority check or source-key codec proof.
 */
public final class ValidationRootHintOutbox implements AutoCloseable {
    public record Limits(int packets, long bytes, int changes, long batchBytes,
                         int attempts, long retryNanos, long windowNanos) {
        public Limits {
            if (packets < 1 || packets > 256 || bytes < 1 || bytes > 64L * 1024 * 1024
                    || changes < 1 || changes > ValidationRootHintBatch.MAX_CHANGES
                    || batchBytes < 1 || batchBytes > ValidationRootHintBatch.MAX_BYTES
                    || attempts < 1 || attempts > 100 || retryNanos < 0 || windowNanos < 1
                    || windowNanos > 86_400_000_000_000L || retryNanos > windowNanos) throw invalid("hint.outboxLimits");
        }
        /** Includes a single constructor scratch envelope before a packet's actual size is known. */
        public long reservationBytes() { return bytes + ValidationRootHintBatch.MAX_BYTES + 64L * 1024; }
    }
    public enum Offer { ACCEPTED, FAILED, CLOSED }
    public enum Settlement { ACCEPTED, RETRY, FAILED, CLOSED, ALREADY_SETTLED }
    public record Snapshot(int packets, long bytes, boolean inFlight, boolean closed, Throwable failure) {}

    public static final class Lease {
        private final ValidationRootHintOutbox owner;
        private final Entry entry;
        private boolean settled;
        private Lease(ValidationRootHintOutbox owner, Entry entry) { this.owner = owner; this.entry = entry; }
        public ValidationRootHintBatch batch() { return entry.batch; }
    }
    private static final class Entry {
        private final ValidationRootHintBatch batch;
        private final long charge;
        private final long createdAt;
        private long dueAt;
        private int attempts;
        private Lease lease;
        private Entry(ValidationRootHintBatch batch, long now) {
            this.batch = batch; this.charge = 512 + batch.retainedBytes(); this.createdAt = now; this.dueAt = now;
        }
    }
    private final ValidationRunIdentity run;
    private final Sender sender;
    private final Limits limits;
    private final LongSupplier nanoTime;
    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<Entry> entries = new ArrayDeque<>();
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private volatile boolean closed;
    private long sequence;
    private long bytes;

    /** nanoTime is local, monotonic and nonblocking; the caller reserves limits.reservationBytes() first. */
    public ValidationRootHintOutbox(ValidationRunIdentity run, Sender sender, Limits limits, LongSupplier nanoTime) {
        this.run = Objects.requireNonNull(run, "run"); this.sender = Objects.requireNonNull(sender, "sender");
        this.limits = Objects.requireNonNull(limits, "limits"); this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        if (run.mode() != ValidationMode.INCREMENTAL) throw invalid("hint.mode");
    }

    /**
     * One nonwaiting capture-side attempt. Inputs are a stable, admitted list of immutable key facts,
     * not borrowed row/driver values. Rejection retires hint coverage, never normal synchronization.
     * Ordinary side-channel errors are latched for the background controller; fatal Errors still escape.
     */
    public Offer tryOffer(List<Change> changes) {
        if (closed) return terminalOffer();
        if (!lock.tryLock()) { fail(invalid("hint.senderBusy")); return Offer.FAILED; }
        try {
            if (closed) return terminalOffer();
            if (entries.size() >= limits.packets()) { fail(invalid("hint.senderFull")); return Offer.FAILED; }
            if (changes == null || changes.isEmpty() || changes.size() > limits.changes() || sequence == Long.MAX_VALUE) {
                fail(invalid("hint.senderBatch")); return Offer.FAILED;
            }
            var batch = new ValidationRootHintBatch(run, sender, sequence + 1, changes);
            long charge = 512 + batch.retainedBytes();
            if (batch.retainedBytes() > limits.batchBytes() || charge > limits.bytes() - bytes) {
                fail(invalid("hint.senderBytes")); return Offer.FAILED;
            }
            long now = nanoTime.getAsLong();
            if (closed) return terminalOffer();
            entries.addLast(new Entry(batch, now)); bytes += charge; sequence++;
            return Offer.ACCEPTED;
        } catch (RuntimeException problem) {
            fail(problem); return Offer.FAILED;
        } catch (Error fatal) {
            fail(fatal); throw fatal;
        } finally { lock.unlock(); }
    }

    /** Background-only. No network call occurs under the lock; the returned lease retains the packet. */
    public Optional<Lease> poll() {
        boolean completeDrain;
        lock.lock();
        try {
            Entry first = entries.peekFirst();
            if (!closed && first != null) {
                long now = nanoTime.getAsLong();
                if (closed) return Optional.empty();
                if (elapsed(now, first.createdAt) >= limits.windowNanos()) fail(invalid("hint.deliveryWindow"));
                else if (first.lease == null && now - first.dueAt >= 0) {
                    if (first.attempts == limits.attempts()) fail(invalid("hint.deliveryAttempts"));
                    else {
                        first.attempts++;
                        first.lease = new Lease(this, first);
                        return Optional.of(first.lease);
                    }
                }
            }
            return Optional.empty();
        } catch (RuntimeException | Error problem) {
            fail(problem); throw problem;
        } finally {
            discardIdleIfClosed();
            completeDrain = closed && entries.isEmpty();
            lock.unlock();
            if (completeDrain) drained.complete(null);
        }
    }

    /**
     * Background-only, after the physical request has exited. Null means an unknown delivery outcome;
     * BUSY also retries. Only a matching accepted/duplicate receipt releases the logical packet.
     */
    public Settlement settle(Lease lease, ValidationRootHintInbox.Receipt receipt) {
        Objects.requireNonNull(lease, "lease");
        if (lease.owner != this) throw invalid("hint.foreignLease");
        boolean completeDrain;
        lock.lock();
        try {
            if (lease.settled) return Settlement.ALREADY_SETTLED;
            Entry entry = lease.entry;
            if (entries.peekFirst() != entry || entry.lease != lease) throw new IllegalStateException("hint lease ownership was lost");
            lease.settled = true; entry.lease = null;
            if (closed) return failure.get() == null ? Settlement.CLOSED : Settlement.FAILED;
            long now = nanoTime.getAsLong();
            if (closed) return failure.get() == null ? Settlement.CLOSED : Settlement.FAILED;
            if (elapsed(now, entry.createdAt) >= limits.windowNanos()) {
                fail(invalid("hint.deliveryWindow")); return Settlement.FAILED;
            }
            if (receipt != null) {
                if (!run.equals(receipt.run()) || !sender.equals(receipt.sender()) || receipt.sequence() != entry.batch.sequence()
                        || receipt.disposition() == null) {
                    fail(invalid("hint.foreignReceipt")); return Settlement.FAILED;
                }
                switch (receipt.disposition()) {
                    case ACCEPTED, DUPLICATE -> {
                        entries.removeFirst(); bytes -= entry.charge;
                        return Settlement.ACCEPTED;
                    }
                    case BUSY -> { }
                    default -> { fail(invalid("hint.rejectedDelivery")); return Settlement.FAILED; }
                }
            }
            if (entry.attempts == limits.attempts()) {
                fail(invalid("hint.deliveryAttempts")); return Settlement.FAILED;
            }
            entry.dueAt = now + limits.retryNanos();
            return Settlement.RETRY;
        } catch (RuntimeException | Error problem) {
            fail(problem); throw problem;
        } finally {
            discardIdleIfClosed();
            completeDrain = closed && entries.isEmpty();
            lock.unlock();
            if (completeDrain) drained.complete(null);
        }
    }

    /** Safe to sample without completing callbacks on the capture thread. */
    public Optional<Throwable> failure() { return Optional.ofNullable(failure.get()); }

    public Snapshot snapshot() {
        lock.lock();
        try { return new Snapshot(entries.size(), bytes, !entries.isEmpty() && entries.peekFirst().lease != null, closed, failure.get()); }
        finally { lock.unlock(); }
    }

    /** Packet ownership only; transport resources and controller/run reservations have their own drains. */
    public CompletionStage<Void> drained() { return drained.minimalCompletionStage(); }

    /** Background-only cancellation. It cannot release a still-running physical transport request. */
    @Override public void close() {
        boolean completeDrain;
        lock.lock();
        try { closed = true; discardIdleIfClosed(); completeDrain = entries.isEmpty(); }
        finally { lock.unlock(); }
        if (completeDrain) drained.complete(null);
    }

    private Offer terminalOffer() { return failure.get() == null ? Offer.CLOSED : Offer.FAILED; }
    private void fail(Throwable problem) { failure.record(problem); closed = true; }
    private void discardIdleIfClosed() {
        if (!closed) return;
        Iterator<Entry> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();
            if (entry.lease == null) { bytes -= entry.charge; iterator.remove(); }
        }
    }
    private static long elapsed(long now, long before) {
        long elapsed = now - before;
        if (elapsed < 0) throw invalid("hint.monotonicClock");
        return elapsed;
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
