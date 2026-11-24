package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRootHintBatch.Change;
import io.tapstate.core.validation.ValidationRootHintBatch.Sender;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
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
    // Publish FIFO order and its byte charge together. Delivery owns the mutable fields on each entry.
    private record Buffer(List<Entry> entries, long bytes) {
        private static final Buffer EMPTY = new Buffer(List.of(), 0);

        Entry first() { return entries.isEmpty() ? null : entries.getFirst(); }

        Buffer append(Entry entry) {
            var next = new ArrayList<Entry>(entries.size() + 1);
            next.addAll(entries);
            next.add(entry);
            return new Buffer(List.copyOf(next), bytes + entry.charge);
        }

        Buffer removeFirst() {
            return new Buffer(List.copyOf(entries.subList(1, entries.size())), bytes - entries.getFirst().charge);
        }
    }
    private final ValidationRunIdentity run;
    private final Sender sender;
    private final Limits limits;
    private final LongSupplier nanoTime;
    private final ReentrantLock offerLock = new ReentrantLock();
    private final ReentrantLock deliveryLock = new ReentrantLock();
    private final AtomicReference<Buffer> buffer = new AtomicReference<>(Buffer.EMPTY);
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private volatile boolean closed;
    private long sequence;

    /** nanoTime is local, monotonic and nonblocking; the caller reserves limits.reservationBytes() first. */
    public ValidationRootHintOutbox(ValidationRunIdentity run, Sender sender, Limits limits, LongSupplier nanoTime) {
        this.run = Objects.requireNonNull(run, "run"); this.sender = Objects.requireNonNull(sender, "sender");
        this.limits = Objects.requireNonNull(limits, "limits"); this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        if (run.mode() != ValidationMode.INCREMENTAL) throw invalid("hint.mode");
    }

    public ValidationRunIdentity run() { return run; }
    public Sender sender() { return sender; }
    public Limits limits() { return limits; }
    /** Local open state only, never a renewal or proof of distributed authority. */
    public boolean isOpen() { return !closed; }

    /**
     * Records an extraction or capture fault without transport, locking the buffer, or running callbacks.
     * The background controller must observe failure/liveness and retire the original run. A caller that
     * caught an Error must still rethrow it after recording; this method does not consume that Error.
     */
    public void reportFailure(Throwable problem) { fail(Objects.requireNonNull(problem, "problem")); }

    /**
     * One nonwaiting capture-side attempt. Inputs are a stable, admitted list of immutable key facts,
     * not borrowed row/driver values. Rejection retires hint coverage, never normal synchronization.
     * Ordinary side-channel errors are latched for the background controller; fatal Errors still escape.
     */
    public Offer tryOffer(List<Change> changes) {
        if (closed) return terminalOffer();
        if (!offerLock.tryLock()) {
            if (closed) return terminalOffer();
            fail(invalid("hint.senderBusy")); return Offer.FAILED;
        }
        try {
            if (closed) return terminalOffer();
            if (buffer.get().entries().size() >= limits.packets()) { fail(invalid("hint.senderFull")); return Offer.FAILED; }
            if (changes == null || changes.isEmpty() || changes.size() > limits.changes() || sequence == Long.MAX_VALUE) {
                fail(invalid("hint.senderBatch")); return Offer.FAILED;
            }
            var batch = new ValidationRootHintBatch(run, sender, sequence + 1, changes);
            long charge = 512 + batch.retainedBytes();
            if (batch.retainedBytes() > limits.batchBytes() || charge > limits.bytes() - buffer.get().bytes()) {
                fail(invalid("hint.senderBytes")); return Offer.FAILED;
            }
            var entry = new Entry(batch, nanoTime.getAsLong());
            // With one offer active, delivery can only remove existing packets. Each retry frees capacity.
            while (!closed) {
                var current = buffer.get();
                if (buffer.compareAndSet(current, current.append(entry))) {
                    sequence++;
                    return Offer.ACCEPTED;
                }
            }
            return terminalOffer();
        } catch (RuntimeException problem) {
            fail(problem); return Offer.FAILED;
        } catch (Error fatal) {
            fail(fatal); throw fatal;
        } finally { offerLock.unlock(); }
    }

    /** Background-only. No network call occurs under the lock; the returned lease retains the packet. */
    public Optional<Lease> poll() {
        boolean completeDrain;
        deliveryLock.lock();
        try {
            Entry first = buffer.get().first();
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
            completeDrain = discardIdleIfClosed();
            deliveryLock.unlock();
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
        deliveryLock.lock();
        try {
            if (lease.settled) return Settlement.ALREADY_SETTLED;
            Entry entry = Objects.requireNonNull(lease.entry, "entry");
            requireActiveLease(entry, lease);
            lease.settled = true; entry.lease = null;
            if (closed) return failure.get() == null ? Settlement.CLOSED : Settlement.FAILED;
            long now = nanoTime.getAsLong();
            if (closed) return failure.get() == null ? Settlement.CLOSED : Settlement.FAILED;
            return settleEntry(entry, receipt, now);
        } catch (RuntimeException | Error problem) {
            fail(problem); throw problem;
        } finally {
            completeDrain = discardIdleIfClosed();
            deliveryLock.unlock();
            if (completeDrain) drained.complete(null);
        }
    }

    private void requireActiveLease(Entry entry, Lease lease) {
        if (buffer.get().first() != entry || entry.lease != lease) {
            throw new IllegalStateException("hint lease ownership was lost");
        }
    }

    private Settlement settleEntry(Entry entry, ValidationRootHintInbox.Receipt receipt, long now) {
        if (elapsed(now, entry.createdAt) >= limits.windowNanos()) {
            fail(invalid("hint.deliveryWindow")); return Settlement.FAILED;
        }
        Settlement receiptResult = settleReceipt(entry, receipt);
        if (receiptResult != null) return receiptResult;
        if (entry.attempts == limits.attempts()) {
            fail(invalid("hint.deliveryAttempts")); return Settlement.FAILED;
        }
        entry.dueAt = now + limits.retryNanos();
        return Settlement.RETRY;
    }

    private Settlement settleReceipt(Entry entry, ValidationRootHintInbox.Receipt receipt) {
        if (receipt == null) return null;
        if (!run.equals(receipt.run()) || !sender.equals(receipt.sender()) || receipt.sequence() != entry.batch.sequence()
                || receipt.disposition() == null) {
            fail(invalid("hint.foreignReceipt")); return Settlement.FAILED;
        }
        return switch (receipt.disposition()) {
            case ACCEPTED, DUPLICATE -> {
                buffer.updateAndGet(Buffer::removeFirst);
                yield Settlement.ACCEPTED;
            }
            case BUSY -> null;
            default -> {
                fail(invalid("hint.rejectedDelivery"));
                yield Settlement.FAILED;
            }
        };
    }

    /** Safe to sample without completing callbacks on the capture thread. */
    public Optional<Throwable> failure() { return Optional.ofNullable(failure.get()); }

    public Snapshot snapshot() {
        deliveryLock.lock();
        try {
            var current = buffer.get();
            var first = current.first();
            return new Snapshot(current.entries().size(), current.bytes(), first != null && first.lease != null, closed, failure.get());
        } finally { deliveryLock.unlock(); }
    }

    /** Packet ownership only; transport resources and controller/run reservations have their own drains. */
    public CompletionStage<Void> drained() { return drained.minimalCompletionStage(); }

    /** Background-only cancellation. It cannot release a still-running physical transport request. */
    @Override public void close() {
        boolean completeDrain;
        deliveryLock.lock();
        try { closed = true; completeDrain = discardIdleIfClosed(); }
        finally { deliveryLock.unlock(); }
        if (completeDrain) drained.complete(null);
    }

    private Offer terminalOffer() { return failure.get() == null ? Offer.CLOSED : Offer.FAILED; }
    private void fail(Throwable problem) { failure.record(problem); closed = true; }
    private boolean discardIdleIfClosed() {
        if (!closed) return false;
        // Only a closed delivery waits for an active offer to release its constructor reservation.
        offerLock.lock();
        try {
            var first = buffer.get().first();
            if (first != null && first.lease != null) {
                buffer.set(new Buffer(List.of(first), first.charge));
                return false;
            }
            buffer.set(Buffer.EMPTY);
            return true;
        } finally { offerLock.unlock(); }
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
