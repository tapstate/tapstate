package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Bounded run-local candidate identities, never business rows, confirmation facts or repair authority.
 * Old and new affected roots enter atomically. A capture callback uses tryOffer and never waits for the
 * consumer lock; FULL/BUSY/CLOSED mean nothing was accepted. The caller must stop incremental coverage
 * on a rejected hint, or pace a full scan without retaining rows. Rejection is never silent success.
 *
 * <p>One owner-local consumer polls and settles leases. Revision numbers never repeat in this queue,
 * including after a root is removed and offered again. A settled lease is consumed by the caller;
 * publication still needs its original run/observation fence in the result coordinator and store.
 * This queue does not claim a stable window, clear an existing confirmed finding or reserve a problem
 * quota slot. The caller admits the queue envelope and bounded producer scratch space before use.
 */
public final class ValidationCandidateQueue implements AutoCloseable {
    public record Limits(int roots, long bytes, int batch, int attempts, long delayNanos, long windowNanos) {
        public Limits {
            if (roots < 1 || roots > 4096 || bytes < 1 || bytes > 64L * 1024 * 1024
                    || batch < 1 || batch > 256 || attempts < 1 || attempts > 100
                    || delayNanos < 0 || windowNanos < 1 || windowNanos > 86_400_000_000_000L
                    || delayNanos > windowNanos) throw invalid("candidate.limits");
        }
    }
    public enum Offer { ACCEPTED, FULL, BUSY, CLOSED }
    /** RETRY requests another independent check; SETTLED acknowledges a separately handled outcome. */
    public enum Completion { RETRY, SETTLED }
    public enum Settlement { REMOVED, RETRY_SCHEDULED, SUPERSEDED, CLOSED, ALREADY_SETTLED }
    public record Snapshot(int roots, int leased, long bytes, long revision, boolean closed) {}

    public static final class Lease {
        private final ValidationCandidateQueue owner;
        private final Entry entry;
        private final ValidationObservation observation;
        private final boolean windowExhausted;
        private boolean settled;
        private Lease(ValidationCandidateQueue owner, Entry entry, boolean exhausted) {
            this.owner = owner; this.entry = entry; this.windowExhausted = exhausted;
            observation = new ValidationObservation(owner.run, entry.root, entry.revision);
        }
        public ValidationObservation observation() { return observation; }
        /** An exhausted candidate needs an explicit unsettled outcome, never a confirmed difference. */
        public boolean windowExhausted() { return windowExhausted; }
    }

    private static final class Entry {
        private final ValidationRoot root;
        private final long charge;
        private final long createdAt;
        private long revision;
        private long dueAt;
        private int attempts;
        private Lease lease;
        private Entry(ValidationRoot root, long charge, long revision, long now, long delay) {
            this.root = root; this.charge = charge; this.revision = revision; this.createdAt = now;
            dueAt = now + delay;
        }
    }
    private final ValidationRunIdentity run;
    private final Limits limits;
    private final LongSupplier nanoTime;
    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedHashMap<ValidationRoot, Entry> entries = new LinkedHashMap<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private long bytes;
    private long revision;
    private int leased;
    private boolean closed;

    /** nanoTime is local, monotonic and nonblocking; no store, connector or user callback runs under the lock. */
    public ValidationCandidateQueue(ValidationRunIdentity run, Limits limits, LongSupplier nanoTime) {
        this.run = Objects.requireNonNull(run, "run"); this.limits = Objects.requireNonNull(limits, "limits");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /** The caller supplies a stable, admitted batch of complete root identities, including both sides of a move. */
    public Offer tryOffer(List<ValidationRoot> roots) {
        if (roots == null || roots.isEmpty() || roots.size() > limits.batch()) throw invalid("candidate.batch");
        if (!lock.tryLock()) return Offer.BUSY;
        try {
            if (closed) return Offer.CLOSED;
            var distinct = new LinkedHashSet<ValidationRoot>();
            int additions = 0;
            long additionalBytes = 0;
            for (ValidationRoot root : roots) {
                Objects.requireNonNull(root, "root");
                if (!root.ancestors().isEmpty()) throw invalid("candidate.wholeRoot");
                if (distinct.add(root) && !entries.containsKey(root)) {
                    additions++;
                    additionalBytes += charge(root);
                }
            }
            if (additions > limits.roots() - entries.size() || additionalBytes > limits.bytes() - bytes) return Offer.FULL;
            if (revision == Long.MAX_VALUE) throw limit("revision");
            long now = nanoTime.getAsLong();
            long next = ++revision;
            for (ValidationRoot root : distinct) {
                Entry entry = entries.get(root);
                if (entry == null) {
                    entry = new Entry(root, charge(root), next, now, limits.delayNanos());
                    entries.put(root, entry);
                    bytes += entry.charge;
                } else {
                    entry.revision = next;
                    // Keep the earlier due time and original attempt/window budget under continuous changes.
                }
            }
            return Offer.ACCEPTED;
        } finally { lock.unlock(); }
    }

    /** Returns one due identity; no query or business comparison runs on the producer thread. */
    public Optional<Lease> poll() {
        lock.lock();
        try {
            if (closed) return Optional.empty();
            long now = nanoTime.getAsLong();
            for (Entry entry : entries.values()) {
                if (entry.lease != null) continue;
                boolean exhausted = entry.attempts >= limits.attempts() || elapsed(now, entry.createdAt) >= limits.windowNanos();
                if (!exhausted && now - entry.dueAt < 0) continue;
                entry.lease = new Lease(this, entry, exhausted);
                if (!exhausted) entry.attempts++;
                leased++;
                return Optional.of(entry.lease);
            }
            return Optional.empty();
        } finally { lock.unlock(); }
    }

    /**
     * Called after this attempt's worker/read ownership has ended and its outcome has been handled.
     * This releases queue ownership only, never checker, driver, result or retained-history reservations.
     * A superseded attempt cannot remove the latest dirty revision. Exhausted attempts cannot retry.
     */
    public Settlement settle(Lease lease, Completion completion) {
        Objects.requireNonNull(lease, "lease"); Objects.requireNonNull(completion, "completion");
        if (lease.owner != this) throw invalid("candidate.foreignLease");
        boolean completeDrain = false;
        lock.lock();
        try {
            if (lease.settled) return Settlement.ALREADY_SETTLED;
            Entry entry = lease.entry;
            if (entry.lease != lease || entries.get(entry.root) != entry) throw new IllegalStateException("candidate lease ownership was lost");
            if (!closed && lease.windowExhausted && completion == Completion.RETRY) throw invalid("candidate.exhaustedRetry");
            boolean current = entry.revision == lease.observation.revision();
            long retryAt = !closed && current && completion == Completion.RETRY
                    ? nanoTime.getAsLong() + limits.delayNanos() : 0;
            lease.settled = true; entry.lease = null; leased--;
            if (closed) {
                remove(entry);
                completeDrain = entries.isEmpty();
                return Settlement.CLOSED;
            }
            if (!current) {
                entries.putLast(entry.root, entry);
                return Settlement.SUPERSEDED;
            }
            if (completion == Completion.SETTLED) {
                remove(entry);
                return Settlement.REMOVED;
            }
            entry.dueAt = retryAt;
            entries.putLast(entry.root, entry);
            return Settlement.RETRY_SCHEDULED;
        } finally {
            lock.unlock();
            if (completeDrain) drained.complete(null);
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try { return new Snapshot(entries.size(), leased, bytes, revision, closed); }
        finally { lock.unlock(); }
    }
    /** Completes only after close and every outstanding queue lease is returned. */
    public CompletionStage<Void> drained() { return drained.minimalCompletionStage(); }

    @Override public void close() {
        boolean completeDrain;
        lock.lock();
        try {
            closed = true;
            Iterator<Entry> iterator = entries.values().iterator();
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                if (entry.lease == null) { bytes -= entry.charge; iterator.remove(); }
            }
            completeDrain = entries.isEmpty();
        } finally { lock.unlock(); }
        if (completeDrain) drained.complete(null);
    }

    private void remove(Entry entry) { entries.remove(entry.root); bytes -= entry.charge; }
    private static long charge(ValidationRoot root) {
        return 512L + 2L * (root.mappingId().length() + root.outputId().length()) + root.rootKey().retainedBytes();
    }
    private static long elapsed(long now, long before) {
        long elapsed = now - before;
        if (elapsed < 0) throw invalid("candidate.monotonicClock");
        return elapsed;
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String field) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "candidate." + field), null);
    }
}
