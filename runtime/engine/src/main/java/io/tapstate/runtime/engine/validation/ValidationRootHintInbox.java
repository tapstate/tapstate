package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRootHintBatch.Sender;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Owner-local admission of key-only direct-root hints from a frozen set of registered capture scopes.
 * Use only on the background validation transport, never on a source callback. The caller authenticates
 * the sender and checks current run/owner authority before dispatch, and closes this inbox on loss.
 * Identity equality here cannot renew that authority or substitute for result publication fences.
 *
 * <p>Each sender has one ordered logical packet in flight, starts at sequence one, and retries identical
 * content until its receipt is known. Sequences describe validation delivery only, never source ACKs or
 * offsets. Only the latest accepted fingerprint is retained; older packets are rejected as stale without
 * claiming to verify their payload. No sender is replaced in this run. New tenures need a new run.
 *
 * <p>A receipt proves only that all roots in this packet entered this local queue. It proves neither
 * complete capture/transport coverage, a stable source window, writer commit, nor successful checking.
 * Lost final packets and disconnected senders need a separate bounded transport/liveness controller.
 * A known gap, conflicting retry or exhausted queue latches failure and closes candidate admission.
 */
public final class ValidationRootHintInbox implements AutoCloseable {
    public record Limits(int senders, long registrationBytes, int changes, long batchBytes) {
        public Limits {
            if (senders < 1 || senders > 256 || registrationBytes < 1 || registrationBytes > 16L * 1024 * 1024
                    || changes < 1 || changes > ValidationRootHintBatch.MAX_CHANGES
                    || batchBytes < 1 || batchBytes > ValidationRootHintBatch.MAX_BYTES) throw invalid("hint.inboxLimits");
        }
    }
    public enum Disposition { ACCEPTED, DUPLICATE, STALE, BUSY, FAILED, CLOSED }
    public record Receipt(ValidationRunIdentity run, Sender sender, long sequence, Disposition disposition) {}
    public record Snapshot(int senders, long registrationBytes, boolean closed, Throwable failure) {}

    private static final class SenderState {
        private final Sender sender;
        private long accepted;
        private String fingerprint;
        private SenderState(Sender sender) { this.sender = sender; }
    }
    private final ValidationRunIdentity run;
    private final ValidationCandidateQueue queue;
    private final Limits limits;
    private final Map<String, SenderState> senders;
    private final long registrationBytes;
    private final ReentrantLock lock = new ReentrantLock();
    private final CompletableFuture<Throwable> failed = new CompletableFuture<>();
    private boolean closed;
    private Throwable failure;

    /** The caller admits the registration envelope and every in-flight batch/scratch reservation separately. */
    public ValidationRootHintInbox(ValidationCandidateQueue queue, List<Sender> registered, Limits limits) {
        this.queue = Objects.requireNonNull(queue, "queue"); this.run = queue.run();
        this.limits = Objects.requireNonNull(limits, "limits");
        if (run.mode() != ValidationMode.INCREMENTAL || queue.limits().batch() < 2 * limits.changes()) {
            throw invalid("hint.queueContract");
        }
        if (registered == null || registered.isEmpty() || registered.size() > limits.senders()) throw invalid("hint.senders");
        var states = new LinkedHashMap<String, SenderState>();
        long bytes = 16 * 1024;
        for (Sender sender : registered) {
            Objects.requireNonNull(sender, "sender");
            bytes += 512L + sender.retainedBytes();
            if (bytes > limits.registrationBytes()) throw invalid("hint.registrationBytes");
            if (states.putIfAbsent(sender.registrationId(), new SenderState(sender)) != null) throw invalid("hint.duplicateSender");
        }
        senders = Map.copyOf(states);
        registrationBytes = bytes;
    }

    /** BUSY accepts nothing; bounded background retries may retry it. FAILED ends this run's hint coverage. */
    public Receipt tryAccept(ValidationRootHintBatch batch) {
        Objects.requireNonNull(batch, "batch");
        if (!lock.tryLock()) return receipt(batch, Disposition.BUSY);
        Throwable notifyFailure = null;
        try {
            if (closed) return receipt(batch, failure == null ? Disposition.CLOSED : Disposition.FAILED);
            SenderState state = senders.get(batch.sender().registrationId());
            if (!run.equals(batch.run()) || state == null || !state.sender.equals(batch.sender())) {
                return receipt(batch, Disposition.STALE);
            }
            if (batch.changes().size() > limits.changes() || batch.retainedBytes() > limits.batchBytes()) {
                notifyFailure = fail("hint.batchBudget");
                return receipt(batch, Disposition.FAILED);
            }
            if (batch.sequence() < state.accepted) return receipt(batch, Disposition.STALE);
            String fingerprint = batch.contentFingerprint();
            if (batch.sequence() == state.accepted) {
                if (fingerprint.equals(state.fingerprint)) return receipt(batch, Disposition.DUPLICATE);
                notifyFailure = fail("hint.conflictingRetry");
                return receipt(batch, Disposition.FAILED);
            }
            if (state.accepted == Long.MAX_VALUE || batch.sequence() != state.accepted + 1) {
                notifyFailure = fail("hint.sequenceGap");
                return receipt(batch, Disposition.FAILED);
            }
            var offer = queue.tryOffer(batch.oneToOneRoots());
            if (offer == ValidationCandidateQueue.Offer.BUSY) return receipt(batch, Disposition.BUSY);
            if (offer != ValidationCandidateQueue.Offer.ACCEPTED) {
                notifyFailure = fail(offer == ValidationCandidateQueue.Offer.FULL ? "hint.candidateFull" : "hint.candidateClosed");
                return receipt(batch, Disposition.FAILED);
            }
            state.accepted = batch.sequence();
            state.fingerprint = fingerprint;
            return receipt(batch, Disposition.ACCEPTED);
        } catch (RuntimeException | Error problem) {
            if (!closed) notifyFailure = fail(problem);
            throw problem;
        } finally {
            lock.unlock();
            // Queue draining and future completion may invoke arbitrary callbacks. Neither holds our lock.
            if (notifyFailure != null) {
                queue.close();
                failed.complete(notifyFailure);
            }
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try { return new Snapshot(senders.size(), registrationBytes, closed, failure); }
        finally { lock.unlock(); }
    }

    /** Failure is latched locally even if an external notification is lost. Normal close is not failure. */
    public CompletionStage<Throwable> failure() { return failed.minimalCompletionStage(); }

    /** Also stops new queue work; outstanding queue leases still drain through their original owner. */
    @Override public void close() {
        lock.lock();
        try { closed = true; }
        finally { lock.unlock(); }
        queue.close();
    }

    private Throwable fail(String field) { return fail(invalid(field)); }
    private Throwable fail(Throwable problem) {
        closed = true;
        failure = problem;
        return problem;
    }
    private static Receipt receipt(ValidationRootHintBatch batch, Disposition disposition) {
        return new Receipt(batch.run(), batch.sender(), batch.sequence(), disposition);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
