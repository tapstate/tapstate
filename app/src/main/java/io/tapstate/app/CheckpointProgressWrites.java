package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterProgressUpdate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.MDC;

/** Bounded original-run progress folding with completion only after the owning store commits. */
final class CheckpointProgressWrites implements AutoCloseable {
    static final String USER_CONTEXT_KEY = "tapstate.checkpoint-progress-writes";
    private static final int REQUEST_OVERHEAD = 512;
    private final int maximumEntries;
    private final long maximumBytes;
    private final int maximumBatch;
    private final Map<Key, Pending> pending = new LinkedHashMap<>();
    private final ScheduledExecutorService worker;
    private long retainedBytes;
    private int callers;
    private int waitingCallers;
    private long waitingBytes;
    private boolean closed;
    private Thread writing;
    private boolean flushing;

    CheckpointProgressWrites() { this(1024, 4L * 1024 * 1024, 64, Duration.ofMillis(25)); }

    CheckpointProgressWrites(int maximumEntries, long maximumBytes, int maximumBatch, Duration interval) {
        if (maximumEntries < 1 || maximumBytes < REQUEST_OVERHEAD || maximumBatch < 1
                || interval.isNegative() || interval.isZero()) throw new IllegalArgumentException("invalid progress write bounds");
        this.maximumEntries = maximumEntries;
        this.maximumBytes = maximumBytes;
        this.maximumBatch = maximumBatch;
        worker = Executors.newSingleThreadScheduledExecutor(task -> ShutdownAwait.daemon(task, "checkpoint-progress-writes"));
        worker.scheduleWithFixedDelay(this::flush, interval.toNanos(), interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    static CheckpointProgressWrites of(HazelcastInstance member) {
        Object value = member.getUserContext().get(USER_CONTEXT_KEY);
        return value instanceof CheckpointProgressWrites writes ? writes : null;
    }

    void writer(ScopedSrsConsumerAccess.Bound bound, WorkloadClaimFence claimed, String runId,
                String table, String writer, WriterProgress progress) {
        if (!submit(new SrsWriter(bound, claimed, runId, table, writer, progress)))
            throw ScopedSrsConsumerAccess.refused(bound.run());
    }

    boolean read(ScopedSrsConsumerAccess.Bound bound, String table, long sequence) {
        return submit(new SrsRead(bound, table, sequence));
    }

    boolean writer(ScopedDirectSinkAckFactory.Bound bound, WorkloadClaimFence claimed, String runId,
                   String table, String writer, WriterProgress progress) {
        return submit(new DirectWriter(bound, claimed, runId, table, writer, progress));
    }

    private boolean submit(Request request) {
        request.verify();
        Receipt receipt = admit(request);
        try {
            boolean result = receipt.completion.get();
            request.verify();
            return result;
        }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw StorageRuntime.unavailable();
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failed.getCause() instanceof Error error) throw error;
            throw new IllegalStateException("progress write failed with a checked exception", failed.getCause());
        } finally {
            synchronized (this) {
                receipt.returned = true;
                release(receipt);
            }
        }
    }

    private synchronized Receipt admit(Request request) {
        if (Thread.currentThread() == writing) throw new IllegalStateException("a progress write cannot wait on its own worker");
        long callerBytes = request.bytes() + REQUEST_OVERHEAD;
        if (request.bytes() + callerBytes > maximumBytes) throw StorageRuntime.unavailable();
        Receipt receipt = tryAdmit(request, callerBytes);
        if (receipt != null) return receipt;

        // Callers applying backpressure retain their original request on their own stacks.
        if (waitingCallers == maximumEntries || waitingBytes + callerBytes > maximumBytes)
            throw StorageRuntime.unavailable();
        waitingCallers++;
        waitingBytes += callerBytes;
        try {
            while (receipt == null) {
                wait();
                receipt = tryAdmit(request, callerBytes);
            }
            return receipt;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw StorageRuntime.unavailable();
        } finally {
            waitingCallers--;
            waitingBytes -= callerBytes;
        }
    }

    private Receipt tryAdmit(Request request, long callerBytes) {
        if (closed) throw StorageRuntime.unavailable();
        Key key = request.key();
        Pending entry = pending.get(key);
        Request merged = entry == null ? request : entry.request.merge(request);
        long increase = merged.bytes() - (entry == null ? 0 : entry.request.bytes()) + callerBytes;
        if (callers >= maximumEntries || retainedBytes + increase > maximumBytes) return null;

        Receipt receipt = new Receipt(callerBytes);
        if (entry == null) {
            entry = new Pending(merged);
            pending.put(key, entry);
        } else {
            entry.request = merged;
        }
        entry.completions.add(receipt);
        retainedBytes += increase;
        callers++;
        return receipt;
    }

    void flush() {
        List<Pending> batch = new ArrayList<>();
        synchronized (this) {
            if (flushing) return;
            flushing = true;
            writing = Thread.currentThread();
            var iterator = pending.entrySet().iterator();
            while (iterator.hasNext() && batch.size() < maximumBatch) {
                batch.add(iterator.next().getValue());
                iterator.remove();
            }
        }
        try {
            for (List<Pending> group : groups(batch)) {
                commit(group);
            }
        } finally {
            synchronized (this) {
                writing = null;
                flushing = false;
                notifyAll();
            }
        }
    }

    private void commit(List<Pending> group) {
        String previous = MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, group.getFirst().request.pipeline());
        try {
            boolean committed = group.getFirst().request.commit(group.stream().map(entry -> entry.request).toList());
            group.forEach(entry -> entry.completions.forEach(receipt -> receipt.completion.complete(committed)));
        } catch (RuntimeException | Error failure) {
            group.forEach(entry -> entry.completions.forEach(receipt -> receipt.completion.completeExceptionally(failure)));
        } finally {
            synchronized (this) {
                for (Pending entry : group) retire(entry);
                notifyAll();
            }
            if (previous == null) MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
            else MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, previous);
        }
    }

    private void retire(Pending entry) {
        retainedBytes -= entry.request.bytes();
        entry.request = null;
        for (Receipt receipt : entry.completions) {
            receipt.retired = true;
            release(receipt);
        }
        entry.completions.clear();
    }

    private synchronized void release(Receipt receipt) {
        if (receipt.returned && receipt.retired && !receipt.released) {
            receipt.released = true;
            retainedBytes -= receipt.bytes;
            callers--;
            notifyAll();
        }
    }

    private static List<List<Pending>> groups(List<Pending> batch) {
        Map<GroupKey, List<Pending>> groups = new LinkedHashMap<>();
        for (Pending entry : batch) {
            Key key = entry.request.key();
            var group = new GroupKey(key.run(), key.kind(), key.claimed(), key.runId(),
                    entry.request instanceof SrsRead ? key.table() : null);
            groups.computeIfAbsent(group, ignored -> new ArrayList<>()).add(entry);
        }
        return new ArrayList<>(groups.values());
    }

    synchronized int retainedRequests() { return callers; }
    synchronized long retainedBytes() { return retainedBytes; }

    @Override public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            for (Pending entry : pending.values()) {
                entry.completions.forEach(receipt -> receipt.completion.completeExceptionally(StorageRuntime.unavailable()));
                retire(entry);
            }
            pending.clear();
            notifyAll();
        }
        worker.shutdown();
        ShutdownAwait.executor(worker, ShutdownAwait.deadline());
    }

    private record Key(Object run, String kind, WorkloadClaimFence claimed, String runId, String table, String writer) {}
    private record GroupKey(Object run, String kind, WorkloadClaimFence claimed, String runId, String table) {}
    private sealed interface Request permits SrsWriter, SrsRead, DirectWriter {
        Key key();
        String pipeline();
        Request merge(Request later);
        long bytes();
        void verify();
        boolean commit(List<Request> batch);
    }
    private record SrsWriter(ScopedSrsConsumerAccess.Bound bound, WorkloadClaimFence claimed, String runId,
                             String table, String writer, WriterProgress progress) implements Request {
        @Override public Key key() { return new Key(bound.run(), "srs-writer", claimed, runId, table, writer); }
        @Override public String pipeline() { return bound.run().scope().pipelineId(); }
        @Override public Request merge(Request later) {
            return new SrsWriter(bound, claimed, runId, table, writer, combine(progress, ((SrsWriter) later).progress));
        }
        @Override public long bytes() { return strings(runId, table, writer) + progressBytes(progress); }
        @Override public void verify() { bound.withWriterStore(claimed, ignored -> {}); }
        @Override public boolean commit(List<Request> batch) {
            batch.forEach(Request::verify);
            boolean[] result = {false};
            bound.withWriterStore(claimed, store -> {
                if (batch.size() == 1) {
                    result[0] = store.advanceWriter(bound.run().context(), bound.run().membershipRevision(), runId,
                            table, writer, progress, bound.run().originalClaim());
                } else {
                    var updates = batch.stream().map(request -> (SrsWriter) request)
                            .map(request -> new WriterProgressUpdate(request.table, request.writer, request.progress)).toList();
                    result[0] = store.advanceWriters(bound.run().context(), bound.run().membershipRevision(), runId,
                            updates, bound.run().originalClaim());
                }
            });
            return result[0];
        }
    }
    private record SrsRead(ScopedSrsConsumerAccess.Bound bound, String table, long sequence) implements Request {
        @Override public Key key() { return new Key(bound.run(), "srs-read", null, null, table, null); }
        @Override public String pipeline() { return bound.run().scope().pipelineId(); }
        @Override public Request merge(Request later) { return new SrsRead(bound, table, Math.max(sequence, ((SrsRead) later).sequence)); }
        @Override public long bytes() { return strings(table); }
        @Override public void verify() { bound.requireClaim(null); }
        @Override public boolean commit(List<Request> batch) {
            batch.forEach(Request::verify);
            return bound.store().advanceReadSequence(bound.run().context(), bound.run().membershipRevision(), table, sequence, bound.run().originalClaim());
        }
    }
    private record DirectWriter(ScopedDirectSinkAckFactory.Bound bound, WorkloadClaimFence claimed, String runId,
                                String table, String writer, WriterProgress progress) implements Request {
        @Override public Key key() { return new Key(bound.run(), "direct-writer", claimed, runId, table, writer); }
        @Override public String pipeline() { return bound.run().execution().pipelineId(); }
        @Override public Request merge(Request later) {
            return new DirectWriter(bound, claimed, runId, table, writer, combine(progress, ((DirectWriter) later).progress));
        }
        @Override public long bytes() { return strings(runId, table, writer) + progressBytes(progress); }
        @Override public void verify() { bound.requireClaim(claimed); }
        @Override public boolean commit(List<Request> batch) {
            batch.forEach(Request::verify);
            if (batch.size() == 1)
                return bound.store().advanceWriter(bound.run().context(), bound.run().epoch(), runId, table, writer, progress, bound.run().pipelineClaim());
            var updates = batch.stream().map(request -> (DirectWriter) request)
                    .map(request -> new WriterProgressUpdate(request.table, request.writer, request.progress)).toList();
            return bound.store().advanceWriters(bound.run().context(), bound.run().epoch(), runId, updates, bound.run().pipelineClaim());
        }
    }

    private static WriterProgress combine(WriterProgress before, WriterProgress after) {
        var durable = before.durableThrough().compareTo(after.durableThrough()) >= 0 ? before.durableThrough() : after.durableThrough();
        ChainPosition tokened = before.lastTokened();
        if (after.lastTokened() != null && (tokened == null || after.lastTokened().order().compareTo(tokened.order()) > 0)) tokened = after.lastTokened();
        return new WriterProgress(durable, tokened);
    }
    private static long progressBytes(WriterProgress progress) {
        Objects.requireNonNull(progress);
        return progress.lastTokened() == null ? 64 : 96 + strings(progress.lastTokened().token());
    }
    private static long strings(String... values) {
        long bytes = 0;
        for (String value : values) if (value != null) bytes += 32L + 2L * value.length();
        return bytes;
    }
    private static final class Receipt {
        private final CompletableFuture<Boolean> completion = new CompletableFuture<>();
        private final long bytes;
        private boolean returned;
        private boolean retired;
        private boolean released;
        private Receipt(long bytes) { this.bytes = bytes; }
    }

    private static final class Pending {
        private Request request;
        private final List<Receipt> completions = new ArrayList<>();
        private Pending(Request request) { this.request = request; }
    }
}
