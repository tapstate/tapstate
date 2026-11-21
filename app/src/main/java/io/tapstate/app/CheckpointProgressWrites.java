package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterProgressOutcome;
import io.tapstate.spi.store.WriterProgressUpdate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.MDC;

/** Bounded nonblocking progress admission, with original-run receipts completed after persistence. */
final class CheckpointProgressWrites implements AutoCloseable {
    static final String USER_CONTEXT_KEY = "tapstate.checkpoint-progress-writes";
    private static final int REQUEST_OVERHEAD = 512;
    private final int maximumEntries;
    private final long maximumBytes;
    private final int maximumBatch;
    private final Map<Key, Pending> pending = new LinkedHashMap<>();
    private final Set<String> activePipelines = new HashSet<>();
    private final Set<Thread> writers = new HashSet<>();
    private final ScheduledExecutorService timer;
    private final ExecutorService worker;
    private long retainedBytes;
    private int callers;
    private boolean closed;

    CheckpointProgressWrites() { this(1024, 4L * 1024 * 1024, 64, Duration.ofMillis(25)); }

    CheckpointProgressWrites(int maximumEntries, long maximumBytes, int maximumBatch, Duration interval) {
        if (maximumEntries < 1 || maximumBytes < REQUEST_OVERHEAD || maximumBatch < 1
                || interval.isNegative() || interval.isZero()) throw new IllegalArgumentException("invalid progress write bounds");
        this.maximumEntries = maximumEntries;
        this.maximumBytes = maximumBytes;
        this.maximumBatch = maximumBatch;
        timer = Executors.newSingleThreadScheduledExecutor(
                task -> ShutdownAwait.daemon(task, "checkpoint-progress-writes"));
        worker = Executors.newThreadPerTaskExecutor(task -> ShutdownAwait.virtual(task, "checkpoint-progress-write"));
        timer.scheduleWithFixedDelay(this::dispatch, interval.toNanos(), interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    static CheckpointProgressWrites of(HazelcastInstance member) {
        Object value = member.getUserContext().get(USER_CONTEXT_KEY);
        return value instanceof CheckpointProgressWrites writes ? writes : null;
    }

    CompletableFuture<Boolean> offerWriter(ScopedSrsConsumerAccess.Bound bound, WorkloadClaimFence claimed,
            String runId, String table, String writer, WriterProgress progress) {
        return offer(new SrsWriter(bound, claimed, runId, table, writer, progress));
    }

    CompletableFuture<Boolean> offerWriter(ScopedDirectSinkAckFactory.Bound bound, WorkloadClaimFence claimed,
            String runId, String table, String writer, WriterProgress progress) {
        return offer(new DirectWriter(bound, claimed, runId, table, writer, progress));
    }

    /** Explicit lifecycle boundaries can await the original report while ordinary reports only offer it. */
    void writer(ScopedSrsConsumerAccess.Bound bound, WorkloadClaimFence claimed, String runId,
                String table, String writer, WriterProgress progress) {
        if (!await(offerWriter(bound, claimed, runId, table, writer, progress)))
            throw ScopedSrsConsumerAccess.refused(bound.run());
    }

    boolean writer(ScopedDirectSinkAckFactory.Bound bound, WorkloadClaimFence claimed, String runId,
                   String table, String writer, WriterProgress progress) {
        return await(offerWriter(bound, claimed, runId, table, writer, progress));
    }

    private boolean await(CompletableFuture<Boolean> completion) {
        synchronized (this) {
            if (writers.contains(Thread.currentThread())) throw new IllegalStateException("a progress write cannot wait on its own worker");
        }
        try { return completion.get(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw StorageRuntime.unavailable();
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failed.getCause() instanceof Error error) throw error;
            throw new IllegalStateException("progress write failed with a checked exception", failed.getCause());
        }
    }

    private synchronized CompletableFuture<Boolean> offer(Request request) {
        if (closed) throw StorageRuntime.unavailable();
        Key key = request.key();
        Pending entry = pending.get(key);
        Request merged = entry == null ? request : entry.request.merge(request);
        long increase = merged.bytes() - (entry == null ? 0 : entry.request.bytes()) + REQUEST_OVERHEAD;
        if (callers >= maximumEntries || retainedBytes + increase > maximumBytes) throw StorageRuntime.unavailable();
        Receipt receipt = new Receipt();
        if (entry == null) {
            entry = new Pending(merged);
            pending.put(key, entry);
        } else entry.request = merged;
        entry.completions.add(receipt);
        retainedBytes += increase;
        callers++;
        return receipt.completion;
    }

    void flush() {
        dispatch().forEach(CompletableFuture::join);
    }

    private synchronized List<CompletableFuture<Void>> dispatch() {
        if (closed || pending.isEmpty()) return List.of();
        Map<String, List<Pending>> selected = new LinkedHashMap<>();
        var iterator = pending.values().iterator();
        int entries = 0;
        while (iterator.hasNext() && entries < maximumBatch) {
            var entry = iterator.next();
            String pipeline = entry.request.pipeline();
            if (activePipelines.contains(pipeline)) continue;
            selected.computeIfAbsent(pipeline, ignored -> new ArrayList<>()).add(entry);
            iterator.remove();
            entries++;
        }
        var dispatched = new ArrayList<CompletableFuture<Void>>(selected.size());
        // One task per selected Pipeline, with its existing admission charges held until the writes retire.
        selected.forEach((pipeline, batch) -> {
            activePipelines.add(pipeline);
            dispatched.add(CompletableFuture.runAsync(() -> writePipeline(pipeline, batch), worker));
        });
        return dispatched;
    }

    private void writePipeline(String pipeline, List<Pending> batch) {
        synchronized (this) { writers.add(Thread.currentThread()); }
        try {
            groups(batch).forEach(this::commit);
        } finally {
            synchronized (this) {
                writers.remove(Thread.currentThread());
                activePipelines.remove(pipeline);
                notifyAll();
            }
        }
    }

    private void commit(List<Pending> group) {
        String previous = MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, group.getFirst().request.pipeline());
        try {
            var outcomes = group.getFirst().request.commit(group.stream().map(entry -> entry.request).toList());
            if (outcomes.size() != group.size()) throw new IllegalStateException("progress store returned the wrong number of receipts");
            for (int index = 0; index < group.size(); index++) {
                var entry = group.get(index);
                try {
                    var outcome = outcomes.get(index);
                    if (outcome.failure() == null && outcome.committed()) entry.request.verify();
                    entry.completions.forEach(receipt -> receipt.complete(outcome));
                } catch (RuntimeException | Error failure) {
                    entry.completions.forEach(receipt -> receipt.completion.completeExceptionally(failure));
                }
            }
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
        retainedBytes -= entry.request.bytes() + (long) REQUEST_OVERHEAD * entry.completions.size();
        callers -= entry.completions.size();
        entry.request = null;
        entry.completions.clear();
    }

    private static List<List<Pending>> groups(List<Pending> batch) {
        Map<GroupKey, List<Pending>> groups = new LinkedHashMap<>();
        for (Pending entry : batch) {
            Key key = entry.request.key();
            var group = new GroupKey(key.run(), key.kind(), key.claimed(), key.runId());
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
        timer.shutdown();
        worker.shutdown();
        long deadline = ShutdownAwait.deadline();
        ShutdownAwait.executor(timer, deadline);
        ShutdownAwait.executor(worker, deadline);
    }

    private record Key(Object run, String kind, WorkloadClaimFence claimed, String runId, String table, String writer) {}
    private record GroupKey(Object run, String kind, WorkloadClaimFence claimed, String runId) {}
    private sealed interface Request permits SrsWriter, DirectWriter {
        Key key();
        String pipeline();
        Request merge(Request later);
        long bytes();
        void verify();
        List<WriterProgressOutcome> commit(List<Request> batch);
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
        @Override public List<WriterProgressOutcome> commit(List<Request> batch) {
            batch.forEach(Request::verify);
            var result = new AtomicReference<List<WriterProgressOutcome>>();
            bound.withWriterStore(claimed, store -> {
                if (batch.size() == 1) {
                    result.set(receipt(store.advanceWriter(bound.run().context(), bound.run().membershipRevision(), runId,
                            table, writer, progress, bound.run().originalClaim())));
                } else {
                    var updates = batch.stream().map(SrsWriter.class::cast)
                            .map(request -> new WriterProgressUpdate(request.table, request.writer, request.progress)).toList();
                    result.set(store.advanceWriters(bound.run().context(), bound.run().membershipRevision(), runId,
                            updates, bound.run().originalClaim()));
                }
            });
            return result.get();
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
        @Override public List<WriterProgressOutcome> commit(List<Request> batch) {
            batch.forEach(Request::verify);
            if (batch.size() == 1)
                return receipt(bound.store().advanceWriter(bound.run().context(), bound.run().epoch(), runId,
                        table, writer, progress, bound.run().pipelineClaim()));
            var updates = batch.stream().map(DirectWriter.class::cast)
                    .map(request -> new WriterProgressUpdate(request.table, request.writer, request.progress)).toList();
            return bound.store().advanceWriters(bound.run().context(), bound.run().epoch(), runId, updates, bound.run().pipelineClaim());
        }
    }

    private static List<WriterProgressOutcome> receipt(boolean committed) {
        return List.of(new WriterProgressOutcome(committed, null));
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
        private void complete(WriterProgressOutcome outcome) {
            if (outcome.failure() != null) completion.completeExceptionally(outcome.failure());
            else completion.complete(outcome.committed());
        }
    }

    private static final class Pending {
        private Request request;
        private final List<Receipt> completions = new ArrayList<>();
        private Pending(Request request) { this.request = request; }
    }
}
