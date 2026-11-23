package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StateStore;
import org.bson.Document;

import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.Optional;

/** Writes the native checkpoint within each caller's original CACHE transaction. */
final class MongoCachePipelineStateStore implements StateStore {
    private static final int MAX_CALLERS = 1024;
    private static final class Lane {
        private final String pipeline;
        private final ReentrantLock transaction = new ReentrantLock(true);
        private int users;
        private Lane(String pipeline) { this.pipeline = pipeline; }
    }

    private static final class Writer {
        private final Semaphore capacity = new Semaphore(MAX_CALLERS);
        private final Map<String, Lane> lanes = new HashMap<>();

        private synchronized boolean ownsAll(List<String> pipelines) {
            return pipelines.stream().allMatch(pipeline -> {
                Lane lane = lanes.get(pipeline);
                return lane != null && lane.transaction.isHeldByCurrentThread();
            });
        }

        private synchronized List<Lane> retain(List<String> pipelines) {
            var retained = new ArrayList<Lane>(pipelines.size());
            for (String pipeline : pipelines) {
                Lane lane = lanes.computeIfAbsent(pipeline, Lane::new);
                lane.users++;
                retained.add(lane);
            }
            return retained;
        }

        private synchronized void release(List<Lane> retained) {
            for (Lane lane : retained) {
                if (--lane.users == 0) lanes.remove(lane.pipeline);
            }
        }
    }

    private static final Map<MongoClient, Writer> WRITERS = new WeakHashMap<>();

    /** Admit a Pipeline write before its transaction while preserving independent Pipeline progress. */
    static <T> T writeTransaction(MongoClient client, String pipeline, Supplier<T> operation) {
        return writeOrderedTransaction(client, List.of(pipeline), null, operation);
    }

    /** The original pin budget includes waiting for the checkpoint writer. */
    static <T> T writeTransaction(MongoClient client, String pipeline, MongoReadBudget budget, Supplier<T> operation) {
        return writeOrderedTransaction(client, List.of(pipeline), Objects.requireNonNull(budget, "budget"), operation);
    }

    /** A transaction touching several Pipelines acquires their lanes in one stable order. */
    static <T> T writeTransaction(MongoClient client, Collection<String> pipelines, Supplier<T> operation) {
        return writeOrderedTransaction(client, List.copyOf(new TreeSet<>(pipelines)), null, operation);
    }

    private static <T> T writeOrderedTransaction(MongoClient client, List<String> selected, MongoReadBudget budget, Supplier<T> operation) {
        if (budget != null) budget.requireRemaining();
        if (selected.isEmpty()) return operation.get();
        Writer writer;
        synchronized (WRITERS) {
            writer = WRITERS.computeIfAbsent(client, ignored -> new Writer());
        }
        if (writer.ownsAll(selected)) return operation.get();
        if (!writer.capacity.tryAcquire()) throw unavailable("checkpoint writer capacity exhausted");
        try {
            List<Lane> retained = writer.retain(selected);
            int acquired = 0;
            try {
                for (Lane lane : retained) {
                    if (budget == null) lane.transaction.lockInterruptibly();
                    else if (!lane.transaction.tryLock(budget.remainingMillis(), java.util.concurrent.TimeUnit.MILLISECONDS))
                        throw unavailable("checkpoint writer deadline exceeded");
                    acquired++;
                }
                if (budget != null) budget.requireRemaining();
                return operation.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw unavailable("checkpoint writer interrupted");
            } finally {
                for (int index = acquired - 1; index >= 0; index--) retained.get(index).transaction.unlock();
                writer.release(retained);
            }
        } finally {
            writer.capacity.release();
        }
    }

    private static TapstateException unavailable(String detail) {
        return new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", detail), null);
    }

    private final MongoCachePipelineAccess access;
    private final MongoStructureProof structures;

    MongoCachePipelineStateStore(MongoCachePipelineAccess access, MongoStructureProof structures) {
        this.access = access;
        this.structures = structures;
    }

    static void advanceFence(MongoDatabase database, ClientSession session, Document current,
            Document values, boolean advanceEpoch) {
        long revision = current.getLong("runtimeWriteFence");
        if (revision == Long.MAX_VALUE - 1
                || (advanceEpoch && current.getLong("epoch") == Long.MAX_VALUE - 1))
            throw MongoCachePipelineAccess.fenced();
        Document increments = new Document("runtimeWriteFence", 1L);
        if (advanceEpoch) increments.append("epoch", 1L);
        Document changes = new Document("$inc", increments);
        if (!values.isEmpty()) changes.append("$set", values);
        if (!update(database, session, current, changes))
            throw new IllegalStateException("Pipeline state changed within its transaction snapshot");
    }

    static boolean update(MongoDatabase database, ClientSession session, Document current, Document changes) {
        return SystemCollections.CACHE_PIPELINE_STATE.on(database).updateOne(session, version(current), changes)
                .getMatchedCount() == 1;
    }

    static boolean seed(MongoDatabase database, ClientSession session, Document initial, Document retired) {
        var rows = SystemCollections.CACHE_PIPELINE_STATE.on(database);
        if (retired == null) {
            rows.insertOne(session, initial);
            return true;
        }
        return rows.replaceOne(session, version(retired), initial).getMatchedCount() == 1;
    }

    private static Document version(Document row) {
        return new Document("_id", row.get("_id")).append("scope", row.get("scope"))
                .append("epoch", row.get("epoch")).append("runtimeWriteFence", row.get("runtimeWriteFence"));
    }

    @Override public Optional<CheckpointDoc> read(String pipelineId) {
        access.requirePipeline(pipelineId);
        return Optional.of(access.readCheckpoint(structures));
    }

    @Override public void create(String pipelineId, String stateJson, Instant touchTime) {
        access.requirePipeline(pipelineId);
        throw MongoCachePipelineAccess.fenced();
    }

    @Override public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
        access.requirePipeline(pipelineId); Objects.requireNonNull(nextStateJson, "stateJson"); Objects.requireNonNull(touchTime, "touchTime");
        if (expectedEpoch < 0) throw new IllegalArgumentException("expected epoch must be nonnegative");
        return access.checkpointTransaction(session -> {
            Document current = access.state(session, true);
            access.proveClaim(session, true);
            long epoch = current.getLong("epoch");
            if (epoch != expectedEpoch) return new CasOutcome.Fenced(epoch);
            var values = new Document("stateJson", nextStateJson).append("touchMillis", touchTime.toEpochMilli());
            var previous = MongoStateStore.leaseRecovery(current);
            boolean failed = io.tapstate.core.lifecycle.StateJson.of(io.tapstate.core.lifecycle.PipelineState.FAILED).equals(nextStateJson);
            if (previous != null && !failed) values.append("leaseRecovery", null);
            access.updateState(session, current, values, true);
            return new CasOutcome.Applied(new CheckpointDoc(pipelineId, nextStateJson, epoch + 1,
                    Instant.ofEpochMilli(touchTime.toEpochMilli()), failed ? previous : null));
        });
    }

    @Override public CasOutcome recordLeaseRecovery(String pipelineId, CheckpointDoc expected,
            CheckpointDoc.LeaseRecovery recovery, Instant at) {
        access.requirePipeline(pipelineId);
        return access.checkpointTransaction(session -> {
            Document current = access.state(session, true);
            var checkpoint = MongoStateStore.toCheckpoint(current);
            if (checkpoint.epoch() != expected.epoch()
                    || !Objects.equals(checkpoint.leaseRecovery(), expected.leaseRecovery()))
                return new CasOutcome.Fenced(checkpoint.epoch());
            access.proveClaim(session, true);
            var value = new Document("intentRevision", recovery.intentRevision())
                    .append("failedExecution", recovery.failedExecution()).append("attempts", recovery.attempts())
                    .append("retryAfter", recovery.retryAfter().toEpochMilli());
            access.updateState(session, current, new Document("leaseRecovery", value), false);
            return new CasOutcome.Applied(new CheckpointDoc(pipelineId, checkpoint.stateJson(), checkpoint.epoch(),
                    checkpoint.touchTime(), recovery));
        });
    }

    @Override public void delete(String pipelineId) {
        access.requirePipeline(pipelineId);
        throw MongoCachePipelineAccess.fenced();
    }
}
