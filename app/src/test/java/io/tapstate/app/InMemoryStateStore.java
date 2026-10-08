package io.tapstate.app;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.spi.store.StateStore;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * In-memory {@link StateStore} double for the convergence wiring tests, applying the real
 * {@link EpochCas} fence. A pipeline can be marked failing so a convergence pass over it throws,
 * to witness the driver isolating a per-pipeline failure.
 */
final class InMemoryStateStore implements StateStore {

    private final Map<String, CheckpointDoc> docs = new HashMap<>();
    private final Set<String> failing = new HashSet<>();

    synchronized void failFor(String pipelineId) {
        failing.add(pipelineId);
    }

    synchronized void recover(String pipelineId) {
        failing.remove(pipelineId);
    }

    @Override
    public synchronized Optional<CheckpointDoc> read(String pipelineId) {
        if (failing.contains(pipelineId)) {
            throw new IllegalStateException("state read failed for " + pipelineId);
        }
        return Optional.ofNullable(docs.get(pipelineId));
    }

    @Override
    public synchronized void create(String pipelineId, String stateJson, Instant touchTime) {
        docs.put(pipelineId, CheckpointDoc.initial(pipelineId, stateJson, touchTime));
    }

    @Override
    public synchronized void delete(String pipelineId) {
        docs.remove(pipelineId);
    }

    @Override
    public synchronized CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
        CasOutcome outcome = EpochCas.swap(docs.get(pipelineId), expectedEpoch, nextStateJson, touchTime);
        if (outcome instanceof CasOutcome.Applied applied) {
            docs.put(pipelineId, applied.next());
        }
        return outcome;
    }
}
