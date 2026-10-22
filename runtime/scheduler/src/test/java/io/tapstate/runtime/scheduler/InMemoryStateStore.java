package io.tapstate.runtime.scheduler;

import io.tapstate.spi.store.WorkloadClaimFence;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.spi.store.StateStore;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A faithful in-memory {@link StateStore} double for the converge-loop tests: it applies the pure
 * {@link EpochCas} exactly as the Mongo adapter applies it atomically, so the converger's fencing and
 * rebase behaviour is exercised against real fencing semantics rather than a mock. A {@code beforeSwap}
 * hook lets a test slip a competing writer in just before a compare-and-swap, staging the artificial
 * failover that a single node never produces on its own.
 */
final class InMemoryStateStore implements StateStore {
    @Override
    public void delete(String pipelineId) {
        throw new UnsupportedOperationException("removal is not exercised by this double");
    }


    private final Map<String, CheckpointDoc> docs = new HashMap<>();
    private Runnable beforeSwap = () -> {};
    private int swapAttempts = 0;
    WorkloadClaimFence liveFence;

    @Override
    public Optional<CheckpointDoc> read(String pipelineId) {
        return Optional.ofNullable(docs.get(pipelineId));
    }

    @Override
    public void create(String pipelineId, String stateJson, Instant touchTime) {
        if (docs.containsKey(pipelineId)) {
            throw new IllegalStateException("create on an already-seeded pipeline " + pipelineId);
        }
        docs.put(pipelineId, CheckpointDoc.initial(pipelineId, stateJson, touchTime));
    }

    @Override
    public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
        swapAttempts++;
        beforeSwap.run();
        return applySwap(pipelineId, expectedEpoch, nextStateJson, touchTime);
    }

    @Override
    public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson,
            Instant touchTime, WorkloadClaimFence fence) {
        swapAttempts++;
        beforeSwap.run();
        return fence.equals(liveFence) ? applySwap(pipelineId, expectedEpoch, nextStateJson, touchTime)
                : new CasOutcome.Fenced(docs.get(pipelineId).epoch());
    }

    /** Applies the fence without running the {@code beforeSwap} hook — the seam a competitor writes through. */
    CasOutcome applySwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
        CheckpointDoc current = docs.get(pipelineId);
        if (current == null) {
            throw new IllegalStateException("compareAndSwap on an unseeded pipeline " + pipelineId);
        }
        CasOutcome outcome = EpochCas.swap(current, expectedEpoch, nextStateJson, touchTime);
        if (outcome instanceof CasOutcome.Applied applied) {
            docs.put(pipelineId, applied.next());
        }
        return outcome;
    }

    void onBeforeSwap(Runnable hook) {
        this.beforeSwap = hook;
    }

    /** How many times the converger has called {@link #compareAndSwap} — the retry count under test. */
    int swapAttempts() {
        return swapAttempts;
    }
}
