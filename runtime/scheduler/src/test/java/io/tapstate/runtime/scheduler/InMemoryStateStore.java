package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.function.Function;

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
    private final Map<String, StopReservation> stops = new HashMap<>();
    private DesiredStore stopIntents;
    private Function<String, StopAuthority> authorities;
    private Runnable beforeComplete = () -> { };
    private Runnable afterStopSupersession = () -> { };
    private int reservations;

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

    /** Applies the fence without running the {@code beforeSwap} hook — the seam a competitor writes through. */
    CasOutcome applySwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
        CheckpointDoc current = docs.get(pipelineId);
        if (current == null) {
            throw new IllegalStateException("compareAndSwap on an unseeded pipeline " + pipelineId);
        }
        if (stops.containsKey(pipelineId)) { return new CasOutcome.Fenced(current.epoch()); }
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

    void enableStops(DesiredStore intents, Function<String, StopAuthority> currentAuthority) {
        stopIntents = Objects.requireNonNull(intents);
        authorities = Objects.requireNonNull(currentAuthority);
    }

    @Override public boolean supportsStopReservations() { return stopIntents != null; }

    @Override public synchronized Optional<StopReservation> readStopReservation(String pipelineId) {
        return Optional.ofNullable(stops.get(pipelineId));
    }

    @Override public synchronized Optional<StopReservation> reserveStop(
            CheckpointDoc expected, StopReservation proposal, Instant at) {
        if (!expected.equals(docs.get(expected.pipelineId())) || stops.containsKey(expected.pipelineId())
                || expected.epoch() != proposal.sourceEpoch() || proposal.reservedEpoch() != expected.epoch() + 1
                || !intent(proposal.originalDesired()) || !authority(proposal.pipelineId(), proposal.authorityOrNull())) {
            return Optional.empty();
        }
        CasOutcome.Applied admitted = (CasOutcome.Applied) EpochCas.swap(expected, expected.epoch(),
                expected.stateJson(), at);
        docs.put(expected.pipelineId(), admitted.next());
        stops.put(expected.pipelineId(), proposal);
        reservations++;
        return Optional.of(proposal);
    }

    @Override public synchronized Optional<StopReservation> replaceStop(
            StopReservation expected, StopReservation proposal, Instant at) {
        if (!exact(expected) || !expected.pipelineId().equals(proposal.pipelineId())
                || expected.token().equals(proposal.token())
                || expected.originalDesired().equals(proposal.originalDesired())
                || proposal.sourceEpoch() != expected.reservedEpoch()
                || proposal.reservedEpoch() != Math.incrementExact(expected.reservedEpoch())
                || !intent(proposal.originalDesired()) || !authority(proposal.pipelineId(), proposal.authorityOrNull())) {
            return Optional.empty();
        }
        advance(expected, docs.get(expected.pipelineId()).stateJson(), at);
        stops.put(expected.pipelineId(), proposal);
        reservations++;
        afterStopSupersession.run();
        return Optional.of(proposal);
    }

    @Override public synchronized Optional<StopReservation> rebindStop(
            StopReservation expected, StopAuthority successor, Instant at) {
        if (!exact(expected) || !intent(expected.originalDesired()) || !authority(expected.pipelineId(), successor)) {
            return Optional.empty();
        }
        StopReservation rebound = expected.rebind(successor, Math.incrementExact(expected.reservedEpoch()));
        advance(expected, docs.get(expected.pipelineId()).stateJson(), at);
        stops.put(expected.pipelineId(), rebound);
        return Optional.of(rebound);
    }

    @Override public synchronized Optional<CheckpointDoc> completeStop(StopReservation expected, Instant at) {
        beforeComplete.run();
        if (!exact(expected) || !intent(expected.originalDesired())
                || !authority(expected.pipelineId(), expected.authorityOrNull())) {
            return Optional.empty();
        }
        CheckpointDoc completed = advance(expected, StateJson.of(PipelineState.STOPPED), at);
        stops.remove(expected.pipelineId());
        return Optional.of(completed);
    }

    @Override public synchronized Optional<CheckpointDoc> retireStop(StopReservation expected,
            DesiredState successor, StopAuthority authority, Instant at) {
        if (!exact(expected) || expected.originalDesired().equals(successor) || !intent(successor)
                || !authority(expected.pipelineId(), authority)) {
            return Optional.empty();
        }
        CheckpointDoc retired = advance(expected, docs.get(expected.pipelineId()).stateJson(), at);
        stops.remove(expected.pipelineId());
        afterStopSupersession.run();
        return Optional.of(retired);
    }

    private boolean exact(StopReservation marker) {
        CheckpointDoc doc = docs.get(marker.pipelineId());
        return marker.equals(stops.get(marker.pipelineId())) && doc != null && doc.epoch() == marker.reservedEpoch();
    }

    private boolean intent(DesiredState intent) {
        return stopIntents.read(intent.pipelineId()).filter(intent::equals).isPresent();
    }

    private boolean authority(String pipeline, StopAuthority expected) {
        return Objects.equals(expected, authorities.apply(pipeline));
    }

    private CheckpointDoc advance(StopReservation expected, String stateJson, Instant at) {
        CheckpointDoc current = docs.get(expected.pipelineId());
        CasOutcome.Applied applied = (CasOutcome.Applied) EpochCas.swap(current, expected.reservedEpoch(), stateJson, at);
        docs.put(expected.pipelineId(), applied.next());
        return applied.next();
    }

    void onBeforeComplete(Runnable action) { beforeComplete = action; }
    /** Models process loss immediately after the superseding marker write has committed. */
    void onAfterStopSupersession(Runnable action) { afterStopSupersession = action; }
    int stopReservations() { return reservations; }
}
