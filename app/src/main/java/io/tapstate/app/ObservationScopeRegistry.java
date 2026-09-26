package io.tapstate.app;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** The execution identity and bounded cumulative continuation retained for local telemetry publishes. */
final class ObservationScopeRegistry {

    private static final class Entry {
        private volatile ObservationStore.Scope current;
        private Observation last;
        private ObservationStore.Scope pendingFrom;
        private MetricContinuation pending;
        private MetricContinuation active;
        private CardinalityBudget.Folder folder = CardinalityBudget.folder();
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    ObservationStore.Scope begin(String pipelineId, String incarnation, long generation) {
        ObservationStore.Scope scope = new ObservationStore.Scope(incarnation, generation);
        Entry entry = entries.computeIfAbsent(Objects.requireNonNull(pipelineId, "pipelineId"), id -> new Entry());
        synchronized (entry) {
            if (scope.equals(entry.current)) {
                return scope;
            }
            if (entry.pending != null && entry.pendingFrom != null
                    && entry.pendingFrom.pipelineIncarnationId().equals(incarnation)
                    && generation > entry.pendingFrom.executionGeneration()) {
                entry.active = entry.pending;
            } else {
                entry.active = null;
                entry.pending = null;
                entry.pendingFrom = null;
                entry.folder = CardinalityBudget.folder();
            }
            entry.last = null;
            entry.current = scope;
        }
        return scope;
    }

    Optional<ObservationStore.Scope> current(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.current);
    }

    boolean needsStoredFallback(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return false;
        }
        synchronized (entry) {
            return entry.current != null && MetricContinuation.capture(entry.last).isEmpty();
        }
    }

    /** Freezes only known cumulative facts of the current execution before its producer is released. */
    void prepareRebuildingResume(String pipelineId, Optional<ObservationStore.Stored> stored) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            ObservationStore.Scope owner = entry.current;
            if (owner == null) {
                return;
            }
            Observation source = entry.last;
            if (MetricContinuation.capture(source).isEmpty()) {
                source = stored.filter(saved -> saved.scope().filter(owner::equals).isPresent())
                        .map(ObservationStore.Stored::observation).orElse(source);
            }
            entry.pending = MetricContinuation.capture(source).boundedBy(entry.folder);
            entry.pendingFrom = owner;
        }
    }

    /** An ordinary stop ends pending carry; the current scope keeps its final known totals. */
    void clearContinuation(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            entry.pending = null;
            entry.pendingFrom = null;
        }
    }

    /** Applies the frozen baseline before latest, history and export diverge. */
    ObservationPublisher.Prepared continueFrame(ObservationPublisher.Prepared prepared,
            ObservationStore.Scope scope) {
        Observation observation = prepared.observation();
        Entry entry = entries.get(observation.pipelineId());
        if (entry == null || scope == null) {
            return prepared;
        }
        synchronized (entry) {
            if (!scope.equals(entry.current)) {
                return prepared;
            }
            ObservationPublisher.Prepared continued = prepared;
            if (entry.active != null && observation.observedAt() != null) {
                List<MetricFact> measured = entry.active.apply(observation.facts(), observation.observedAt());
                if (entry.last != null) {
                    measured = MetricContinuation.capture(entry.last).atLeast(measured, observation.observedAt());
                }
                continued = prepared.withFacts(measured.stream().map(entry.folder::fold).toList());
                if (entry.pending == entry.active) {
                    entry.pending = null;
                    entry.pendingFrom = null;
                }
            }
            if (newer(continued.observation(), entry.last)) {
                entry.last = continued.observation();
            }
            return continued;
        }
    }

    private static boolean newer(Observation candidate, Observation previous) {
        return previous == null || previous.observedAt() == null
                || (candidate.observedAt() != null && candidate.observedAt().isAfter(previous.observedAt()));
    }

    void discard(String pipelineId, ObservationStore.Scope scope) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            if (scope.equals(entry.current)) {
                entry.current = null;
                entry.last = null;
                entry.active = null;
            }
        }
    }

    void retain(Collection<String> pipelineIds) {
        entries.keySet().retainAll(pipelineIds);
    }
}
