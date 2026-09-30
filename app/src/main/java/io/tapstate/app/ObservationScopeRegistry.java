package io.tapstate.app;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;

import java.time.Instant;
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
        private MetricContinuation restored;
        private long revision;
        private RestoreTicket restore;
        private MetricProducerEpochs epochs = new MetricProducerEpochs();
        private CardinalityBudget.Folder folder = CardinalityBudget.folder();
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    record RecoverySignal(ConvergeResult result, ObservationFailure failure) { }
    record FailureAttempt(ObservationFailure failure, boolean first) { }
    record RestoreTelemetryFailure(ObservationStore.Scope scope, Instant occurredAt, long lastFailureNanos) { }

    /** The entry object and revision fence begin/delete/recreate, including an absent current scope. */
    static final class RestoreTicket {
        private final String pipelineId;
        private final Entry entry;
        private final long revision;
        private final ObservationScopeRecovery.Owner owner;
        private RecoverySignal firstFailure;
        private RecoverySignal transition;
        private boolean failurePrepared;
        private ObservationPublisher.Prepared prepared;
        private ObservationStore.Scope preparedScope;
        private CheckpointDoc preparedFor;
        private MetricContinuation baseline;
        private RestoreTelemetryFailure telemetryFailure;

        private RestoreTicket(String pipelineId, Entry entry, ObservationScopeRecovery.Owner owner) {
            this.pipelineId = pipelineId;
            this.entry = entry;
            this.revision = entry.revision;
            this.owner = owner;
        }

        ObservationScopeRecovery.Owner owner() { return owner; }
    }

    Optional<RestoreTicket> restoration(String pipelineId, ConvergeResult result, ObservationFailure failure) {
        return restoration(pipelineId, result, failure, null);
    }

    Optional<RestoreTicket> restoration(String pipelineId, ConvergeResult result, ObservationFailure failure,
            ObservationScopeRecovery.Owner owner) {
        Entry entry = entries.computeIfAbsent(Objects.requireNonNull(pipelineId, "pipelineId"), id -> new Entry());
        synchronized (entry) {
            if (entry.current != null) {
                return Optional.empty();
            }
            if (entry.restore != null && !Objects.equals(owner, entry.restore.owner)) {
                entry.revision++;
                entry.restore = null;
            }
            if (entry.restore == null) {
                entry.restore = new RestoreTicket(pipelineId, entry, owner);
            }
            RestoreTicket ticket = entry.restore;
            if (result != null && result.checkpoint().isPresent()) {
                RecoverySignal signal = new RecoverySignal(result, failure);
                if (failure != null && ticket.firstFailure == null) {
                    ticket.firstFailure = signal;
                }
                if (result.transitionFrom().isPresent()) {
                    ticket.transition = signal;
                }
            }
            return Optional.of(ticket);
        }
    }

    boolean awaiting(RestoreTicket ticket) {
        synchronized (ticket.entry) {
            return valid(ticket) && ticket.entry.current == null;
        }
    }

    void rememberRestorationFailure(RestoreTicket ticket, ObservationStore.Scope scope, Instant occurredAt,
            long failedNanos) {
        synchronized (ticket.entry) {
            if (!valid(ticket) || scope == null) {
                return;
            }
            var before = ticket.telemetryFailure;
            if (before == null) {
                ticket.telemetryFailure = new RestoreTelemetryFailure(scope, occurredAt, failedNanos);
            } else if (before.scope().equals(scope) && failedNanos - before.lastFailureNanos() > 0) {
                ticket.telemetryFailure = new RestoreTelemetryFailure(scope, before.occurredAt(), failedNanos);
            }
        }
    }

    Optional<RestoreTelemetryFailure> restorationTelemetryFailure(RestoreTicket ticket,
            ObservationStore.Scope scope) {
        synchronized (ticket.entry) {
            return valid(ticket) && ticket.telemetryFailure != null && ticket.telemetryFailure.scope().equals(scope)
                    ? Optional.of(ticket.telemetryFailure) : Optional.empty();
        }
    }

    private boolean valid(RestoreTicket ticket) {
        return entries.get(ticket.pipelineId) == ticket.entry && ticket.entry.revision == ticket.revision
                && ticket.entry.restore == ticket;
    }

    /** Supplies the real failure only while its exact checkpoint is still current. */
    FailureAttempt restorationFailure(RestoreTicket ticket, CheckpointDoc checkpoint) {
        synchronized (ticket.entry) {
            if (!valid(ticket) || ticket.firstFailure == null
                    || ticket.firstFailure.result().checkpoint().filter(checkpoint::equals).isEmpty()) {
                return null;
            }
            boolean first = !ticket.failurePrepared;
            ticket.failurePrepared = true;
            return new FailureAttempt(ticket.firstFailure.failure(), first);
        }
    }

    Optional<ObservationPublisher.Prepared> restorationPrepared(RestoreTicket ticket,
            ObservationScopeRecovery.Qualified qualified) {
        synchronized (ticket.entry) {
            return valid(ticket) && qualified.scope().equals(ticket.preparedScope)
                    && qualified.checkpoint().equals(ticket.preparedFor)
                    ? Optional.ofNullable(ticket.prepared) : Optional.empty();
        }
    }

    boolean rememberRestoration(RestoreTicket ticket, ObservationScopeRecovery.Qualified qualified,
            ObservationPublisher.Prepared prepared) {
        synchronized (ticket.entry) {
            if (!valid(ticket) || ticket.entry.current != null) {
                return false;
            }
            ticket.prepared = prepared;
            ticket.preparedFor = qualified.checkpoint();
            if (!qualified.scope().equals(ticket.preparedScope) || ticket.baseline == null) {
                ticket.baseline = MetricContinuation.capture(qualified.stored().observation())
                        .boundedBy(ticket.entry.folder);
            }
            ticket.preparedScope = qualified.scope();
            return true;
        }
    }

    void retryRestoration(RestoreTicket ticket) {
        synchronized (ticket.entry) {
            if (valid(ticket)) {
                ticket.prepared = null;
            }
        }
    }

    /** A cold STOPPED frame can retain final totals without treating them as a live native reading. */
    ObservationPublisher.Prepared restorationFrame(RestoreTicket ticket, ObservationStore.Stored stored,
            ObservationPublisher.Prepared prepared) {
        synchronized (ticket.entry) {
            if (!valid(ticket) || prepared.observation().state() != PipelineState.STOPPED) {
                return prepared;
            }
            var observation = prepared.observation();
            MetricContinuation known = ticket.baseline == null ? MetricContinuation.capture(stored.observation())
                    .boundedBy(ticket.entry.folder) : ticket.baseline;
            return prepared.withFacts(known.atLeast(observation.facts(), observation.observedAt()));
        }
    }

    List<RecoverySignal> restorationSignals(RestoreTicket ticket) {
        synchronized (ticket.entry) {
            if (!valid(ticket)) {
                return List.of();
            }
            if (ticket.firstFailure == null) {
                return ticket.transition == null ? List.of() : List.of(ticket.transition);
            }
            return ticket.transition == null || ticket.transition.result().checkpoint()
                    .equals(ticket.firstFailure.result().checkpoint())
                    ? List.of(ticket.firstFailure) : List.of(ticket.firstFailure, ticket.transition);
        }
    }

    boolean restore(RestoreTicket ticket, ObservationStore.Stored stored) {
        synchronized (ticket.entry) {
            if (!valid(ticket) || ticket.entry.current != null || stored.scope().isEmpty()
                    || !ticket.pipelineId.equals(stored.observation().pipelineId())) {
                return false;
            }
            Entry entry = ticket.entry;
            entry.current = stored.scope().orElseThrow();
            entry.restored = ticket.baseline == null
                    ? MetricContinuation.capture(stored.observation()).boundedBy(entry.folder) : ticket.baseline;
            entry.last = null;
            entry.epochs = new MetricProducerEpochs();
            return true;
        }
    }

    void restored(RestoreTicket ticket) {
        synchronized (ticket.entry) {
            if (valid(ticket)) {
                ticket.entry.restore = null;
            }
        }
    }

    void cancelRestoration(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        if (entry != null) {
            synchronized (entry) {
                if (entry.restore != null) {
                    entry.revision++;
                    entry.restore = null;
                }
            }
        }
    }

    void cancelRestorations() {
        entries.keySet().forEach(this::cancelRestoration);
    }

    ObservationStore.Scope begin(String pipelineId, String incarnation, long generation) {
        ObservationStore.Scope scope = new ObservationStore.Scope(incarnation, generation);
        Entry entry = entries.computeIfAbsent(Objects.requireNonNull(pipelineId, "pipelineId"), id -> new Entry());
        synchronized (entry) {
            if (scope.equals(entry.current)) {
                return scope;
            }
            entry.revision++;
            entry.restore = null;
            entry.restored = null;
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
            entry.epochs = new MetricProducerEpochs();
            entry.current = scope;
        }
        return scope;
    }

    Optional<ObservationStore.Scope> current(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.current);
    }

    /** Invalidates a deleted resource without clearing a newer incarnation already under the same id. */
    void forgetIncarnation(String pipelineId, String incarnationId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            ObservationStore.Scope current = entry.current;
            if (current != null && !Objects.equals(current.pipelineIncarnationId(), incarnationId)) {
                return;
            }
            entry.revision++;
            entry.restore = null;
            entry.restored = null;
            // Keep the entry object: a concurrent begin may already hold it after computeIfAbsent.
            entry.current = null;
            entry.last = null;
            entry.pendingFrom = null;
            entry.pending = null;
            entry.active = null;
            entry.epochs = new MetricProducerEpochs();
            entry.folder = CardinalityBudget.folder();
        }
    }

    /** Whether this execution is carrying a bounded cumulative baseline from its predecessor. */
    boolean continuing(String pipelineId, ObservationStore.Scope scope) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return false;
        }
        synchronized (entry) {
            return scope.equals(entry.current) && entry.active != null;
        }
    }

    boolean needsStoredFallback(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return false;
        }
        synchronized (entry) {
            return entry.current != null && MetricContinuation.capture(entry.last).isEmpty()
                    && (entry.restored == null || entry.restored.isEmpty());
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
            List<MetricFact> known = source == null ? List.of() : source.facts();
            java.time.Instant at = source == null ? null : source.observedAt();
            if (entry.active != null) {
                known = entry.active.atLeast(known, at);
            }
            if (entry.restored != null) {
                known = entry.restored.atLeast(known, at);
            }
            known = MetricContinuation.captureFacts(entry.epochs.knownFacts(at)).atLeast(known, at);
            entry.pending = MetricContinuation.captureFacts(known).boundedBy(entry.folder);
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
            if (!newer(observation, entry.last)) {
                return prepared;
            }
            ObservationPublisher.Prepared continued = prepared;
            List<MetricFact> nativeFacts = observation.facts().stream().map(entry.folder::fold).toList();
            if (entry.active != null && observation.observedAt() != null) {
                List<MetricFact> measured = entry.active.apply(nativeFacts, observation.observedAt())
                        .stream().map(entry.folder::fold).toList();
                if (entry.last != null) {
                    measured = MetricContinuation.capture(entry.last).atLeast(measured, observation.observedAt());
                }
                continued = prepared.withFacts(measured.stream().map(entry.folder::fold).toList());
                if (entry.pending == entry.active) {
                    entry.pending = null;
                    entry.pendingFrom = null;
                }
            }
            if (observation.observedAt() != null) {
                List<MetricFact> projected = continued.observation().facts().stream().map(entry.folder::fold).toList();
                List<MetricFact> facts = entry.epochs.continueNative(observation.facts(), nativeFacts,
                        projected, observation.observedAt());
                if (observation.state() == PipelineState.STOPPED) {
                    facts = MetricContinuation.captureFacts(entry.epochs.knownFacts(observation.observedAt()))
                            .atLeast(facts, observation.observedAt());
                    if (entry.restored != null) {
                        facts = entry.restored.atLeast(facts, observation.observedAt());
                    }
                }
                // Frames written before facts were carried retain their existing flat metrics.
                continued = observation.facts().isEmpty() && facts.isEmpty()
                        ? prepared : continued.withFacts(facts);
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
            if (entry.current == null) {
                entry.revision++;
                entry.restore = null;
            }
            if (scope.equals(entry.current)) {
                entry.revision++;
                entry.restore = null;
                entry.restored = null;
                entry.current = null;
                entry.last = null;
                entry.active = null;
                entry.epochs = new MetricProducerEpochs();
            }
        }
    }

    void retain(Collection<String> pipelineIds) {
        entries.keySet().retainAll(pipelineIds);
    }
}
