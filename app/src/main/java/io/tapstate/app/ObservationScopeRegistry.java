package io.tapstate.app;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.HandoffIdentity;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

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
        private long bindingRevision;
        private FailedAdmission failedAdmission;
        private RestoreTicket restore;
        private ColdRebuildTicket coldRebuild;
        private ContinuationTicket continuationTicket;
        private SourceSnapshot sourceSnapshot;
        private ContinuationKey sourceKey;
        private ContinuationKey continuationKey;
        private ActualTarget actualTarget;
        private List<MetricFact> continuationBase = List.of();
        private Optional<ActualTarget> baselineOrigin = Optional.empty();
        private boolean continuationUnknown;
        private boolean unknownProven;
        private ObservationStore.ContinuationReceipt privateReceipt;
        private ObservationStore.ContinuationReceipt durableReceipt;
        private MetricProducerEpochs epochs = new MetricProducerEpochs();
        private CardinalityBudget.Folder folder = CardinalityBudget.folder();
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    /** The original admission that actually refused to build, never a native submission identity. */
    record FailedAdmission(ObservationStore.Scope scope, ObservationScopeRecovery.Owner owner) {
        FailedAdmission { Objects.requireNonNull(scope, "scope"); }
    }

    record RecoverySignal(ConvergeResult result, ObservationFailure failure) { }
    record FailureAttempt(ObservationFailure failure, boolean first) { }
    record RestoreTelemetryFailure(ObservationStore.Scope scope, Instant occurredAt, long lastFailureNanos) { }

    enum PreparationStatus { KNOWN, UNKNOWN, INVALIDATED }
    enum SourceReadStatus { READABLE, UNAVAILABLE }

    record ContinuationKey(String token, ObservationStore.Scope sourceScope,
            StopReservation.CounterPolicy policy, StopAuthority completingAuthority) {
        ContinuationKey {
            Objects.requireNonNull(token, "token"); Objects.requireNonNull(policy, "policy");
            if (token.isBlank() || sourceScope != null && completingAuthority == null) {
                throw new IllegalArgumentException("a continuation key needs its token and source authority");
            }
        }
    }

    record ActualTarget(ObservationStore.Scope scope, StopReservation.JobIdentity job) {
        ActualTarget { Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(job, "job"); }
        private ObservationContinuation.Target storedTarget() {
            return new ObservationContinuation.Target(scope, Optional.of(job));
        }
    }

    /** A qualified source snapshot carries data, not permission to install a native execution. */
    static final class SourceSnapshot {
        private final String pipelineId;
        private final ContinuationKey key;
        private final Optional<ActualTarget> origin;
        private final List<MetricFact> facts;
        private final boolean unknownProven;

        private SourceSnapshot(String pipelineId, ContinuationKey key, Optional<ActualTarget> origin,
                List<MetricFact> facts, boolean unknownProven) {
            this.pipelineId = pipelineId; this.key = key; this.origin = origin;
            this.facts = List.copyOf(facts); this.unknownProven = unknownProven;
        }

        String token() { return key.token(); }
        ObservationStore.Scope sourceScope() { return key.sourceScope(); }
        List<MetricFact> baselineFacts() { return facts; }
        Optional<ActualTarget> baselineOrigin() { return origin; }
        boolean knownBaseline() { return !facts.isEmpty(); }
        boolean unknownProven() { return unknownProven; }
        ObservationContinuation continuation() {
            return new ObservationContinuation(token(), sourceScope(), Optional.empty(),
                    origin.map(ActualTarget::storedTarget), facts, List.of());
        }
        ObservationContinuation toContinuation() { return continuation(); }
    }

    record SourcePreparation(PreparationStatus status, Optional<SourceSnapshot> snapshot) { }
    record TargetPreparation(PreparationStatus status) { }

    abstract static class ContinuationTicket {
        final String pipelineId;
        final Entry entry;
        final long revision;
        final ObservationStore.Scope currentAtStart;
        final ContinuationKey key;
        final BooleanSupplier current;
        private ContinuationTicket previous;

        private ContinuationTicket(String id, Entry entry, ContinuationKey key, BooleanSupplier current) {
            this.pipelineId = id; this.entry = entry; this.revision = entry.revision;
            this.currentAtStart = entry.current; this.key = key; this.current = current;
            this.previous = entry.continuationTicket;
        }
    }

    static final class SourceTicket extends ContinuationTicket {
        private final Optional<ActualTarget> previousSource;
        private SourceSnapshot staged;
        private SourceTicket(String id, Entry entry, ContinuationKey key,
                Optional<ActualTarget> previousSource, BooleanSupplier current) {
            super(id, entry, key, current); this.previousSource = previousSource;
        }
    }

    static final class TargetTicket extends ContinuationTicket {
        private final ActualTarget target;
        private long activatedRevision = -1;
        private TargetTicket(String id, Entry entry, ContinuationKey key, ActualTarget target,
                BooleanSupplier current) { super(id, entry, key, current); this.target = target; }
    }

    static final class PublicationTicket {
        private final String pipelineId;
        private final Entry entry;
        private final long revision;
        private final ContinuationKey key;
        private final ActualTarget target;
        private final boolean unknownProven;
        private final BooleanSupplier current;
        private PublicationTicket(String id, Entry entry, BooleanSupplier current) {
            this.pipelineId = id; this.entry = entry; this.revision = entry.revision;
            this.key = entry.continuationKey; this.target = entry.actualTarget;
            this.unknownProven = entry.unknownProven; this.current = current;
        }
    }

    record ContinuationPublication(ObservationPublisher.Prepared projected, ObservationStore.Scope scope,
            Optional<ObservationContinuation> snapshot, PublicationTicket ticket,
            Optional<ObservationStore.ContinuationReceipt> expectedReceipt, boolean unknownProven) { }

    Optional<SourceTicket> beginSourceContinuation(String id, ContinuationKey key, BooleanSupplier current) {
        return beginSourceContinuation(id, key, Optional.empty(), current);
    }

    Optional<SourceTicket> beginSourceContinuation(String id, ContinuationKey key,
            Optional<ActualTarget> previousSource, BooleanSupplier current) {
        Objects.requireNonNull(id, "pipelineId"); Objects.requireNonNull(key, "key");
        Objects.requireNonNull(previousSource, "previousSource"); Objects.requireNonNull(current, "current");
        Entry entry = entries.computeIfAbsent(id, ignored -> new Entry());
        SourceTicket ticket;
        synchronized (entry) { ticket = new SourceTicket(id, entry, key, previousSource, current); }
        if (!current.getAsBoolean()) { return Optional.empty(); }
        synchronized (entry) {
            if (!unchanged(ticket)) { return Optional.empty(); }
            entry.continuationTicket = ticket;
            return Optional.of(ticket);
        }
    }

    SourcePreparation prepareSourceContinuation(SourceTicket ticket, Optional<ObservationStore.Stored> stored,
            Optional<ObservationContinuation> continuation) {
        return prepareSourceContinuation(ticket, stored, continuation, SourceReadStatus.UNAVAILABLE);
    }

    SourcePreparation prepareSourceContinuation(SourceTicket ticket, Optional<ObservationStore.Stored> stored,
            Optional<ObservationContinuation> continuation, SourceReadStatus readStatus) {
        Objects.requireNonNull(ticket, "ticket"); Objects.requireNonNull(stored, "stored");
        Objects.requireNonNull(continuation, "continuation"); Objects.requireNonNull(readStatus, "readStatus");
        if (!ticket.current.getAsBoolean()) { cancelContinuation(ticket); return invalidSource(); }
        SourceSnapshot candidate;
        synchronized (ticket.entry) {
            if (!valid(ticket)) { return invalidSource(); }
            candidate = freezeSource(ticket, stored, continuation, readStatus);
        }
        if (!ticket.current.getAsBoolean()) { cancelContinuation(ticket); return invalidSource(); }
        synchronized (ticket.entry) {
            if (!valid(ticket)) { return invalidSource(); }
            ticket.entry.sourceSnapshot = candidate; ticket.entry.sourceKey = ticket.key;
            ticket.staged = candidate;
            return new SourcePreparation(candidate.knownBaseline() ? PreparationStatus.KNOWN
                    : PreparationStatus.UNKNOWN, Optional.of(candidate));
        }
    }

    private SourceSnapshot freezeSource(SourceTicket ticket, Optional<ObservationStore.Stored> stored,
            Optional<ObservationContinuation> continuation, SourceReadStatus readStatus) {
        Entry entry = ticket.entry;
        SourceSnapshot before = entry.sourceSnapshot;
        List<MetricFact> known = before != null && sourceMatches(before, ticket.pipelineId, ticket.key)
                && before.origin.equals(ticket.previousSource) ? before.facts : List.of();
        if (ticket.key.policy() != StopReservation.CounterPolicy.CONTINUE || ticket.key.sourceScope() == null) {
            return new SourceSnapshot(ticket.pipelineId, ticket.key, ticket.previousSource, List.of(),
                    readStatus == SourceReadStatus.READABLE);
        }
        ObservationStore.Scope measuredScope = ticket.previousSource.map(ActualTarget::scope)
                .orElse(ticket.key.sourceScope());
        if (measuredScope.equals(entry.current) && (ticket.previousSource.isEmpty()
                || ticket.previousSource.filter(source -> source.equals(entry.actualTarget)).isPresent())) {
            List<MetricFact> warm = entry.last == null ? List.of() : entry.last.facts();
            Instant at = latestPointTime(warm);
            if (entry.active != null) { warm = entry.active.atLeast(warm, at); }
            if (entry.restored != null) { warm = entry.restored.atLeast(warm, at); }
            warm = MetricContinuation.captureFacts(entry.epochs.knownFacts(at)).atLeast(warm, at);
            known = mergeKnown(known, warm);
        }
        Optional<ObservationStore.Stored> source = stored.filter(saved ->
                ticket.pipelineId.equals(saved.observation().pipelineId())
                        && saved.scope().filter(measuredScope::equals).isPresent());
        if (source.isPresent()) { known = mergeKnown(known, source.orElseThrow().observation().facts()); }
        if (continuation.isPresent()) {
            ObservationContinuation saved = continuation.orElseThrow();
            if (ticket.previousSource.isPresent() && saved.target().filter(target ->
                    target.scope().equals(measuredScope) && target.realJob()
                            .filter(ticket.previousSource.orElseThrow().job()::equals).isPresent()).isPresent()
                    && (saved.token().equals(ticket.key.token())
                            && Objects.equals(saved.sourceScope(), ticket.key.sourceScope())
                            || measuredScope.equals(ticket.key.sourceScope()))) {
                known = mergeKnown(known, saved.baselineFacts());
                known = mergeKnown(known, publishedFacts(saved.producerStates()));
            } else if (ticket.previousSource.isEmpty() && saved.token().equals(ticket.key.token())
                    && Objects.equals(saved.sourceScope(), ticket.key.sourceScope())
                    && saved.baselineOrigin().isEmpty()) {
                known = mergeKnown(known, saved.baselineFacts());
            } else if (saved.token().equals(ticket.key.token())
                    && Objects.equals(saved.sourceScope(), ticket.key.sourceScope())
                    && saved.baselineOrigin().equals(ticket.previousSource.map(ActualTarget::storedTarget))
                    && saved.target().flatMap(ObservationContinuation.Target::realJob).isEmpty()) {
                known = mergeKnown(known, saved.baselineFacts());
            }
        }
        List<MetricFact> bounded = boundedFacts(known, CardinalityBudget.folder());
        boolean proven = bounded.isEmpty() && (readStatus == SourceReadStatus.READABLE
                || before != null && sourceMatches(before, ticket.pipelineId, ticket.key)
                        && before.origin.equals(ticket.previousSource) && before.unknownProven);
        return new SourceSnapshot(ticket.pipelineId, ticket.key, ticket.previousSource, bounded, proven);
    }

    Optional<TargetTicket> beginTargetContinuation(String id, ContinuationKey key, ActualTarget target,
            BooleanSupplier current) {
        Objects.requireNonNull(id, "pipelineId"); Objects.requireNonNull(key, "key");
        Objects.requireNonNull(target, "actualTarget"); Objects.requireNonNull(current, "current");
        if (key.completingAuthority() == null
                || key.completingAuthority().executionGeneration() != target.scope().executionGeneration()
                || !key.completingAuthority().clusterId().equals(target.job().clusterId())
                || key.sourceScope() != null && !key.sourceScope().pipelineIncarnationId()
                        .equals(target.scope().pipelineIncarnationId())) { return Optional.empty(); }
        Entry entry = entries.computeIfAbsent(id, ignored -> new Entry());
        TargetTicket ticket;
        synchronized (entry) {
            if (entry.current != null && (!entry.current.pipelineIncarnationId()
                    .equals(target.scope().pipelineIncarnationId())
                    || entry.current.executionGeneration() > target.scope().executionGeneration())
                    || target.scope().equals(entry.current) && entry.actualTarget != null
                            && !target.equals(entry.actualTarget)) { return Optional.empty(); }
            ticket = new TargetTicket(id, entry, key, target, current);
        }
        if (!current.getAsBoolean()) { return Optional.empty(); }
        synchronized (entry) {
            if (!unchanged(ticket)) { return Optional.empty(); }
            entry.continuationTicket = ticket;
            return Optional.of(ticket);
        }
    }

    TargetPreparation adoptTargetContinuation(TargetTicket ticket,
            Optional<ObservationContinuation> continuation, Optional<SourceSnapshot> source) {
        Objects.requireNonNull(ticket, "ticket"); Objects.requireNonNull(continuation, "continuation");
        Objects.requireNonNull(source, "source");
        if (!ticket.current.getAsBoolean()) { cancelContinuation(ticket); return invalidTarget(); }
        ObservationContinuation chosen;
        boolean retainKnown;
        boolean proven;
        synchronized (ticket.entry) {
            if (!valid(ticket)) { return invalidTarget(); }
            Entry entry = ticket.entry;
            retainKnown = sameActive(entry, ticket.key, ticket.target) && !entry.continuationUnknown;
            chosen = continuation.filter(saved -> continuationMatches(saved, ticket.key, ticket.target)).orElse(null);
            SourceSnapshot frozen = source.filter(snapshot -> sourceMatches(snapshot, ticket.pipelineId, ticket.key))
                    .orElseGet(() -> entry.sourceSnapshot != null
                            && sourceMatches(entry.sourceSnapshot, ticket.pipelineId, ticket.key)
                                    ? entry.sourceSnapshot : null);
            if ((chosen == null || !chosen.knownBaseline()) && frozen != null && frozen.knownBaseline()
                    && frozen.origin.filter(origin -> origin.scope().executionGeneration()
                            >= ticket.target.scope().executionGeneration()).isEmpty()) {
                chosen = new ObservationContinuation(ticket.key.token(), ticket.key.sourceScope(),
                        Optional.of(ticket.target.storedTarget()), frozen.origin.map(ActualTarget::storedTarget),
                        frozen.facts, List.of());
            }
            proven = frozen != null && frozen.unknownProven
                    || sameActive(entry, ticket.key, ticket.target) && entry.unknownProven;
        }
        if (!ticket.current.getAsBoolean()) { cancelContinuation(ticket); return invalidTarget(); }
        synchronized (ticket.entry) {
            if (!valid(ticket)) { return invalidTarget(); }
            Entry entry = ticket.entry;
            if (retainKnown) {
                if (chosen != null && chosen.knownBaseline() && chosen.target()
                        .flatMap(ObservationContinuation.Target::realJob).filter(ticket.target.job()::equals).isPresent()) {
                    entry.epochs = MetricProducerEpochs.restore(chosen.producerStates(), entry.folder,
                            entry.epochs, entry.continuationBase);
                }
                if (!ticket.key.equals(entry.continuationKey)) {
                    entry.revision++; entry.continuationKey = ticket.key;
                }
                ticket.activatedRevision = entry.revision;
                return new TargetPreparation(PreparationStatus.KNOWN);
            }
            boolean same = sameActive(entry, ticket.key, ticket.target);
            if (same && entry.continuationUnknown && (chosen == null || !chosen.knownBaseline())) {
                if (!ticket.key.equals(entry.continuationKey)) {
                    entry.revision++; entry.continuationKey = ticket.key;
                }
                entry.unknownProven |= proven;
                ticket.activatedRevision = entry.revision;
                return new TargetPreparation(PreparationStatus.UNKNOWN);
            }
            entry.revision++; entry.restore = null; entry.coldRebuild = null;
            entry.bindingRevision++;
            entry.current = ticket.target.scope(); entry.actualTarget = ticket.target; entry.failedAdmission = null;
            entry.continuationKey = ticket.key;
            entry.pending = null; entry.pendingFrom = null; entry.restored = null; entry.last = null;
            entry.folder = CardinalityBudget.folder();
            boolean known = ticket.key.policy() == StopReservation.CounterPolicy.CONTINUE
                    && chosen != null && chosen.knownBaseline();
            entry.continuationBase = known ? boundedFacts(chosen.baselineFacts(), entry.folder) : List.of();
            entry.active = known ? MetricContinuation.captureFacts(entry.continuationBase) : null;
            entry.baselineOrigin = known ? chosen.baselineOrigin().map(bound -> new ActualTarget(bound.scope(),
                    bound.realJob().orElseThrow())) : Optional.empty();
            entry.epochs = known && chosen.target().flatMap(ObservationContinuation.Target::realJob)
                    .filter(ticket.target.job()::equals).isPresent()
                            ? MetricProducerEpochs.restore(chosen.producerStates(), entry.folder)
                            : new MetricProducerEpochs();
            entry.continuationUnknown = ticket.key.policy() == StopReservation.CounterPolicy.CONTINUE && !known;
            entry.unknownProven = !known && proven;
            if (entry.durableReceipt != null && !entry.durableReceipt.matches(identity(ticket.pipelineId,
                    ticket.key, ticket.target))) { entry.durableReceipt = null; }
            ticket.activatedRevision = entry.revision;
            return new TargetPreparation(entry.continuationUnknown ? PreparationStatus.UNKNOWN : PreparationStatus.KNOWN);
        }
    }

    Optional<ActualTarget> activeContinuationTarget(String id) {
        Entry entry = entries.get(id);
        if (entry == null) { return Optional.empty(); }
        synchronized (entry) { return entry.continuationKey == null ? Optional.empty() : Optional.ofNullable(entry.actualTarget); }
    }

    Optional<ContinuationKey> activeContinuationKey(String id) {
        Entry entry = entries.get(id);
        if (entry == null) { return Optional.empty(); }
        synchronized (entry) { return Optional.ofNullable(entry.continuationKey); }
    }

    Optional<ObservationStore.ContinuationReceipt> durableReceipt(String id, HandoffIdentity expected) {
        Entry entry = entries.get(id);
        if (entry == null) { return Optional.empty(); }
        synchronized (entry) {
            return entry.continuationKey != null && entry.actualTarget != null
                    && identity(id, entry.continuationKey, entry.actualTarget).equals(expected)
                    ? Optional.ofNullable(entry.durableReceipt).filter(receipt -> receipt.matches(expected)) : Optional.empty();
        }
    }

    /** Records an actual cold read after target qualification, without manufacturing a write receipt. */
    boolean continuationRead(TargetTicket ticket, ObservationStore.StoredContinuation stored) {
        Objects.requireNonNull(ticket, "ticket");
        Objects.requireNonNull(stored, "stored");
        if (!ticket.current.getAsBoolean()) { return false; }
        synchronized (ticket.entry) {
            if (!activated(ticket) || !stored.receipt().matches(identity(ticket.pipelineId, ticket.key, ticket.target))
                    || !continuationMatches(stored.continuation(), ticket.key, ticket.target)) { return false; }
            if (!stored.receipt().knownBaseline()) { ticket.entry.unknownProven = true; }
        }
        return publicationAccepted(ticket, stored.receipt());
    }

    boolean continuationAttached(TargetTicket ticket, ObservationStore.ContinuationReceipt receipt) {
        return publicationAccepted(ticket, receipt);
    }

    /** A pending source receipt is a CAS token for later target attach, never target completion proof. */
    boolean continuationAttached(SourceTicket ticket, ObservationStore.ContinuationReceipt receipt) {
        Objects.requireNonNull(ticket, "ticket"); Objects.requireNonNull(receipt, "receipt");
        if (!ticket.current.getAsBoolean()) { return false; }
        synchronized (ticket.entry) {
            if (!valid(ticket) || ticket.staged == null || !receipt.pipelineId().equals(ticket.pipelineId)
                    || !receipt.token().equals(ticket.key.token())
                    || !Objects.equals(receipt.sourceScope(), ticket.key.sourceScope())
                    || !receipt.baselineOrigin().equals(ticket.staged.origin.map(ActualTarget::storedTarget))
                    || !receipt.knownBaseline() && !ticket.staged.unknownProven) { return false; }
            ticket.entry.privateReceipt = receipt;
        }
        return ticket.current.getAsBoolean();
    }

    Optional<ContinuationPublication> prepareContinuationPublication(ObservationPublisher.Prepared raw,
            ActualTarget measured, BooleanSupplier current) {
        Objects.requireNonNull(raw, "raw"); Objects.requireNonNull(measured, "measured"); Objects.requireNonNull(current, "current");
        String id = raw.observation().pipelineId();
        Entry entry = entries.get(id);
        if (entry == null || !current.getAsBoolean()) { return Optional.empty(); }
        ContinuationPublication packet;
        synchronized (entry) {
            if (!measured.scope().equals(entry.current) || entry.actualTarget != null && !measured.equals(entry.actualTarget)
                    || !newer(raw.observation(), entry.last)) { return Optional.empty(); }
            entry.actualTarget = measured;
            ObservationPublisher.Prepared projected;
            if (entry.continuationUnknown) {
                projected = raw.withFacts(raw.observation().facts().stream().filter(fact -> fact.type() == MetricType.GAUGE).toList());
                entry.last = projected.observation();
            } else { projected = continueFrame(raw, measured.scope()); }
            Optional<ObservationContinuation> snapshot = entry.continuationKey == null
                    || entry.continuationKey.policy() == StopReservation.CounterPolicy.RESET ? Optional.empty()
                            : Optional.of(new ObservationContinuation(entry.continuationKey.token(),
                                    entry.continuationKey.sourceScope(), Optional.of(measured.storedTarget()),
                                    entry.baselineOrigin.map(ActualTarget::storedTarget), entry.continuationBase,
                                    entry.continuationUnknown ? List.of() : entry.epochs.snapshot()));
            packet = new ContinuationPublication(projected, measured.scope(), snapshot,
                    new PublicationTicket(id, entry, current), Optional.ofNullable(entry.privateReceipt), entry.unknownProven);
        }
        return current.getAsBoolean() ? Optional.of(packet) : Optional.empty();
    }

    boolean publicationAccepted(PublicationTicket ticket, ObservationStore.ContinuationReceipt receipt) {
        Objects.requireNonNull(ticket, "ticket"); Objects.requireNonNull(receipt, "receipt");
        if (!ticket.current.getAsBoolean()) { return false; }
        synchronized (ticket.entry) {
            if (entries.get(ticket.pipelineId) != ticket.entry || ticket.entry.revision != ticket.revision
                    || !Objects.equals(ticket.key, ticket.entry.continuationKey)
                    || !Objects.equals(ticket.target, ticket.entry.actualTarget)
                    || ticket.key == null || !receipt.matches(identity(ticket.pipelineId, ticket.key, ticket.target))
                    || !receipt.knownBaseline() && !ticket.unknownProven) { return false; }
            ticket.entry.privateReceipt = receipt; ticket.entry.durableReceipt = receipt;
        }
        boolean accepted = ticket.current.getAsBoolean();
        if (!accepted) { withdrawReadyReceipt(ticket.entry, ticket.revision, receipt); }
        return accepted;
    }

    boolean publicationAccepted(TargetTicket ticket, ObservationStore.ContinuationReceipt receipt) {
        Objects.requireNonNull(ticket, "ticket"); Objects.requireNonNull(receipt, "receipt");
        if (!ticket.current.getAsBoolean()) { return false; }
        synchronized (ticket.entry) {
            if (!activated(ticket) || !receipt.matches(identity(ticket.pipelineId, ticket.key, ticket.target))
                    || !receipt.knownBaseline() && !ticket.entry.unknownProven) { return false; }
            ticket.entry.privateReceipt = receipt; ticket.entry.durableReceipt = receipt;
        }
        boolean accepted = ticket.current.getAsBoolean();
        if (!accepted) { withdrawReadyReceipt(ticket.entry, ticket.activatedRevision, receipt); }
        return accepted;
    }

    private static void withdrawReadyReceipt(Entry entry, long revision, ObservationStore.ContinuationReceipt receipt) {
        synchronized (entry) {
            if (entry.revision == revision && entry.durableReceipt == receipt) { entry.durableReceipt = null; }
        }
    }

    void cancelContinuation(ContinuationTicket ticket) {
        Objects.requireNonNull(ticket, "ticket");
        synchronized (ticket.entry) {
            if (!valid(ticket)) { return; }
            if (ticket instanceof SourceTicket source && source.staged == ticket.entry.sourceSnapshot) {
                ticket.entry.sourceSnapshot = null; ticket.entry.sourceKey = null;
            }
            ticket.entry.continuationTicket = null;
        }
    }

    ObservationStore.Scope beginResetExecution(String id, ObservationStore.Scope scope) {
        Objects.requireNonNull(id, "pipelineId"); Objects.requireNonNull(scope, "scope");
        Entry entry = entries.computeIfAbsent(id, ignored -> new Entry());
        synchronized (entry) {
            entry.revision++; entry.restore = null; entry.coldRebuild = null; entry.continuationTicket = null;
            entry.sourceSnapshot = null; entry.sourceKey = null; entry.continuationKey = null; entry.actualTarget = null;
            entry.continuationBase = List.of(); entry.baselineOrigin = Optional.empty(); entry.continuationUnknown = false;
            entry.unknownProven = false; entry.privateReceipt = null; entry.durableReceipt = null;
            entry.pending = null; entry.pendingFrom = null; entry.active = null; entry.restored = null; entry.last = null;
            entry.bindingRevision++;
            entry.epochs = new MetricProducerEpochs(); entry.folder = CardinalityBudget.folder(); entry.current = scope;
            entry.failedAdmission = null;
        }
        return scope;
    }

    /** Scope admission invalidates the active native target while preserving one still-qualified source. */
    private static void clearActiveContinuation(Entry entry) {
        entry.continuationTicket = null; entry.continuationKey = null; entry.actualTarget = null;
        entry.continuationBase = List.of(); entry.baselineOrigin = Optional.empty();
        entry.continuationUnknown = false; entry.unknownProven = false;
        entry.durableReceipt = null;
    }

    private boolean unchanged(ContinuationTicket ticket) {
        return entries.get(ticket.pipelineId) == ticket.entry && ticket.entry.revision == ticket.revision
                && Objects.equals(ticket.currentAtStart, ticket.entry.current)
                && ticket.entry.continuationTicket == ticket.previous;
    }

    private boolean valid(ContinuationTicket ticket) {
        return entries.get(ticket.pipelineId) == ticket.entry && ticket.entry.revision == ticket.revision
                && Objects.equals(ticket.currentAtStart, ticket.entry.current) && ticket.entry.continuationTicket == ticket;
    }

    private boolean activated(TargetTicket ticket) {
        return entries.get(ticket.pipelineId) == ticket.entry && ticket.activatedRevision == ticket.entry.revision
                && ticket.key.equals(ticket.entry.continuationKey) && ticket.target.equals(ticket.entry.actualTarget);
    }

    private static boolean sameActive(Entry entry, ContinuationKey key, ActualTarget target) {
        return target.equals(entry.actualTarget) && entry.continuationKey != null
                && entry.continuationKey.token().equals(key.token())
                && Objects.equals(entry.continuationKey.sourceScope(), key.sourceScope())
                && entry.continuationKey.policy() == key.policy();
    }

    private static boolean sourceMatches(SourceSnapshot snapshot, String id, ContinuationKey key) {
        return snapshot.pipelineId.equals(id) && snapshot.key.token().equals(key.token())
                && Objects.equals(snapshot.key.sourceScope(), key.sourceScope()) && snapshot.key.policy() == key.policy();
    }

    private static boolean continuationMatches(ObservationContinuation saved, ContinuationKey key, ActualTarget target) {
        return saved.token().equals(key.token()) && Objects.equals(saved.sourceScope(), key.sourceScope())
                && saved.target().map(bound -> bound.scope().equals(target.scope())
                        && bound.realJob().map(target.job()::equals).orElse(true)).orElse(true);
    }

    private static HandoffIdentity identity(String id, ContinuationKey key, ActualTarget target) {
        return new HandoffIdentity(id, key.token(), key.policy(), key.sourceScope(), target.scope(), target.job());
    }

    private static SourcePreparation invalidSource() { return new SourcePreparation(PreparationStatus.INVALIDATED, Optional.empty()); }
    private static TargetPreparation invalidTarget() { return new TargetPreparation(PreparationStatus.INVALIDATED); }

    private static Instant latestPointTime(List<MetricFact> facts) {
        return facts.stream().flatMap(fact -> fact.points().stream()).map(MetricPoint::observedAt)
                .max(Instant::compareTo).orElse(Instant.EPOCH);
    }

    private static List<MetricFact> mergeKnown(List<MetricFact> before, List<MetricFact> after) {
        List<MetricFact> cumulative = knownStartedFacts(after);
        return MetricContinuation.captureFacts(knownStartedFacts(before)).atLeast(cumulative, latestPointTime(cumulative));
    }

    private static List<MetricFact> boundedFacts(List<MetricFact> facts, CardinalityBudget.Folder folder) {
        return knownStartedFacts(facts).stream().map(folder::fold).toList();
    }

    private static List<MetricFact> knownStartedFacts(List<MetricFact> facts) {
        return facts.stream().filter(fact -> fact.type() != MetricType.GAUGE
                && CardinalityBudget.forInstrument(fact.name()).isPresent())
                .map(fact -> new MetricFact(fact.name(), fact.type(), fact.unit(), fact.points().stream()
                        .filter(point -> point.startTime() != null).toList()))
                .filter(fact -> !fact.points().isEmpty()).toList();
    }

    private static List<MetricFact> publishedFacts(List<ObservationContinuation.ProducerState> states) {
        Map<String, MetricFact> facts = new LinkedHashMap<>();
        for (ObservationContinuation.ProducerState state : states) {
            MetricFact fact = new MetricFact(state.name(), state.type(), state.unit(), state.published());
            facts.merge(state.name(), fact, (before, after) -> before.with(after.points()));
        }
        return List.copyOf(facts.values());
    }

    /** A real old execution's continuation permission, fenced across caller-owned stored reads. */
    static final class ColdRebuildTicket {
        private final String pipelineId;
        private final Entry entry;
        private final long revision;
        private final ObservationStore.Scope existingScope;
        private final ObservationStore.Scope currentAtStart;
        private final BooleanSupplier current;
        private MetricContinuation staged;

        private ColdRebuildTicket(String pipelineId, Entry entry, ObservationStore.Scope existingScope,
                BooleanSupplier current) {
            this.pipelineId = pipelineId; this.entry = entry; this.revision = entry.revision;
            this.existingScope = existingScope; this.currentAtStart = entry.current; this.current = current;
        }
    }

    /** Captures the registry before the ownership/intent check; the predicate may perform IO outside its lock. */
    Optional<ColdRebuildTicket> beginColdRebuild(String pipelineId, ObservationStore.Scope existingScope,
            BooleanSupplier current) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(existingScope, "existingScope"); Objects.requireNonNull(current, "current");
        Entry entry = entries.computeIfAbsent(pipelineId, ignored -> new Entry());
        ColdRebuildTicket ticket;
        ColdRebuildTicket previous;
        synchronized (entry) {
            if (entry.current != null && !existingScope.equals(entry.current)
                    || entry.pendingFrom != null && (!entry.pendingFrom.pipelineIncarnationId()
                            .equals(existingScope.pipelineIncarnationId())
                            || entry.pendingFrom.executionGeneration() > existingScope.executionGeneration())) {
                return Optional.empty();
            }
            ticket = new ColdRebuildTicket(pipelineId, entry, existingScope, current);
            previous = entry.coldRebuild;
        }
        if (!current.getAsBoolean()) { return Optional.empty(); }
        synchronized (entry) {
            if (entries.get(pipelineId) != entry || entry.revision != ticket.revision
                    || !Objects.equals(entry.current, ticket.currentAtStart) || entry.coldRebuild != previous) {
                return Optional.empty();
            }
            entry.coldRebuild = ticket;
            return Optional.of(ticket);
        }
    }

    /**
     * Processes a still-current rebuild ticket. Missing, empty or differently scoped telemetry adds no
     * facts and does not block the rebuild; an already matching floor remains known. False means only
     * that the ticket, registry, ownership or intent is no longer current. No scope or native epoch is
     * installed, and every stored read belongs to the caller outside the registry lock.
     */
    boolean prepareColdRebuildingResume(ColdRebuildTicket ticket, Optional<ObservationStore.Stored> stored) {
        Objects.requireNonNull(ticket, "ticket"); Objects.requireNonNull(stored, "stored");
        if (!ticket.current.getAsBoolean()) { cancelColdRebuild(ticket); return false; }
        synchronized (ticket.entry) {
            if (!valid(ticket)) { return false; }
            Entry entry = ticket.entry;
            Optional<ObservationStore.Stored> qualified = stored.filter(saved ->
                    ticket.pipelineId.equals(saved.observation().pipelineId())
                            && saved.scope().filter(ticket.existingScope::equals).isPresent());
            if (ticket.currentAtStart != null) {
                prepareRebuildingResume(entry, ticket.existingScope, qualified);
            } else {
                Observation source = qualified.map(ObservationStore.Stored::observation).orElse(null);
                MetricContinuation known = MetricContinuation.capture(source);
                if (ticket.existingScope.equals(entry.pendingFrom) && entry.pending != null) {
                    known = MetricContinuation.captureFacts(entry.pending.atLeast(
                            source == null ? List.of() : source.facts(), source == null ? null : source.observedAt()));
                }
                if (!known.isEmpty()) {
                    entry.pending = known.boundedBy(entry.folder);
                    entry.pendingFrom = ticket.existingScope;
                }
            }
            ticket.staged = ticket.existingScope.equals(entry.pendingFrom) ? entry.pending : null;
            return true;
        }
    }

    private boolean valid(ColdRebuildTicket ticket) {
        return entries.get(ticket.pipelineId) == ticket.entry && ticket.entry.revision == ticket.revision
                && ticket.entry.coldRebuild == ticket && Objects.equals(ticket.entry.current, ticket.currentAtStart);
    }

    /** Cancels only this still-current ticket, never the floor already inherited by a newer begin. */
    void cancelColdRebuild(ColdRebuildTicket ticket) {
        Objects.requireNonNull(ticket, "ticket");
        synchronized (ticket.entry) {
            if (!valid(ticket)) { return; }
            if (ticket.staged != null && ticket.entry.pending == ticket.staged
                    && ticket.existingScope.equals(ticket.entry.pendingFrom)) {
                ticket.entry.pending = null; ticket.entry.pendingFrom = null;
            }
            ticket.entry.coldRebuild = null;
        }
    }

    /** The entry object and revision fence begin/delete/recreate, including an absent current scope. */
    static final class RestoreTicket {
        private final String pipelineId;
        private final Entry entry;
        private final long revision;
        private final ObservationScopeRecovery.Owner owner;
        private RecoverySignal firstFailure;
        private TelemetryDispatcher.FailureLog failureLog;
        private RecoverySignal transition;
        private boolean failurePrepared;
        private long deliveryVersion;
        private long preparingVersion = -1;
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
        return restoration(pipelineId, result, failure, owner, null);
    }

    Optional<RestoreTicket> restoration(String pipelineId, ConvergeResult result, ObservationFailure failure,
            ObservationScopeRecovery.Owner owner, TelemetryDispatcher.FailureLog diagnostic) {
        Entry entry = entries.computeIfAbsent(Objects.requireNonNull(pipelineId, "pipelineId"), id -> new Entry());
        synchronized (entry) {
            if (entry.current != null) {
                return Optional.empty();
            }
            if (entry.restore != null && !Objects.equals(owner, entry.restore.owner)) {
                clearStagedColdRebuild(entry);
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
                    ticket.failureLog = diagnostic;
                    ticket.deliveryVersion++;
                    ticket.prepared = null;
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

    OptionalLong beginRestorationDelivery(RestoreTicket ticket) {
        synchronized (ticket.entry) {
            if (!valid(ticket) || ticket.entry.current != null) { return OptionalLong.empty(); }
            ticket.preparingVersion = ticket.deliveryVersion;
            return OptionalLong.of(ticket.deliveryVersion);
        }
    }

    boolean awaiting(RestoreTicket ticket, long deliveryVersion) {
        synchronized (ticket.entry) {
            return valid(ticket) && ticket.entry.current == null && ticket.deliveryVersion == deliveryVersion;
        }
    }

    private static boolean deliveryUnchanged(RestoreTicket ticket) {
        return ticket.preparingVersion < 0 || ticket.preparingVersion == ticket.deliveryVersion;
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

    Optional<TelemetryDispatcher.FailureLog> restorationFailureLog(RestoreTicket ticket, CheckpointDoc checkpoint) {
        synchronized (ticket.entry) {
            return valid(ticket) && ticket.firstFailure != null
                    && ticket.firstFailure.result().checkpoint().filter(checkpoint::equals).isPresent()
                    ? Optional.ofNullable(ticket.failureLog) : Optional.empty();
        }
    }

    Optional<ObservationPublisher.Prepared> restorationPrepared(RestoreTicket ticket,
            ObservationScopeRecovery.Qualified qualified) {
        synchronized (ticket.entry) {
            return valid(ticket) && deliveryUnchanged(ticket) && qualified.scope().equals(ticket.preparedScope)
                    && qualified.checkpoint().equals(ticket.preparedFor)
                    ? Optional.ofNullable(ticket.prepared) : Optional.empty();
        }
    }

    boolean rememberRestoration(RestoreTicket ticket, ObservationScopeRecovery.Qualified qualified,
            ObservationPublisher.Prepared prepared) {
        synchronized (ticket.entry) {
            if (!valid(ticket) || !deliveryUnchanged(ticket) || ticket.entry.current != null) {
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
            if (valid(ticket) && deliveryUnchanged(ticket)) {
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
            if (!valid(ticket) || !deliveryUnchanged(ticket) || ticket.entry.current != null || stored.scope().isEmpty()
                    || !ticket.pipelineId.equals(stored.observation().pipelineId())) {
                return false;
            }
            Entry entry = ticket.entry;
            if (entry.coldRebuild != null && !entry.coldRebuild.existingScope.equals(stored.scope().orElseThrow())) {
                clearStagedColdRebuild(entry);
            }
            entry.bindingRevision++;
            entry.current = stored.scope().orElseThrow();
            entry.failedAdmission = null;
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
                if (entry.restore != null || entry.coldRebuild != null) {
                    clearStagedColdRebuild(entry);
                    entry.revision++;
                    entry.restore = null;
                }
            }
        }
    }

    void cancelRestorations() {
        entries.keySet().forEach(this::cancelRestoration);
    }

    private static void clearStagedColdRebuild(Entry entry) {
        ColdRebuildTicket ticket = entry.coldRebuild;
        if (ticket != null && ticket.staged != null && entry.pending == ticket.staged
                && ticket.existingScope.equals(entry.pendingFrom)) {
            entry.pending = null; entry.pendingFrom = null;
        }
        entry.coldRebuild = null;
    }

    ObservationStore.Scope begin(String pipelineId, String incarnation, long generation) {
        ObservationStore.Scope scope = new ObservationStore.Scope(incarnation, generation);
        Entry entry = entries.computeIfAbsent(Objects.requireNonNull(pipelineId, "pipelineId"), id -> new Entry());
        synchronized (entry) {
            if (scope.equals(entry.current)) {
                return scope;
            }
            entry.revision++;
            entry.failedAdmission = null;
            clearActiveContinuation(entry);
            if (entry.sourceSnapshot != null && entry.sourceSnapshot.sourceScope() != null
                    && !entry.sourceSnapshot.sourceScope().pipelineIncarnationId().equals(incarnation)) {
                entry.sourceSnapshot = null; entry.sourceKey = null; entry.privateReceipt = null;
            }
            entry.restore = null;
            entry.coldRebuild = null;
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
            entry.bindingRevision++;
            entry.current = scope;
        }
        return scope;
    }

    void rememberFailedAdmission(String pipelineId, ObservationStore.Scope scope,
            ObservationScopeRecovery.Owner owner) {
        Objects.requireNonNull(scope, "scope");
        if (owner != null && (!pipelineId.equals(owner.key().resourceId())
                || owner.key().type() != io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION
                || owner.executionGeneration() != scope.executionGeneration())) {
            throw new IllegalArgumentException("a failed admission receipt must match its pipeline and scope");
        }
        Entry entry = entries.get(pipelineId);
        if (entry == null) { return; }
        synchronized (entry) {
            if (entries.get(pipelineId) != entry || !scope.equals(entry.current)) { return; }
            entry.failedAdmission = new FailedAdmission(scope, owner);
        }
    }

    Optional<FailedAdmission> failedAdmission(String pipelineId, ObservationStore.Scope scope) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) { return Optional.empty(); }
        synchronized (entry) {
            return entries.get(pipelineId) == entry
                    && entry.failedAdmission != null && entry.failedAdmission.scope().equals(scope)
                    && scope.equals(entry.current) ? Optional.of(entry.failedAdmission) : Optional.empty();
        }
    }

    boolean currentFailedAdmission(String pipelineId, FailedAdmission expected) {
        return expected != null && failedAdmission(pipelineId, expected.scope()).orElse(null) == expected;
    }

    /** Local binding identity, including invalidation while both scopes are absent. */
    static final class BindingIdentity {
        private final Entry entry;
        private final long revision;
        private final ObservationStore.Scope scope;

        private BindingIdentity(Entry entry, long revision, ObservationStore.Scope scope) {
            this.entry = entry; this.revision = revision; this.scope = scope;
        }

        boolean known() { return scope != null; }

        boolean matchesScope(ObservationStore.Scope expected) { return Objects.equals(scope, expected); }

        @Override public boolean equals(Object other) {
            return other instanceof BindingIdentity identity && entry == identity.entry
                    && revision == identity.revision && Objects.equals(scope, identity.scope);
        }

        @Override public int hashCode() {
            return Objects.hash(System.identityHashCode(entry), revision, scope);
        }
    }

    BindingIdentity bindingIdentity(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) { return new BindingIdentity(null, 0, null); }
        synchronized (entry) {
            return new BindingIdentity(entry, entry.bindingRevision, entry.current);
        }
    }

    /** The mutation is only a local projection; no store, native or ownership callback runs here. */
    void withBindingIdentity(String pipelineId, BindingIdentity expected, Runnable mutation) {
        if (expected == null || expected.entry == null) { return; }
        Entry entry = entries.get(pipelineId);
        if (entry != expected.entry) { return; }
        synchronized (entry) {
            if (entries.get(pipelineId) == entry && entry.bindingRevision == expected.revision
                    && Objects.equals(entry.current, expected.scope)) {
                mutation.run();
            }
        }
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
            if (current == null && entry.coldRebuild != null
                    && !entry.coldRebuild.existingScope.pipelineIncarnationId().equals(incarnationId)) {
                return;
            }
            if (current == null && entry.sourceSnapshot != null && entry.sourceSnapshot.sourceScope() != null
                    && !entry.sourceSnapshot.sourceScope().pipelineIncarnationId().equals(incarnationId)) { return; }
            entry.revision++;
            clearActiveContinuation(entry);
            entry.sourceSnapshot = null; entry.sourceKey = null; entry.privateReceipt = null;
            entry.restore = null;
            entry.coldRebuild = null;
            entry.restored = null;
            // Keep the entry object: a concurrent begin may already hold it after computeIfAbsent.
            entry.bindingRevision++;
            entry.current = null;
            entry.failedAdmission = null;
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
            prepareRebuildingResume(entry, owner, stored);
        }
    }

    private static void prepareRebuildingResume(Entry entry, ObservationStore.Scope owner,
            Optional<ObservationStore.Stored> stored) {
        Observation source = entry.last;
        if (MetricContinuation.capture(source).isEmpty()) {
            source = stored.filter(saved -> saved.scope().filter(owner::equals).isPresent())
                    .map(ObservationStore.Stored::observation).orElse(source);
        }
        List<MetricFact> known = source == null ? List.of() : source.facts();
        Instant at = source == null ? null : source.observedAt();
        if (entry.active != null) { known = entry.active.atLeast(known, at); }
        if (entry.restored != null) { known = entry.restored.atLeast(known, at); }
        known = MetricContinuation.captureFacts(entry.epochs.knownFacts(at)).atLeast(known, at);
        if (owner.equals(entry.pendingFrom) && entry.pending != null) { known = entry.pending.atLeast(known, at); }
        entry.pending = MetricContinuation.captureFacts(known).boundedBy(entry.folder);
        entry.pendingFrom = owner;
    }

    /** An ordinary stop ends pending carry; the current scope keeps its final known totals. */
    void clearContinuation(String pipelineId) {
        Entry entry = entries.get(pipelineId);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            entry.revision++;
            entry.continuationTicket = null;
            entry.sourceSnapshot = null; entry.sourceKey = null;
            entry.restore = null;
            entry.coldRebuild = null;
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
                boolean stopped = observation.state() == PipelineState.STOPPED;
                boolean continuedTerminal = (observation.state() == PipelineState.FAILED
                        || observation.state() == PipelineState.COMPLETED)
                        && entry.continuationKey != null
                        && entry.continuationKey.policy() == StopReservation.CounterPolicy.CONTINUE
                        && entry.actualTarget != null && scope.equals(entry.actualTarget.scope())
                        && !entry.continuationUnknown;
                if (stopped || continuedTerminal) {
                    facts = MetricContinuation.captureFacts(entry.epochs.knownFacts(observation.observedAt()))
                            .atLeast(facts, observation.observedAt());
                }
                if (stopped && entry.restored != null) {
                    facts = entry.restored.atLeast(facts, observation.observedAt());
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
                entry.failedAdmission = null;
                clearActiveContinuation(entry);
                entry.restore = null;
                entry.coldRebuild = null;
            }
            if (scope.equals(entry.current)) {
                entry.revision++;
                entry.failedAdmission = null;
                clearActiveContinuation(entry);
                entry.restore = null;
                entry.coldRebuild = null;
                entry.restored = null;
                entry.bindingRevision++;
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
