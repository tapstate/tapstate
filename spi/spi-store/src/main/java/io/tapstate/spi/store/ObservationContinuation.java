package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** One private cumulative floor and current native-producer checkpoint for a rebuilding execution. */
public record ObservationContinuation(
        String token, ObservationStore.Scope sourceScope, Optional<Target> target,
        Optional<Target> baselineOrigin, List<MetricFact> baselineFacts, List<ProducerState> producerStates) {

    public ObservationContinuation {
        Objects.requireNonNull(token, "token");
        if (token.isBlank() || token.length() > 256) {
            throw new IllegalArgumentException("a continuation needs one bounded handoff token");
        }
        target = Objects.requireNonNull(target, "target");
        baselineOrigin = Objects.requireNonNull(baselineOrigin, "baselineOrigin");
        baselineFacts = List.copyOf(Objects.requireNonNull(baselineFacts, "baselineFacts"));
        producerStates = List.copyOf(Objects.requireNonNull(producerStates, "producerStates"));
        if (baselineFacts.size() > ObservationContinuationBounds.maximumFacts()
                || producerStates.size() > ObservationContinuationBounds.maximumProducerGroups()) {
            throw new IllegalArgumentException("one private continuation has bounded fact and native-group counts");
        }
        if ((sourceScope == null || baselineFacts.isEmpty()) && !producerStates.isEmpty()
                || sourceScope == null && !baselineFacts.isEmpty()) {
            throw new IllegalArgumentException("an unknown cumulative floor has no fabricated facts or native state");
        }
        if (!producerStates.isEmpty() && target.flatMap(Target::realJob).isEmpty()) {
            throw new IllegalArgumentException("native producer state requires one real bound target job");
        }
        if (baselineOrigin.isPresent() && baselineOrigin.orElseThrow().realJob().isEmpty()) {
            throw new IllegalArgumentException("a measured baseline origin requires its real job");
        }
        ObservationStore.Scope floorScope = baselineOrigin.map(Target::scope).orElse(sourceScope);
        if (sourceScope != null && baselineOrigin.isPresent()) {
            ObservationStore.Scope origin = baselineOrigin.orElseThrow().scope();
            if (!sourceScope.pipelineIncarnationId().equals(origin.pipelineIncarnationId())
                    || origin.executionGeneration() < sourceScope.executionGeneration()) {
                throw new IllegalArgumentException("a measured floor belongs to the original resource lineage");
            }
        }
        if (floorScope != null && target.isPresent()) {
            ObservationStore.Scope next = target.orElseThrow().scope();
            if (!floorScope.pipelineIncarnationId().equals(next.pipelineIncarnationId())
                    || next.executionGeneration() <= floorScope.executionGeneration()) {
                throw new IllegalArgumentException("a continuation target follows its qualified baseline execution");
            }
        }
        CardinalityBudget.Folder folder = CardinalityBudget.folder();
        Set<String> baselineNames = new HashSet<>();
        Map<String, Signature> signatures = new HashMap<>();
        for (MetricFact fact : baselineFacts) {
            if (fact.points().isEmpty() || !baselineNames.add(fact.name())) {
                throw new IllegalArgumentException("a cumulative baseline has unique nonempty metric facts");
            }
            validateFact(fact, folder, signatures);
        }
        Set<Group> groups = new HashSet<>();
        for (ProducerState state : producerStates) {
            if (!groups.add(new Group(state.name(), state.type(), state.unit(), state.direction(), state.stage()))) {
                throw new IllegalArgumentException("one native producer group has one current checkpoint");
            }
            validateFact(new MetricFact(state.name(), state.type(), state.unit(), state.offsets()), folder, signatures);
            validateFact(new MetricFact(state.name(), state.type(), state.unit(), state.published()), folder, signatures);
        }
    }

    /** Empty facts represent an explicit unknown floor, never a numeric zero. */
    public boolean knownBaseline() {
        return !baselineFacts.isEmpty();
    }

    /** A durable admitted scope may precede the discovery of the real submitted job. */
    public record Target(ObservationStore.Scope scope, Optional<StopReservation.JobIdentity> realJob) {
        public Target {
            Objects.requireNonNull(scope, "scope");
            realJob = Objects.requireNonNull(realJob, "realJob");
        }
    }

    /** Native epoch is independent of the original public accumulation starts carried by the points. */
    public record ProducerState(
            String name, MetricType type, String unit, String direction, String stage, Instant nativeStart,
            List<MetricPoint> offsets, List<MetricPoint> published) {
        public ProducerState {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(unit, "unit");
            Objects.requireNonNull(direction, "direction");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(nativeStart, "nativeStart");
            offsets = List.copyOf(Objects.requireNonNull(offsets, "offsets"));
            published = List.copyOf(Objects.requireNonNull(published, "published"));
            if (type == MetricType.GAUGE || name.isBlank() || unit.isBlank()
                    || !direction.isEmpty() && !MetricAttributes.DIRECTIONS.contains(direction)
                    || !stage.isEmpty() && !MetricAttributes.closedDomain(MetricAttributes.STAGE)
                            .orElseThrow().contains(stage)) {
                throw new IllegalArgumentException("a native checkpoint needs one declared cumulative producer group");
            }
            ObservationContinuationBounds.validateGroup(name, direction, stage);
            for (List<MetricPoint> points : List.of(offsets, published)) {
                for (MetricPoint point : points) {
                    if (!direction.equals(point.attributes().getOrDefault(MetricAttributes.DIRECTION, ""))
                            || !stage.equals(point.attributes().getOrDefault(MetricAttributes.STAGE, ""))) {
                        throw new IllegalArgumentException("checkpoint points must belong to their native producer group");
                    }
                }
            }
            Map<String, Signature> signatures = new HashMap<>();
            CardinalityBudget.Folder folder = CardinalityBudget.folder();
            validateFact(new MetricFact(name, type, unit, offsets), folder, signatures);
            validateFact(new MetricFact(name, type, unit, published), folder, signatures);
        }
    }

    private record Group(String name, MetricType type, String unit, String direction, String stage) { }
    private record Signature(MetricType type, String unit) { }

    private static void validateFact(MetricFact fact, CardinalityBudget.Folder folder,
            Map<String, Signature> signatures) {
        if (fact.type() == MetricType.GAUGE || CardinalityBudget.forInstrument(fact.name()).isEmpty()
                || fact.points().stream().anyMatch(point -> point.startTime() == null)) {
            throw new IllegalArgumentException("a continuation retains only budgeted cumulative facts with known starts");
        }
        if (fact.points().size() > ObservationContinuationBounds.maximumPoints(fact.name())) {
            throw new IllegalArgumentException("a private continuation exceeds its point limit");
        }
        fact.points().forEach(point -> ObservationContinuationBounds.validatePoint(fact.name(), point));
        Signature signature = new Signature(fact.type(), fact.unit());
        Signature previous = signatures.putIfAbsent(fact.name(), signature);
        if (previous != null && !signature.equals(previous)) {
            throw new IllegalArgumentException("a continuation metric keeps its type and unit");
        }
        if (!folder.fold(fact).equals(fact)) {
            throw new IllegalArgumentException("continuation points must already satisfy their shared cardinality budget");
        }
    }
}
