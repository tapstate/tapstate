package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Private checkpoint evidence. A bound baseline belongs to one intent and allocator fence. */
public final class ResumeMetrics {
    private ResumeMetrics() {}

    public enum Action { PAUSE, REBUILD, IN_PLACE }

    public record Transition(Action action, long revision, long jobId, List<MetricFact> raw) {
        public Transition {
            Objects.requireNonNull(action);
            raw = List.copyOf(raw);
            if (revision < 1) throw new IllegalArgumentException("missing intent revision");
        }
    }

    public record Baseline(long pauseRevision, long epoch, WorkloadClaimFence origin, long jobId,
            List<MetricFact> facts, boolean unavailable) {
        public Baseline {
            Objects.requireNonNull(origin);
            facts = List.copyOf(facts);
            if (pauseRevision < 1 || epoch < 0) throw new IllegalArgumentException("invalid pause identity");
        }
    }

    public record Binding(Baseline baseline, long revision, WorkloadClaimFence fence) {
        public Binding {
            Objects.requireNonNull(baseline);
            Objects.requireNonNull(fence);
            if (revision < 1) throw new IllegalArgumentException("missing resume intent");
        }
    }

    public record Handoff(Binding applied, Baseline candidate) {
        public static final Handoff EMPTY = new Handoff(null, null);

        public Handoff transition(Transition command, WorkloadClaimFence claim, long epoch, List<MetricFact> observed) {
            if (command == null) return EMPTY;
            var prior = applied != null && applied.fence().equals(claim) ? applied : null;
            return switch (command.action()) {
                case PAUSE -> {
                    var facts = qualified(observed, command.raw(), prior);
                    boolean unavailable = prior != null && prior.baseline().unavailable();
                    yield new Handoff(prior, facts.isEmpty() && !unavailable ? null :
                            new Baseline(command.revision(), epoch, claim, command.jobId(), facts, unavailable));
                }
                case REBUILD -> candidate != null && candidate.origin().equals(claim)
                        && candidate.jobId() == command.jobId() && candidate.epoch() + 1 == epoch
                        && candidate.pauseRevision() + 1 == command.revision()
                        ? new Handoff(new Binding(candidate, command.revision(), claim), null) : EMPTY;
                case IN_PLACE -> new Handoff(prior == null ? null :
                        new Binding(prior.baseline(), command.revision(), claim), null);
            };
        }

        public Handoff advance(WorkloadClaimFence before, WorkloadClaimFence after, long revision) {
            if (applied == null || applied.revision() != revision || !applied.fence().equals(before)) return EMPTY;
            return new Handoff(new Binding(applied.baseline(), revision, after), null);
        }
    }

    public enum Producer { IN, OUT, STAGE }

    public record Slot(long startMillis, boolean carry) {}

    public record Window(long intentRevision, long jobId, Map<Producer, Slot> slots) {
        public Window {
            if (intentRevision < 1) throw new IllegalArgumentException("missing resume intent");
            slots = Map.copyOf(slots);
        }
    }

    /** A checked read of the observation at the checkpoint's complete original key. */
    public record Prior(boolean exists, Instant observedAt, Window window, Set<Producer> cumulative) {
        public static final Prior ABSENT = new Prior(false, null, null, Set.of());
        public Prior { cumulative = Set.copyOf(cumulative); }
    }

    public record ObservationKey(PipelineIntentScope scope, long epoch, WorkloadClaimFence fence) {
        public ObservationKey { Objects.requireNonNull(scope); Objects.requireNonNull(fence); }
    }

    public record Publication(ObservationKey key, Prior expected, Window window) {
        public Publication {
            Objects.requireNonNull(key);
            Objects.requireNonNull(expected);
            if (window == null && expected.exists())
                throw new IllegalArgumentException("a provisional observation requires a proved empty key");
        }
    }

    public record Checkpoint(CheckpointDoc state, Binding applied, long unavailableRevision, Prior prior) {
        public Checkpoint(CheckpointDoc state, Binding applied, long unavailableRevision) {
            this(state, applied, unavailableRevision, null);
        }
        public Checkpoint(CheckpointDoc state, Binding applied) { this(state, applied, 0); }
    }

    /** Select only the saved stable series identified by the captured native producer. */
    public static List<MetricFact> qualified(List<MetricFact> saved, List<MetricFact> raw, Binding prior) {
        var measured = index(raw);
        var applied = prior == null ? Map.<Series,MetricPoint>of() : index(prior.baseline().facts());
        var selected = new ArrayList<MetricFact>();
        for (var fact : saved) {
            var points = fact.points().stream().filter(point -> {
                var key = new Series(fact.name(), point.attributes());
                var producer = measured.get(key);
                if (point.startTime() == null) return false;
                var baseline = applied.get(key);
                return producer != null && point.startTime().equals(producer.startTime())
                        || baseline != null && point.startTime().equals(baseline.startTime());
            }).toList();
            if (!points.isEmpty()) selected.add(new MetricFact(fact.name(), fact.type(), fact.unit(), points));
        }
        return List.copyOf(selected);
    }

    public record Series(String name, Map<String, String> attributes) {}

    public static Map<Series, MetricPoint> index(List<MetricFact> facts) {
        var result = new HashMap<Series, MetricPoint>();
        for (var fact : facts) for (var point : fact.points())
            result.put(new Series(fact.name(), point.attributes()), point);
        return result;
    }
}
