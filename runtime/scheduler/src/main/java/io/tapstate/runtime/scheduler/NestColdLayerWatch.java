package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.NestStateWindow;
import io.tapstate.spi.store.ObservationStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

/**
 * Watches each nest namespace's readings go by and says when one stops being served from memory.
 *
 * <p>What it holds is the last judged reading and the last observed reading of every namespace. The
 * counters run from the beginning of a run, so consecutive small intervals must accumulate until there
 * is enough traffic to judge. The last observed reading also detects a counter restart before the old
 * judged baseline could be reused.
 *
 * <p>It reports the two edges rather than the state. A condition that holds for a day is one thing that
 * happened, not one thing per pass, and reporting it per pass would drown the record it is written to;
 * reporting the entry alone would leave the last thing anyone heard being wrong for however long the
 * recovery lasts.
 *
 * <p>A namespace that stops appearing is dropped rather than called recovered — a pipeline that stopped did
 * not start being served from memory again. Both what was held for it and whether it was over are dropped
 * together, so a namespace that comes back is judged from its own counts. Keeping only the reading would be
 * worse than keeping neither: the next run would be measured against a dead one's totals, and a run that
 * was over from its first reading would be silently taken for the old one still being over.
 *
 * <p>Namespaces are held under their pipeline rather than under a name built from both. Namespaces are
 * named from what a pipeline compiles to, so two pipelines running nests of the same shape would otherwise
 * be differenced against each other's counts — and a joined name would answer that with a separator that
 * has to be absent from every pipeline id there will ever be.
 *
 * <p>A preparation may read the previous window while a telemetry worker commits another pipeline's
 * observation. Each update replaces an immutable per-pipeline snapshot, and alert callbacks run after
 * that snapshot is visible.
 */
public final class NestColdLayerWatch {

    private final NestColdLayerPressure pressure;
    private final NestColdLayerAlert alert;
    private final Map<String, ScopedSeen> byPipeline = new ConcurrentHashMap<>();

    public NestColdLayerWatch(NestColdLayerPressure pressure, NestColdLayerAlert alert) {
        this.pressure = Objects.requireNonNull(pressure, "pressure");
        this.alert = Objects.requireNonNull(alert, "alert");
    }

    /**
     * Takes {@code readings} as the latest of {@code pipelineId}'s namespaces, reporting any that has just
     * stopped or just resumed being served from memory. Namespaces of this pipeline that are not in
     * {@code readings} are forgotten.
     */
    public void saw(String pipelineId, Map<String, NestStateReading> readings) {
        saw(pipelineId, null, readings);
    }

    /** Records a successful observation under its internal execution owner. */
    public void saw(String pipelineId, ObservationStore.Scope scope, Map<String, NestStateReading> readings) {
        saw(pipelineId, scope, readings, () -> true);
    }

    /** The predicate is a local account check; callbacks remain outside the per-pipeline map update. */
    public void saw(String pipelineId, ObservationStore.Scope scope, Map<String, NestStateReading> readings,
            BooleanSupplier current) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(readings, "readings");
        Objects.requireNonNull(current, "current");
        List<Alert> alerts = new ArrayList<>();
        byPipeline.compute(pipelineId, (id, previous) -> {
            if (!current.getAsBoolean() || stale(scope, previous)) {
                return previous;
            }
            Map<String, Seen> held = new HashMap<>(matching(scope, previous));
            held.keySet().retainAll(readings.keySet());
            readings.forEach((namespace, reading) -> judge(held, namespace, reading, alerts));
            return held.isEmpty() ? null : new ScopedSeen(scope, Map.copyOf(held));
        });
        alerts.forEach(change -> {
            if (!current.getAsBoolean()) { return; }
            if (change.over()) {
                alert.crossed(pipelineId, change.namespace(), change.window());
            } else {
                alert.cleared(pipelineId, change.namespace(), change.window());
            }
        });
    }

    /** Current threshold decisions from the next measured window; quiet windows have no decision. */
    Map<String, Long> assessment(String pipelineId, Map<String, NestStateReading> readings) {
        return assessment(pipelineId, null, readings);
    }

    Map<String, Long> assessment(String pipelineId, ObservationStore.Scope scope,
            Map<String, NestStateReading> readings) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(readings, "readings");
        Map<String, Seen> held = matching(scope, byPipeline.get(pipelineId));
        Map<String, Long> judged = new HashMap<>();
        readings.forEach((namespace, reading) -> {
            Assessment next = assess(held.get(namespace), reading);
            if (next.enough()) {
                judged.put(namespace, pressure.isOver(next.window()) ? 1L : 0L);
            }
        });
        return Map.copyOf(judged);
    }

    /** Which namespaces are currently held, as pipeline and namespace together — what a caller asserts on. */
    Set<String> watching() {
        Set<String> held = new HashSet<>();
        byPipeline.forEach((pipelineId, scoped) ->
                scoped.namespaces().keySet().forEach(namespace -> held.add(pipelineId + "/" + namespace)));
        return held;
    }

    /** Releases windows for resources that have left the live artifact set. */
    public void forgetPipelinesOutside(Collection<String> live) {
        byPipeline.keySet().retainAll(Set.copyOf(Objects.requireNonNull(live, "live")));
    }

    /** Captures exact old windows; executing later cannot remove a replacement window. */
    public Runnable captureForgetPipelinesOutside(Collection<String> live) {
        Set<String> kept = Set.copyOf(Objects.requireNonNull(live, "live"));
        Map<String, ScopedSeen> captured = new HashMap<>();
        byPipeline.forEach((id, seen) -> { if (!kept.contains(id)) { captured.put(id, seen); } });
        return () -> captured.forEach((id, expected) ->
                byPipeline.computeIfPresent(id, (key, current) -> current == expected ? null : current));
    }

    private void judge(Map<String, Seen> held, String namespace, NestStateReading reading, List<Alert> alerts) {
        Seen before = held.get(namespace);
        Assessment next = assess(before, reading);
        if (!next.enough()) {
            held.put(namespace, new Seen(next.restarted() || before == null ? null : before.judged(),
                    reading, next.restarted() || before == null ? null : before.over()));
            return;
        }
        NestStateWindow window = next.window();
        boolean nowOver = pressure.isOver(window);
        boolean wasOver = !next.restarted() && before != null && Boolean.TRUE.equals(before.over());
        held.put(namespace, new Seen(reading, reading, nowOver));
        if (nowOver && !wasOver) {
            alerts.add(new Alert(namespace, window, true));
        } else if (!nowOver && wasOver) {
            alerts.add(new Alert(namespace, window, false));
        }
    }

    private static Map<String, Seen> matching(ObservationStore.Scope scope, ScopedSeen previous) {
        return previous != null && Objects.equals(scope, previous.scope())
                ? previous.namespaces() : Map.of();
    }

    private static boolean stale(ObservationStore.Scope scope, ScopedSeen previous) {
        return scope != null && previous != null && previous.scope() != null
                && scope.executionGeneration() < previous.scope().executionGeneration();
    }

    private Assessment assess(Seen before, NestStateReading reading) {
        boolean restarted = before != null && (reading.accesses() < before.observed().accesses()
                || reading.backfills() < before.observed().backfills()
                || reading.backfillMillis() < before.observed().backfillMillis());
        NestStateReading baseline = before == null || restarted ? null : before.judged();
        NestStateWindow window = baseline == null
                ? NestStateWindow.fromStart(reading)
                : NestStateWindow.between(baseline, reading);
        return new Assessment(window, window.accesses() >= pressure.leastAccesses(), restarted);
    }

    /** A null judgement means this namespace has not yet carried enough traffic to assess. */
    private record Seen(NestStateReading judged, NestStateReading observed, Boolean over) {
    }

    private record Assessment(NestStateWindow window, boolean enough, boolean restarted) {
    }

    private record ScopedSeen(ObservationStore.Scope scope, Map<String, Seen> namespaces) {
    }

    private record Alert(String namespace, NestStateWindow window, boolean over) {
    }
}
