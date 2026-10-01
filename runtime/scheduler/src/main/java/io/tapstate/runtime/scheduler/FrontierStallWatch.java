package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.FrontierStall;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.spi.store.ObservationStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

/**
 * Watches each chain's pinned-for reading go by and says when one has been pinned too long, and when it
 * is moving again.
 *
 * <p>What it holds is whether each chain it has seen was over the last time it looked, because what is
 * reported is the two edges rather than the state. A position pinned for a day is one thing that happened,
 * not one thing per pass, and saying it every pass would drown the record it is written to; saying only
 * the entry would leave the last thing anyone heard being wrong for as long as the recovery lasts.
 *
 * <p>The duration does the judging and the distance rides along, because they answer different halves of
 * one question: how long says the position is pinned at all — which the distance cannot, reading zero both
 * for a chain held back by pending changes and for one keeping up — and the distance says which of the two
 * pins it is, and therefore which end to work on. A chain reporting no distance keeps that absence rather
 * than being given a zero; it has never advanced, which is the worse case, not the resting one.
 *
 * <p>A chain that stops being reported is dropped rather than called recovered. It has either caught up
 * or its job has stopped, and those are indistinguishable from here while meaning opposite things — a
 * stopped job's durable position is pinned exactly where it was, and the retention window goes on being
 * consumed while nothing at all is running. Announcing recovery there would be the single belief this
 * alarm exists to prevent. The ordinary recovery still reports: a chain that advances is reported again
 * with its duration restarted, and falling back under the threshold is the edge.
 *
 * <p>Chains are held under their pipeline rather than under a name built from both. A chain is named for
 * the table it carries, so two pipelines reading the same table would otherwise be judged against each
 * other's readings — and a joined name would answer that with a separator that has to be absent from every
 * pipeline id there will ever be.
 *
 * <p>Each successful publication replaces an immutable scoped snapshot. Alert callbacks run after
 * the per-pipeline update, so one slow alert cannot hold another pipeline's measurement account.
 */
public final class FrontierStallWatch {

    private final FrontierStallPressure pressure;
    private final FrontierStallAlert alert;
    private record Seen(ObservationStore.Scope scope, Map<String, Boolean> chains) { }
    private record Alert(FrontierStall stall, boolean over) { }
    private final Map<String, Seen> byPipeline = new ConcurrentHashMap<>();

    public FrontierStallWatch(FrontierStallPressure pressure, FrontierStallAlert alert) {
        this.pressure = Objects.requireNonNull(pressure, "pressure");
        this.alert = Objects.requireNonNull(alert, "alert");
    }

    /**
     * Takes {@code stalls} as the latest pinned-for readings of {@code pipelineId}'s chains and
     * {@code gaps} as their distances, reporting any chain that has just crossed the threshold or just
     * fallen back under it. Chains of this pipeline that are not in {@code stalls} are forgotten.
     *
     * <p>The two maps do not cover the same chains and are not merged on the assumption that they do: a
     * chain that is pinned may report no distance, and a chain that reports one may not be pinned. Only
     * the pinned ones are judged here, each carrying whatever distance it has.
     */
    public void saw(String pipelineId, Map<String, Long> stalls, Map<String, Long> gaps) {
        saw(pipelineId, null, stalls, gaps, () -> true);
    }

    public void saw(String pipelineId, ObservationStore.Scope scope, Map<String, Long> stalls, Map<String, Long> gaps,
            BooleanSupplier current) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stalls, "stalls");
        Objects.requireNonNull(gaps, "gaps");
        Objects.requireNonNull(current, "current");
        List<Alert> alerts = new ArrayList<>();
        byPipeline.compute(pipelineId, (id, previous) -> {
            if (!current.getAsBoolean() || stale(scope, previous)) { return previous; }
            Map<String, Boolean> held = new HashMap<>(previous != null
                    && Objects.equals(scope, previous.scope()) ? previous.chains() : Map.of());
            held.keySet().retainAll(stalls.keySet());
            stalls.forEach((chain, pinnedMillis) -> {
                Long gap = gaps.get(chain);
                FrontierStall stall = new FrontierStall(chain, pinnedMillis,
                        gap == null ? OptionalLong.empty() : OptionalLong.of(gap));
                boolean nowOver = pressure.isOver(stall);
                boolean wasOver = Boolean.TRUE.equals(held.put(chain, nowOver));
                if (nowOver != wasOver) { alerts.add(new Alert(stall, nowOver)); }
            });
            return held.isEmpty() ? null : new Seen(scope, Map.copyOf(held));
        });
        alerts.forEach(change -> {
            if (!current.getAsBoolean()) { return; }
            if (change.over()) { alert.crossed(pipelineId, change.stall()); }
            else { alert.cleared(pipelineId, change.stall()); }
        });
    }

    /** Which chains are currently held, as pipeline and chain together — what a caller asserts on. */
    Set<String> watching() {
        Set<String> held = new HashSet<>();
        byPipeline.forEach((pipelineId, seen) ->
                seen.chains().keySet().forEach(chain -> held.add(pipelineId + "/" + chain)));
        return held;
    }

    public void forgetPipelinesOutside(Collection<String> live) {
        byPipeline.keySet().retainAll(Set.copyOf(Objects.requireNonNull(live, "live")));
    }

    /** A delayed cleanup matches the captured window object rather than only its pipeline id. */
    public Runnable captureForgetPipelinesOutside(Collection<String> live) {
        Set<String> kept = Set.copyOf(Objects.requireNonNull(live, "live"));
        Map<String, Seen> captured = new HashMap<>();
        byPipeline.forEach((id, seen) -> { if (!kept.contains(id)) { captured.put(id, seen); } });
        return () -> captured.forEach((id, expected) ->
                byPipeline.computeIfPresent(id, (key, current) -> current == expected ? null : current));
    }

    private static boolean stale(ObservationStore.Scope scope, Seen previous) {
        return previous != null && previous.scope() != null && (scope == null
                || !scope.equals(previous.scope()) && scope.executionGeneration() <= previous.scope().executionGeneration());
    }
}
