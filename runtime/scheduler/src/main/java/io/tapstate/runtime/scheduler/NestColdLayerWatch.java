package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.NestStateWindow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Watches each nest namespace's readings go by and says when one stops being served from memory.
 *
 * <p>What it holds is the previous reading of every namespace it has seen, because the counts in a reading
 * run from the beginning of the run and the question is about now. Two readings and the interval between
 * them are the smallest thing that can answer it.
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
 * <p>Not thread-safe, and does not need to be: it is fed from the pass that publishes observations, which
 * is one caller at a time.
 */
public final class NestColdLayerWatch {

    private final NestColdLayerPressure pressure;
    private final NestColdLayerAlert alert;
    private final Map<String, Map<String, Seen>> byPipeline = new HashMap<>();

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
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(readings, "readings");
        Map<String, Seen> held = byPipeline.computeIfAbsent(pipelineId, ignored -> new HashMap<>());
        held.keySet().retainAll(readings.keySet());
        readings.forEach((namespace, reading) -> judge(pipelineId, held, namespace, reading));
        if (held.isEmpty()) {
            byPipeline.remove(pipelineId);
        }
    }

    /** Which namespaces are currently held, as pipeline and namespace together — what a caller asserts on. */
    Set<String> watching() {
        Set<String> held = new HashSet<>();
        byPipeline.forEach((pipelineId, namespaces) ->
                namespaces.keySet().forEach(namespace -> held.add(pipelineId + "/" + namespace)));
        return held;
    }

    private void judge(String pipelineId, Map<String, Seen> held, String namespace, NestStateReading reading) {
        Seen before = held.get(namespace);
        boolean wasOver = before != null && before.over();
        NestStateReading baseline = before == null || restarted(before.latest(), reading)
                ? null : before.baseline();
        NestStateWindow window = baseline == null
                ? NestStateWindow.fromStart(reading)
                : NestStateWindow.between(baseline, reading);
        if (window.accesses() < pressure.leastAccesses()) {
            // Retain the last judged boundary, so several small samples can make a real window.
            // An idle or undersized sample says nothing about recovery from the previous pressure.
            held.put(namespace, new Seen(reading, baseline, wasOver));
            return;
        }
        boolean nowOver = pressure.isOver(window);
        held.put(namespace, new Seen(reading, reading, nowOver));
        if (nowOver && !wasOver) {
            alert.crossed(pipelineId, namespace, window);
        } else if (!nowOver && wasOver) {
            alert.cleared(pipelineId, namespace, window);
        }
    }

    private static boolean restarted(NestStateReading previous, NestStateReading current) {
        return current.accesses() < previous.accesses()
                || current.backfills() < previous.backfills()
                || current.backfillMillis() < previous.backfillMillis();
    }

    /** Observe resets on every sample while accumulating traffic since the last judged boundary. */
    private record Seen(NestStateReading latest, NestStateReading baseline, boolean over) {
    }
}
