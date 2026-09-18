package io.tapstate.adapters.otel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import io.opentelemetry.sdk.resources.Resource;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The facts of every pipeline, as the SDK's readers ask for them. Holds the latest facts offered for
 * each pipeline and, on each collection, projects them onto the SDK's metric data model: a counter fact
 * is a cumulative monotonic sum, a gauge fact a gauge, a histogram fact a cumulative histogram with the
 * facts' own buckets. Names, attributes, units, times and numbers are the facts', untouched; what the
 * SDK adds is the wire and the cadence.
 *
 * <p>Cumulative always. Every reader this adapter brings up asks for cumulative data, and a counter that
 * carries its own start is already the cumulative shape; converting to deltas would be this adapter
 * doing arithmetic on numbers whose meaning the contract fixed elsewhere.
 *
 * <p>Two fences on how many series leave. Before anything is held, each fact passes the cardinality
 * budget's fold: past its budget, the one dimension a pipeline's data grows is folded away per pipeline,
 * first-seen values keeping their names. Then, on collection, an instrument that still exceeds
 * {@link CardinalityBudget#EXPORT_SERIES_LIMIT} across every pipeline keeps the first series it ever saw
 * and folds the rest into one overflow series, marked the way the SDK marks its own -- the fold that
 * cannot be applied per pipeline, because "how many pipelines this deployment runs" is visible only
 * from above all of them. Which series stay named is decided by first sight rather than per
 * collection, for the same reason the budget's fold decides it that way: a series that alternated
 * between named and folded from one scrape to the next would draw as a broken line over unchanged data.
 *
 * <p>The state rides beside the facts and is projected the one way the contract allows a state on the
 * metrics side: a gauge per state per pipeline, one at 1 and the rest at 0, never a number that stands
 * for a state.
 */
final class FactsMetricProducer implements MetricProducer {

    /** The gauge that projects each pipeline's state: one series per state, 1 where the pipeline is. */
    static final String STATE_METRIC = "tapstate.pipeline.state";

    /** The attribute the state gauge names the state under; the value is the state's lower-case name. */
    static final String STATE_ATTRIBUTE = "state";

    private static final Attributes OVERFLOW_ONLY = Attributes.of(
            AttributeKey.stringKey(MetricAttributes.OVERFLOW), "true");

    /** What one pipeline last offered. */
    private record Offered(PipelineState state, Instant observedAt, List<MetricFact> facts) {
    }

    /** One instrument's points across every pipeline, gathered for one collection. */
    private static final class Series {
        private final MetricType type;
        private final String unit;
        private final List<MetricPoint> points = new ArrayList<>();

        private Series(MetricType type, String unit) {
            this.type = type;
            this.unit = unit;
        }
    }

    private final Instant started;
    private final CardinalityBudget.Folder folder = CardinalityBudget.folder();
    private final Map<String, Offered> latest = new ConcurrentHashMap<>();

    /** Per instrument, the attribute sets that hold a series of their own past the export limit, in first-seen order. */
    private final Map<String, Set<Map<String, String>>> named = new HashMap<>();

    FactsMetricProducer(Instant started) {
        this.started = Objects.requireNonNull(started, "started");
    }

    /** Holds {@code facts} as the latest of {@code pipelineId}, folded to the budget on the way in. */
    void offer(String pipelineId, PipelineState state, Instant observedAt, List<MetricFact> facts) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(facts, "facts");
        List<MetricFact> folded = new ArrayList<>(facts.size());
        // The folder remembers which values it named; one offer at a time keeps that memory whole.
        synchronized (folder) {
            for (MetricFact fact : facts) {
                folded.add(folder.fold(fact));
            }
        }
        latest.put(pipelineId, new Offered(state, observedAt, List.copyOf(folded)));
    }

    /** Drops what is held for every pipeline outside {@code pipelineIds}; their series stop with the next collection. */
    void forgetPipelinesOutside(Collection<String> pipelineIds) {
        Set<String> kept = Set.copyOf(pipelineIds);
        latest.keySet().retainAll(kept);
        // The per-pipeline fold remembers which values of each open dimension it named, and that memory
        // has the same expiry as the names below: it belongs to a pipeline, and the pipeline is gone.
        synchronized (folder) {
            folder.forgetPipelinesOutside(kept);
        }
        // The names go with the pipelines they were held for. A name is a promise to a chart: this series
        // will not begin folding while the data behind it has not changed. A pipeline that is gone has
        // neither chart nor data, and keeping its names would turn the export limit into a ratchet — a
        // process that creates and removes pipelines would spend it on pipelines that no longer exist and
        // fold the ones that do.
        // Swept on every pass rather than only when the set has changed. A pass costs one pipeline-id
        // lookup per named series, which at the export limit is a few hundred thousand a second and a
        // couple of milliseconds; a guard on "has anything left since last time" has to know what was
        // named since last time as well, and a guard that gets that wrong is a name never given back.
        synchronized (this) {
            named.values().forEach(sets -> sets.removeIf(attributes -> {
                String pipeline = attributes.get(MetricAttributes.PIPELINE_ID);
                return pipeline != null && !kept.contains(pipeline);
            }));
        }
    }

    /** The pipelines currently held, for a reader of this producer that wants to say what it exports. */
    Set<String> pipelines() {
        return Set.copyOf(latest.keySet());
    }

    @Override
    public Collection<MetricData> produce(Resource resource) {
        Map<String, Offered> snapshot = Map.copyOf(latest);
        Map<String, Series> byInstrument = new TreeMap<>();
        for (Offered offered : snapshot.values()) {
            for (MetricFact fact : offered.facts()) {
                byInstrument.computeIfAbsent(fact.name(), name -> new Series(fact.type(), fact.unit()))
                        .points.addAll(fact.points());
            }
        }
        List<MetricData> out = new ArrayList<>();
        byInstrument.forEach((name, series) ->
                out.add(metric(resource, name, series.type, series.unit, backstop(name, series.type, series.points))));
        if (!snapshot.isEmpty()) {
            out.add(stateGauge(resource, snapshot));
        }
        return out;
    }

    /**
     * {@code points} with everything past the export limit folded into one overflow series. First sight
     * decides which series stay named, and stays decided: a series seen while there was room keeps its
     * name for as long as its pipeline is exported, so a chart of it does not break and mend as other
     * pipelines come and go. Deciding it per collection instead — from whatever order the pipelines
     * currently held happen to iterate in — is what that would do.
     *
     * <p>A name is given up only with the pipeline it was held for, in {@link #forgetPipelinesOutside}.
     */
    private synchronized List<MetricPoint> backstop(String instrument, MetricType type, List<MetricPoint> points) {
        Set<Map<String, String>> namedHere = named.computeIfAbsent(instrument, name -> new LinkedHashSet<>());
        if (points.size() <= CardinalityBudget.EXPORT_SERIES_LIMIT) {
            // Naming them here, while there is room for all of them, is what makes first sight first. Left
            // until the limit is crossed, this set would still be empty on the collection that crosses it,
            // and the names would go to whichever series that one collection happened to iterate first --
            // an order that is a function of the pipelines currently held, not of when each was first seen.
            for (MetricPoint point : points) {
                if (namedHere.size() >= CardinalityBudget.EXPORT_SERIES_LIMIT - 1) {
                    break;
                }
                namedHere.add(point.attributes());
            }
            return points;
        }
        List<MetricPoint> kept = new ArrayList<>();
        List<MetricPoint> overflow = new ArrayList<>();
        for (MetricPoint point : points) {
            if (namedHere.contains(point.attributes())
                    || namedHere.size() < CardinalityBudget.EXPORT_SERIES_LIMIT - 1) {
                namedHere.add(point.attributes());
                kept.add(point);
            } else {
                overflow.add(point);
            }
        }
        if (!overflow.isEmpty()) {
            kept.add(CardinalityBudget.merge(instrument, type, Map.of(MetricAttributes.OVERFLOW, "true"),
                    overflow));
        }
        return kept;
    }

    private MetricData metric(Resource resource, String name, MetricType type, String unit, List<MetricPoint> points) {
        return switch (type) {
            case COUNTER -> new FactMetricData(resource, name, unit, MetricDataType.LONG_SUM,
                    SumData.createLongSumData(true, AggregationTemporality.CUMULATIVE, longPoints(points)));
            case GAUGE -> new FactMetricData(resource, name, unit, MetricDataType.LONG_GAUGE,
                    GaugeData.createLongGaugeData(longPoints(points)));
            case HISTOGRAM -> new FactMetricData(resource, name, unit, MetricDataType.HISTOGRAM,
                    HistogramData.create(AggregationTemporality.CUMULATIVE, histogramPoints(points)));
        };
    }

    private List<LongPointData> longPoints(List<MetricPoint> points) {
        List<LongPointData> out = new ArrayList<>(points.size());
        for (MetricPoint point : points) {
            out.add(LongPointData.create(startNanos(point), nanos(point.observedAt()), attributes(point.attributes()),
                    point.value()));
        }
        return out;
    }

    private List<HistogramPointData> histogramPoints(List<MetricPoint> points) {
        List<HistogramPointData> out = new ArrayList<>(points.size());
        for (MetricPoint point : points) {
            HistogramValue histogram = point.histogram();
            out.add(HistogramPointData.create(startNanos(point), nanos(point.observedAt()),
                    attributes(point.attributes()), histogram.sum(), false, 0.0, false, 0.0,
                    histogram.bounds(), histogram.bucketCounts()));
        }
        return out;
    }

    private MetricData stateGauge(Resource resource, Map<String, Offered> snapshot) {
        List<LongPointData> points = new ArrayList<>();
        new TreeMap<>(snapshot).forEach((pipelineId, offered) -> {
            long epoch = nanos(offered.observedAt() == null ? started : offered.observedAt());
            for (PipelineState state : PipelineState.values()) {
                Attributes attributes = Attributes.of(
                        AttributeKey.stringKey(MetricAttributes.PIPELINE_ID), pipelineId,
                        AttributeKey.stringKey(STATE_ATTRIBUTE), state.name().toLowerCase(Locale.ROOT));
                points.add(LongPointData.create(nanos(started), epoch, attributes, state == offered.state() ? 1L : 0L));
            }
        });
        return new FactMetricData(resource, STATE_METRIC, "", MetricDataType.LONG_GAUGE,
                GaugeData.createLongGaugeData(points));
    }

    /** A counter counts from its own start; a reading that has none is dated from when this exporter came up. */
    private long startNanos(MetricPoint point) {
        return nanos(point.startTime() == null ? started : point.startTime());
    }

    private static long nanos(Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    private static Attributes attributes(Map<String, String> attributes) {
        AttributesBuilder builder = Attributes.builder();
        new TreeMap<>(attributes).forEach(builder::put);
        return builder.build();
    }

    /** The attributes the overflow series carries, for a reader that wants to recognise it. */
    static Attributes overflowOnly() {
        return OVERFLOW_ONLY;
    }
}
