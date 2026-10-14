package io.tapstate.e2e;

import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonReader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.TreeMap;

/**
 * The typed facts one pipeline's metrics face carries at one read, and what changed between two reads.
 *
 * <p>A counter or histogram is differenced only within one account. A point whose start time moved began
 * a new account after the earlier read, so its whole later value is the change, and the change says that
 * its account restarted. Gauges are never differenced: the later reading is reported as it stands. A point
 * that is absent stays absent; nothing here turns an unmeasured reading into zero.
 */
final class BenchmarkPipelineFacts {

    private BenchmarkPipelineFacts() {
    }

    enum Type { COUNTER, GAUGE, HISTOGRAM }

    /** One point as published: a single value, or a distribution with one more bucket count than bounds. */
    record Point(Map<String, String> attributes, String startTime, String observedAt, Long value,
                 Long count, Double sum, List<Double> bounds, List<Long> bucketCounts) {
        Point {
            attributes = Map.copyOf(attributes);
            bounds = bounds == null ? null : List.copyOf(bounds);
            bucketCounts = bucketCounts == null ? null : List.copyOf(bucketCounts);
            boolean distribution = count != null || sum != null || bounds != null || bucketCounts != null;
            if (distribution == (value != null)) {
                throw new AssertionError("a metric point carries neither or both of a value and a distribution");
            }
            if (distribution && (count == null || sum == null || bounds == null || bucketCounts == null
                    || bucketCounts.size() != bounds.size() + 1
                    || bucketCounts.stream().mapToLong(Long::longValue).sum() != count)) {
                throw new AssertionError("a histogram point is incomplete or its buckets do not add up to its count: "
                        + attributes + " count=" + count + " sum=" + sum + " bounds=" + bounds + " buckets=" + bucketCounts);
            }
        }

        boolean distribution() {
            return count != null;
        }
    }

    record Fact(String name, Type type, String unit, List<Point> points) {
        Fact {
            points = List.copyOf(points);
            for (Point point : points) {
                if (point.distribution() != (type == Type.HISTOGRAM)) {
                    throw new AssertionError(name + " carries a point that does not match its type " + type);
                }
                if (type != Type.GAUGE && point.startTime() == null) {
                    throw new AssertionError(name + " accumulates without a start time");
                }
            }
        }
    }

    /** One read of one pipeline's metrics face; the facts are keyed by their canonical name. */
    record Snapshot(String pipelineId, long readAtNanos, Map<String, Long> flat, Map<String, Fact> facts) {
        Snapshot {
            flat = Map.copyOf(flat);
            facts = Map.copyOf(facts);
        }

        Optional<Fact> fact(String name) {
            return Optional.ofNullable(facts.get(name));
        }
    }

    /**
     * What one point did between two reads. For a counter, {@code value} is the change; for a histogram,
     * {@code count}, {@code sum} and {@code bucketCounts} are; for a gauge, {@code value} is the later reading.
     */
    record Change(String name, Type type, String unit, Map<String, String> attributes, boolean accountRestarted,
                  Long value, Long count, Double sum, List<Double> bounds, List<Long> bucketCounts) {
        Change {
            attributes = Map.copyOf(attributes);
            bounds = bounds == null ? null : List.copyOf(bounds);
            bucketCounts = bucketCounts == null ? null : List.copyOf(bucketCounts);
        }

        Map<String, Object> evidence() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("name", name);
            out.put("type", type.name());
            out.put("unit", unit);
            out.put("attributes", new TreeMap<>(attributes));
            out.put("accountRestarted", accountRestarted);
            if (value != null) { out.put("value", value); }
            if (count != null) {
                out.put("count", count);
                out.put("sum", sum);
                out.put("bounds", bounds);
                out.put("bucketCounts", bucketCounts);
            }
            return out;
        }
    }

    /** Empty when the pipeline has published no observation yet; any other answer but 200 is a failure. */
    static Optional<Snapshot> parse(String pipelineId, long readAtNanos, int status, String body) {
        Object parsed = JsonReader.parse(body);
        if (status == 404 && parsed instanceof Map<?, ?> refusal
                && MonitorError.NO_OBSERVATION.code().equals(refusal.get("code"))) {
            return Optional.empty();
        }
        if (status != 200 || !(parsed instanceof Map<?, ?> answer)) {
            throw new AssertionError("could not read the metrics of " + pipelineId + ": HTTP " + status + " - " + body);
        }
        if (!pipelineId.equals(answer.get("pipelineId")) || !(answer.get("metrics") instanceof Map<?, ?> metrics)) {
            throw new AssertionError("metrics answer is not the flat face of " + pipelineId + ": " + body);
        }
        Map<String, Long> flat = new TreeMap<>();
        metrics.forEach((name, value) -> flat.put(String.valueOf(name), number(value, "flat metric " + name)));
        Map<String, Fact> facts = new TreeMap<>();
        Object listed = answer.get("facts");
        if (listed != null) {
            if (!(listed instanceof List<?> rows)) { throw new AssertionError("metrics facts are not a list: " + body); }
            for (Object row : rows) {
                Fact fact = fact(row);
                if (facts.put(fact.name(), fact) != null) {
                    throw new AssertionError("metrics answer repeats the fact " + fact.name());
                }
            }
        }
        return Optional.of(new Snapshot(pipelineId, readAtNanos, flat, facts));
    }

    /** Every point of every fact present in {@code after}, compared with the same point in {@code before}. */
    static List<Change> changes(Snapshot before, Snapshot after) {
        if (!before.pipelineId().equals(after.pipelineId()) || after.readAtNanos() < before.readAtNanos()) {
            throw new AssertionError("metric reads compared across pipelines or backward in time");
        }
        List<Change> changes = new ArrayList<>();
        for (Fact later : after.facts().values()) {
            Fact earlier = before.facts().get(later.name());
            if (earlier != null && (earlier.type() != later.type() || !earlier.unit().equals(later.unit()))) {
                throw new AssertionError(later.name() + " changed its type or unit between two reads");
            }
            for (Point point : later.points()) {
                Point previous = earlier == null ? null : earlier.points().stream()
                        .filter(candidate -> candidate.attributes().equals(point.attributes())).findFirst().orElse(null);
                changes.add(change(later, point, previous));
            }
        }
        return List.copyOf(changes);
    }

    private static Change change(Fact fact, Point point, Point previous) {
        if (fact.type() == Type.GAUGE) {
            return new Change(fact.name(), fact.type(), fact.unit(), point.attributes(), false,
                    point.value(), null, null, null, null);
        }
        boolean restarted = previous == null || !previous.startTime().equals(point.startTime());
        if (fact.type() == Type.COUNTER) {
            long delta = restarted ? point.value() : point.value() - previous.value();
            if (delta < 0) { throw new AssertionError(fact.name() + " moved backward within one account"); }
            return new Change(fact.name(), fact.type(), fact.unit(), point.attributes(), restarted,
                    delta, null, null, null, null);
        }
        if (!restarted && !previous.bounds().equals(point.bounds())) {
            throw new AssertionError(fact.name() + " changed its bucket bounds within one account");
        }
        List<Long> buckets = new ArrayList<>(point.bucketCounts().size());
        for (int i = 0; i < point.bucketCounts().size(); i++) {
            long bucket = restarted ? point.bucketCounts().get(i) : point.bucketCounts().get(i) - previous.bucketCounts().get(i);
            if (bucket < 0) { throw new AssertionError(fact.name() + " lost observations within one account"); }
            buckets.add(bucket);
        }
        long count = restarted ? point.count() : point.count() - previous.count();
        double sum = restarted ? point.sum() : point.sum() - previous.sum();
        return new Change(fact.name(), fact.type(), fact.unit(), point.attributes(), restarted,
                null, count, sum, point.bounds(), buckets);
    }

    /**
     * The upper bound of the bucket holding quantile {@code q} of a distribution's change: an upper bound on
     * that quantile, never an interpolation inside a bucket. Positive infinity when it falls past the last
     * bound; empty when nothing was observed.
     */
    static OptionalDouble quantileUpperBound(List<Double> bounds, List<Long> bucketCounts, double q) {
        if (q <= 0 || q > 1 || bucketCounts.size() != bounds.size() + 1) {
            throw new IllegalArgumentException("a quantile needs 0 < q <= 1 and one bucket count more than bounds");
        }
        long total = bucketCounts.stream().mapToLong(Long::longValue).sum();
        if (total == 0) { return OptionalDouble.empty(); }
        long rank = (long) Math.ceil(q * total);
        long seen = 0;
        for (int i = 0; i < bucketCounts.size(); i++) {
            seen += bucketCounts.get(i);
            if (seen >= rank) {
                return OptionalDouble.of(i < bounds.size() ? bounds.get(i) : Double.POSITIVE_INFINITY);
            }
        }
        throw new AssertionError("bucket counts do not reach their own total");
    }

    private static Fact fact(Object row) {
        if (!(row instanceof Map<?, ?> fact) || !(fact.get("name") instanceof String name)
                || !(fact.get("type") instanceof String type) || !(fact.get("unit") instanceof String unit)
                || !(fact.get("points") instanceof List<?> points)) {
            throw new AssertionError("a metric fact lacks its name, type, unit or points: " + row);
        }
        List<Point> parsed = new ArrayList<>(points.size());
        for (Object raw : points) {
            if (!(raw instanceof Map<?, ?> point) || !(point.get("attributes") instanceof Map<?, ?> attributes)) {
                throw new AssertionError(name + " has a point without attributes: " + raw);
            }
            Map<String, String> labels = new TreeMap<>();
            attributes.forEach((key, value) -> labels.put(String.valueOf(key), String.valueOf(value)));
            try {
                parsed.add(new Point(labels, text(point.get("startTime")), text(point.get("observedAt")),
                        optionalNumber(point.get("value"), name), optionalNumber(point.get("count"), name),
                        point.get("sum") == null ? null : decimal(point.get("sum"), name),
                        point.get("bounds") == null ? null : decimals(point.get("bounds"), name),
                        point.get("bucketCounts") == null ? null : numbers(point.get("bucketCounts"), name)));
            } catch (AssertionError invalid) {
                throw new AssertionError(name + ": " + invalid.getMessage(), invalid);
            }
        }
        Type kind = switch (type) {
            case "counter" -> Type.COUNTER;
            case "gauge" -> Type.GAUGE;
            case "histogram" -> Type.HISTOGRAM;
            default -> throw new AssertionError(name + " has an unknown kind " + type);
        };
        return new Fact(name, kind, unit, parsed);
    }

    private static String text(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Long optionalNumber(Object value, String name) {
        return value == null ? null : number(value, name);
    }

    private static long number(Object value, String name) {
        if (!(value instanceof Long number)) { throw new AssertionError(name + " carries a non-integer count " + value); }
        return number;
    }

    private static double decimal(Object value, String name) {
        if (!(value instanceof Number number)) { throw new AssertionError(name + " carries a non-numeric sum " + value); }
        return number.doubleValue();
    }

    private static List<Double> decimals(Object value, String name) {
        if (!(value instanceof List<?> items)) { throw new AssertionError(name + " bounds are not a list"); }
        return items.stream().map(item -> decimal(item, name)).toList();
    }

    private static List<Long> numbers(Object value, String name) {
        if (!(value instanceof List<?> items)) { throw new AssertionError(name + " bucket counts are not a list"); }
        return items.stream().map(item -> number(item, name)).toList();
    }
}
