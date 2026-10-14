package io.tapstate.e2e;

import io.tapstate.e2e.BenchmarkPipelineFacts.Change;
import io.tapstate.e2e.BenchmarkPipelineFacts.Snapshot;
import io.tapstate.e2e.BenchmarkPipelineFacts.Type;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The pipeline facts a drained benchmark phase must carry to name its bottleneck, and the checks that keep
 * them honest.
 *
 * <p>Shape: each fact has its declared kind, unit and attribute keys, closed attributes stay inside their
 * sets, and open ones stay inside their point budget. Presence: every required fact is published, and a
 * stage-keyed one for every stage the pipeline runs. No pseudo-zero: a phase that delivered rows shows work
 * in every required accumulation. Boundaries: delivery durations are taken once per acknowledged row, so
 * their count equals the acknowledged records; a batch write duration is taken when that batch settles, so
 * once nothing is pending its count equals the issued batches; and the typed records agree with the flat
 * face. Facts no generic source can measure, connector pools and executor saturation, stay absent.
 */
final class BenchmarkPerformanceFacts {

    private BenchmarkPerformanceFacts() {
    }

    enum Presence { REQUIRED, WHEN_OCCURRED }

    /** One fact's declared shape; {@code closed} lists the allowed values of attributes that have a fixed set. */
    record Spec(String name, Type type, String unit, Set<String> keys, Map<String, Set<String>> closed,
                int maxPoints, Presence presence) {
        Spec {
            keys = Set.copyOf(keys);
            closed = Map.copyOf(closed);
        }
    }

    static final String RECORDS = "tapstate.pipeline.records";
    static final String BYTES = "tapstate.pipeline.bytes";
    static final String LAG = "tapstate.pipeline.lag";
    static final String DELIVERY = "tapstate.pipeline.record.delivery.duration";
    static final String PROCESS = "tapstate.pipeline.process.duration";
    static final String ACTIVE = "tapstate.pipeline.process.active";
    static final String STAGE_QUEUE_DEPTH = "tapstate.pipeline.stage.queue.depth";
    static final String STAGE_QUEUE_CAPACITY = "tapstate.pipeline.stage.queue.capacity";
    static final String STAGE_QUEUE_HIGH = "tapstate.pipeline.stage.queue.high_water";
    static final String OUTPUT_REFUSED = "tapstate.pipeline.stage.output.refused";
    static final String OUTPUT_RETRY = "tapstate.pipeline.stage.output.retry.duration";
    static final String BATCH_ISSUED = "tapstate.pipeline.sink.batch.issued";
    static final String BATCH_RECORDS = "tapstate.pipeline.sink.batch.records";
    static final String BATCH_MAX = "tapstate.pipeline.sink.batch.records.max";
    static final String BATCH_PENDING = "tapstate.pipeline.sink.batch.pending";
    static final String BATCH_LIMIT = "tapstate.pipeline.sink.batch.limit";
    static final String BACKPRESSURED = "tapstate.pipeline.sink.backpressured";
    static final String WRITE = "tapstate.pipeline.sink.batch.write.duration";
    static final String BACKPRESSURE = "tapstate.pipeline.sink.backpressure.duration";
    static final String STATE_CALLS = "tapstate.pipeline.state.store.operation.count";
    static final String STATE_TIME = "tapstate.pipeline.state.store.operation.duration.sum";
    static final String STATE_BYTES = "tapstate.pipeline.state.store.operation.payload.bytes";
    static final String CODEC_CALLS = "tapstate.pipeline.state.store.serialization.count";
    static final String CODEC_BYTES = "tapstate.pipeline.state.store.serialization.bytes";

    private static final String TABLE = "tapstate.table.id";
    private static final String STAGE = "stage";
    private static final String NAMESPACE = "tapstate.state.namespace";
    /** Attributes a point may carry beyond its declared keys without changing what it measures. */
    private static final Set<String> CARRIED = Set.of("tapstate.pipeline.id", "otel.metric.overflow");
    private static final Set<String> STAGES = Set.of("source", "transform", "join", "nest", "sink");
    private static final Set<String> STATE_OPERATIONS = Set.of("load", "load_all", "save", "delete");
    private static final List<String> UNWIRED = List.of("connector.pool", "pool.active", "pool.waiting",
            "executor.saturation", "executor.busy");

    private static final List<Spec> COMMON = List.of(
            new Spec(RECORDS, Type.COUNTER, "{record}", Set.of(TABLE, "direction", "op"),
                    Map.of("direction", Set.of("in", "out"),
                            "op", Set.of("insert", "update", "delete", "read", "ddl", "other")), 1_000 * 12, Presence.REQUIRED),
            new Spec(BYTES, Type.COUNTER, "By", Set.of(TABLE, "direction"), Map.of("direction", Set.of("in", "out")),
                    1_000 * 2, Presence.REQUIRED),
            new Spec(LAG, Type.GAUGE, "s", Set.of(TABLE), Map.of(), 1_000, Presence.REQUIRED),
            new Spec(DELIVERY, Type.HISTOGRAM, "s", Set.of(TABLE), Map.of(), 1_000, Presence.REQUIRED),
            new Spec(PROCESS, Type.HISTOGRAM, "s", Set.of(STAGE), Map.of(STAGE, STAGES), 5, Presence.REQUIRED),
            new Spec(ACTIVE, Type.GAUGE, "{work}", Set.of(STAGE), Map.of(STAGE, STAGES), 5, Presence.WHEN_OCCURRED),
            new Spec(STAGE_QUEUE_DEPTH, Type.GAUGE, "{item}", Set.of(STAGE), Map.of(STAGE, STAGES), 5, Presence.WHEN_OCCURRED),
            new Spec(STAGE_QUEUE_CAPACITY, Type.GAUGE, "{item}", Set.of(STAGE), Map.of(STAGE, STAGES), 5, Presence.WHEN_OCCURRED),
            new Spec(STAGE_QUEUE_HIGH, Type.GAUGE, "{item}", Set.of(STAGE), Map.of(STAGE, STAGES), 5, Presence.WHEN_OCCURRED),
            new Spec(OUTPUT_REFUSED, Type.COUNTER, "{offer}", Set.of(STAGE), Map.of(STAGE, STAGES), 5, Presence.WHEN_OCCURRED),
            new Spec(OUTPUT_RETRY, Type.HISTOGRAM, "s", Set.of(STAGE), Map.of(STAGE, STAGES), 5, Presence.WHEN_OCCURRED),
            new Spec(BATCH_ISSUED, Type.COUNTER, "{batch}", Set.of(), Map.of(), 1, Presence.REQUIRED),
            new Spec(BATCH_RECORDS, Type.COUNTER, "{record}", Set.of(), Map.of(), 1, Presence.REQUIRED),
            new Spec(BATCH_MAX, Type.GAUGE, "{record}", Set.of(), Map.of(), 1, Presence.REQUIRED),
            new Spec(BATCH_PENDING, Type.GAUGE, "{batch}", Set.of(), Map.of(), 1, Presence.REQUIRED),
            new Spec(BATCH_LIMIT, Type.GAUGE, "{batch}", Set.of(), Map.of(), 1, Presence.REQUIRED),
            new Spec(BACKPRESSURED, Type.GAUGE, "{sink}", Set.of(), Map.of(), 1, Presence.REQUIRED),
            new Spec(WRITE, Type.HISTOGRAM, "s", Set.of(), Map.of(), 1, Presence.REQUIRED),
            new Spec(BACKPRESSURE, Type.HISTOGRAM, "s", Set.of(), Map.of(), 1, Presence.WHEN_OCCURRED));

    private static final List<Spec> STATE = List.of(
            new Spec(STATE_CALLS, Type.COUNTER, "{operation}", Set.of(NAMESPACE, "state.operation", "state.outcome"),
                    Map.of("state.operation", STATE_OPERATIONS, "state.outcome", Set.of("success", "failure")),
                    1_000 * 8, Presence.REQUIRED),
            new Spec(STATE_TIME, Type.COUNTER, "ns", Set.of(NAMESPACE, "state.operation"),
                    Map.of("state.operation", STATE_OPERATIONS), 1_000 * 4, Presence.REQUIRED),
            new Spec(STATE_BYTES, Type.COUNTER, "By", Set.of(NAMESPACE, "state.operation"),
                    Map.of("state.operation", STATE_OPERATIONS), 1_000 * 4, Presence.REQUIRED),
            new Spec(CODEC_CALLS, Type.COUNTER, "{serialization}", Set.of(NAMESPACE, "state.codec"),
                    Map.of("state.codec", Set.of("encode", "decode")), 1_000 * 2, Presence.REQUIRED),
            new Spec(CODEC_BYTES, Type.COUNTER, "By", Set.of(NAMESPACE, "state.codec"),
                    Map.of("state.codec", Set.of("encode", "decode")), 1_000 * 2, Presence.REQUIRED));

    /** What one pipeline of a fixed workload runs: its business stages, and whether it keeps cold state. */
    record Role(Set<String> stages, boolean coldState) {
        Role {
            stages = Set.copyOf(stages);
        }
    }

    static Map<String, Role> roles(String workload) {
        return switch (workload) {
            case "copy" -> Map.of("bench_copy", new Role(Set.of("source", "sink"), false));
            case "stateless" -> Map.of("bench_stateless", new Role(Set.of("source", "transform", "sink"), false));
            case "stateful" -> Map.of("bench_stateful_join", new Role(Set.of("source", "join", "sink"), false),
                    "bench_stateful_nest", new Role(Set.of("source", "nest", "sink"), true));
            default -> throw new IllegalArgumentException("unknown benchmark workload " + workload);
        };
    }

    static List<Spec> specs(Role role) {
        List<Spec> specs = new ArrayList<>(COMMON);
        if (role.coldState()) { specs.addAll(STATE); }
        return List.copyOf(specs);
    }

    /**
     * Checks one drained phase of {@code workload}; {@code flatRecordsOut} is the flat face's own change of
     * acknowledged records over the same reads. Returns the checked readings; throws listing every violation.
     */
    static Map<String, Object> verify(String workload, Map<String, Snapshot> settled,
            Map<String, List<Change>> changes, long flatRecordsOut) {
        return verify(workload, roles(workload).keySet(), settled, changes, flatRecordsOut);
    }

    /**
     * The same for a phase that moves rows only through {@code active} pipelines: the others must still carry
     * no unwired reading and count toward the flat comparison, but are neither shape- nor work-checked.
     */
    static Map<String, Object> verify(String workload, Set<String> active, Map<String, Snapshot> settled,
            Map<String, List<Change>> changes, long flatRecordsOut) {
        if (active.isEmpty() || !roles(workload).keySet().containsAll(active)) {
            throw new IllegalArgumentException("active pipelines are not pipelines of " + workload + ": " + active);
        }
        List<String> violations = new ArrayList<>();
        Map<String, Object> checked = new LinkedHashMap<>();
        long typedOut = 0;
        for (var entry : new TreeMap<>(roles(workload)).entrySet()) {
            String pipeline = entry.getKey();
            Role role = entry.getValue();
            Snapshot after = settled.get(pipeline);
            List<Change> moved = changes.get(pipeline);
            if (after == null || moved == null) { violations.add(pipeline + ": no settled facts"); continue; }
            for (String name : after.facts().keySet()) {
                if (UNWIRED.stream().anyMatch(name::contains)) {
                    violations.add(pipeline + ": " + name + " has no generic source and must stay absent");
                }
            }
            long out = sum(moved, RECORDS, change -> "out".equals(change.attributes().get("direction")));
            typedOut += out;
            if (!active.contains(pipeline)) {
                checked.put(pipeline, Map.of("active", false, "recordsOut", out));
                continue;
            }
            for (Spec spec : specs(role)) { shape(pipeline, spec, after, role, violations); }
            long in = sum(moved, RECORDS, change -> "in".equals(change.attributes().get("direction")));
            long delivered = count(moved, DELIVERY, change -> true);
            long issued = sum(moved, BATCH_ISSUED, change -> true);
            long settledWrites = count(moved, WRITE, change -> true);
            long handed = sum(moved, BATCH_RECORDS, change -> true);
            if (out <= 0) { violations.add(pipeline + ": no acknowledged records in a phase that delivered rows"); }
            if (in <= 0) { violations.add(pipeline + ": no source handoffs in a phase that delivered rows"); }
            if (delivered != out) {
                violations.add(pipeline + ": " + delivered + " delivery durations for " + out
                        + " acknowledged records; a delivery is timed once per acknowledged row");
            }
            if (handed < out) { violations.add(pipeline + ": fewer records handed to batches than acknowledged"); }
            Long pending = gauge(after, BATCH_PENDING);
            if (pending == null || pending != 0) {
                violations.add(pipeline + ": batches still pending after the phase settled: " + pending);
            } else if (settledWrites != issued) {
                violations.add(pipeline + ": " + settledWrites + " batch write durations for " + issued
                        + " issued batches with none pending; a write is timed when its batch settles");
            }
            Map<String, Long> stageCalls = new TreeMap<>();
            for (String stage : role.stages()) {
                long calls = count(moved, PROCESS, change -> stage.equals(change.attributes().get(STAGE)));
                stageCalls.put(stage, calls);
                if (calls <= 0) { violations.add(pipeline + ": stage " + stage + " timed no work while rows moved through it"); }
            }
            if (role.coldState() && sum(moved, STATE_CALLS, change -> true) <= 0) {
                violations.add(pipeline + ": no cold-state calls in a phase that read cold state");
            }
            checked.put(pipeline, Map.of("active", true, "recordsIn", in, "recordsOut", out, "deliveryDurations", delivered,
                    "batchesIssued", issued, "batchWrites", settledWrites, "recordsHandedToBatches", handed,
                    "stageTimedCalls", stageCalls));
        }
        if (typedOut != flatRecordsOut) {
            violations.add("typed acknowledged records " + typedOut + " differ from the flat face's " + flatRecordsOut);
        }
        if (!violations.isEmpty()) {
            throw new AssertionError("performance facts cannot name the bottleneck of " + workload + ": " + violations);
        }
        return checked;
    }

    /**
     * The bottleneck signals of one drained phase, strongest first: each business stage's share of the timed
     * stage work, the share of the drain spent with the sink at its in-flight limit, and completed sink
     * backpressure time against the drain's length. These rank evidence; they do not prove saturation.
     */
    static List<Map<String, Object>> signals(Map<String, List<Change>> changes, long drainNanos) {
        if (drainNanos <= 0) { throw new IllegalArgumentException("a drain has a positive length"); }
        List<Map<String, Object>> signals = new ArrayList<>();
        double totalSeconds = 0;
        Map<String, Double> byStage = new TreeMap<>();
        for (var entry : changes.entrySet()) {
            for (Change change : entry.getValue()) {
                if (change.name().equals(PROCESS) && change.sum() != null) {
                    String key = entry.getKey() + "/" + change.attributes().get(STAGE);
                    byStage.merge(key, change.sum(), Double::sum);
                    totalSeconds += change.sum();
                }
            }
        }
        for (var stage : byStage.entrySet()) {
            signals.add(Map.of("signal", "STAGE_TIME_SHARE", "where", stage.getKey(),
                    "share", totalSeconds <= 0 ? 0.0 : stage.getValue() / totalSeconds, "seconds", stage.getValue()));
        }
        for (var entry : changes.entrySet()) {
            double waited = entry.getValue().stream().filter(change -> change.name().equals(BACKPRESSURE) && change.sum() != null)
                    .mapToDouble(Change::sum).sum();
            if (waited > 0) {
                signals.add(Map.of("signal", "SINK_BACKPRESSURE_TIME", "where", entry.getKey() + "/sink",
                        "share", waited * 1e9 / drainNanos, "seconds", waited));
            }
        }
        signals.sort(Comparator.comparingDouble((Map<String, Object> signal) -> (double) signal.get("share")).reversed());
        return List.copyOf(signals);
    }

    private static void shape(String pipeline, Spec spec, Snapshot after, Role role, List<String> violations) {
        var fact = after.fact(spec.name());
        if (fact.isEmpty()) {
            if (spec.presence() == Presence.REQUIRED) { violations.add(pipeline + ": " + spec.name() + " is absent"); }
            return;
        }
        var actual = fact.get();
        if (actual.type() != spec.type() || !actual.unit().equals(spec.unit())) {
            violations.add(pipeline + ": " + spec.name() + " is " + actual.type() + " in " + actual.unit()
                    + ", declared " + spec.type() + " in " + spec.unit());
        }
        if (actual.points().size() > spec.maxPoints()) {
            violations.add(pipeline + ": " + spec.name() + " exceeds its point budget " + spec.maxPoints());
        }
        for (var point : actual.points()) {
            var keys = new java.util.TreeSet<>(point.attributes().keySet());
            keys.removeAll(CARRIED);
            if (!spec.keys().containsAll(keys) || (spec.name().equals(RECORDS) ? !keys.contains("direction")
                    : !keys.equals(spec.keys()))) {
                violations.add(pipeline + ": " + spec.name() + " point carries " + keys + ", declared " + spec.keys());
            }
            spec.closed().forEach((key, allowed) -> {
                String value = point.attributes().get(key);
                if (value != null && !allowed.contains(value)) {
                    violations.add(pipeline + ": " + spec.name() + " has " + key + "=" + value + " outside its closed set");
                }
            });
        }
        if (spec.presence() == Presence.REQUIRED && spec.keys().contains(STAGE)) {
            for (String stage : role.stages()) {
                if (actual.points().stream().noneMatch(point -> stage.equals(point.attributes().get(STAGE)))) {
                    violations.add(pipeline + ": " + spec.name() + " has no " + stage + " stage");
                }
            }
        }
    }

    private static long sum(List<Change> changes, String name, java.util.function.Predicate<Change> filter) {
        return changes.stream().filter(change -> change.name().equals(name) && change.value() != null && filter.test(change))
                .mapToLong(Change::value).sum();
    }

    private static long count(List<Change> changes, String name, java.util.function.Predicate<Change> filter) {
        return changes.stream().filter(change -> change.name().equals(name) && change.count() != null && filter.test(change))
                .mapToLong(Change::count).sum();
    }

    private static Long gauge(Snapshot snapshot, String name) {
        return snapshot.fact(name).filter(fact -> fact.points().size() == 1).map(fact -> fact.points().getFirst().value())
                .orElse(null);
    }
}
