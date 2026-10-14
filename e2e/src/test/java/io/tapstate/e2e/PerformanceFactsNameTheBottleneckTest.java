package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The facts each fixed workload's drained phase must carry, read through the same wire shape the metrics face
 * renders. A healthy pair of reads for copy, stateless and stateful passes; each mutation below moves one
 * measure to a wrong boundary, fills it with zero, drops a stage, changes its unit or attributes, or invents
 * an unwired reading, and must be refused with the reason that discriminates it.
 */
class PerformanceFactsNameTheBottleneckTest {

    private static final int ROWS = 48_000;
    private static final int BATCHES = 480;
    private static final String START = "2026-10-14T00:00:00Z";
    private static final String RESTART = "2026-10-14T00:05:00Z";

    @Test
    void eachFixedWorkloadCarriesTheFactsThatNameItsBottleneck() {
        for (String workload : List.of("copy", "stateless", "stateful")) {
            var reads = healthy(workload);
            var checked = BenchmarkPerformanceFacts.verify(workload, reads.settled(), reads.changes(), reads.flatOut());
            assertThat(checked).as(workload).containsOnlyKeys(BenchmarkPerformanceFacts.roles(workload).keySet());
        }
    }

    @Test
    void aDeliveryTimedOncePerBatchInsteadOfPerRowIsRefused() {
        assertRefused("copy", face -> face.histogram(BenchmarkPerformanceFacts.DELIVERY, "s",
                Map.of("tapstate.table.id", "orders"), START, 10_000 + BATCHES), "timed once per acknowledged row");
    }

    @Test
    void aBatchWriteCountThatMissesSettledBatchesIsRefused() {
        assertRefused("copy", face -> face.histogram(BenchmarkPerformanceFacts.WRITE, "s", Map.of(), START, 100 + BATCHES - 3),
                "a write is timed when its batch settles");
    }

    @Test
    void aStageThatReportsZeroWorkWhileRowsCrossedItIsRefused() {
        assertRefused("stateless", face -> face.histogram(BenchmarkPerformanceFacts.PROCESS, "s",
                Map.of("stage", "transform"), START, 100), "stage transform timed no work");
    }

    @Test
    void aMissingRequiredStageIsRefused() {
        assertRefused("stateless", face -> face.drop(BenchmarkPerformanceFacts.PROCESS, Map.of("stage", "transform")),
                "has no transform stage");
    }

    @Test
    void aWrongUnitIsRefusedAndSoIsAUnitThatChangesBetweenReads() {
        var reads = healthy("copy", face -> face.relabel(BenchmarkPerformanceFacts.LAG, "ms"), true);
        assertThatThrownBy(() -> BenchmarkPerformanceFacts.verify("copy", reads.settled(), reads.changes(), reads.flatOut()))
                .hasMessageContaining("declared GAUGE in s");
        assertThatThrownBy(() -> healthy("copy", face -> face.relabel(BenchmarkPerformanceFacts.LAG, "ms")))
                .hasMessageContaining("changed its type or unit between two reads");
    }

    @Test
    void aStageFactCarryingAnOpenAttributeIsRefused() {
        assertRefused("copy", face -> face.histogram(BenchmarkPerformanceFacts.PROCESS, "s",
                Map.of("stage", "sink", "tapstate.table.id", "orders"), START, 3), "point carries");
    }

    @Test
    void anInventedConnectorPoolReadingIsRefused() {
        assertRefused("copy", face -> face.gauge("tapstate.pipeline.connector.pool.active", "{connection}", Map.of(), 0),
                "must stay absent");
    }

    @Test
    void noColdStateCallsInTheNestPipelineIsRefused() {
        assertRefused("stateful", face -> {
            if (face.pipeline.equals("bench_stateful_nest")) {
                for (String operation : List.of("load", "save")) {
                    face.counter(BenchmarkPerformanceFacts.STATE_CALLS, "{operation}", Map.of("tapstate.state.namespace", "items",
                            "state.operation", operation, "state.outcome", "success"), START, 1_000);
                }
            }
        }, "no cold-state calls");
    }

    @Test
    void aPipelineWithNothingToDoInAPhaseIsNotRequiredToShowWork() {
        var reads = healthy("stateful", face -> {
            if (face.pipeline.equals("bench_stateful_join")) { face.idle(); }
        });
        assertThatThrownBy(() -> BenchmarkPerformanceFacts.verify("stateful", reads.settled(), reads.changes(), reads.flatOut()))
                .hasMessageContaining("bench_stateful_join: no acknowledged records");
        assertThat(BenchmarkPerformanceFacts.verify("stateful", Set.of("bench_stateful_nest"), reads.settled(), reads.changes(),
                reads.flatOut())).containsEntry("bench_stateful_join", Map.of("active", false, "recordsOut", 0L));
    }

    @Test
    void typedAndFlatAcknowledgedRecordsMustAgree() {
        var reads = healthy("copy");
        assertThatThrownBy(() -> BenchmarkPerformanceFacts.verify("copy", reads.settled(), reads.changes(), reads.flatOut() - 1))
                .hasMessageContaining("differ from the flat face");
    }

    @Test
    void anAccountThatRestartedReportsItsWholeLaterValue() {
        var held = new Face("bench_copy").counter(BenchmarkPerformanceFacts.BATCH_ISSUED, "{batch}", Map.of(), START, 900).read(1);
        var settled = new Face("bench_copy").counter(BenchmarkPerformanceFacts.BATCH_ISSUED, "{batch}", Map.of(), RESTART, 40).read(2);
        assertThat(BenchmarkPipelineFacts.changes(held, settled)).singleElement().satisfies(change -> {
            assertThat(change.accountRestarted()).isTrue();
            assertThat(change.value()).isEqualTo(40);
        });
    }

    @Test
    void theBusiestStageIsNamedFirst() {
        var reads = healthy("stateless");
        var signals = BenchmarkPerformanceFacts.signals(reads.changes(), 60_000_000_000L);
        assertThat(signals.getFirst()).containsEntry("where", "bench_stateless/sink");
        assertThat(signals).anySatisfy(signal -> assertThat(signal).containsEntry("signal", "SINK_BACKPRESSURE_TIME"));
        assertThat(BenchmarkPipelineFacts.quantileUpperBound(List.of(0.1, 1.0), List.of(98L, 1L, 1L), 0.99))
                .hasValue(1.0);
        assertThat(BenchmarkPipelineFacts.quantileUpperBound(List.of(0.1, 1.0), List.of(98L, 1L, 1L), 1.0))
                .hasValue(Double.POSITIVE_INFINITY);
    }

    @Test
    void aMetricsAnswerWithoutAnObservationIsAbsentRatherThanZero() {
        String refusal = "{\"code\":\"" + io.tapstate.control.core.MonitorError.NO_OBSERVATION.code() + "\"}";
        assertThat(BenchmarkPipelineFacts.parse("bench_copy", 1, 404, refusal)).isEmpty();
        assertThatThrownBy(() -> new Face("bench_copy").raw(BenchmarkPerformanceFacts.WRITE, "histogram", "s",
                Map.of("count", 3L, "sum", 0.5, "bounds", List.of(0.1), "bucketCounts", List.of(1L, 1L), "startTime", START)).read(1))
                .hasMessageContaining("do not add up");
    }

    private static void assertRefused(String workload, Consumer<Face> mutation, String reason) {
        var reads = healthy(workload, mutation);
        assertThatThrownBy(() -> BenchmarkPerformanceFacts.verify(workload, reads.settled(), reads.changes(), reads.flatOut()))
                .isInstanceOf(AssertionError.class).hasMessageContaining(reason);
    }

    private record Reads(Map<String, BenchmarkPipelineFacts.Snapshot> settled,
                         Map<String, List<BenchmarkPipelineFacts.Change>> changes, long flatOut) { }

    private static Reads healthy(String workload) {
        return healthy(workload, face -> { });
    }

    private static Reads healthy(String workload, Consumer<Face> mutation) {
        return healthy(workload, mutation, false);
    }

    /** A held read before resume and a settled read after the drain; the mutation edits the settled read, or both. */
    private static Reads healthy(String workload, Consumer<Face> mutation, boolean bothReads) {
        Map<String, BenchmarkPipelineFacts.Snapshot> settled = new LinkedHashMap<>();
        Map<String, List<BenchmarkPipelineFacts.Change>> changes = new LinkedHashMap<>();
        long flatOut = 0;
        for (var role : new TreeMap<>(BenchmarkPerformanceFacts.roles(workload)).entrySet()) {
            Face held = face(role.getKey(), role.getValue(), 0);
            Face after = face(role.getKey(), role.getValue(), 1);
            mutation.accept(after);
            if (bothReads) { mutation.accept(held); }
            var before = held.read(1);
            var read = after.read(2);
            settled.put(role.getKey(), read);
            changes.put(role.getKey(), BenchmarkPipelineFacts.changes(before, read));
            flatOut += read.flat().get("records.out") - before.flat().get("records.out");
        }
        return new Reads(settled, changes, flatOut);
    }

    /** One pipeline's face after {@code drains} drained phases of the fixed size. */
    private static Face face(String pipeline, BenchmarkPerformanceFacts.Role role, int drains) {
        long rows = (long) ROWS * drains;
        long batches = (long) BATCHES * drains;
        Face face = new Face(pipeline);
        face.flat("records.out", 10_000 + rows);
        face.counter(BenchmarkPerformanceFacts.RECORDS, "{record}", Map.of("tapstate.table.id", "orders",
                "direction", "in", "op", "update"), START, 10_000 + rows);
        face.counter(BenchmarkPerformanceFacts.RECORDS, "{record}", Map.of("tapstate.table.id", "orders",
                "direction", "out", "op", "update"), START, 10_000 + rows);
        face.counter(BenchmarkPerformanceFacts.BYTES, "By", Map.of("tapstate.table.id", "orders", "direction", "in"),
                START, 640_000 + rows * 64);
        face.counter(BenchmarkPerformanceFacts.BYTES, "By", Map.of("tapstate.table.id", "orders", "direction", "out"),
                START, 640_000 + rows * 64);
        face.gauge(BenchmarkPerformanceFacts.LAG, "s", Map.of("tapstate.table.id", "orders"), 1);
        face.histogram(BenchmarkPerformanceFacts.DELIVERY, "s", Map.of("tapstate.table.id", "orders"), START, 10_000 + rows);
        for (String stage : role.stages()) {
            double busy = stage.equals("sink") ? 40.0 : stage.equals("transform") ? 12.0 : 4.0;
            face.histogram(BenchmarkPerformanceFacts.PROCESS, "s", Map.of("stage", stage), START, 100 + rows / 10, busy * drains);
        }
        face.counter(BenchmarkPerformanceFacts.BATCH_ISSUED, "{batch}", Map.of(), START, 100 + batches);
        face.counter(BenchmarkPerformanceFacts.BATCH_RECORDS, "{record}", Map.of(), START, 10_000 + rows);
        face.gauge(BenchmarkPerformanceFacts.BATCH_MAX, "{record}", Map.of(), 100);
        face.gauge(BenchmarkPerformanceFacts.BATCH_PENDING, "{batch}", Map.of(), 0);
        face.gauge(BenchmarkPerformanceFacts.BATCH_LIMIT, "{batch}", Map.of(), 4);
        face.gauge(BenchmarkPerformanceFacts.BACKPRESSURED, "{sink}", Map.of(), 0);
        face.histogram(BenchmarkPerformanceFacts.WRITE, "s", Map.of(), START, 100 + batches);
        if (drains > 0) {
            face.histogram(BenchmarkPerformanceFacts.BACKPRESSURE, "s", Map.of(), START, 30, 9.0);
        }
        if (role.coldState()) {
            for (String operation : List.of("load", "save")) {
                face.counter(BenchmarkPerformanceFacts.STATE_CALLS, "{operation}", Map.of("tapstate.state.namespace", "items",
                        "state.operation", operation, "state.outcome", "success"), START, 1_000 + rows);
                face.counter(BenchmarkPerformanceFacts.STATE_TIME, "ns", Map.of("tapstate.state.namespace", "items",
                        "state.operation", operation), START, 9_000_000 + rows * 1_000);
                face.counter(BenchmarkPerformanceFacts.STATE_BYTES, "By", Map.of("tapstate.state.namespace", "items",
                        "state.operation", operation), START, 64_000 + rows * 64);
            }
            for (String codec : List.of("encode", "decode")) {
                face.counter(BenchmarkPerformanceFacts.CODEC_CALLS, "{serialization}", Map.of("tapstate.state.namespace", "items",
                        "state.codec", codec), START, 1_000 + rows);
                face.counter(BenchmarkPerformanceFacts.CODEC_BYTES, "By", Map.of("tapstate.state.namespace", "items",
                        "state.codec", codec), START, 64_000 + rows * 64);
            }
        }
        return face;
    }

    /** A metrics face under construction, rendered to the wire's JSON shape and parsed back. */
    private static final class Face {
        private final String pipeline;
        private final Map<String, Long> flat = new TreeMap<>();
        private final Map<String, Map<String, Object>> facts = new LinkedHashMap<>();

        Face(String pipeline) {
            this.pipeline = pipeline;
        }

        Face flat(String name, long value) {
            flat.put(name, value);
            return this;
        }

        Face counter(String name, String unit, Map<String, String> attributes, String start, long value) {
            return point(name, "counter", unit, attributes, Map.of("startTime", start, "value", value));
        }

        Face gauge(String name, String unit, Map<String, String> attributes, long value) {
            return point(name, "gauge", unit, attributes, Map.of("value", value));
        }

        Face histogram(String name, String unit, Map<String, String> attributes, String start, long count) {
            return histogram(name, unit, attributes, start, count, count * 0.01);
        }

        Face histogram(String name, String unit, Map<String, String> attributes, String start, long count, double sum) {
            return point(name, "histogram", unit, attributes, Map.of("startTime", start, "count", count, "sum", sum,
                    "bounds", List.of(0.01, 0.1, 1.0), "bucketCounts", List.of(count, 0L, 0L, 0L)));
        }

        Face raw(String name, String type, String unit, Map<String, Object> values) {
            return point(name, type, unit, Map.of(), values);
        }

        Face drop(String name, Map<String, String> attributes) {
            @SuppressWarnings("unchecked")
            var points = (List<Map<String, Object>>) facts.get(name).get("points");
            points.removeIf(point -> point.get("attributes").equals(new TreeMap<>(attributes)));
            return this;
        }

        /** Turns this settled read back into the held one: nothing moved through the pipeline. */
        Face idle() {
            Face held = face(pipeline, BenchmarkPerformanceFacts.roles(pipeline.startsWith("bench_stateful") ? "stateful"
                    : pipeline.substring("bench_".length())).get(pipeline), 0);
            flat.clear();
            flat.putAll(held.flat);
            facts.clear();
            facts.putAll(held.facts);
            return this;
        }

        Face relabel(String name, String unit) {
            facts.get(name).put("unit", unit);
            return this;
        }

        private Face point(String name, String type, String unit, Map<String, String> attributes, Map<String, Object> values) {
            var fact = facts.computeIfAbsent(name, ignored -> {
                Map<String, Object> created = new LinkedHashMap<>();
                created.put("name", name);
                created.put("type", type);
                created.put("unit", unit);
                created.put("points", new ArrayList<Map<String, Object>>());
                return created;
            });
            @SuppressWarnings("unchecked")
            var points = (List<Map<String, Object>>) fact.get("points");
            var sorted = new TreeMap<>(attributes);
            points.removeIf(point -> point.get("attributes").equals(sorted));
            Map<String, Object> point = new LinkedHashMap<>(values);
            point.put("attributes", sorted);
            point.put("observedAt", "2026-10-14T00:10:00Z");
            points.add(point);
            return this;
        }

        BenchmarkPipelineFacts.Snapshot read(long at) {
            String body = JsonWriter.write(Map.of("pipelineId", pipeline, "metrics", flat,
                    "facts", new ArrayList<>(facts.values())));
            return BenchmarkPipelineFacts.parse(pipeline, at, 200, body).orElseThrow();
        }
    }
}
