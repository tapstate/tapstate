package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Bounded receipts from actual public cumulative accounts, with each series' own accumulation start. */
final class CumulativeMetricWitness {
    private static final int MAX_POINTS = 32_768;
    private static final String CURRENT_RUN = "tapstate.pipeline.snapshot.rows.current_run";

    private CumulativeMetricWitness() { }

    record Key(String name, MetricType type, String unit, Map<String, String> attributes) {
        Key { attributes = Map.copyOf(new TreeMap<>(attributes)); }
        Map<String, Object> receipt() {
            return Map.of("name", name, "type", type.name(), "unit", unit, "attributes", attributes);
        }
    }

    record Snapshot(ObservationStore.Stored stored, Map<Key, MetricPoint> points) {
        Snapshot { points = Map.copyOf(points); }
        Map<String, Object> receipt() {
            var observation = stored.observation();
            var scope = stored.scope().orElseThrow();
            List<Map<String, Object>> captured = new ArrayList<>();
            points.forEach((key, point) -> {
                Map<String, Object> row = new LinkedHashMap<>(key.receipt());
                row.put("startTimeState", point.startTime() == null ? "UNAVAILABLE" : "KNOWN");
                if (point.startTime() != null) { row.put("startTime", point.startTime().toString()); }
                row.put("observedAt", point.observedAt().toString());
                if (key.type() == MetricType.COUNTER) { row.put("value", point.value()); }
                else {
                    var histogram = point.histogram();
                    row.put("histogram", Map.of("count", histogram.count(), "sum", histogram.sum(),
                            "bounds", histogram.bounds(), "bucketCounts", histogram.bucketCounts()));
                }
                captured.add(Map.copyOf(row));
            });
            return Map.of("pipelineId", observation.pipelineId(), "incarnation", scope.pipelineIncarnationId(),
                    "generation", scope.executionGeneration(), "state", observation.state().name(),
                    "observedAt", observation.observedAt().toString(), "points", captured,
                    "accountSource", "PUBLIC_OBSERVATION_POINTS", "nativeRawSnapshotReadingCaptured", false);
        }
    }

    static Snapshot capture(ObservationStore.Stored stored) {
        assertThat(stored.scope()).as("the cumulative receipt has an actual execution scope").isPresent();
        Map<Key, MetricPoint> points = new LinkedHashMap<>();
        for (var fact : stored.observation().facts()) {
            if (!fact.name().startsWith("tapstate.pipeline.")
                    || fact.type() != MetricType.COUNTER && fact.type() != MetricType.HISTOGRAM) { continue; }
            for (var point : fact.points()) {
                assertThat(point.attributes().get(MetricAttributes.PIPELINE_ID))
                        .as("the measured pipeline point belongs to its actual observation")
                        .isEqualTo(stored.observation().pipelineId());
                Key key = new Key(fact.name(), fact.type(), fact.unit(), point.attributes());
                assertThat(points.putIfAbsent(key, point)).as("duplicate cumulative point key %s", key).isNull();
                assertThat(points.size()).as("bounded cumulative diagnostic point count").isLessThanOrEqualTo(MAX_POINTS);
            }
        }
        return new Snapshot(stored, points);
    }

    /** Quiet frames are not filled in; active receipts wait for real target-acknowledged distributions. */
    static Snapshot running(MongoObservationStore store, String pipeline, ObservationStore.Scope scope,
            Duration wait, Snapshot previous, BenchmarkLiveReport report, String action) {
        AtomicReference<Snapshot> last = new AtomicReference<>();
        try {
            return Await.answered("actual cumulative RUNNING points for " + action, wait, () -> {
                var stored = store.readStored(pipeline);
                if (stored.isEmpty()) { return java.util.Optional.empty(); }
                Snapshot candidate = capture(stored.orElseThrow());
                last.set(candidate);
                boolean fresh = previous == null || candidate.stored().observation().observedAt()
                        .isAfter(previous.stored().observation().observedAt());
                return candidate.stored().scope().filter(scope::equals).isPresent()
                        && candidate.stored().observation().state() == PipelineState.RUNNING && fresh
                        && deliveryKnown(candidate)
                        && candidate.points().values().stream().allMatch(point -> point.startTime() != null)
                        && (previous == null || candidate.points().keySet().containsAll(previous.points().keySet()))
                        ? java.util.Optional.of(candidate) : java.util.Optional.empty();
            });
        } catch (AssertionError failure) {
            Map<String, Object> receipt = new LinkedHashMap<>();
            receipt.put("status", "MISSING_EVIDENCE");
            if (previous != null) { receipt.put("before", previous.receipt()); }
            if (last.get() != null) {
                receipt.put("lastActual", last.get().receipt());
                receipt.put("missingKnownPoints", missing(previous, last.get()));
            }
            receipt.put("diagnostic", failure.getMessage());
            record(report, action, receipt);
            throw failure;
        }
    }

    private static boolean deliveryKnown(Snapshot snapshot) {
        return snapshot.points().entrySet().stream().anyMatch(entry ->
                entry.getKey().name().equals("tapstate.pipeline.records")
                        && entry.getKey().type() == MetricType.COUNTER
                        && "out".equals(entry.getKey().attributes().get(MetricAttributes.DIRECTION))
                        && entry.getValue().value() > 0)
                && snapshot.points().entrySet().stream().anyMatch(entry ->
                entry.getKey().name().equals("tapstate.pipeline.bytes")
                        && entry.getKey().type() == MetricType.COUNTER
                        && "out".equals(entry.getKey().attributes().get(MetricAttributes.DIRECTION))
                        && entry.getValue().value() > 0)
                && snapshot.points().entrySet().stream().anyMatch(entry ->
                entry.getKey().name().equals("tapstate.pipeline.record.delivery.duration")
                        && entry.getKey().type() == MetricType.HISTOGRAM
                        && entry.getValue().histogram().count() > 0);
    }

    static void continued(BenchmarkLiveReport report, String action, Snapshot before, Snapshot after, boolean rebuilt) {
        verify(report, action, before, after, rebuilt ? "REBUILD_PUBLIC_CONTINUE" : "SAME_EXECUTION_CONTINUE", () -> {
            assertThat(after.stored().scope().orElseThrow().pipelineIncarnationId())
                    .isEqualTo(before.stored().scope().orElseThrow().pipelineIncarnationId());
            if (rebuilt) {
                assertThat(after.stored().scope().orElseThrow().executionGeneration())
                        .isGreaterThan(before.stored().scope().orElseThrow().executionGeneration());
            } else { assertThat(after.stored().scope()).isEqualTo(before.stored().scope()); }
            assertThat(missing(before, after)).as("known public cumulative points retained after %s", action).isEmpty();
            before.points().forEach((key, old) -> continuedPoint(key, old, after.points().get(key)));
        });
    }

    /** A paused frame may omit quiet producers; any offered known point must still belong to its account. */
    static void paused(BenchmarkLiveReport report, String action, Snapshot before, Snapshot paused) {
        verify(report, action, before, paused, "QUIET_PAUSED_FRAME", () -> {
            assertThat(paused.stored().observation().state()).isEqualTo(PipelineState.PAUSED);
            assertThat(paused.stored().scope()).isEqualTo(before.stored().scope());
            before.points().forEach((key, old) -> {
                MetricPoint offered = paused.points().get(key);
                if (offered != null) { continuedPoint(key, old, offered); }
            });
        });
    }

    static void reset(BenchmarkLiveReport report, String action, Snapshot before, Snapshot after) {
        verify(report, action, before, after, "FRESH_PUBLIC_ACCOUNT", () -> {
            assertThat(after.stored().observation().state()).isEqualTo(PipelineState.RUNNING);
            assertThat(after.stored().scope().orElseThrow().executionGeneration())
                    .isGreaterThan(before.stored().scope().orElseThrow().executionGeneration());
            assertThat(after.points()).as("a fresh account has actual offered cumulative points").isNotEmpty();
            after.points().forEach((key, point) -> {
                assertThat(point.startTime()).as("fresh own start for %s", key)
                        .isNotNull()
                        .isAfter(before.stored().observation().observedAt());
                MetricPoint old = before.points().get(key);
                if (old != null) { assertThat(point.startTime()).as("old start not reused for %s", key).isAfter(old.startTime()); }
            });
        });
    }

    private static void continuedPoint(Key key, MetricPoint before, MetricPoint after) {
        assertThat(before.startTime()).as("known original public start for %s", key).isNotNull();
        assertThat(after.startTime()).as("retained own public start for %s", key).isEqualTo(before.startTime());
        if (key.type() == MetricType.COUNTER) {
            assertThat(after.value()).as("nondecreasing public counter %s", key).isGreaterThanOrEqualTo(before.value());
        } else {
            var old = before.histogram(); var fresh = after.histogram();
            assertThat(fresh.bounds()).as("unchanged histogram bounds for %s", key).isEqualTo(old.bounds());
            assertThat(fresh.count()).as("nondecreasing histogram count for %s", key).isGreaterThanOrEqualTo(old.count());
            assertThat(fresh.sum()).as("nondecreasing histogram sum for %s", key).isGreaterThanOrEqualTo(old.sum());
            for (int bucket = 0; bucket < old.bucketCounts().size(); bucket++) {
                assertThat(fresh.bucketCounts().get(bucket)).as("nondecreasing histogram bucket %d for %s", bucket, key)
                        .isGreaterThanOrEqualTo(old.bucketCounts().get(bucket));
            }
        }
    }

    private static List<Map<String, Object>> missing(Snapshot before, Snapshot after) {
        return before == null ? List.of() : before.points().keySet().stream()
                .filter(key -> !after.points().containsKey(key)).map(Key::receipt).toList();
    }

    private static void verify(BenchmarkLiveReport report, String action, Snapshot before, Snapshot after,
            String policy, Runnable assertion) {
        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("policy", policy); receipt.put("before", before.receipt()); receipt.put("after", after.receipt());
        receipt.put("absentKnownPoints", missing(before, after));
        // Raw capture progress starts with its physical capture. A qualified public CONTINUE retains its public account.
        receipt.put("currentRunBoundary", Map.of("instrument", CURRENT_RUN, "rawSource", "CURRENT_PHYSICAL_CAPTURE",
                "publicPolicy", "QUALIFIED_CONTINUE_OR_FRESH_RESET", "rawNativeStartInferredFromPublicStart", false));
        try { assertion.run(); receipt.put("status", "VERIFIED"); }
        catch (AssertionError failure) {
            receipt.put("status", "FAILED"); receipt.put("diagnostic", failure.getMessage());
            record(report, action, receipt); throw failure;
        }
        record(report, action, receipt);
    }

    private static void record(BenchmarkLiveReport report, String action, Map<String, Object> receipt) {
        if (report != null) { report.addFork(Map.of("action", "cumulative-" + action, "receipt", receipt)); }
    }
}
