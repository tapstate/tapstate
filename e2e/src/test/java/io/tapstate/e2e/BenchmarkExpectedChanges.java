package io.tapstate.e2e;

import org.bson.Document;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts the frozen measured SQL batches into the physical target changes the external Mongo
 * observer must see. This is deliberately a parser of this fixture's narrow SQL shapes, so a change to
 * a batch cannot silently leave an old target expectation in place.
 */
final class BenchmarkExpectedChanges {

    private static final Pattern UPDATE = Pattern.compile(
            "^UPDATE ([a-z_]+) SET .+ WHERE id BETWEEN (\\d+) AND (\\d+)( AND id % 2 = 0)?$");
    private static final Pattern NEST_ITEM = Pattern.compile(
            "\\((\\d+),(\\d+),'(hot|cold)-(\\d+)'\\)");

    private BenchmarkExpectedChanges() {
    }

    record TargetPlan(
            BenchmarkWorkloadDefinitions.TargetExpectation target,
            Function<Document, String> keyOf,
            List<List<BenchmarkMongoDeliveryObserver.ExpectedChange>> batches) {
        TargetPlan {
            batches = batches.stream().map(List::copyOf).toList();
        }

        List<BenchmarkMongoDeliveryObserver.ExpectedChange> forBatch(int index) {
            return batches.get(index);
        }

        long totalChanges() {
            return batches.stream().mapToLong(List::size).sum();
        }
    }

    /** One plan per target, with batch indices aligned exactly to {@code phase.batches()}. */
    static List<TargetPlan> forPhase(BenchmarkWorkloadDefinitions.Workload workload,
                                     BenchmarkWorkloadDefinitions.Phase phase) {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(phase, "phase");
        if (!workload.phase(phase.id()).equals(phase)) {
            throw new IllegalArgumentException("phase does not belong to workload " + workload.id());
        }
        if (!phase.measured()) {
            throw new IllegalArgumentException("non-measured phase cannot register target deliveries: "
                    + phase.id());
        }

        Map<BenchmarkWorkloadDefinitions.Projection,
                List<List<BenchmarkMongoDeliveryObserver.ExpectedChange>>> byProjection =
                new EnumMap<>(BenchmarkWorkloadDefinitions.Projection.class);
        for (BenchmarkWorkloadDefinitions.TargetExpectation target : phase.targets()) {
            if (byProjection.put(target.projection(), new ArrayList<>()) != null) {
                throw new AssertionError("two targets share projection " + target.projection());
            }
        }

        for (List<String> sourceBatch : phase.batches()) {
            Map<BenchmarkWorkloadDefinitions.Projection,
                    List<BenchmarkMongoDeliveryObserver.ExpectedChange>> targetBatch =
                    new EnumMap<>(BenchmarkWorkloadDefinitions.Projection.class);
            byProjection.keySet().forEach(projection -> targetBatch.put(projection, new ArrayList<>()));
            for (String sql : sourceBatch) {
                Decoded decoded = decode(workload.id(), phase.stage(), sql);
                List<BenchmarkMongoDeliveryObserver.ExpectedChange> expected =
                        targetBatch.get(decoded.projection());
                if (expected == null) {
                    throw new AssertionError("SQL writes a target absent from phase " + phase.id()
                            + ": " + decoded.projection());
                }
                expected.addAll(decoded.changes());
            }
            targetBatch.forEach((projection, expected) ->
                    byProjection.get(projection).add(List.copyOf(expected)));
        }

        List<TargetPlan> plans = new ArrayList<>();
        long total = 0;
        for (BenchmarkWorkloadDefinitions.TargetExpectation target : phase.targets()) {
            TargetPlan plan = new TargetPlan(target, keyOf(target.projection()),
                    byProjection.get(target.projection()));
            plans.add(plan);
            total += plan.totalChanges();
        }
        if (total != phase.expectedLogicalOutputChanges()) {
            throw new AssertionError("measured target changes for " + workload.id() + "/" + phase.id()
                    + " are " + total + ", expected " + phase.expectedLogicalOutputChanges());
        }
        return List.copyOf(plans);
    }

    static TargetPlan forTarget(BenchmarkWorkloadDefinitions.Workload workload,
                                BenchmarkWorkloadDefinitions.Phase phase,
                                BenchmarkWorkloadDefinitions.TargetExpectation target) {
        return forPhase(workload, phase).stream().filter(plan -> plan.target().equals(target))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(
                        "target is absent from phase " + phase.id() + ": " + target));
    }

    /**
     * Hot-state is setup, not a measured delivery window: its child rows all touch one root and may be
     * coalesced into fewer Mongo writes. This summary describes logical source touches only.
     */
    static Map<String, Long> hotStateLogicalRootTouches(BenchmarkWorkloadDefinitions.Workload workload) {
        if (!"stateful".equals(workload.id())) {
            throw new IllegalArgumentException("hot-state root touches belong to the stateful workload");
        }
        BenchmarkWorkloadDefinitions.Phase phase = workload.phase("hot-state");
        if (phase.measured() || phase.stage() != BenchmarkWorkloadDefinitions.Stage.HOT_STATE) {
            throw new AssertionError("hot-state must stay outside the measured observer window");
        }
        Map<String, Long> touches = new LinkedHashMap<>();
        for (String sql : phase.sql()) {
            for (long root : nestInsertRoots(sql, "hot")) {
                touches.merge(String.valueOf(root), 1L, Long::sum);
            }
        }
        if (touches.values().stream().mapToLong(Long::longValue).sum()
                != phase.expectedLogicalOutputChanges()) {
            throw new AssertionError("hot-state source touches drifted from its fixture count");
        }
        return Map.copyOf(touches);
    }

    private record Decoded(BenchmarkWorkloadDefinitions.Projection projection,
                           List<BenchmarkMongoDeliveryObserver.ExpectedChange> changes) {
    }

    private static Decoded decode(String workload, BenchmarkWorkloadDefinitions.Stage stage, String sql) {
        if (stage == BenchmarkWorkloadDefinitions.Stage.COLD_READ && "stateful".equals(workload)) {
            return new Decoded(BenchmarkWorkloadDefinitions.Projection.NEST,
                    updatesFor(nestInsertRoots(sql, "cold")));
        }
        if (stage != BenchmarkWorkloadDefinitions.Stage.CDC_UPDATE) {
            throw new AssertionError("unsupported measured phase " + workload + "/" + stage);
        }
        Matcher update = UPDATE.matcher(sql);
        if (!update.matches()) {
            throw new AssertionError("measured SQL no longer has a fixed update range: " + sql);
        }
        String table = update.group(1);
        long first = Long.parseLong(update.group(2));
        long last = Long.parseLong(update.group(3));
        if (first < 1 || last < first || last > BenchmarkWorkloadDefinitions.SNAPSHOT_ROWS) {
            throw new AssertionError("measured source update escaped the fixed row range: " + sql);
        }
        boolean evenOnly = update.group(4) != null;
        List<BenchmarkMongoDeliveryObserver.ExpectedChange> changes = new ArrayList<>();
        switch (workload) {
            case "copy" -> {
                requireTable(table, "bench_copy_orders", evenOnly, false);
                for (long id = first; id <= last; id++) {
                    changes.add(change(String.valueOf(id)));
                }
                return new Decoded(BenchmarkWorkloadDefinitions.Projection.COPY, changes);
            }
            case "stateless" -> {
                requireTable(table, "bench_stateless_orders", evenOnly, true);
                for (long id = first; id <= last; id++) {
                    if (id % 2 == 0) {
                        changes.add(change(id + ":0"));
                        changes.add(change(id + ":1"));
                    }
                }
                return new Decoded(BenchmarkWorkloadDefinitions.Projection.STATELESS, changes);
            }
            case "stateful" -> {
                if (evenOnly) {
                    throw new AssertionError("stateful CDC unexpectedly filters source rows");
                }
                BenchmarkWorkloadDefinitions.Projection projection = switch (table) {
                    case "bench_join_orders" -> BenchmarkWorkloadDefinitions.Projection.JOIN;
                    case "bench_nest_items" -> BenchmarkWorkloadDefinitions.Projection.NEST;
                    default -> throw new AssertionError("unknown stateful measured table: " + table);
                };
                for (long id = first; id <= last; id++) {
                    changes.add(change(String.valueOf(id)));
                }
                return new Decoded(projection, changes);
            }
            default -> throw new AssertionError("unknown workload: " + workload);
        }
    }

    private static List<Long> nestInsertRoots(String sql, String phase) {
        String prefix = "INSERT INTO bench_nest_items (id,order_id,sku) VALUES ";
        if (!sql.startsWith(prefix)) {
            throw new AssertionError("nest item batch changed shape: " + sql);
        }
        String[] tuples = sql.substring(prefix.length()).split("(?<=\\)),(?=\\()");
        List<Long> roots = new ArrayList<>(tuples.length);
        for (String tuple : tuples) {
            Matcher item = NEST_ITEM.matcher(tuple);
            if (!item.matches() || !phase.equals(item.group(3))) {
                throw new AssertionError("nest item row changed shape: " + tuple);
            }
            long itemId = Long.parseLong(item.group(1));
            long rootId = Long.parseLong(item.group(2));
            long suffix = Long.parseLong(item.group(4));
            long offset = "hot".equals(phase) ? 200_000 : 300_000;
            if (itemId != offset + suffix || ("hot".equals(phase) && rootId != 1)
                    || ("cold".equals(phase) && rootId != suffix)) {
                throw new AssertionError("nest item no longer targets its fixed root: " + tuple);
            }
            roots.add(rootId);
        }
        return List.copyOf(roots);
    }

    private static void requireTable(String actual, String expected, boolean actualEven, boolean expectedEven) {
        if (!expected.equals(actual) || actualEven != expectedEven) {
            throw new AssertionError("measured source update changed table or selector: " + actual);
        }
    }

    private static List<BenchmarkMongoDeliveryObserver.ExpectedChange> updatesFor(List<Long> keys) {
        return keys.stream().map(id -> change(String.valueOf(id))).toList();
    }

    private static BenchmarkMongoDeliveryObserver.ExpectedChange change(String key) {
        return new BenchmarkMongoDeliveryObserver.ExpectedChange(
                key, BenchmarkMongoDeliveryObserver.Kind.UPDATE);
    }

    private static Function<Document, String> keyOf(BenchmarkWorkloadDefinitions.Projection projection) {
        return switch (projection) {
            case COPY, NEST -> document -> String.valueOf(number(document, "id"));
            case JOIN -> document -> String.valueOf(number(document, "order_id"));
            case STATELESS -> document -> number(document, "id") + ":" + number(document, "item_index");
        };
    }

    private static long number(Document document, String field) {
        Object value = document.get(field);
        if (!(value instanceof Number number)) {
            throw new AssertionError("target key field " + field + " is not numeric: " + document);
        }
        return number.longValue();
    }
}
