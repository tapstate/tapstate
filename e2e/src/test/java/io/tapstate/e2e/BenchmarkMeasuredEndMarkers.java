package io.tapstate.e2e;

import io.tapstate.core.event.Op;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** The final source row whose connector token closes each fixed measured phase. */
final class BenchmarkMeasuredEndMarkers {

    private static final Pattern FINAL_UPDATE_RANGE = Pattern.compile("WHERE id BETWEEN [0-9]+ AND ([0-9]+)");

    private BenchmarkMeasuredEndMarkers() {
    }

    static Map<String, Long> forChain(BenchmarkWorkloadDefinitions.Workload workload,
                                      BenchmarkWorkloadDefinitions.SourceChain chain) {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(chain, "source chain");
        if (!workload.sourceChains().contains(chain)) {
            throw new IllegalArgumentException("unknown source chain " + chain.id() + " for " + workload.id());
        }
        long lastSnapshotRow = BenchmarkWorkloadDefinitions.SNAPSHOT_ROWS;
        Map<String, Long> markers = switch (workload.id() + "/" + chain.sourceId() + "/" + chain.table()) {
            case "copy/src_bench_copy/bench_copy_orders",
                 "stateless/src_bench_stateless/bench_stateless_orders",
                 "stateful/src_bench_join_orders/bench_join_orders" ->
                    Map.of("cdc-update", lastSnapshotRow);
            case "stateful/src_bench_nest_items/bench_nest_items" ->
                    orderedNestItems(300_000L + lastSnapshotRow, lastSnapshotRow);
            case "stateful/src_bench_join_customers/bench_join_customers",
                 "stateful/src_bench_nest_orders/bench_nest_orders" -> Map.of();
            default -> throw new IllegalArgumentException("unsupported source chain " + chain.id());
        };
        for (Map.Entry<String, Long> marker : markers.entrySet()) {
            var phase = workload.phase(marker.getKey());
            if (!phase.measured() || marker.getValue() == chain.terminalRowId()
                    || marker.getValue() == BenchmarkPreflightWrites.warmupRowId(workload, chain)
                    || marker.getValue() == BenchmarkBoundaryWrites.forChain(workload, chain).rowId()) {
                throw new IllegalStateException("measured-end marker overlaps an unmeasured row or phase");
            }
            String finalSql = phase.sql().stream()
                    .filter(sql -> sql.startsWith("UPDATE " + chain.table() + " ")
                            || sql.startsWith("INSERT INTO " + chain.table() + " "))
                    .reduce((before, after) -> after)
                    .orElseThrow(() -> new IllegalStateException("measured source phase has no selected table SQL"));
            if (marker.getKey().equals("cold-read")) {
                if (!finalSql.startsWith("INSERT INTO ")
                        || !finalSql.contains("(" + marker.getValue() + ",")) {
                    throw new IllegalStateException("cold-read marker is not in its final source batch");
                }
            } else {
                Matcher range = FINAL_UPDATE_RANGE.matcher(finalSql);
                if (!finalSql.startsWith("UPDATE ") || !range.find()
                        || Long.parseLong(range.group(1)) != marker.getValue()) {
                    throw new IllegalStateException("cdc-update marker is not in its final source batch");
                }
            }
        }
        return markers;
    }

    static List<String> phaseOrder(Map<String, Long> markers) {
        if (markers.keySet().stream().anyMatch(phase -> !phase.equals("cold-read")
                && !phase.equals("cdc-update"))) {
            throw new IllegalArgumentException("unknown measured-end phase");
        }
        return List.of("cold-read", "cdc-update").stream().filter(markers::containsKey).toList();
    }

    static Op operation(String phaseId) {
        return switch (phaseId) {
            case "cold-read" -> Op.INSERT;
            case "cdc-update" -> Op.UPDATE;
            default -> throw new IllegalArgumentException("unknown measured-end phase " + phaseId);
        };
    }

    private static Map<String, Long> orderedNestItems(long coldReadEnd, long cdcEnd) {
        Map<String, Long> markers = new LinkedHashMap<>();
        markers.put("cold-read", coldReadEnd);
        markers.put("cdc-update", cdcEnd);
        return Collections.unmodifiableMap(markers);
    }
}
