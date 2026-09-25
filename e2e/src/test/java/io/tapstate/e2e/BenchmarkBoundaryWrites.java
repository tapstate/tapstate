package io.tapstate.e2e;

import java.util.Objects;

/**
 * Two fixed, unmeasured CDC writes per source chain after warm-up and hot-state setup. Each chosen
 * source row is outside those phases' mutations. The second write restores its exact seeded value,
 * so the measured phase still starts from the workload's frozen target state.
 */
final class BenchmarkBoundaryWrites {

    private BenchmarkBoundaryWrites() {
    }

    record Boundary(String table, long rowId, String field, String seededValueSql,
                    String changedSql, String restoredSql) {
    }

    static Boundary forChain(BenchmarkWorkloadDefinitions.Workload workload,
                             BenchmarkWorkloadDefinitions.SourceChain chain) {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(chain, "source chain");
        if (!workload.sourceChains().contains(chain)) {
            throw new IllegalArgumentException("unknown source chain " + chain.id() + " for " + workload.id());
        }
        if (workload.seed() != BenchmarkWorkloadDefinitions.SEED) {
            throw new IllegalArgumentException("boundary source seed changed for " + workload.id());
        }
        Value value = switch (workload.id() + "/" + chain.sourceId() + "/" + chain.table()) {
            case "copy/src_bench_copy/bench_copy_orders" ->
                    numeric(257, "amount", (257 * 17 + workload.seed()) % 1_000);
            case "stateless/src_bench_stateless/bench_stateless_orders" ->
                    numeric(258, "qty", (258 * 13 + workload.seed()) % 500);
            case "stateful/src_bench_join_orders/bench_join_orders" ->
                    numeric(257, "qty", (257 * 7 + workload.seed()) % 1_000);
            case "stateful/src_bench_join_customers/bench_join_customers" ->
                    text(2, "name", "customer-2");
            case "stateful/src_bench_nest_orders/bench_nest_orders" ->
                    text(257, "label", "root-257");
            case "stateful/src_bench_nest_items/bench_nest_items" ->
                    text(257, "sku", "sku-257");
            default -> throw new IllegalArgumentException("unsupported source chain " + chain.id());
        };
        String row = " WHERE id = " + value.rowId();
        String changed = "UPDATE " + chain.table() + " SET " + value.field() + " = " + value.changedSql()
                + row + " AND " + value.field() + " = " + value.seededSql();
        String restored = "UPDATE " + chain.table() + " SET " + value.field() + " = " + value.seededSql()
                + row + " AND " + value.field() + " = " + value.changedSql();
        return new Boundary(chain.table(), value.rowId(), value.field(), value.seededSql(),
                changed, restored);
    }

    static String changedSql(BenchmarkWorkloadDefinitions.Workload workload,
                             BenchmarkWorkloadDefinitions.SourceChain chain) {
        return forChain(workload, chain).changedSql();
    }

    static String restoredSql(BenchmarkWorkloadDefinitions.Workload workload,
                              BenchmarkWorkloadDefinitions.SourceChain chain) {
        return forChain(workload, chain).restoredSql();
    }

    private record Value(long rowId, String field, String seededSql, String changedSql) {
    }

    private static Value numeric(long rowId, String field, long seeded) {
        return new Value(rowId, field, String.valueOf(seeded), String.valueOf(seeded + 1));
    }

    private static Value text(long rowId, String field, String seeded) {
        return new Value(rowId, field, "'" + seeded + "'", "'" + seeded + "-boundary'");
    }
}
