package io.tapstate.e2e;

import java.util.Objects;

/**
 * Fixed source-row changes for the sidecar's twenty present-start preflight attempts. Preflight runs
 * after source seeding and before the workload's declared warm-up phase, outside measured delivery.
 * Every adjacent attempt writes a different value, and attempt twenty restores the seeded value.
 */
final class BenchmarkPreflightWrites {

    static final int ATTEMPTS = 20;

    private BenchmarkPreflightWrites() {
    }

    static long warmupRowId(BenchmarkWorkloadDefinitions.Workload workload,
                            BenchmarkWorkloadDefinitions.SourceChain chain) {
        return probe(workload, chain).rowId();
    }

    static String sql(BenchmarkWorkloadDefinitions.Workload workload,
                      BenchmarkWorkloadDefinitions.SourceChain chain, int attempt) {
        if (attempt < 1 || attempt > ATTEMPTS) {
            throw new IllegalArgumentException("preflight attempt must be 1 through " + ATTEMPTS);
        }
        Probe probe = probe(workload, chain);
        String value = attempt % 2 == 1 ? probe.changedSql() : probe.seededSql();
        return "UPDATE " + chain.table() + " SET " + probe.field() + " = " + value
                + " WHERE id = " + probe.rowId();
    }

    private record Probe(long rowId, String field, String seededSql, String changedSql) {
    }

    private static Probe probe(BenchmarkWorkloadDefinitions.Workload workload,
                               BenchmarkWorkloadDefinitions.SourceChain chain) {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(chain, "source chain");
        if (!workload.sourceChains().contains(chain)) {
            throw new IllegalArgumentException("unknown source chain " + chain.id() + " for " + workload.id());
        }
        if (workload.seed() != BenchmarkWorkloadDefinitions.SEED) {
            throw new IllegalArgumentException("preflight source seed changed for " + workload.id());
        }
        return switch (workload.id() + "/" + chain.sourceId() + "/" + chain.table()) {
            case "copy/src_bench_copy/bench_copy_orders" -> numeric(1,
                    "amount", (workload.seed() + 17) % 1_000);
            case "stateless/src_bench_stateless/bench_stateless_orders" -> numeric(2,
                    "qty", (workload.seed() + 2 * 13) % 500);
            case "stateful/src_bench_join_orders/bench_join_orders" -> numeric(1,
                    "qty", (workload.seed() + 7) % 1_000);
            case "stateful/src_bench_join_customers/bench_join_customers" -> text(1,
                    "name", "customer-1");
            case "stateful/src_bench_nest_orders/bench_nest_orders" -> text(1,
                    "label", "root-1");
            case "stateful/src_bench_nest_items/bench_nest_items" -> text(1,
                    "sku", "sku-1");
            default -> throw new IllegalArgumentException("unsupported source chain " + chain.id());
        };
    }

    private static Probe numeric(long rowId, String field, long seeded) {
        return new Probe(rowId, field, String.valueOf(seeded), String.valueOf(seeded + 1));
    }

    private static Probe text(long rowId, String field, String seeded) {
        return new Probe(rowId, field, "'" + seeded + "'", "'" + seeded + "-probe'");
    }
}
