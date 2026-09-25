package io.tapstate.e2e;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Physical Mongo changes owed by the fixed terminal source rows. These are unmeasured checkpoint
 * events, registered before terminal SQL and checked after terminal target ACK. A nested root is first
 * inserted and then updated when its child arrives, so its two physical writes are distinct from the
 * workload phase's logical output count.
 */
final class BenchmarkTerminalTargetChanges {

    private BenchmarkTerminalTargetChanges() {
    }

    static List<BenchmarkMongoDeliveryObserver.ExpectedChange> forTarget(
            BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkWorkloadDefinitions.TargetExpectation target) {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(target, "target");
        BenchmarkWorkloadDefinitions.Phase terminal = workload.phase("terminal");
        if (terminal.stage() != BenchmarkWorkloadDefinitions.Stage.TERMINAL
                || terminal.measured() || !terminal.targets().contains(target)) {
            throw new IllegalArgumentException("target is not a terminal checkpoint of " + workload.id());
        }
        requireSourceTerminals(workload, terminal);
        return switch (workload.id()) {
            case "copy" -> {
                requireTarget(target, "bench_copy", "bench_copy_orders",
                        BenchmarkWorkloadDefinitions.Projection.COPY,
                        BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO);
                requireSql(terminal.sql(), List.of(
                        "INSERT INTO bench_copy_orders (id,amount,payload,marker)"
                                + " VALUES (900001,7,'terminal','copy-orders-terminal')"));
                yield List.of(insert("900001"));
            }
            case "stateless" -> {
                requireTarget(target, "bench_stateless", "bench_stateless_orders",
                        BenchmarkWorkloadDefinitions.Projection.STATELESS,
                        BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO);
                requireSql(terminal.sql(), List.of(
                        "INSERT INTO bench_stateless_orders (id,qty,region,items) VALUES"
                                + " (900002,7,'keep',ARRAY['terminal-left','terminal-right'])"));
                yield List.of(insert("900002:0"), insert("900002:1"));
            }
            case "stateful" -> {
                requireSql(terminal.sql(), List.of(
                        "INSERT INTO bench_join_customers"
                                + " (id,name,marker) VALUES (900012,'terminal-customer','join-customers-terminal')",
                        "INSERT INTO bench_join_orders"
                                + " (id,customer_id,qty,marker) VALUES"
                                + " (900011,900012,7,'join-orders-terminal')",
                        "INSERT INTO bench_nest_orders"
                                + " (id,label,marker) VALUES (900013,'terminal-root','nest-orders-terminal')",
                        "INSERT INTO bench_nest_items"
                                + " (id,order_id,sku,marker) VALUES"
                                + " (900014,900013,'terminal-item','nest-items-terminal')"));
                if (target.projection() == BenchmarkWorkloadDefinitions.Projection.JOIN) {
                    requireTarget(target, "bench_stateful_join", "bench_join_output",
                            BenchmarkWorkloadDefinitions.Projection.JOIN,
                            BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW);
                    yield List.of(insert("900011"));
                }
                requireTarget(target, "bench_stateful_nest", "bench_nest_orders",
                        BenchmarkWorkloadDefinitions.Projection.NEST,
                        BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO);
                yield List.of(insert("900013"), update("900013"));
            }
            default -> throw new IllegalArgumentException("unknown benchmark workload: " + workload.id());
        };
    }

    private static void requireSourceTerminals(BenchmarkWorkloadDefinitions.Workload workload,
                                               BenchmarkWorkloadDefinitions.Phase terminal) {
        Map<String, Long> coverage = terminal.expectedLogicalCoverage();
        for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
            if (!Long.valueOf(1).equals(coverage.get(chain.terminalLogicalId()))) {
                throw new AssertionError("terminal source chain lost its unique sentinel: " + chain.id());
            }
        }
        if (coverage.size() != workload.sourceChains().size()) {
            throw new AssertionError("terminal source coverage has an unexpected chain");
        }
    }

    private static void requireTarget(BenchmarkWorkloadDefinitions.TargetExpectation target,
                                      String pipeline, String table,
                                      BenchmarkWorkloadDefinitions.Projection projection,
                                      BenchmarkWorkloadDefinitions.TargetLocation location) {
        if (!pipeline.equals(target.pipelineId()) || !table.equals(target.table())
                || target.projection() != projection || target.location() != location) {
            throw new AssertionError("terminal target no longer has its fixed physical address: " + target);
        }
    }

    private static void requireSql(List<String> actual, List<String> expected) {
        if (!expected.equals(actual)) {
            throw new AssertionError("terminal SQL changed without a new physical target witness");
        }
    }

    private static BenchmarkMongoDeliveryObserver.ExpectedChange insert(String key) {
        return new BenchmarkMongoDeliveryObserver.ExpectedChange(key,
                BenchmarkMongoDeliveryObserver.Kind.INSERT);
    }

    private static BenchmarkMongoDeliveryObserver.ExpectedChange update(String key) {
        return new BenchmarkMongoDeliveryObserver.ExpectedChange(key,
                BenchmarkMongoDeliveryObserver.Kind.UPDATE);
    }
}
