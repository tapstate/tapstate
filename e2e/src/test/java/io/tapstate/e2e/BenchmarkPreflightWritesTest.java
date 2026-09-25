package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The sidecar's preflight sequence leaves every fixed source row as it was seeded. */
class BenchmarkPreflightWritesTest {

    private static final Map<String, String> SEEDED_SQL = Map.of(
            "src_bench_copy", "UPDATE bench_copy_orders SET amount = 259 WHERE id = 1",
            "src_bench_stateless", "UPDATE bench_stateless_orders SET qty = 268 WHERE id = 2",
            "src_bench_join_orders", "UPDATE bench_join_orders SET qty = 249 WHERE id = 1",
            "src_bench_join_customers", "UPDATE bench_join_customers SET name = 'customer-1' WHERE id = 1",
            "src_bench_nest_orders", "UPDATE bench_nest_orders SET label = 'root-1' WHERE id = 1",
            "src_bench_nest_items", "UPDATE bench_nest_items SET sku = 'sku-1' WHERE id = 1");

    @Test
    void everyFrozenChainHasTwentyAlternatingMutationsAndRestoresItsSeed() {
        Map<String, BenchmarkWorkloadDefinitions.SourceChain> all = new LinkedHashMap<>();
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                all.put(chain.sourceId(), chain);
                long rowId = BenchmarkPreflightWrites.warmupRowId(workload, chain);
                assertThat(rowId).isPositive().isNotEqualTo(chain.terminalRowId());
                String changed = BenchmarkPreflightWrites.sql(workload, chain, 1);
                String seeded = SEEDED_SQL.get(chain.sourceId());
                assertThat(seeded).isNotNull();
                assertThat(changed).isNotEqualTo(seeded);
                for (int attempt = 1; attempt <= BenchmarkPreflightWrites.ATTEMPTS; attempt++) {
                    assertThat(BenchmarkPreflightWrites.sql(workload, chain, attempt))
                            .as(chain.id() + " attempt " + attempt)
                            .isEqualTo(attempt % 2 == 1 ? changed : seeded);
                }
                assertThat(BenchmarkPreflightWrites.sql(workload, chain, 20)).isEqualTo(seeded);
            }
        }
        assertThat(all).hasSize(6);
    }

    @Test
    void unknownChainsAndOutOfRangeAttemptsAreRefusedBeforeAnySqlIsReturned() {
        BenchmarkWorkloadDefinitions.Workload copy = BenchmarkWorkloadDefinitions.byId("copy");
        BenchmarkWorkloadDefinitions.SourceChain real = copy.sourceChains().getFirst();
        BenchmarkWorkloadDefinitions.SourceChain madeUp = new BenchmarkWorkloadDefinitions.SourceChain(
                "other", real.pipelineId(), "other_source", real.table(), "other/terminal", 800_000);
        assertThatThrownBy(() -> BenchmarkPreflightWrites.warmupRowId(copy, madeUp))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown source chain");
        assertThatThrownBy(() -> BenchmarkPreflightWrites.sql(copy, real, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 through 20");
        assertThatThrownBy(() -> BenchmarkPreflightWrites.sql(copy, real, 21))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 through 20");

        BenchmarkWorkloadDefinitions.Workload changedSeed = new BenchmarkWorkloadDefinitions.Workload(
                copy.id(), copy.seed() + 1, copy.database(), copy.pipelineIds(), copy.sourceChains(),
                copy.setupSql(), copy.phases());
        assertThatThrownBy(() -> BenchmarkPreflightWrites.sql(changedSeed, real, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("seed changed");
    }
}
