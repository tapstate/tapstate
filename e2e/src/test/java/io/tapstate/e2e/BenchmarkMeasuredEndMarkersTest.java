package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Every measured source chain has a final row in the fixed phase's last SQL batch. */
class BenchmarkMeasuredEndMarkersTest {

    @Test
    void fixedMarkersMatchTheLastSourceBatchAndInactiveChainsStayEmpty() {
        Map<String, Map<String, Long>> expected = Map.of(
                "src_bench_copy", Map.of("cdc-update", 12_000L),
                "src_bench_stateless", Map.of("cdc-update", 12_000L),
                "src_bench_join_orders", Map.of("cdc-update", 12_000L),
                "src_bench_join_customers", Map.of(),
                "src_bench_nest_orders", Map.of(),
                "src_bench_nest_items", Map.of("cold-read", 312_000L, "cdc-update", 12_000L));
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                Map<String, Long> markers = BenchmarkMeasuredEndMarkers.forChain(workload, chain);
                assertThat(markers).as(chain.id()).isEqualTo(expected.get(chain.sourceId()));
                for (Map.Entry<String, Long> marker : markers.entrySet()) {
                    var phase = workload.phase(marker.getKey());
                    String lastSql = phase.sql().stream()
                            .filter(sql -> sql.startsWith("UPDATE " + chain.table() + " ")
                                    || sql.startsWith("INSERT INTO " + chain.table() + " "))
                            .reduce((before, after) -> after).orElseThrow();
                    if (marker.getKey().equals("cold-read")) {
                        assertThat(lastSql).contains("(" + marker.getValue() + ",");
                    } else {
                        assertThat(lastSql).contains("WHERE id BETWEEN 11901 AND 12000");
                    }
                }
            }
        }
    }
}
