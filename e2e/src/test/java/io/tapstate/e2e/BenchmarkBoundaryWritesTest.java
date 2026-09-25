package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Boundary probes use seeded source rows outside the workload's unmeasured mutations. */
class BenchmarkBoundaryWritesTest {

    private static final Pattern RANGE = Pattern.compile(" WHERE id BETWEEN (\\d+) AND (\\d+)");
    private static final Pattern EXACT = Pattern.compile(" WHERE id = (\\d+)");

    @Test
    void allSixChainsHaveAChangedWriteAndAnExactRestoreOnUntouchedRows() {
        Map<String, Long> expectedRows = Map.of(
                "src_bench_copy", 257L,
                "src_bench_stateless", 258L,
                "src_bench_join_orders", 257L,
                "src_bench_join_customers", 2L,
                "src_bench_nest_orders", 257L,
                "src_bench_nest_items", 257L);
        Map<String, BenchmarkBoundaryWrites.Boundary> boundaries = new LinkedHashMap<>();
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                BenchmarkBoundaryWrites.Boundary boundary = BenchmarkBoundaryWrites.forChain(workload, chain);
                boundaries.put(chain.sourceId(), boundary);
                assertThat(boundary.table()).isEqualTo(chain.table());
                assertThat(boundary.rowId()).isEqualTo(expectedRows.get(chain.sourceId()));
                assertThat(boundary.rowId()).isNotEqualTo(chain.terminalRowId());
                assertThat(boundary.changedSql()).isEqualTo(BenchmarkBoundaryWrites.changedSql(workload, chain));
                assertThat(boundary.restoredSql()).isEqualTo(BenchmarkBoundaryWrites.restoredSql(workload, chain));
                assertThat(boundary.changedSql()).isNotEqualTo(boundary.restoredSql());
                assertThat(boundary.changedSql()).contains(" AND " + boundary.field() + " = "
                        + boundary.seededValueSql());
                assertThat(boundary.restoredSql()).contains(" SET " + boundary.field() + " = "
                        + boundary.seededValueSql());
                List<String> setup = workload.phases().stream()
                        .filter(phase -> phase.stage() == BenchmarkWorkloadDefinitions.Stage.WARM_UP
                                || phase.stage() == BenchmarkWorkloadDefinitions.Stage.HOT_STATE)
                        .flatMap(phase -> phase.sql().stream()).toList();
                assertThat(setup).allSatisfy(sql -> assertSourceRowUntouched(sql, boundary));
            }
        }
        assertThat(boundaries).hasSize(6);
    }

    @Test
    void unknownChainOrChangedSeedFailsBeforeReturningSql() {
        BenchmarkWorkloadDefinitions.Workload copy = BenchmarkWorkloadDefinitions.byId("copy");
        BenchmarkWorkloadDefinitions.SourceChain real = copy.sourceChains().getFirst();
        BenchmarkWorkloadDefinitions.SourceChain unknown = new BenchmarkWorkloadDefinitions.SourceChain(
                "other", real.pipelineId(), "other", real.table(), "other/terminal", 900_000);
        assertThatThrownBy(() -> BenchmarkBoundaryWrites.forChain(copy, unknown))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown source chain");

        BenchmarkWorkloadDefinitions.Workload changedSeed = new BenchmarkWorkloadDefinitions.Workload(
                copy.id(), copy.seed() + 1, copy.database(), copy.pipelineIds(), copy.sourceChains(),
                copy.setupSql(), copy.phases());
        assertThatThrownBy(() -> BenchmarkBoundaryWrites.changedSql(changedSeed, real))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("seed changed");
    }

    private static void assertSourceRowUntouched(String sql, BenchmarkBoundaryWrites.Boundary boundary) {
        if (!sql.contains(" " + boundary.table() + " ")) {
            return;
        }
        if (sql.startsWith("UPDATE ")) {
            Matcher range = RANGE.matcher(sql);
            if (range.find()) {
                long first = Long.parseLong(range.group(1));
                long last = Long.parseLong(range.group(2));
                assertThat(boundary.rowId() < first || boundary.rowId() > last)
                        .as("boundary row is outside warm-up range: %s", sql).isTrue();
                return;
            }
            Matcher exact = EXACT.matcher(sql);
            assertThat(exact.find()).as("fixed update names a row: %s", sql).isTrue();
            assertThat(boundary.rowId()).isNotEqualTo(Long.parseLong(exact.group(1)));
        } else if (sql.startsWith("INSERT INTO ")) {
            assertThat(sql).doesNotContain("(" + boundary.rowId() + ",");
        }
    }
}
