package io.tapstate.e2e;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** The same frozen workload definitions are consumed by both application jars. */
class BenchmarkWorkloadDefinitionsTest {

    private static final Map<String, Object> SOURCE = Map.of(
            "host", "127.0.0.1", "port", 3306, "database", "bench_fork",
            "username", "bench", "password", "bench-password");

    @Test
    void everyRealConnectorResourceParsesAndEveryPhaseHasAnIndependentTargetAnswer() {
        DslParser parser = new DslParser();
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            Map<String, String> resources = workload.resources(SOURCE, "mongodb://127.0.0.1:27017/bench_target");
            long pipelines = 0;
            long sources = 0;
            for (String yaml : resources.values()) {
                Resource resource = parser.parse(yaml);
                pipelines += resource instanceof PipelineResource ? 1 : 0;
                sources += resource instanceof SourceResource ? 1 : 0;
            }
            assertThat(pipelines).as(workload.id() + " pipelines").isEqualTo(workload.pipelineIds().size());
            assertThat(sources).as(workload.id() + " sources")
                    .isEqualTo(workload.sourceChains().size() + 1);
            assertThat(workload.phases()).extracting(BenchmarkWorkloadDefinitions.Phase::stage)
                    .startsWith(BenchmarkWorkloadDefinitions.Stage.SNAPSHOT,
                            BenchmarkWorkloadDefinitions.Stage.WARM_UP)
                    .endsWith(BenchmarkWorkloadDefinitions.Stage.TERMINAL);
            assertThat(workload.phases().getFirst().sql()).isEmpty();
            assertThat(workload.phases().getLast().targets())
                    .containsExactlyInAnyOrderElementsOf(workload.targetResets());
            assertThat(workload.phases())
                    .allSatisfy(phase -> assertThat(phase.targets())
                            .extracting(BenchmarkWorkloadDefinitions.TargetExpectation::pipelineId)
                            .containsExactlyInAnyOrderElementsOf(workload.pipelineIds()));
            assertThat(workload.phases())
                    .allSatisfy(phase -> assertThat(phase.targets())
                            .allSatisfy(target -> {
                                assertThat(target.rows()).isPositive();
                                assertThat(target.checksum()).hasSize(64);
                            }));
        }
    }

    @Test
    void eachSourceChainHasItsOwnTerminalAndMeasuredCoverageIsLargeEnough() {
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            Set<String> chainIds = new HashSet<>();
            Set<String> terminalIds = new HashSet<>();
            Set<Long> terminalRows = new HashSet<>();
            for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                assertThat(workload.pipelineIds()).contains(chain.pipelineId());
                assertThat(chainIds.add(chain.id())).isTrue();
                assertThat(terminalIds.add(chain.terminalLogicalId())).isTrue();
                assertThat(terminalRows.add(chain.terminalRowId())).isTrue();
                assertThat(workload.phases().getLast().expectedLogicalCoverage())
                        .containsEntry(chain.terminalLogicalId(), 1L);
                assertThat(workload.phase("terminal").sql())
                        .anySatisfy(sql -> assertThat(sql).contains("(" + chain.terminalRowId() + ","));
                assertThat(workload.phase("warm-up").expectedLogicalCoverage())
                        .containsKey(chain.id() + "/warm-up");
            }
            long measuredLogicalOutputChanges = workload.phases().stream()
                    .filter(BenchmarkWorkloadDefinitions.Phase::measured)
                    .mapToLong(BenchmarkWorkloadDefinitions.Phase::expectedLogicalOutputChanges).sum();
            assertThat(measuredLogicalOutputChanges).as(workload.id() + " measured logical output changes")
                    .isGreaterThanOrEqualTo(10_000);
            assertThat(workload.seed()).isEqualTo(BenchmarkWorkloadDefinitions.SEED);
            assertThat(workload.connectorConfig(SOURCE))
                    .containsEntry(workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                            ? "highPerformance" : "logPluginName",
                            workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                                    ? false : "pgoutput");
        }
    }

    @Test
    void statefulIncludesBothJoinAndNestAndColdReadFollowsHotState() {
        BenchmarkWorkloadDefinitions.Workload stateful = BenchmarkWorkloadDefinitions.byId("stateful");
        assertThat(stateful.pipelineIds()).containsExactly("bench_stateful_join", "bench_stateful_nest");
        assertThat(stateful.phases()).extracting(BenchmarkWorkloadDefinitions.Phase::stage)
                .containsExactly(BenchmarkWorkloadDefinitions.Stage.SNAPSHOT,
                        BenchmarkWorkloadDefinitions.Stage.WARM_UP,
                        BenchmarkWorkloadDefinitions.Stage.HOT_STATE,
                        BenchmarkWorkloadDefinitions.Stage.COLD_READ,
                        BenchmarkWorkloadDefinitions.Stage.CDC_UPDATE,
                        BenchmarkWorkloadDefinitions.Stage.TERMINAL);
        assertThat(stateful.phase("snapshot").targets())
                .anySatisfy(target -> assertThat(target.location())
                        .isEqualTo(BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW))
                .anySatisfy(target -> assertThat(target.location())
                        .isEqualTo(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO));
        assertThat(stateful.phase("snapshot").targets())
                .allSatisfy(target -> assertThat(target.rows()).isEqualTo(12_000));
        assertThat(stateful.phase("terminal").targets())
                .allSatisfy(target -> assertThat(target.rows()).isEqualTo(12_001));
    }

    @Test
    void measuredCdcAndColdReadsArePacedAsOneHundredRowBatches() {
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            BenchmarkWorkloadDefinitions.Phase cdc = workload.phase("cdc-update");
            assertThat(cdc.batchInterval()).isEqualTo(Duration.ofMillis(1));
            assertThat(cdc.batches()).hasSize(120);
            assertThat(cdc.batches()).allSatisfy(batch ->
                    assertThat(batch).hasSize(workload.id().equals("stateful") ? 2 : 1));
        }
        BenchmarkWorkloadDefinitions.Phase cold = BenchmarkWorkloadDefinitions.byId("stateful")
                .phase("cold-read");
        assertThat(cold.batchInterval()).isEqualTo(Duration.ofMillis(1));
        assertThat(cold.batches()).hasSize(120);
        assertThat(cold.batches()).allSatisfy(batch -> assertThat(batch).hasSize(1));
    }

    @Test
    void anExtraPhysicalRowCannotMatchTheFrozenChecksum() {
        BenchmarkWorkloadDefinitions.TargetExpectation expected =
                BenchmarkWorkloadDefinitions.byId("copy").phase("terminal").targets().getFirst();
        String actual = BenchmarkWorkloadDefinitions.checksumOf(expected,
                List.of(new Document("id", 900001L).append("amount", 7L)
                        .append("payload", "terminal").append("marker", "copy-orders-terminal")));
        assertThat(actual).isNotEqualTo(expected.checksum());
        assertThat(expected.rows()).isEqualTo(12_001);
    }
}
