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
    @Test void fullSettlingKeepsEveryProtocolMarkerOutWhileWarmingTheWholeFixedCohort() {
        var workload = BenchmarkWorkloadDefinitions.cdcFullSettlingCalibration("copy");
        var chain = workload.sourceChains().getFirst();
        long probe = BenchmarkPreflightWrites.warmupRowId(workload, chain);
        long boundary = BenchmarkBoundaryWrites.forChain(workload, chain).rowId();
        var ends = BenchmarkMeasuredEndMarkers.forChain(workload, chain).values();
        assertThat(ends).containsExactly(96_000L);
        var protectedRows = new HashSet<Long>(ends);
        protectedRows.add(probe);
        protectedRows.add(boundary);
        assertThat(protectedRows).allSatisfy(row ->
                assertThat(workload.inFixedCohort(Long.toString(row))).isFalse());
        for (String phaseId : List.of("cdc-settling-raised", "cdc-settling-restored")) {
            var phase = workload.phase(phaseId);
            assertThat(phase.expectedLogicalOutputChanges()).isEqualTo(95_997L);
            assertThat(phase.sql()).allSatisfy(sql -> {
                assertThat(sql).contains("id NOT IN (");
                int begin = sql.indexOf("id NOT IN (") + "id NOT IN (".length();
                int end = sql.indexOf(')', begin);
                var excluded = java.util.Arrays.stream(sql.substring(begin, end).split(","))
                        .map(String::trim).map(Long::valueOf).collect(java.util.stream.Collectors.toSet());
                assertThat(excluded).containsAll(protectedRows);
                assertThat(excluded).allSatisfy(row ->
                        assertThat(workload.inFixedCohort(Long.toString(row))).isFalse());
            });
        }
        assertThat(BenchmarkWorkloadDefinitions.cdcSettlingCalibration("copy")
                .phase("cdc-settling-raised").expectedLogicalOutputChanges()).isEqualTo(23_998L);
    }
    @Test void largerSetupDoesNotExtendMeasuredOrTerminalCompletionBudgets() {
        var original = BenchmarkWorkloadDefinitions.byId("copy");
        assertThat(BenchmarkForkEnvironment.targetWait(original, original.phase("snapshot")))
                .isEqualTo(Duration.ofMinutes(5));
        assertThat(BenchmarkForkEnvironment.targetWait(original, original.phase("warm-up")))
                .isEqualTo(Duration.ofMinutes(5));
        var full = BenchmarkWorkloadDefinitions.cdcFullSettlingCalibration("copy");
        for (String id : List.of("snapshot", "warm-up", "cdc-settling-raised", "cdc-settling-restored")) {
            assertThat(BenchmarkForkEnvironment.targetWait(full, full.phase(id)))
                    .isEqualTo(Duration.ofMinutes(40));
        }
        for (String id : List.of("cdc-update", "terminal")) {
            assertThat(BenchmarkForkEnvironment.targetWait(full, full.phase(id)))
                    .isEqualTo(Duration.ofMinutes(5));
        }
    }
    @Test void fullCopyCdcSettlingRestoresTheOriginalMeasuredInputAndKeepsDiagnosticProfilesSeparate() {
        var original = BenchmarkWorkloadDefinitions.steadyPilot("copy");
        var diagnostic = BenchmarkWorkloadDefinitions.cdcFullSettlingCalibration("copy");
        assertThat(diagnostic.setupSql()).isEqualTo(original.setupSql());
        assertThat(diagnostic.sourceChains()).isEqualTo(original.sourceChains());
        assertThat(diagnostic.phase("snapshot")).isEqualTo(original.phase("snapshot"));
        assertThat(diagnostic.phase("warm-up")).isEqualTo(original.phase("warm-up"));
        assertThat(diagnostic.phase("cdc-update")).isEqualTo(original.phase("cdc-update"));
        assertThat(diagnostic.phase("terminal")).isEqualTo(original.phase("terminal"));
        var chain = diagnostic.sourceChains().getFirst();
        long probe = BenchmarkPreflightWrites.warmupRowId(diagnostic, chain);
        long boundary = BenchmarkBoundaryWrites.forChain(diagnostic, chain).rowId();
        long measuredEnd = BenchmarkMeasuredEndMarkers.forChain(diagnostic, chain).get("cdc-update");
        long excluded = java.util.stream.LongStream.of(probe, boundary, measuredEnd).distinct()
                .filter(id -> id >= 1 && id <= diagnostic.rows()).count();
        for (String id : List.of("cdc-settling-raised", "cdc-settling-restored")) {
            var phase = diagnostic.phase(id);
            assertThat(phase.measured()).isFalse();
            assertThat(phase.expectedLogicalOutputChanges()).isEqualTo(diagnostic.rows() - excluded);
            assertThat(phase.sql()).hasSize(960).allSatisfy(sql ->
                    assertThat(sql).contains("id NOT IN (" + probe + "," + boundary + "," + measuredEnd + ")"));
        }
        assertThat(diagnostic.phase("cdc-settling-raised").targets())
                .isNotEqualTo(original.phase("warm-up").targets());
        assertThat(diagnostic.phase("cdc-settling-restored").targets())
                .isEqualTo(original.phase("warm-up").targets());
        assertThat(original.phases()).hasSize(4);
        assertThat(BenchmarkWorkloadDefinitions.cdcSettlingCalibration("copy")
                .phase("cdc-settling-raised").sql()).hasSize(240);
        for (String id : List.of("stateless", "stateful")) {
            org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                    .isThrownBy(() -> BenchmarkWorkloadDefinitions.cdcFullSettlingCalibration(id));
        }
    }
    @Test void copySettlingUsesOnlyTheUnmeasuredFirstQuarterAndRestoresTheExactMeasuredStart() {
        var pilot = BenchmarkWorkloadDefinitions.cdcSettlingCalibration("copy");
        assertThat(pilot.phase("cdc-settling-raised").measured()).isFalse();
        assertThat(pilot.phase("cdc-settling-restored").measured()).isFalse();
        assertThat(pilot.phase("cdc-settling-restored").targets()).isEqualTo(pilot.phase("warm-up").targets());
        assertThat(pilot.phase("cdc-settling-raised").targets()).isNotEqualTo(pilot.phase("warm-up").targets());
        assertThat(pilot.phase("cdc-settling-raised").sql()).hasSize(240).allSatisfy(sql ->
                assertThat(sql).contains("amount = amount + 1").contains("id NOT IN"));
        assertThat(pilot.phase("cdc-settling-restored").sql()).hasSize(240).allSatisfy(sql ->
                assertThat(sql).contains("amount = amount - 1").contains("id NOT IN"));
        assertThat(pilot.phase("cdc-update").batches()).hasSize(960);
        assertThat(pilot.phase("cdc-update").expectedLogicalOutputChanges()).isEqualTo(96_000);
        assertThat(pilot.phase("cdc-update").batchInterval()).isEqualTo(Duration.ofMillis(1));
        assertThat(pilot.inFixedCohort("24000")).isFalse();
        assertThat(pilot.inFixedCohort("24001")).isTrue();
        assertThat(BenchmarkWorkloadDefinitions.byId("copy").phases()).hasSize(4);
        assertThat(BenchmarkWorkloadDefinitions.steadyPilot("copy").phases()).hasSize(4);
        assertThat(pilot.phase("cdc-update")).isEqualTo(BenchmarkWorkloadDefinitions.steadyPilot("copy").phase("cdc-update"));
    }
    @Test
    void fixedPacingCalibrationChangesOnlyThePredeclaredMeasuredBatchSchedule() {
        for (String id : List.of("copy", "stateless", "stateful")) {
            var original = BenchmarkWorkloadDefinitions.steadyPilot(id);
            var paced = BenchmarkWorkloadDefinitions.pacedCalibration(id);
            assertThat(paced.rows()).isEqualTo(original.rows());
            assertThat(paced.seed()).isEqualTo(original.seed());
            assertThat(paced.setupSql()).isEqualTo(original.setupSql());
            assertThat(paced.sourceChains()).isEqualTo(original.sourceChains());
            assertThat(paced.phases()).hasSameSizeAs(original.phases());
            for (int i = 0; i < paced.phases().size(); i++) {
                var before = original.phases().get(i);
                var after = paced.phases().get(i);
                assertThat(after.sql()).isEqualTo(before.sql());
                assertThat(after.expectedLogicalCoverage()).isEqualTo(before.expectedLogicalCoverage());
                assertThat(after.targets()).isEqualTo(before.targets());
                assertThat(after.statementsPerBatch()).isEqualTo(before.statementsPerBatch());
                assertThat(after.batchInterval()).isEqualTo(before.measured()
                        ? Duration.ofMillis(id.equals("stateful") ? 50 : 5) : before.batchInterval());
            }
        }
    }

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

    @Test
    void theLargerPilotKeepsOldFixturesAndAllSemanticTargetAnswers() {
        for (var original : BenchmarkWorkloadDefinitions.all()) {
            var pilot = BenchmarkWorkloadDefinitions.steadyPilot(original.id());
            assertThat(original.rows()).isEqualTo(12_000);
            assertThat(pilot.rows()).isEqualTo(96_000).isLessThan(100_000);
            String suffix = original.id().equals("stateless") ? ":0" : "";
            assertThat(pilot.inFixedCohort("24000" + suffix)).isFalse();
            assertThat(pilot.inFixedCohort("24001" + suffix)).isTrue();
            assertThat(pilot.inFixedCohort("72000" + suffix)).isTrue();
            assertThat(pilot.inFixedCohort("72001" + suffix)).isFalse();
            assertThat(pilot.seed()).isEqualTo(original.seed());
            assertThat(pilot.resources(SOURCE, "mongodb://127.0.0.1:27017/bench_target"))
                    .isEqualTo(original.resources(SOURCE, "mongodb://127.0.0.1:27017/bench_target"));
            assertThat(pilot.phase("cdc-update").batches()).hasSize(960);
            for (var phase : pilot.phases()) {
                if (!phase.measured()) { continue; }
                assertThat(BenchmarkExpectedChanges.forPhase(pilot, phase).stream()
                        .mapToLong(BenchmarkExpectedChanges.TargetPlan::totalChanges).sum())
                        .isEqualTo(phase.expectedLogicalOutputChanges());
            }
            for (var chain : pilot.sourceChains()) {
                var markers = BenchmarkMeasuredEndMarkers.forChain(pilot, chain);
                if (markers.containsKey("cdc-update")) { assertThat(markers.get("cdc-update")).isEqualTo(96_000); }
                if (markers.containsKey("cold-read")) { assertThat(markers.get("cold-read")).isEqualTo(396_000); }
            }
            assertThat(pilot.phase("terminal").targets()).allSatisfy(target ->
                    assertThat(target.rows()).isEqualTo(original.id().equals("stateless") ? 96_002 : 96_001));
        }
    }

    @Test
    void statelessCohortKeepsBothActualUnwindKeysForEachSelectedRoot() {
        var workload = BenchmarkWorkloadDefinitions.steadyPilot("stateless");
        assertThat(workload.inFixedCohort("24002:0")).isTrue();
        assertThat(workload.inFixedCohort("24002:1")).isTrue();
        assertThat(workload.inFixedCohort("24000:0")).isFalse();
        assertThat(workload.inFixedCohort("72002:1")).isFalse();
    }

    @Test
    void statelessPilotSettlingRestoresTheExactMeasuredStartState() {
        var pilot = BenchmarkWorkloadDefinitions.steadyPilot("stateless");
        assertThat(pilot.phase("cdc-settling-raised").measured()).isFalse();
        assertThat(pilot.phase("cdc-settling-restored").targets()).isEqualTo(pilot.phase("warm-up").targets());
        assertThat(pilot.phase("cdc-settling-raised").targets()).isNotEqualTo(pilot.phase("warm-up").targets());
        assertThat(pilot.phase("cdc-settling-raised").sql()).hasSize(240);
        assertThat(pilot.phase("cdc-settling-restored").sql()).hasSize(240);
        assertThat(BenchmarkWorkloadDefinitions.byId("stateless").phases()).hasSize(4);
    }
}
