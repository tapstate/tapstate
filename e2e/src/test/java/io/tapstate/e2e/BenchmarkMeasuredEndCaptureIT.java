package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Real connector streams emit the final row of each fixed measured source phase with a position. */
class BenchmarkMeasuredEndCaptureIT {

    private static final Duration WAIT = Duration.ofSeconds(90);

    @BeforeAll
    static void requireSources() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres");
    }

    @Test
    void mysqlCopyFinalUpdateHasItsOwnSourceToken() throws Exception {
        exercise("copy", "src_bench_copy", "mysql");
    }

    @Test
    void postgresStatelessFinalUpdateHasItsOwnSourceToken() throws Exception {
        exercise("stateless", "src_bench_stateless", "postgres");
    }

    @Test
    void mysqlNestItemsColdInsertAndFinalUpdateHaveSeparatePhaseTokens() throws Exception {
        exercise("stateful", "src_bench_nest_items", "mysql");
    }

    private static void exercise(String workloadId, String sourceId, String connectorId) throws Exception {
        BenchmarkWorkloadDefinitions.Workload workload = BenchmarkWorkloadDefinitions.byId(workloadId);
        BenchmarkWorkloadDefinitions.SourceChain chain = workload.sourceChains().stream()
                .filter(source -> source.sourceId().equals(sourceId)).findFirst().orElseThrow();
        BenchmarkBoundaryWrites.Boundary boundary = BenchmarkBoundaryWrites.forChain(workload, chain);
        Map<String, Long> markers = BenchmarkMeasuredEndMarkers.forChain(workload, chain);
        Map<String, Object> source = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.settings("benchmark_measured_end_" + sourceId)
                : SharedPostgres.settings("benchmark_measured_end_" + sourceId);
        try (Connection database = connect(workload, source)) {
            executeAll(database, workload.setupSql());
            Map<String, Object> connectorConfig = workload.connectorConfig(source);
            Path jar = jar(connectorId);
            BenchmarkConnectorPositionCoverage.MySqlSourceLineage mysqlLineage =
                    workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                            ? BenchmarkSourceLineage.readMySql(source).decoderLineage() : null;
            try (BenchmarkTerminalCapture sidecar = BenchmarkTerminalCapture.open(
                    connectorId, jar, connectorConfig, chain.table(),
                    BenchmarkPreflightWrites.warmupRowId(workload, chain), chain.terminalRowId(),
                    boundary, markers);
                    BenchmarkConnectorPositionCoverage coverage = BenchmarkConnectorPositionCoverage.open(
                            workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                                    ? BenchmarkConnectorPositionCoverage.Mode.MYSQL_DEFAULT
                                    : BenchmarkConnectorPositionCoverage.Mode.POSTGRES_PGOUTPUT,
                            jar, connectorConfig, mysqlLineage)) {
                sidecar.awaitWarmup(attempt -> assertOne(database,
                        BenchmarkPreflightWrites.sql(workload, chain, attempt)), WAIT);
                for (BenchmarkWorkloadDefinitions.Phase phase : workload.phases()) {
                    if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.WARM_UP
                            || phase.stage() == BenchmarkWorkloadDefinitions.Stage.HOT_STATE) {
                        executeAll(database, phase.sql());
                    }
                }
                assertOne(database, boundary.changedSql());
                assertOne(database, boundary.restoredSql());
                String boundaryToken = sidecar.awaitBoundary(WAIT);
                assertThat(coverage.covers(boundaryToken, boundaryToken)).isTrue();
                sidecar.sealBoundary();

                for (String phaseId : BenchmarkMeasuredEndMarkers.phaseOrder(markers)) {
                    BenchmarkWorkloadDefinitions.Phase phase = workload.phase(phaseId);
                    String lastSourceSql = phase.sql().stream()
                            .filter(sql -> sql.startsWith("UPDATE " + chain.table() + " ")
                                    || sql.startsWith("INSERT INTO " + chain.table() + " "))
                            .reduce((before, after) -> after).orElseThrow();
                    execute(database, lastSourceSql);
                    String token = sidecar.awaitMeasuredEnd(phaseId, WAIT);
                    assertThat(token).startsWith("rO0AB");
                    assertThat(coverage.covers(token, token)).isTrue();
                }
            }
        }
    }

    private static Connection connect(BenchmarkWorkloadDefinitions.Workload workload,
                                      Map<String, Object> source) throws SQLException {
        return workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.connect(source) : SharedPostgres.connect(source);
    }

    private static void executeAll(Connection database, Iterable<String> sql) throws SQLException {
        for (String command : sql) {
            execute(database, command);
        }
    }

    private static void execute(Connection database, String sql) throws SQLException {
        try (Statement statement = database.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void assertOne(Connection database, String sql) {
        try (Statement statement = database.createStatement()) {
            assertThat(statement.executeUpdate(sql)).as(sql).isEqualTo(1);
        } catch (SQLException failure) {
            throw new AssertionError("cannot write the fixed source preflight or boundary row", failure);
        }
    }

    private static Path jar(String connector) throws Exception {
        Path directory = Path.of(System.getProperty("tapstate.e2e.connectors-dir"));
        try (var jars = Files.list(directory)) {
            List<Path> matches = jars.filter(path -> path.getFileName().toString().startsWith(connector + "-"))
                    .filter(path -> path.getFileName().toString().endsWith(".jar")).toList();
            assertThat(matches).as("exactly one %s connector artifact", connector).hasSize(1);
            return matches.getFirst();
        }
    }
}
