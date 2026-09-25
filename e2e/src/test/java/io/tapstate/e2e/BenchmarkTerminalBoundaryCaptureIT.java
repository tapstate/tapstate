package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The independent connector reader identifies the restored fixed boundary row on both source engines. */
class BenchmarkTerminalBoundaryCaptureIT {

    private static final Duration WAIT = Duration.ofSeconds(60);

    @BeforeAll
    static void requireSources() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres");
    }

    @Test
    void mysqlCopyBoundaryHasAConnectorIssuedRestoreToken() throws Exception {
        exercise("copy", "mysql");
    }

    @Test
    void postgresStatelessBoundaryHasAConnectorIssuedRestoreToken() throws Exception {
        exercise("stateless", "postgres");
    }

    private static void exercise(String workloadId, String connectorId) throws Exception {
        BenchmarkWorkloadDefinitions.Workload workload = BenchmarkWorkloadDefinitions.byId(workloadId);
        BenchmarkWorkloadDefinitions.SourceChain chain = workload.sourceChains().getFirst();
        BenchmarkBoundaryWrites.Boundary boundary = BenchmarkBoundaryWrites.forChain(workload, chain);
        Map<String, Object> source = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.settings("benchmark_sidecar_boundary_copy")
                : SharedPostgres.settings("benchmark_sidecar_boundary_stateless");
        try (Connection database = connect(workload, source)) {
            for (String setup : workload.setupSql()) {
                execute(database, setup);
            }
            assertThat(value(database, boundary)).isEqualTo(unquote(boundary.seededValueSql()));
            Map<String, Object> connectorConfig = workload.connectorConfig(source);
            Path jar = jar(connectorId);
            BenchmarkConnectorPositionCoverage.MySqlSourceLineage mysqlLineage =
                    workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                            ? BenchmarkSourceLineage.readMySql(source).decoderLineage() : null;
            try (BenchmarkTerminalCapture sidecar = BenchmarkTerminalCapture.open(
                    connectorId, jar, connectorConfig, chain.table(),
                    BenchmarkPreflightWrites.warmupRowId(workload, chain), chain.terminalRowId(), boundary);
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
                        for (String command : phase.sql()) {
                            execute(database, command);
                        }
                    }
                }
                assertOne(database, boundary.changedSql());
                assertOne(database, boundary.restoredSql());
                String restoredToken = sidecar.awaitBoundary(WAIT);
                assertThat(restoredToken).startsWith("rO0AB");
                assertThat(coverage.covers(restoredToken, restoredToken)).isTrue();
                assertThat(value(database, boundary)).isEqualTo(unquote(boundary.seededValueSql()));
                sidecar.close();
                assertThat(sidecar.awaitBoundary(Duration.ofSeconds(1))).isEqualTo(restoredToken);
            }
        }
    }

    private static Connection connect(BenchmarkWorkloadDefinitions.Workload workload,
                                      Map<String, Object> source) throws SQLException {
        return workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.connect(source) : SharedPostgres.connect(source);
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
            throw new AssertionError("cannot write the fixed boundary row", failure);
        }
    }

    private static String value(Connection database, BenchmarkBoundaryWrites.Boundary boundary)
            throws SQLException {
        try (Statement statement = database.createStatement();
                ResultSet rows = statement.executeQuery("SELECT " + boundary.field() + " FROM "
                        + boundary.table() + " WHERE id = " + boundary.rowId())) {
            return rows.next() ? rows.getString(1) : null;
        }
    }

    private static String unquote(String sqlValue) {
        return sqlValue.startsWith("'") && sqlValue.endsWith("'")
                ? sqlValue.substring(1, sqlValue.length() - 1) : sqlValue;
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
