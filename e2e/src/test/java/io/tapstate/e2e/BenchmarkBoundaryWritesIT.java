package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Real MySQL and PostgreSQL rows return to their exact value after the boundary pair. */
class BenchmarkBoundaryWritesIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void changedAndRestoredSqlLeaveAllSixChainsAtTheirPostSetupValue() throws Exception {
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            Map<String, Object> settings = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? SharedMySql.settings("benchmark_boundary_" + workload.id())
                    : SharedPostgres.settings("benchmark_boundary_" + workload.id());
            try (Connection source = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? SharedMySql.connect(settings) : SharedPostgres.connect(settings)) {
                executeAll(source, workload.setupSql());
                for (BenchmarkWorkloadDefinitions.Phase phase : workload.phases()) {
                    if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.WARM_UP
                            || phase.stage() == BenchmarkWorkloadDefinitions.Stage.HOT_STATE) {
                        executeAll(source, phase.sql());
                    }
                }
                for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                    BenchmarkBoundaryWrites.Boundary boundary =
                            BenchmarkBoundaryWrites.forChain(workload, chain);
                    String before = value(source, boundary);
                    assertThat(before).as(chain.id() + " row before the boundary pair")
                            .isEqualTo(unquote(boundary.seededValueSql()));
                    assertThat(update(source, boundary.changedSql()))
                            .as(chain.id() + " changed one real source row").isEqualTo(1);
                    assertThat(value(source, boundary)).as(chain.id() + " changed value")
                            .isNotEqualTo(before);
                    assertThat(update(source, boundary.restoredSql()))
                            .as(chain.id() + " restored one real source row").isEqualTo(1);
                    assertThat(value(source, boundary)).as(chain.id() + " restored exact value")
                            .isEqualTo(before);
                }
            }
        }
    }

    private static void executeAll(Connection source, Iterable<String> sql) throws SQLException {
        try (Statement statement = source.createStatement()) {
            for (String command : sql) {
                statement.execute(command);
            }
        }
    }

    private static int update(Connection source, String sql) throws SQLException {
        try (Statement statement = source.createStatement()) {
            return statement.executeUpdate(sql);
        }
    }

    private static String value(Connection source, BenchmarkBoundaryWrites.Boundary boundary)
            throws SQLException {
        String sql = "SELECT " + boundary.field() + " FROM " + boundary.table()
                + " WHERE id = " + boundary.rowId();
        try (Statement statement = source.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getString(1) : null;
        }
    }

    private static String unquote(String sqlValue) {
        return sqlValue.startsWith("'") && sqlValue.endsWith("'")
                ? sqlValue.substring(1, sqlValue.length() - 1) : sqlValue;
    }
}
