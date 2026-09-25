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

/** The fixed preflight SQL really changes each seeded MySQL/PostgreSQL row and restores it on write 20. */
class BenchmarkPreflightWritesIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void allSixSourceChainsMutateOnEveryAttemptThenReturnToTheirSeededValues() throws Exception {
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            Map<String, Object> settings = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? SharedMySql.settings("benchmark_preflight_" + workload.id())
                    : SharedPostgres.settings("benchmark_preflight_" + workload.id());
            try (Connection source = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? SharedMySql.connect(settings) : SharedPostgres.connect(settings)) {
                assertCaptureLogEnabled(source, workload.database());
                for (String sql : workload.setupSql()) {
                    execute(source, sql);
                }
                for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                    long rowId = BenchmarkPreflightWrites.warmupRowId(workload, chain);
                    String field = fieldOf(BenchmarkPreflightWrites.sql(workload, chain, 1));
                    String seeded = value(source, chain.table(), field, rowId);
                    assertThat(seeded).as(chain.id() + " has a real seeded row").isNotNull();
                    String prior = seeded;
                    for (int attempt = 1; attempt <= BenchmarkPreflightWrites.ATTEMPTS; attempt++) {
                        String sql = BenchmarkPreflightWrites.sql(workload, chain, attempt);
                        assertThat(update(source, sql))
                                .as(chain.id() + " changed exactly its seeded row at attempt " + attempt)
                                .isEqualTo(1);
                        String current = value(source, chain.table(), field, rowId);
                        assertThat(current).as(chain.id() + " actual value at attempt " + attempt)
                                .isNotEqualTo(prior);
                        if (attempt % 2 == 0) {
                            assertThat(current).as(chain.id() + " restored seed at attempt " + attempt)
                                    .isEqualTo(seeded);
                        }
                        prior = current;
                    }
                    assertThat(value(source, chain.table(), field, rowId)).isEqualTo(seeded);
                }
            }
        }
    }

    private static void assertCaptureLogEnabled(Connection source,
                                                 BenchmarkWorkloadDefinitions.Database database)
            throws SQLException {
        String query = database == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? "SELECT @@log_bin" : "SHOW wal_level";
        try (Statement statement = source.createStatement(); ResultSet result = statement.executeQuery(query)) {
            assertThat(result.next()).isTrue();
            if (database == BenchmarkWorkloadDefinitions.Database.MYSQL) {
                assertThat(result.getInt(1)).isEqualTo(1);
            } else {
                assertThat(result.getString(1)).isEqualTo("logical");
            }
        }
    }

    private static void execute(Connection source, String sql) throws SQLException {
        try (Statement statement = source.createStatement()) {
            statement.execute(sql);
        }
    }

    private static int update(Connection source, String sql) throws SQLException {
        try (Statement statement = source.createStatement()) {
            return statement.executeUpdate(sql);
        }
    }

    private static String value(Connection source, String table, String field, long rowId) throws SQLException {
        try (Statement statement = source.createStatement();
                ResultSet result = statement.executeQuery(
                        "SELECT " + field + " FROM " + table + " WHERE id = " + rowId)) {
            return result.next() ? result.getString(1) : null;
        }
    }

    private static String fieldOf(String sql) {
        int start = sql.indexOf(" SET ");
        int end = sql.indexOf(" = ", start + 5);
        if (start < 0 || end < 0) {
            throw new AssertionError("preflight SQL has no one-field assignment: " + sql);
        }
        return sql.substring(start + 5, end);
    }
}
