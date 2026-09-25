package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorClassLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real connector classes issue the tokens that the benchmark's same-fork ACK oracle compares. */
class BenchmarkConnectorPositionCoverageTest {

    private static final Map<String, Object> MYSQL_CONFIG = Map.of(
            "host", "127.0.0.1", "port", 3306, "database", "benchmark", "highPerformance", false);
    private static final BenchmarkConnectorPositionCoverage.MySqlSourceLineage MYSQL_SOURCE =
            new BenchmarkConnectorPositionCoverage.MySqlSourceLineage(
                    "127.0.0.1", 3306, "benchmark", "11111111-1111-1111-1111-111111111111", 7);

    @BeforeAll
    static void requireConnectors() {
        RealConnectorGate.require("mysql", "postgres");
    }

    @Test
    void mysqlDefaultReaderComparesItsOwnBinlogFilePositionAndIntraEventOrder() throws Exception {
        Path jar = jar("mysql");
        String terminal = mysql(jar, "sidecar-reader", "mysql-bin.000123", 200, 2, 3);
        String ack = mysql(jar, "pipeline-reader", "mysql-bin.000123", 200, 2, 3);
        try (var coverage = BenchmarkConnectorPositionCoverage.open(
                BenchmarkConnectorPositionCoverage.Mode.MYSQL_DEFAULT, jar, MYSQL_CONFIG, MYSQL_SOURCE)) {
            assertThat(coverage.covers(ack, terminal)).isTrue();
            assertThat(coverage.covers(mysql(jar, "pipeline-reader", "mysql-bin.000123", 200, 2, 4), terminal))
                    .isTrue();
            assertThat(coverage.covers(mysql(jar, "pipeline-reader", "mysql-bin.000123", 200, 2, 2), terminal))
                    .isFalse();
            assertThat(coverage.covers(mysql(jar, "pipeline-reader", "mysql-bin.000123", 199, 9, 9), terminal))
                    .isFalse();
            assertThat(coverage.covers(mysql(jar, "pipeline-reader", "mysql-bin.000124", 4, 0, 0), terminal))
                    .isTrue();
            assertThat(coverage.covers(mysql(jar, "pipeline-reader", "mysql-bin.000122", 900, 9, 9), terminal))
                    .isFalse();
        }
    }

    @Test
    void mysqlRejectsOtherReadersMalformedOffsetsAndIncomparableFiles() throws Exception {
        Path jar = jar("mysql");
        String terminal = mysql(jar, "reader-a", "mysql-bin.000123", 200, 2, 3);
        try (var coverage = BenchmarkConnectorPositionCoverage.open(
                BenchmarkConnectorPositionCoverage.Mode.MYSQL_DEFAULT, jar, MYSQL_CONFIG, MYSQL_SOURCE)) {
            assertThat(coverage.covers(mysql(jar, "reader-b", "mysql-bin.000124", 500, 9, 9, 8), terminal))
                    .isFalse();
            assertThat(coverage.covers(mysql(jar, "reader-a", "other-bin.000124", 500, 9, 9), terminal))
                    .isFalse();
            assertThat(coverage.covers(mysql(jar, "reader-a", "mysql-bin.invalid", 500, 9, 9), terminal))
                    .isFalse();
            assertThat(coverage.covers(mysqlRaw(jar, "reader-a", "{\"server\":\"reader-b\"}",
                    "{\"file\":\"mysql-bin.000124\",\"pos\":500}"), terminal)).isFalse();
            assertThat(coverage.covers(mysqlRaw(jar, "reader-a", "{\"server\":\"reader-a\"}",
                    "{\"file\":\"mysql-bin.000124\",\"pos\":-1}"), terminal)).isFalse();
            assertThat(coverage.covers("not-a-token", terminal)).isFalse();
            assertThat(coverage.covers(postgres(jar("postgres"), "{\"lsn\":300}"), terminal)).isFalse();
        }
    }

    @Test
    void postgresPgoutputComparesUnsignedLsnAndRejectsSnapshotOrMalformedOffsets() throws Exception {
        Path jar = jar("postgres");
        String terminal = postgres(jar, "{\"lsn\":9223372036854775807}");
        try (var coverage = BenchmarkConnectorPositionCoverage.open(
                BenchmarkConnectorPositionCoverage.Mode.POSTGRES_PGOUTPUT, jar,
                Map.of("logPluginName", "pgoutput"), null)) {
            assertThat(coverage.covers(postgres(jar, "{\"lsn\":9223372036854775808}"), terminal))
                    .isTrue();
            assertThat(coverage.covers(postgres(jar, "{\"lsn\":9223372036854775806}"), terminal))
                    .isFalse();
            assertThat(coverage.covers(postgres(jar, "{\"lsn\":\"FFFFFFFF/FFFFFFFF\"}"), terminal))
                    .isTrue();
            assertThat(coverage.covers(postgres(jar, "{\"lsn\":\"100000000000000000000\"}"), terminal))
                    .isFalse();
            assertThat(coverage.covers(postgres(jar, "{\"lsn\":1.5}"), terminal)).isFalse();
            assertThat(coverage.covers(postgres(jar, "{\"other\":1}"), terminal)).isFalse();
            assertThat(coverage.covers(postgresSnapshot(jar), terminal)).isFalse();
            assertThat(coverage.covers("not-a-token", terminal)).isFalse();
            assertThat(coverage.covers(mysql(jar("mysql"), "reader-a", "mysql-bin.000123", 200, 2, 3),
                    terminal)).isFalse();
        }
    }

    @Test
    void anAckBehindItsOwnForksTerminalFailsTheOracle() throws Exception {
        Path jar = jar("postgres");
        try (var coverage = BenchmarkConnectorPositionCoverage.open(
                BenchmarkConnectorPositionCoverage.Mode.POSTGRES_PGOUTPUT, jar,
                Map.of("logPluginName", "pgoutput"), null)) {
            var chain = new BenchmarkAckOracle.SourceChain("orders",
                    List.of(new BenchmarkAckOracle.TerminalEvent("terminal-orders",
                            postgres(jar, "{\"lsn\":200}"))),
                    postgres(jar, "{\"lsn\":199}"), coverage);
            var fork = new BenchmarkAckOracle.Fork("fork-1", List.of(chain),
                    Map.of("terminal-orders", 1L), "checksum", 0);
            assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(fork)))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("no authoritative target ACK covering");
        }
    }

    private static String mysql(Path jar, String reader, String file, long pos, long event, long row)
            throws Exception {
        return mysql(jar, reader, file, pos, event, row, 7);
    }

    private static String mysql(Path jar, String reader, String file, long pos, long event, long row,
                                long serverId) throws Exception {
        return mysqlRaw(jar, reader, "{\"server\":\"" + reader + "\"}",
                "{\"file\":\"" + file + "\",\"pos\":" + pos + ",\"event\":" + event
                        + ",\"row\":" + row + ",\"server_id\":" + serverId + "}");
    }

    private static String mysqlRaw(Path jar, String reader, String partition, String offset) throws Exception {
        try (ConnectorClassLoader connector = ConnectorClassLoader.open(List.of(jar))) {
            Class<?> type = connector.load("io.tapdata.connector.mysql.entity.MysqlStreamOffset");
            Object value = type.getConstructor().newInstance();
            type.getMethod("setName", String.class).invoke(value, reader);
            type.getMethod("setOffset", Map.class).invoke(value, Map.of(partition, offset));
            return token(value);
        }
    }

    private static String postgres(Path jar, String sourceOffset) throws Exception {
        try (ConnectorClassLoader connector = ConnectorClassLoader.open(List.of(jar))) {
            Class<?> type = connector.load("io.tapdata.connector.postgres.cdc.offset.PostgresOffset");
            Object value = type.getConstructor().newInstance();
            type.getMethod("setSourceOffset", String.class).invoke(value, sourceOffset);
            return token(value);
        }
    }

    private static String postgresSnapshot(Path jar) throws Exception {
        try (ConnectorClassLoader connector = ConnectorClassLoader.open(List.of(jar))) {
            Class<?> type = connector.load("io.tapdata.connector.postgres.cdc.offset.PostgresOffset");
            Object value = type.getConstructor().newInstance();
            type.getMethod("setOffsetValue", Long.class).invoke(value, 12L);
            return token(value);
        }
    }

    private static String token(Object offset) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(offset);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
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
