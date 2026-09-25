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

/** A real MySQL stream supplies a warm-up witness and the terminal batch's connector token. */
class BenchmarkTerminalCaptureIT {

    @BeforeAll
    static void requireConnector() {
        DockerGate.require();
        RealConnectorGate.require("mysql");
    }

    @Test
    void presentStartWaitsForARealChangeBeforeCapturingTheUniqueTerminal() throws Exception {
        Map<String, Object> source = SharedMySql.settings("benchmark_terminal_probe");
        sql(source, "CREATE TABLE terminal_probe (id BIGINT PRIMARY KEY, marker VARCHAR(128))");
        try (BenchmarkTerminalCapture sidecar = BenchmarkTerminalCapture.open(
                "mysql", jar("mysql"), source, "terminal_probe", 1, 2)) {
            sidecar.awaitWarmup(attempt -> sql(source,
                    "INSERT INTO terminal_probe (id,marker) VALUES (1,'warm-" + attempt + "') "
                            + "ON DUPLICATE KEY UPDATE marker=VALUES(marker)"), Duration.ofSeconds(60));
            sql(source, "INSERT INTO terminal_probe (id,marker) VALUES (2,'terminal')");
            String token = sidecar.awaitTerminal(Duration.ofSeconds(60));
            assertThat(sidecar.closeAndTerminal()).isEqualTo(token);
            assertThat(token).startsWith("rO0AB");
        }
    }

    private static void sql(Map<String, Object> source, String statement) {
        try (Connection connection = SharedMySql.connect(source);
                Statement sql = connection.createStatement()) {
            sql.execute(statement);
        } catch (SQLException failure) {
            throw new AssertionError("cannot write the source witness", failure);
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
