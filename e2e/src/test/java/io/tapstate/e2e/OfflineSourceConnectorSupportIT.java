package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OfflineSourceConnectorSupportIT {
    @ParameterizedTest
    @ValueSource(strings = {"aliyun-rds-mysql", "tencent-db-postgres"})
    void withdrawnConnectorsCannotScaffoldAnOfflineSource(String connector, @TempDir Path home) {
        CliOnce.Run run = scaffold(connector, home);

        assertThat(run.exitCode()).isEqualTo(1);
        assertThat(run.stderr()).contains("TS0330014", connector);
        assertThat(home.resolve("src_test.tap.yml")).doesNotExist();
    }

    @Test
    void standardConnectorCanStillScaffoldAnOfflineSource(@TempDir Path home) {
        CliOnce.Run run = scaffold("mysql", home);

        assertThat(run.exitCode()).as("CLI diagnostics: %s", run.stderr()).isZero();
        assertThat(home.resolve("src_test.tap.yml")).isRegularFile();
    }

    private static CliOnce.Run scaffold(String connector, Path home) {
        return CliOnce.runWithPassword(null, List.of("-Duser.home=" + home),
                "new", "--non-interactive", "--kind", "source", "--connector", connector,
                "--id", "src_test", "--out", home.toString());
    }
}
