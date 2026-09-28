package io.tapstate.app;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class MigrateCommandCloudConfigTest {

    @Test
    void migrateUsesTheSameAllOrNothingCloudConfigurationGateAsServerStartup() {
        Output partial = run(
                "migrate", "--list",
                "--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=sentinel-token");

        assertThat(partial.exitCode()).isEqualTo(1);
        assertThat(partial.err()).contains("managed Cloud startup configuration is incomplete");
        assertThat(partial.err()).doesNotContain("sentinel-token");
    }

    @Test
    void aCompleteCloudTriadCanInspectTheBuildWithoutOpeningTheStore() {
        Output complete = run(
                "migrate", "--list",
                "--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=sentinel-token",
                "--tapstate.cloud.atlas-uri=mongodb://user:sentinel-password@atlas.example/metadata");

        assertThat(complete.exitCode()).isZero();
        assertThat(complete.out()).contains("V1BaselineIndexes");
        assertThat(complete.out()).doesNotContain("sentinel-token", "sentinel-password");
        assertThat(complete.err()).isEmpty();
    }

    private static Output run(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode = MigrateCommand.run(args,
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Output(exitCode, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private record Output(int exitCode, String out, String err) {
    }
}
