package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

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
    void aCompleteCloudConfigurationCanInspectTheBuildWithoutOpeningTheStore() {
        Output complete = run(
                "migrate", "--list",
                "--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=sentinel-token",
                "--tapstate.cloud.atlas-uri=mongodb://user:sentinel-password@atlas.example/" + "c".repeat(63),
                "--tapstate.cloud.cluster-id=migrate-cluster");

        assertThat(complete.exitCode()).isZero();
        assertThat(complete.out()).contains("V1BaselineIndexes");
        assertThat(complete.out()).doesNotContain("sentinel-token", "sentinel-password");
        assertThat(complete.err()).isEmpty();
    }

    @Test
    void migrateCannotAcceptTheOldThreeValueConfigurationWithoutAClusterId() {
        Output partial = run(
                "migrate", "--list",
                "--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=sentinel-token",
                "--tapstate.cloud.atlas-uri=mongodb://user:sentinel-password@atlas.example/metadata");

        assertThat(partial.exitCode()).isEqualTo(1);
        assertThat(partial.err()).contains("managed Cloud startup configuration is incomplete");
        assertThat(partial.err()).doesNotContain("sentinel-token", "sentinel-password");
    }

    private static Output run(String... args) {
        return run(CloudFixtureEnvironment.isolated(), args);
    }

    @Test
    void migrateAcceptsTheSdkClusterIdAliasWithTheOtherThreeValues() {
        var environment = CloudFixtureEnvironment.isolated();
        environment.getPropertySources().addFirst(new SystemEnvironmentPropertySource(
                "sdk-cluster-env", Map.of("CLUSTER_ID", "migrate-alias-cluster")));
        Output complete = run(environment,
                "migrate", "--list",
                "--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=sentinel-token",
                "--tapstate.cloud.atlas-uri=mongodb://user:sentinel-password@atlas.example/metadata");
        assertThat(complete.exitCode()).isZero();
        assertThat(complete.err()).isEmpty();
        assertThat(complete.out()).contains("V1BaselineIndexes");
    }

    @Test
    void firstClearingStopActivationUsesTheConfiguredClusterAndReportsReplay() {
        String[] args = {"migrate", "--enable-first-clearing-stop",
                "--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=sentinel-token",
                "--tapstate.cloud.atlas-uri=mongodb://user:sentinel-password@atlas.example/metadata",
                "--tapstate.cloud.cluster-id=migrate-cluster"};
        try (var configuration = mockStatic(MongoConfigurationDomain.class)) {
            configuration.when(() -> MongoConfigurationDomain.enableFirstClearingStop(
                    any(MongoConnectionSettings.class), eq("migrate-cluster"))).thenReturn(true, false);
            var activated = run(args);
            assertThat(activated.exitCode()).isZero();
            assertThat(activated.out()).contains("First clearing Stop enabled.");
            assertThat(activated.err()).isEmpty();
            var replay = run(args);
            assertThat(replay.exitCode()).isZero();
            assertThat(replay.out()).contains("First clearing Stop is already enabled.");
            assertThat(replay.err()).isEmpty();
        }
    }

    @Test
    void firstClearingStopActivationRefusesEveryOtherMigrationModeBeforeOpeningConfig() {
        try (var configuration = mockStatic(MongoConfigurationDomain.class)) {
            for (String mode : java.util.List.of("--status", "--dry-run", "--list",
                    "--prepare-config-component-identity")) {
                var conflict = run("migrate", "--enable-first-clearing-stop", mode);
                assertThat(conflict.exitCode()).isEqualTo(1);
                assertThat(conflict.err()).contains("Choose either CONFIG component preparation");
            }
            configuration.verifyNoInteractions();
        }
    }

    private static Output run(ConfigurableEnvironment environment, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode = MigrateCommand.run(args,
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8), environment);
        return new Output(exitCode, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private record Output(int exitCode, String out, String err) {
    }
}
