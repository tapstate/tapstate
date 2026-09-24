package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The public lifecycle witness for Oracle LogMiner's identifier preflight.
 *
 * <p>The declarative vocabulary cannot package the harness connector under Oracle's official identity
 * or author an overlong Oracle connection fixture, so this is a Java integration case. The harness
 * connector is deliberate: the refusal belongs to the host and must happen before any connector CDC
 * function runs, so an Oracle database or connector would be downstream of the boundary being proved.
 */
class OracleLogMinerIdentifierPreflightIT {

    private static final String ORACLE = "oracle";
    private static final String PIPELINE = "oracle_logminer_identifier_preflight";
    private static final String SOURCE = "src_oracle";
    private static final String TARGET = "tgt_file";
    private static final String TABLE = "orders";
    private static final String LONG_SCHEMA = "S".repeat(63);
    private static final String FAILURE_CODE = "connector.logminer-identifier-too-long";
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void anUnsupportedSchemaFailsBeforeCdcCanLookHealthy(
            Tiers tier, @TempDir Path directory) throws IOException {
        Path source = directory.resolve("source");
        Path target = directory.resolve("target");
        Files.createDirectories(source);
        Files.createDirectories(target);
        FileEndpoints.replaceTable(source.resolve(TABLE + ".csv"), "id,seq\n1,1\n");

        try (ServerHandle server = tier.launch(
                SharedMongo.replicaSetUrl(
                        "oracle_logminer_preflight_" + tier.name().toLowerCase(Locale.ROOT)))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(ORACLE, read(E2eConnectorJar.buildInto(directory, ORACLE)));
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID, read(E2eConnectorJar.buildInto(directory)));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE + ".tap.yml", sourceYaml(source));
            resources.put(TARGET + ".tap.yml", targetYaml(target));
            resources.put("pipeline.tap.yml", pipelineYaml());
            control.apply(resources);
            control.discoverSchema(SOURCE, ORACLE, sourceSettings(source));

            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the unsupported LogMiner configuration to settle", TIMEOUT,
                    () -> control.state(PIPELINE)
                            .filter(state -> state == PipelineState.FAILED
                                    || state == PipelineState.RUNNING)
                            .isPresent(),
                    () -> "state=" + control.state(PIPELINE)
                            + ", failure=" + control.failureCode(PIPELINE));

            assertThat(control.state(PIPELINE)).contains(PipelineState.FAILED);
            assertThat(control.failureCode(PIPELINE)).contains(FAILURE_CODE);
            assertThat(new FileEndpoints().count(EndpointAddress.uri(target.toString()), TABLE))
                    .as("changes delivered before the LogMiner identifier refusal")
                    .isZero();
        }
    }

    private static Map<String, Object> sourceSettings(Path source) {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("uri", source.toString());
        settings.put("schema", LONG_SCHEMA);
        settings.put("autoLog", false);
        return settings;
    }

    private static String sourceYaml(Path source) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config:
                  uri: "%s"
                  schema: %s
                  autoLog: false
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE, ORACLE, source, LONG_SCHEMA, TABLE);
    }

    private static String targetYaml(Path target) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(TARGET, E2eConnectorJar.CONNECTOR_ID, target);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: cdc_only }
                serve:
                  from: %s
                  sync:
                    - source: %s
                """.formatted(PIPELINE, SOURCE, TABLE, TARGET);
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException cannotRead) {
            throw new UncheckedIOException("cannot read the connector jar", cannotRead);
        }
    }
}
