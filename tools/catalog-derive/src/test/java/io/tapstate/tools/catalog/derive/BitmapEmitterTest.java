package io.tapstate.tools.catalog.derive;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bitmap emitter is catalog-derive's worklist loop: for each manifest entry it resolves the
 * connector id's staged jar and probes the named class, keying the registered capabilities by connector id. Two
 * gaps are expected and survived rather than fatal — a connector with no built jar (not in the OSS
 * dist) and a connector whose jar cannot be opened or classloaded at all, which happens when a module
 * encrypts its own shaded jar during packaging so the result is not a zip — both recorded with a
 * reason and left out of the bitmap, so the refresh completes and the gaps are visible. Jar resolution and probing are injected, so the loop runs without a real checkout.
 */
class BitmapEmitterTest {

    @Test
    void probesEachEntrysResolvedJarAndClassAndKeysCapabilitiesById() {
        List<ManifestEntry> entries = List.of(
                new ManifestEntry("mysql", "mysql-connector", "io.tapdata.connector.mysql.MysqlConnector"),
                new ManifestEntry("kafka", "kafka-connector", "io.tapdata.connector.kafka.KafkaConnector"));

        Function<String, Optional<Path>> jarResolver =
                module -> Optional.of(Path.of("/dist", module + "-connector-v1.0.jar"));
        Map<String, Path> probed = new LinkedHashMap<>();
        BiFunction<Path, String, Set<String>> prober = (jar, connectorClass) -> {
            probed.put(connectorClass, jar);
            return connectorClass.endsWith("MysqlConnector")
                    ? Set.of("batch_read_function", "stream_read_function", "write_record_function")
                    : Set.of("stream_read_function");
        };

        EmitOutcome outcome = BitmapEmitter.emit(entries, jarResolver, prober);

        assertThat(outcome.bitmap()).containsOnlyKeys("mysql", "kafka");
        assertThat(outcome.bitmap().get("mysql")).containsExactlyInAnyOrder(
                "batch_read_function", "stream_read_function", "write_record_function");
        assertThat(outcome.skipped()).isEmpty();
        assertThat(probed.get("io.tapdata.connector.mysql.MysqlConnector"))
                .isEqualTo(Path.of("/dist", "mysql-connector-v1.0.jar"));
    }

    @Test
    void recordsAConnectorWithNoJarAsSkippedWithoutProbing() {
        List<ManifestEntry> entries = List.of(
                new ManifestEntry("mysql", "mysql-connector", "io.tapdata.connector.mysql.MysqlConnector"),
                new ManifestEntry("hazelcast", "hazelcast-connector", "io.tapdata.connector.hazelcast.HazelcastConnector"));

        Function<String, Optional<Path>> jarResolver = module ->
                module.equals("hazelcast") ? Optional.empty()
                        : Optional.of(Path.of("/dist", module + "-connector-v1.0.jar"));
        BiFunction<Path, String, Set<String>> prober = (jar, connectorClass) -> {
            if (connectorClass.contains("hazelcast")) {
                throw new AssertionError("must not probe a connector with no jar");
            }
            return Set.of("batch_read_function");
        };

        EmitOutcome outcome = BitmapEmitter.emit(entries, jarResolver, prober);

        assertThat(outcome.bitmap()).containsOnlyKeys("mysql");
        assertThat(outcome.skipped()).containsEntry("hazelcast", "no built jar");
    }

    @Test
    void recordsAProbeFailureWithItsReasonAndKeepsGoing() {
        List<ManifestEntry> entries = List.of(
                new ManifestEntry("mysql", "mysql-connector", "io.tapdata.connector.mysql.MysqlConnector"),
                new ManifestEntry("postgres", "postgres-connector", "io.tapdata.connector.postgres.PostgresConnector"));

        Function<String, Optional<Path>> jarResolver =
                module -> Optional.of(Path.of("/dist", module + "-connector-v1.0.jar"));
        BiFunction<Path, String, Set<String>> prober = (jar, connectorClass) -> {
            if (connectorClass.endsWith("PostgresConnector")) {
                throw new IllegalStateException("probing " + connectorClass,
                        new ClassNotFoundException(connectorClass));
            }
            return Set.of("batch_read_function");
        };

        EmitOutcome outcome = BitmapEmitter.emit(entries, jarResolver, prober);

        // the failing connector does not abort the run; mysql still derives, postgres is recorded
        assertThat(outcome.bitmap()).containsOnlyKeys("mysql");
        assertThat(outcome.skipped().get("postgres"))
                .contains("ClassNotFoundException")
                .contains("io.tapdata.connector.postgres.PostgresConnector");
    }

    @Test
    void recordsAnErrorThrownDuringProbeRatherThanAbortingTheRefresh() {
        // A connector's static initializer can throw an Error (e.g. an assertion under -ea), which is
        // neither a RuntimeException nor a LinkageError. It must still be recorded and skipped, not
        // abort the whole refresh.
        List<ManifestEntry> entries = List.of(
                new ManifestEntry("mysql", "mysql-connector", "io.tapdata.connector.mysql.MysqlConnector"),
                new ManifestEntry("boom", "boom-connector", "io.tapdata.connector.boom.BoomConnector"));

        Function<String, Optional<Path>> jarResolver =
                module -> Optional.of(Path.of("/dist", module + "-connector-v1.0.jar"));
        BiFunction<Path, String, Set<String>> prober = (jar, connectorClass) -> {
            if (connectorClass.endsWith("BoomConnector")) {
                throw new AssertionError("static init blew up");
            }
            return Set.of("batch_read_function");
        };

        EmitOutcome outcome = BitmapEmitter.emit(entries, jarResolver, prober);

        assertThat(outcome.bitmap()).containsOnlyKeys("mysql");
        assertThat(outcome.skipped().get("boom")).contains("AssertionError").contains("static init blew up");
    }

    @Test
    void emitsAnEmptyOutcomeForAnEmptyManifest() {
        EmitOutcome outcome = BitmapEmitter.emit(
                List.of(), module -> Optional.of(Path.of(module)), (jar, connectorClass) -> Set.of());

        assertThat(outcome.bitmap()).isEmpty();
        assertThat(outcome.skipped()).isEmpty();
    }
}
