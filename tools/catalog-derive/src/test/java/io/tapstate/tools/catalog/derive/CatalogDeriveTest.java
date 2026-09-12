package io.tapstate.tools.catalog.derive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The derive driver wires the whole catalog-derive step: read the probe manifest, resolve each
 * connector id's dist jar, probe it, and write the capability bitmap. Probing is injected so the wiring is
 * tested without classloading; the entry point passes the real {@code ConnectorCapabilityProbe}.
 */
class CatalogDeriveTest {

    @TempDir
    Path tmp;

    @Test
    void readsTheManifestProbesEachJarAndWritesTheBitmap() throws IOException {
        Path manifest = tmp.resolve("manifest.tsv");
        Files.writeString(manifest, "mysql\tmysql-connector\tio.tapdata.connector.mysql.MysqlConnector\n");
        Path dist = Files.createDirectory(tmp.resolve("dist"));
        Files.createFile(dist.resolve("mysql-connector-v1.0-SNAPSHOT.jar"));
        Path bitmap = tmp.resolve("bitmap.tsv");

        BiFunction<Path, String, Set<String>> prober =
                (jar, connectorClass) -> Set.of("batch_read_function", "write_record_function");

        CatalogDerive.run(manifest, dist, bitmap, prober);

        assertThat(Files.readString(bitmap))
                .isEqualTo("mysql\tbatch_read_function\twrite_record_function\n");
    }

    @Test
    void surfacesBothGapKindsThroughRunWhileWritingOnlyTheSurvivors() throws IOException {
        // Drive the whole resilience contract through the real entry point: a resolvable connector, a
        // connector with no built jar, and a connector whose probe throws. The run must not abort, must
        // record both gaps with reasons, and must write the bitmap for the survivor only.
        Path manifest = tmp.resolve("manifest.tsv");
        Files.writeString(manifest, """
                boom\tboom-connector\tio.tapdata.connector.boom.BoomConnector
                mysql\tmysql-connector\tio.tapdata.connector.mysql.MysqlConnector
                nojar\tnojar-connector\tio.tapdata.connector.nojar.NoJarConnector
                """);
        Path dist = Files.createDirectory(tmp.resolve("dist"));
        Files.createFile(dist.resolve("mysql-connector-v1.0-SNAPSHOT.jar"));
        Files.createFile(dist.resolve("boom-connector-v1.0-SNAPSHOT.jar"));
        // no nojar-connector jar on purpose
        Path bitmap = tmp.resolve("bitmap.tsv");

        BiFunction<Path, String, Set<String>> prober = (jar, connectorClass) -> {
            if (connectorClass.endsWith("BoomConnector")) {
                throw new IllegalStateException("boom", new ClassNotFoundException(connectorClass));
            }
            return Set.of("batch_read_function");
        };

        EmitOutcome outcome = CatalogDerive.run(manifest, dist, bitmap, prober);

        assertThat(outcome.bitmap()).containsOnlyKeys("mysql");
        assertThat(outcome.skipped()).containsEntry("nojar", "no built jar");
        assertThat(outcome.skipped().get("boom")).contains("ClassNotFoundException");
        assertThat(Files.readString(bitmap)).isEqualTo("mysql\tbatch_read_function\n");
    }

    @Test
    void resolvesSqlServerByItsPublicIdWhenTheUpstreamModuleIsMssql() throws IOException {
        Path manifest = tmp.resolve("manifest.tsv");
        Files.writeString(manifest, "sqlserver\tmssql-connector\tio.tapdata.connector.mssql.MssqlConnector\n");
        Path dist = Files.createDirectory(tmp.resolve("dist"));
        Files.createFile(dist.resolve("sqlserver-connector-v1.jar"));
        EmitOutcome outcome = CatalogDerive.run(manifest, dist, tmp.resolve("bitmap.tsv"),
                (jar, connectorClass) -> Set.of("batch_read_function"));
        assertThat(outcome.bitmap()).containsKey("sqlserver");
        assertThat(outcome.skipped()).isEmpty();
    }

    @Test
    void mainRejectsAWrongNumberOfArguments() {
        assertThatThrownBy(() -> CatalogDerive.main(new String[]{"only-one"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("usage");
    }
}
