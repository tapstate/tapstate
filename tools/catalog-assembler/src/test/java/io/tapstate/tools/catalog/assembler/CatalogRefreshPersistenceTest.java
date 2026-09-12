package io.tapstate.tools.catalog.assembler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.CatalogJson;
import io.tapstate.core.model.SourceMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogRefreshPersistenceTest {
    @TempDir
    Path temp;

    @Test
    void refreshesBothCheckoutsInOneAssembly() throws IOException {
        Path oss = checkout("oss", "mysql");
        Path enterprise = checkout("enterprise", "oracle");
        GeneratedCatalog result = CatalogGenerator.generate(List.of(oss, enterprise), "oss-sha", "caps",
                Map.of("mysql", Set.of("batch_read_function"), "oracle", Set.of("stream_read_function")));
        assertThat(result.entries()).containsOnlyKeys("mysql", "oracle");
        assertThat(result.report()).contains("Ingested connectors: 2");
        assertThat(CatalogEntryReader.read(result.entries().get("oracle")).modes()).contains(SourceMode.CDC);
    }

    @Test
    void refreshingEnterprisePreservesOssAndReplacingOssPreservesEnterprise() throws IOException {
        Path oss = checkout("oss", "mysql");
        Path enterprise = checkout("enterprise", "oracle");
        Path catalog = temp.resolve("catalog");
        Path bitmap = temp.resolve("bitmap.tsv");
        refresh(oss, catalog, bitmap, "mysql\tbatch_read_function\n");
        String mysql = Files.readString(catalog.resolve("mysql.json"));
        refresh(enterprise, catalog, bitmap, "oracle\tstream_read_function\n");
        assertThat(Files.readString(catalog.resolve("mysql.json"))).isEqualTo(mysql);
        assertConsistent(catalog, bitmap);
        assertThat(Files.readString(temp.resolve("report.md"))).contains("Ingested connectors: 2");
        String oracle = Files.readString(catalog.resolve("oracle.json"));
        refresh(oss, catalog, bitmap, "mysql\tstream_read_function\n");
        assertThat(Files.readString(catalog.resolve("oracle.json"))).isEqualTo(oracle);
        assertThat(BitmapReader.read(Files.readString(bitmap)).get("mysql")).containsExactly("stream_read_function");
        assertConsistent(catalog, bitmap);
    }

    @Test
    void aSkippedCurrentIdLosesOldBitsWhileOtherCheckoutsKeepTheirBits() throws IOException {
        Path oss = checkout("oss", "mysql");
        Path enterprise = checkout("enterprise", "oracle");
        Path catalog = temp.resolve("catalog");
        Path bitmap = temp.resolve("bitmap.tsv");
        refresh(oss, catalog, bitmap, "mysql\tbatch_read_function\n");
        refresh(enterprise, catalog, bitmap, "oracle\tstream_read_function\n");
        refresh(enterprise, catalog, bitmap, "");
        assertThat(BitmapReader.read(Files.readString(bitmap))).containsOnlyKeys("mysql");
        assertThat(CatalogEntryReader.read(Files.readString(catalog.resolve("oracle.json"))).modes()).isEmpty();
        assertThat(Files.exists(catalog.resolve("mysql.json"))).isTrue();
    }

    @Test
    void partialRefreshKeepsUnrelatedReportFindingsAndClearsResolvedFindings() throws IOException {
        Path oss = checkout("oss", "mysql");
        Path enterprise = checkout("enterprise", "oracle");
        Path catalog = temp.resolve("catalog");
        Path bitmap = temp.resolve("bitmap.tsv");
        refresh(oss, catalog, bitmap, "");
        refresh(enterprise, catalog, bitmap, "oracle\tstream_read_function\n");
        assertThat(Files.readString(temp.resolve("report.md"))).contains("- mysql\n", "Ingested connectors: 2");
        refresh(oss, catalog, bitmap, "mysql\tbatch_read_function\n");
        assertThat(Files.readString(temp.resolve("report.md"))).doesNotContain("- mysql\n");
    }

    @Test
    void duplicateIdsAcrossCheckoutsFailBeforeWritingAnything() throws IOException {
        Path first = checkout("first", "mysql");
        Path second = checkout("second", "mysql");
        assertThatThrownBy(() -> CatalogGenerator.generate(List.of(first, second), "sha", "caps", Map.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("collide");
    }

    private void assertConsistent(Path catalog, Path bitmap) throws IOException {
        assertThat(CatalogJson.parse(Files.readString(catalog.resolve("index.json"))))
                .isInstanceOfSatisfying(Map.class, index -> assertThat(index.get("entries")).isEqualTo(List.of("mysql", "oracle")));
        assertThat(BitmapReader.read(Files.readString(bitmap))).containsOnlyKeys("mysql", "oracle");
        try (var files = Files.list(catalog)) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .containsExactlyInAnyOrder("index.json", "mysql.json", "oracle.json");
        }
    }

    private void refresh(Path checkout, Path catalog, Path bitmap, String bits) throws IOException {
        GeneratedCatalog generated = CatalogGenerator.generate(checkout, "sha", "caps", BitmapReader.read(bits));
        CatalogArtifactStore.write(generated, bits, catalog, bitmap, temp.resolve("report.md"));
    }

    private Path checkout(String name, String id) throws IOException {
        Path root = temp.resolve(name);
        Path resources = root.resolve("connectors/" + id + "-connector/src/main/resources");
        Files.createDirectories(resources);
        Path java = resources.getParent().resolve("java/Connector.java");
        Files.createDirectories(java.getParent());
        Files.writeString(java, "package fixture; @TapConnectorClass(\"spec.json\") public class Connector {}");
        Files.writeString(resources.resolve("spec.json"), """
                {"properties":{"id":"%s","name":"%s","tags":["Database"]},
                 "configOptions":{"connection":{"properties":{}}},
                 "messages":{"default":"en_US","en_US":{}}}
                """.formatted(id, id));
        return root;
    }
}
