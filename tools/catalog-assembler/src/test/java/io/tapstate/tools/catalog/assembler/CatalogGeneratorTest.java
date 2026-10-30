package io.tapstate.tools.catalog.assembler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.ConnectorGroup;
import io.tapstate.core.model.SourceMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end: from a connectors checkout and a derived bitmap, the generator walks, normalizes,
 * merges and serializes to a deterministic index, per-connector entries that read back, and an
 * ingest report — the whole PDK-free assembly path, exercised without a real checkout or PDK.
 */
class CatalogGeneratorTest {

    @TempDir
    Path repo;

    @BeforeEach
    void buildFixture() throws IOException {
        write("connectors/mysql-connector/src/main/java/io/tapdata/connector/mysql/MysqlConnector.java", """
                package io.tapdata.connector.mysql;
                @TapConnectorClass("mysql-spec.json")
                public class MysqlConnector {}
                """);
        write("connectors/mysql-connector/src/main/resources/mysql-spec.json", """
                {"properties":{"id":"mysql","name":"Mysql","realName":"MySQL","icon":"icons/mysql.png",
                  "tags":["Database"]},
                 "configOptions":{"capabilities":[
                    {"id":"dml_insert_policy","alternatives":["update_on_exists"]},
                    {"id":"dml_update_policy"}],
                   "connection":{"properties":{"host":{"type":"string","title":"Host","required":true}}}},
                 "messages":{"default":"en_US","en_US":{}}}
                """);
        write("connectors-javascript/github-connector/src/main/resources/spec.json", """
                {"properties":{"id":"github","name":"GitHub","icon":"icon/github.png","tags":["SaaS"]},
                 "configOptions":{"connection":{"properties":{}}},
                 "messages":{"default":"en_US","en_US":{}}}
                """);
    }

    @Test
    void generatesADeterministicIndexInIdOrder() {
        // The head carries the two revisions once for the whole catalog, so a refresh that changed one
        // connector no longer rewrites all of them just to restamp a value none of them differ on.
        assertThat(generate().index()).isEqualTo("""
                {
                  "specSha": "specsha",
                  "capabilitySha": "capsha",
                  "entries": [
                    "github",
                    "mysql"
                  ]
                }
                """);
    }

    @Test
    void generatesEntriesThatReadBackWithDerivedModes() {
        ConnectorCatalogEntry mysql = CatalogEntryReader.read(generate().entries().get("mysql"));
        assertThat(mysql.modes()).containsExactlyInAnyOrder(SourceMode.SNAPSHOT, SourceMode.CDC);
        assertThat(mysql.group()).isEqualTo(ConnectorGroup.DATABASE);
        // Both revisions live in the index head now, so an entry read on its own carries neither.
        assertThat(mysql.provenance().specSha()).isNull();
        assertThat(mysql.provenance().capabilitySha()).isNull();
    }

    @Test
    void reportsTheUndeclaredJavaScriptConnectorAsUnclassified() {
        assertThat(generate().report())
                .contains("## Unclassified — no resolvable mode (need tapstate.modes)")
                .contains("- github");
    }

    @Test
    void specOnlyRefreshUpdatesExistingRowsWithoutAdmittingNewConnectors() throws IOException {
        write("connectors-javascript/new-connector/src/main/resources/spec.json", """
                {"properties":{"id":"new-upstream","name":"New","tags":["Database"]},
                 "configOptions":{"connection":{"properties":{}}},
                 "messages":{"default":"en_US","en_US":{}}}
                """);
        Set<String> existingIds = Set.of("mysql", "github");
        Map<String, Set<String>> bitmap = Map.of("mysql", Set.of("batch_read_function"));

        GeneratedCatalog refreshed = CatalogGenerator.generateSpecOnly(
                List.of(repo), "new-spec", "old-capabilities", bitmap, existingIds);

        assertThat(refreshed.entries()).containsOnlyKeys("mysql", "github");
        ConnectorCatalogEntry mysql = CatalogEntryReader.read(refreshed.entries().get("mysql"));
        assertThat(mysql.config()).extracting(field -> field.name()).contains("host");
        assertThat(mysql.modes()).containsExactly(SourceMode.SNAPSHOT);
        // Existing entries without capability bits are still part of the shipped catalog.
        assertThat(CatalogEntryReader.read(refreshed.entries().get("github")).modes()).isEmpty();
        assertThat(refreshed.index()).contains("\"specSha\": \"new-spec\"",
                "\"capabilitySha\": \"old-capabilities\"").doesNotContain("new-upstream");
        assertThat(refreshed.report()).contains("Ingested connectors: 2").doesNotContain("new-upstream");
        // A full refresh remains the path that can admit a newly discovered connector.
        assertThat(CatalogGenerator.generate(repo, "new-spec", "new-capabilities", bitmap).entries())
                .containsOnlyKeys("mysql", "github", "new-upstream");
    }

    @Test
    void specOnlyAdmissionUsesTheCatalogRatherThanTheBitmapAcrossCheckouts() throws IOException {
        Path enterprise = repo.resolve("enterprise");
        write("enterprise/connectors-javascript/new-connector/src/main/resources/spec.json", """
                {"properties":{"id":"new-enterprise","name":"New","tags":["Database"]},
                 "configOptions":{"connection":{"properties":{}}},
                 "messages":{"default":"en_US","en_US":{}}}
                """);

        GeneratedCatalog refreshed = CatalogGenerator.generateSpecOnly(List.of(repo, enterprise),
                "new-spec", "old-capabilities", Map.of("new-enterprise", Set.of("batch_read_function")),
                Set.of("mysql", "github"));

        assertThat(refreshed.entries()).containsOnlyKeys("mysql", "github");
        assertThat(refreshed.report()).doesNotContain("new-enterprise");
    }

    @Test
    void failsLoudWhenTwoIdsCollideCaseInsensitivelyBecauseEntryFilenamesAreIdDerived() throws IOException {
        // <id>.json filenames collapse on a case-insensitive filesystem (the documented dev platform),
        // which would silently overwrite one entry. Catch the collision at generation, not at write.
        write("connectors-javascript/dup-connector/src/main/resources/spec.json", """
                {"properties":{"id":"dup","name":"Dup","icon":"i.png","tags":["SaaS"]},
                 "configOptions":{"connection":{"properties":{}}},
                 "messages":{"default":"en_US","en_US":{}}}
                """);
        write("connectors-javascript/dup2-connector/src/main/resources/spec.json", """
                {"properties":{"id":"Dup","name":"Dup2","icon":"i.png","tags":["SaaS"]},
                 "configOptions":{"connection":{"properties":{}}},
                 "messages":{"default":"en_US","en_US":{}}}
                """);

        assertThatThrownBy(() -> CatalogGenerator.generate(repo, "specsha", "capsha", Map.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dup");
    }

    private GeneratedCatalog generate() {
        Map<String, Set<String>> bitmap = Map.of("mysql",
                Set.of("batch_read_function", "stream_read_function", "write_record_function"));
        return CatalogGenerator.generate(repo, "specsha", "capsha", bitmap);
    }

    private void write(String relative, String content) throws IOException {
        Path file = repo.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }
}
