package io.tapstate.archtests;

import io.tapstate.core.catalog.OfficialConnectors;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorSupportDocumentationTest {
    private static final List<Path> DOCUMENTS = List.of(
            Path.of("../docs/quickstart-online.md"),
            Path.of("../deploy/quickstart/connectors/README.md"));
    private static final String TABLE_HEADER = "| Database | Connector kind | Certified use |";
    private static final Map<String, String> DATABASE_NAMES = Map.of(
            "mysql", "MySQL", "postgres", "PostgreSQL", "mongodb", "MongoDB",
            "oracle", "Oracle", "sqlserver", "SQL Server");

    @Test
    void bothSupportTablesNameExactlyTheDeclaredDatabaseKinds() throws IOException {
        for (Path document : DOCUMENTS) {
            assertDocumentedKinds(Files.readString(document), OfficialConnectors.IDS_BY_DATABASE_KIND.keySet());
        }
    }

    @Test
    void addingAKindWithoutUpdatingRealDocumentationFails() throws IOException {
        Set<String> expanded = new LinkedHashSet<>(OfficialConnectors.IDS_BY_DATABASE_KIND.keySet());
        expanded.add("sixth-database");
        for (Path document : DOCUMENTS) {
            String text = Files.readString(document);
            assertThatThrownBy(() -> assertDocumentedKinds(text, expanded))
                    .as("the declaration grows while the actual user-facing table stays unchanged: %s", document)
                    .isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void certificationDirectionsAndSupportBoundaryAreExplicit() throws IOException {
        for (Path document : DOCUMENTS) {
            String text = Files.readString(document);
            assertThat(text).as("support boundary in %s", document)
                    .contains("| MySQL | `mysql` | Read and write |",
                            "| PostgreSQL | `postgres` | Read and write |",
                            "| MongoDB | `mongodb` | Read and write |",
                            "| Oracle | `oracle` | Read and write |",
                            "| SQL Server | `sqlserver` | Read and write |",
                            "Oracle Free 23", "SQL Server 2022", "DECIMAL(18,4)", "schema rediscovery",
                            "16 connector ids", "managed variants", "not been live-verified",
                            "on this server", "outside the supported configuration",
                            "server's actual accepted set", "including any additional ids",
                            "versioned releases", "`connectors-preview`", "quickstart",
                            "CI artifacts", "7 days");
        }
    }

    @Test
    void actualBoundaryProseDoesNotAssignTheOverrideButAnAddedAssignmentDoes() throws IOException {
        for (Path document : DOCUMENTS) {
            String text = Files.readString(document);
            assertThat(text).as("the actual new prose must name the option: %s", document)
                    .contains("`tapstate.connectors.also-accept-ids`");
            assertThat(ConnectorAcceptanceGatesTest.assignsTheSetting(text))
                    .as("documenting the boundary must not open it: %s", document).isFalse();
            assertThat(ConnectorAcceptanceGatesTest.assignsTheSetting(
                    text + "\ntapstate.connectors.also-accept-ids: sixth-database\n"))
                    .as("a runnable assignment added to the same document must still be refused: %s", document)
                    .isTrue();
        }
    }

    @Test
    void missingTablesAndIncorrectDatabaseNamesCannotPassAsAnEmptyInventory() throws IOException {
        assertThatThrownBy(() -> documentedKinds("No certification table."))
                .isInstanceOf(AssertionError.class);
        String actual = Files.readString(DOCUMENTS.getFirst());
        assertThatThrownBy(() -> documentedKinds(actual.replace("| MySQL | `mysql`", "| Another DB | `mysql`")))
                .isInstanceOf(AssertionError.class);
    }

    private static void assertDocumentedKinds(String text, Set<String> declared) {
        assertThat(documentedKinds(text)).as("documented database names must reconcile with the declaration")
                .containsExactlyInAnyOrderElementsOf(declared);
    }

    private static Set<String> documentedKinds(String text) {
        int start = text.indexOf(TABLE_HEADER);
        assertThat(start).as("a user-visible certification table must exist").isNotNegative();
        assertThat(text.indexOf(TABLE_HEADER, start + TABLE_HEADER.length()))
                .as("a document must have one authoritative certification table").isEqualTo(-1);
        String table = text.substring(start).split("\\R\\s*\\R", 2)[0];
        List<String> rows = table.lines().toList();
        assertThat(rows.size()).isGreaterThan(2);
        Set<String> kinds = new LinkedHashSet<>();
        for (String row : rows.subList(2, rows.size())) {
            String[] cells = row.split("\\|", -1);
            assertThat(cells).as("support table row: %s", row).hasSize(5);
            String kind = cells[2].strip().replace("`", "");
            assertThat(DATABASE_NAMES).as("known display name for %s", kind).containsKey(kind);
            assertThat(cells[1].strip()).isEqualTo(DATABASE_NAMES.get(kind));
            assertThat(kinds.add(kind)).as("unique documented database kind: %s", kind).isTrue();
        }
        return kinds;
    }
}
