package io.tapstate.core.dsl;

import io.tapstate.core.model.ProjectManifest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A project file names a project and carries nothing else. The day it grows a deployment target or
 * an environment it becomes a second configuration file, kept in step with the context by nobody, so
 * the closed field set is the thing held here - not merely that a well-formed file parses.
 */
class ProjectKindAcceptsOnlyIdAndMetadataTest {

    private final DslParser parser = new DslParser();

    @Test
    @DisplayName("a project file with an id and metadata is read as that project")
    void idAndMetadataAreRead() {
        ProjectManifest manifest = parser.parseProject("""
                version: tapstate/v1
                kind: project
                id: bank_c360
                metadata:
                  description: Customer 360 on the core banking and cards systems
                  labels: { team: field }
                """);

        assertThat(manifest.id()).isEqualTo("bank_c360");
        assertThat(manifest.metadata().description()).startsWith("Customer 360");
        assertThat(manifest.metadata().labels()).containsEntry("team", "field");
    }

    @Test
    @DisplayName("a field beyond id and metadata is refused by name")
    void aDeploymentTargetIsRefused() {
        DslException refused = catchThrowableOfType(DslException.class, () -> parser.parseProject("""
                version: tapstate/v1
                kind: project
                id: bank_c360
                server: https://tapstate.example.com
                """));

        assertThat(refused.code()).isEqualTo(DslError.UNKNOWN_FIELD);
        assertThat(refused.args()).containsEntry("field", "server");
    }

    @Test
    @DisplayName("a project file without an id is refused")
    void anIdIsRequired() {
        DslException refused = catchThrowableOfType(DslException.class, () -> parser.parseProject("""
                version: tapstate/v1
                kind: project
                metadata: { description: nameless }
                """));

        assertThat(refused.code()).isEqualTo(DslError.MISSING_FIELD);
        assertThat(refused.args()).containsEntry("field", "id");
    }

    @Test
    @DisplayName("a project file is set aside by the loader rather than read as a resource")
    void theLoaderDoesNotTakeTheProjectFileForAResource(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve(ProjectManifest.FILE_NAME), """
                version: tapstate/v1
                kind: project
                id: orders_team
                """);
        Files.writeString(dir.resolve("src.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: mysql
                config: { host: db, database: orders, username: u, password: p }
                mode: cdc
                tables: [ orders ]
                """);

        assertThat(WorkspaceLoader.load(dir).resources()).extracting(r -> r.id()).containsExactly("src_orders");
        assertThat(WorkspaceLoader.projectId(dir)).isEqualTo("orders_team");
    }

    @Test
    @DisplayName("a directory with no project file names no project: it is the Default project")
    void aDirectoryWithoutAProjectFileIsTheDefaultProject(@TempDir Path parent) throws IOException {
        Path dir = Files.createDirectory(parent.resolve("orders"));

        assertThat(WorkspaceLoader.projectId(dir)).as("not the directory's name").isNull();
    }
}
