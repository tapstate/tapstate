package io.tapstate.cli;

import io.tapstate.core.dsl.WorkspaceLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A command run anywhere inside a project acts on the whole project: the root is the nearest directory
 * holding a {@code project.tap.yml}, so a validate started in {@code pipeline/} still sees the sources
 * one level up. Without a project file nothing changes from before - the conventional {@code tap-work}.
 */
class RootIsFoundFromASubfolderTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: orders_db
            connector: mysql
            config: { host: db, database: orders, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: orders_sync
            source: orders_db
            view: { id: orders_view, from: orders, primary_key: id }
            """;

    private static final Map<String, String> NO_ENV = Map.of();

    @Test
    void theRootIsTheDirectoryHoldingTheProjectFile(@TempDir Path project) throws IOException {
        Files.writeString(project.resolve("project.tap.yml"), "version: tapstate/v1\nkind: project\nid: orders\n");
        Files.createDirectories(project.resolve("source"));
        Files.createDirectories(project.resolve("pipeline"));
        Files.writeString(project.resolve("source/orders_db.tap.yml"), SOURCE);
        Files.writeString(project.resolve("pipeline/orders_sync.tap.yml"), PIPELINE);
        Path inside = project.resolve("pipeline");

        Path root = inside.resolve(ProjectRoot.resolve(null, NO_ENV::get, inside)).normalize();

        assertThat(root).isEqualTo(project.toAbsolutePath().normalize());
        // The point of finding the root: the pipeline's source, one directory up, is part of what is read.
        assertThat(WorkspaceLoader.load(root).resources()).extracting(r -> r.id())
                .containsExactlyInAnyOrder("orders_db", "orders_sync");
    }

    @Test
    void theProjectDirectoryItselfResolvesToHere(@TempDir Path project) throws IOException {
        Files.writeString(project.resolve("project.tap.yml"), "version: tapstate/v1\nkind: project\nid: orders\n");

        assertThat(ProjectRoot.resolve(null, NO_ENV::get, project)).isEqualTo(Path.of("."));
    }

    @Test
    void withoutAProjectFileTheConventionalDirectoryIsUsed(@TempDir Path somewhere) {
        assertThat(ProjectRoot.resolve(null, NO_ENV::get, somewhere)).isEqualTo(Path.of("tap-work"));
    }

    @Test
    void aNamedDirectoryAndTheEnvironmentComeFirst(@TempDir Path project) throws IOException {
        Files.writeString(project.resolve("project.tap.yml"), "version: tapstate/v1\nkind: project\nid: orders\n");

        assertThat(ProjectRoot.resolve("elsewhere", NO_ENV::get, project)).isEqualTo(Path.of("elsewhere"));
        assertThat(ProjectRoot.resolve(null, Map.of("TAPSTATE_WORKDIR", "from-env")::get, project))
                .isEqualTo(Path.of("from-env"));
    }
}
