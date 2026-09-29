package io.tapstate.core.dsl;

import io.tapstate.core.model.ProjectManifest;
import io.tapstate.core.model.SourceResource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * {@code default} names the Default project, which every server has and which no file declares: a
 * resource joins it by carrying no project label. A project file claiming the name, or a label naming it,
 * would be a second way of saying the same thing that could disagree with the first.
 */
class DefaultIsAReservedProjectIdTest {

    @Test
    @DisplayName("a project file declaring id: default is refused with a code")
    void aProjectFileCannotClaimTheDefaultProject() {
        DslException refused = catchThrowableOfType(DslException.class, () -> new DslParser().parseProject("""
                version: tapstate/v1
                kind: project
                id: default
                """));

        assertThat(refused.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(refused.path()).isEqualTo("id");
        assertThat(refused.args()).containsEntry("value", ProjectManifest.DEFAULT);
    }

    @Test
    @DisplayName("the loader refuses the directory rather than reading it as the Default project")
    void theLoaderRefusesTheDirectory(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve(ProjectManifest.FILE_NAME), "version: tapstate/v1\nkind: project\nid: default\n");

        DslException refused = catchThrowableOfType(DslException.class, () -> WorkspaceLoader.load(dir));

        assertThat(refused.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(refused.source()).isEqualTo(ProjectManifest.FILE_NAME);
    }

    @Test
    @DisplayName("a resource in the Default project may carry no project label at all")
    void aLabelInTheDefaultProjectIsRefused() {
        SourceResource labelled = (SourceResource) new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: orders_db
                metadata: { labels: { project: billing } }
                connector: mysql
                config: { host: db }
                """);

        DslException refused = catchThrowableOfType(DslException.class,
                () -> ProjectLabel.requireConsistent(labelled, null));

        assertThat(refused.code()).isEqualTo(DslError.RESERVED_LABEL);
        assertThat(refused.args()).containsEntry("expected", ProjectManifest.DEFAULT);
    }
}
