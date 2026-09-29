package io.tapstate.cli;

import io.tapstate.core.model.ProjectManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A directory with no project file is brought up into the Default project: every apply names no project,
 * so the server writes no label, and the summary says where the resources went. Naming the project after
 * the directory - the rule this replaced - would put every such directory into a project of its own.
 */
class UpWithoutProjectFileDoesNotLabelTest {

    @Test
    void everyApplyNamesNoProjectAndTheSummarySaysDefaultProject(@TempDir Path home, @TempDir Path ws) {
        UpCmdTest.scaffold(home, ws);
        assertThat(Files.exists(ws.resolve(ProjectManifest.FILE_NAME))).as("new wrote no project file").isFalse();
        UpCmdTest.signIn(home);
        UpCmdTest.FakeUpControlPlane client = new UpCmdTest.FakeUpControlPlane();

        UpCmdTest.Run r = UpCmdTest.up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(client.projects).as("one entry per apply, each naming no project").containsExactly(null, null);
        assertThat(r.out()).startsWith("Project: Default project (" + ws + ")\n")
                .contains("Hint: no project.tap.yml here, so these resources are in the Default project.");
    }
}
