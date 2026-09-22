package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DockerBinaryTest {

    @Test
    void findsTheWindowsDockerExecutableCandidate(@TempDir Path directory) throws IOException {
        Path docker = Files.createFile(directory.resolve("docker.exe"));
        assertThat(docker.toFile().setExecutable(true)).isTrue();

        assertThat(DockerBinary.isOnThePath(directory.toString(), true)).isTrue();
        assertThat(DockerBinary.isOnThePath(directory.toString(), false)).isFalse();
        assertThat(DockerBinary.isOnThePath(" " + File.pathSeparator + " ", true)).isFalse();
    }
}
