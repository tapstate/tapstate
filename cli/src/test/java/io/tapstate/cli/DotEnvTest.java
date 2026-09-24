package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DotEnvTest {

    @Test
    void keepsWhitespaceThatBelongsToASecretValue(@TempDir Path directory) throws IOException {
        Path env = directory.resolve(".env");
        Files.writeString(env, "PASSWORD=ends with a space \n  # comment\nNAME = value\n");

        assertThat(DotEnv.read(env))
                .containsEntry("PASSWORD", "ends with a space ")
                .containsEntry("NAME", " value");
    }
}
