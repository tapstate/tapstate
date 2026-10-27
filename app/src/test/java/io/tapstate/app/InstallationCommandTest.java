package io.tapstate.app;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class InstallationCommandTest {
    @TempDir Path temporary;

    @Test void theExistingServiceBinaryInitializesAndReissuesOnlyTheSameOwnerOperation() throws Exception {
        Path directory = temporary.resolve("installation");
        String initialized = command(directory, "--initialize", 0);
        var files = new InstallationFiles(directory); var first = files.capability().orElseThrow();
        assertThat(initialized).doesNotContain(first.token()).contains("capability.json");
        assertThat(files.configurationUri()).isEmpty();
        String reissued = command(directory, "--authorize", 0);
        var next = files.capability().orElseThrow();
        assertThat(next.scope().operationId()).isEqualTo(first.scope().operationId());
        assertThat(next.scope().generation()).isGreaterThan(first.scope().generation());
        assertThat(next.token()).isNotEqualTo(first.token());
        assertThat(reissued).doesNotContain(first.token(), next.token());
        command(directory, "--initialize", 1);
        assertThat(files.capability()).contains(next);
        assertThat(files.stage().scope()).isEqualTo(next.scope());
    }

    private String command(Path directory, String action, int expected) throws Exception {
        Path log = temporary.resolve("command-" + java.util.UUID.randomUUID() + ".log");
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), Bootstrap.class.getName(), "installation", action,
                "--tapstate.installation.directory=" + directory, "--logging.level.root=ERROR",
                "--spring.config.location=optional:classpath:/application.properties").redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().clear();
        var process = builder.start();
        try {
            assertThat(process.waitFor(20, TimeUnit.SECONDS)).isTrue();
            String output = Files.readString(log);
            assertThat(process.exitValue()).withFailMessage(output).isEqualTo(expected); return output;
        } finally { if (process.isAlive()) { process.destroy(); if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly(); } }
    }
}
