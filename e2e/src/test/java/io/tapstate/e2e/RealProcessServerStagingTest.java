package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A launch's staging directory goes when the launch does.
 *
 * <p>Every launch stages its connectors into a fresh directory under the system temporary directory,
 * and each one holds copies of the staged jars. Left behind, they pile up until the machine restarts:
 * a week of repeated suite runs on one developer machine left thousands of them, well over a hundred
 * gigabytes.
 *
 * <p>The launch is of a file the JVM refuses as a jar, so no store and no build of the product are
 * needed: the staging directory is made before anything is launched, whether or not anything runs.
 */
class RealProcessServerStagingTest {

    /** Never dialled: the launched JVM exits before it reads any setting. */
    private static final String UNUSED_STORE = "mongodb://127.0.0.1:1";

    @Test
    void aClosedLaunchLeavesNoStagingDirectoryBehind(@TempDir Path scratch) throws IOException {
        RealProcessServer server = RealProcessServer.launching(UNUSED_STORE, notAJar(scratch));
        Path staging = server.stagingDirectory();
        assertThat(staging).isDirectory();

        server.close();

        assertThat(staging).as("the staging directory of a closed launch").doesNotExist();
    }

    @Test
    void aKilledLaunchLeavesNoStagingDirectoryBehind(@TempDir Path scratch) throws IOException {
        // A crash witness kills its server and need not close it as well.
        RealProcessServer server = RealProcessServer.launching(UNUSED_STORE, notAJar(scratch));
        Path staging = server.stagingDirectory();
        assertThat(staging).isDirectory();

        server.kill();

        assertThat(staging).as("the staging directory of a killed launch").doesNotExist();
    }

    private static Path notAJar(Path scratch) throws IOException {
        return Files.writeString(scratch.resolve("not-a.jar"), "");
    }
}
