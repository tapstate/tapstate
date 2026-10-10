package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void anUnconfirmedKillRetainsTheOwnedStagingDirectory(@TempDir Path scratch) throws IOException {
        Path staging = Files.createDirectory(scratch.resolve("staging"));
        RealProcessServer server = new RealProcessServer(new UnendedProcess(),
                URI.create("http://127.0.0.1:1"), scratch.resolve("server.out"), staging);

        assertThatThrownBy(server::kill).isInstanceOf(AssertionError.class)
                .hasMessageContaining("did not end");
        assertThat(server.terminated()).isFalse();
        assertThat(staging).as("an unconfirmed child may still need its owned artifacts").isDirectory();
    }

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

    @Test
    void aStagingDirectoryAWitnessNamedStaysWhenTheLaunchEnds(@TempDir Path scratch) throws IOException {
        // The witness made it and may still read it; clearing it is the witness's own business.
        Path named = Files.createDirectory(scratch.resolve("named-staging"));
        RealProcessServer closed = RealProcessServer.launching(UNUSED_STORE, notAJar(scratch), named);
        closed.close();
        RealProcessServer killed = RealProcessServer.launching(UNUSED_STORE, notAJar(scratch), named);
        killed.kill();

        assertThat(closed.stagingDirectory()).isEqualTo(named);
        assertThat(named).as("a staging directory the witness named").isDirectory();
    }

    private static Path notAJar(Path scratch) throws IOException {
        return Files.writeString(scratch.resolve("not-a.jar"), "");
    }

    private static final class UnendedProcess extends Process {
        @Override public java.io.OutputStream getOutputStream() { return java.io.OutputStream.nullOutputStream(); }
        @Override public java.io.InputStream getInputStream() { return java.io.InputStream.nullInputStream(); }
        @Override public java.io.InputStream getErrorStream() { return java.io.InputStream.nullInputStream(); }
        @Override public int waitFor() { throw new UnsupportedOperationException("only bounded waits are expected"); }
        @Override public boolean waitFor(long timeout, TimeUnit unit) { return false; }
        @Override public int exitValue() { throw new IllegalThreadStateException("the controlled child is still alive"); }
        @Override public boolean isAlive() { return true; }
        @Override public void destroy() { }
        @Override public Process destroyForcibly() { return this; }
    }
}
