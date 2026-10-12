package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.tapstate.e2e.ClusterProcessControlFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real child hooks prove shutdown order; synthetic Process cases isolate unobservable OS failures. */
class ClusterProcessShutdownTest {
    @Test
    void everyQuitIsRequestedBeforeWaitingForEitherPeersShutdownHook(@TempDir Path scratch) throws Exception {
        Path enteredA = scratch.resolve("a.entered");
        Path enteredB = scratch.resolve("b.entered");
        Path completedA = scratch.resolve("a.completed");
        Path completedB = scratch.resolve("b.completed");
        try (RealProcessServer a = child(scratch, "a", "wait-peer", enteredA, completedA, enteredB);
             RealProcessServer b = child(scratch, "b", "wait-peer", enteredB, completedB, enteredA);
             RealProcessServer foreign = child(scratch, "foreign", "quiet", null, null, null)) {
            Map<String, RealProcessServer> owned = new LinkedHashMap<>();
            owned.put("a", a);
            owned.put("b", b);
            String runId = closePartitionBatch(owned);
            assertThat(a.isAlive()).isFalse();
            assertThat(b.isAlive()).isFalse();
            assertThat(completedA).isRegularFile();
            assertThat(completedB).isRegularFile();
            assertThat(Files.readString(savedLog(runId, "a")))
                    .contains("control child pid=" + a.pid());
            assertThat(Files.readString(savedLog(runId, "b")))
                    .contains("control child pid=" + b.pid());
            assertThat(foreign.isAlive()).isTrue();
            assertThat(foreign.stagingDirectory()).isDirectory();
        }
    }

    @Test
    void aRealHookThatRefusesTermIsForcedAfterTheOriginalGraceAndForeignChildrenRemainAlive(
            @TempDir Path scratch) throws Exception {
        Path entered = scratch.resolve("refusing.entered");
        Path completed = scratch.resolve("refusing.completed");
        try (RealProcessServer refusing = child(scratch, "refusing", "refuse-term", entered, completed, null);
             RealProcessServer peer = child(scratch, "peer", "quiet", null, null, null);
             RealProcessServer foreign = child(scratch, "foreign", "quiet", null, null, null)) {
            Map<String, RealProcessServer> owned = new LinkedHashMap<>();
            owned.put("refusing", refusing);
            owned.put("peer", peer);
            long before = System.nanoTime();
            String runId = closePartitionBatch(owned);
            // This is the preserved grace contract, not a speed or throughput threshold.
            assertThat(System.nanoTime() - before).isGreaterThanOrEqualTo(Duration.ofSeconds(20).toNanos());
            assertThat(entered).isRegularFile();
            assertThat(completed).doesNotExist();
            assertThat(refusing.isAlive()).isFalse();
            assertThat(peer.isAlive()).isFalse();
            assertThat(refusing.stagingDirectory()).doesNotExist();
            assertThat(peer.stagingDirectory()).doesNotExist();
            assertThat(savedLog(runId, "refusing")).isRegularFile();
            assertThat(savedLog(runId, "peer")).isRegularFile();
            assertThat(foreign.isAlive()).isTrue();
            assertThat(foreign.stagingDirectory()).isDirectory();
        }
    }

    @Test
    void anUnconfirmedForcedWaitRetainsOwnedStaging(@TempDir Path scratch) throws Exception {
        // A same-user child cannot refuse SIGKILL. This explicitly synthetic boundary leaves the
        // exit unknown; it cannot certify a real member, a real shutdown or a performance sample.
        UnknownExit process = new UnknownExit();
        RealProcessServer unknown = synthetic(process, scratch);
        unknown.requestClose();
        Field requested = RealProcessServer.class.getDeclaredField("closeRequestedAt");
        requested.setAccessible(true);
        requested.setLong(unknown, System.nanoTime() - Duration.ofSeconds(21).toNanos());
        assertThatThrownBy(unknown::awaitClosed).isInstanceOf(AssertionError.class)
                .hasMessageContaining("did not exit");
        assertThat(process.forced).isTrue();
        assertThat(process.isAlive()).isTrue();
        assertThat(unknown.stagingDirectory()).isDirectory();
    }

    @Test
    void aCleanupErrorDoesNotPreventAnotherOwnedChildFromExitingOrHideItsOriginalCause(
            @TempDir Path scratch) throws Exception {
        RuntimeException primary = new IllegalStateException("synthetic wait failed");
        UnknownExit badProcess = new UnknownExit() {
            @Override public boolean waitFor(long timeout, TimeUnit unit) { throw primary; }
        };
        RealProcessServer bad = synthetic(badProcess, scratch);
        try (RealProcessServer good = child(scratch, "good", "quiet", null, null, null);
             RealProcessServer foreign = child(scratch, "foreign", "quiet", null, null, null)) {
            Map<String, RealProcessServer> owned = new LinkedHashMap<>();
            owned.put("bad", bad);
            owned.put("good", good);
            assertThatThrownBy(() -> closePartitionBatch(owned)).isSameAs(primary);
            assertThat(good.isAlive()).isFalse();
            assertThat(good.stagingDirectory()).doesNotExist();
            assertThat(bad.stagingDirectory()).isDirectory();
            assertThat(foreign.isAlive()).isTrue();
        }
    }

    @Test
    void interruptionIsReportedAndRestoredAfterARealOwnedChildExit(@TempDir Path scratch) throws Exception {
        boolean originalFlag = Thread.currentThread().isInterrupted();
        try (RealProcessServer owned = child(scratch, "interrupted", "refuse-term", scratch.resolve("interrupted.entered"), null, null)) {
            try {
                Thread.currentThread().interrupt();
                assertThatThrownBy(owned::close).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("interrupted while waiting");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                assertThat(owned.isAlive()).isFalse();
                assertThat(owned.stagingDirectory()).doesNotExist();
            } finally {
                if (!originalFlag) Thread.interrupted();
            }
        }
    }

    private static RealProcessServer synthetic(Process process, Path scratch) throws Exception {
        Path staging = Files.createDirectory(scratch.resolve("synthetic-staging"));
        Path output = Files.writeString(scratch.resolve("synthetic.out"), "synthetic Process boundary; no OS child\n");
        Constructor<RealProcessServer> constructor = RealProcessServer.class.getDeclaredConstructor(
                Process.class, URI.class, Path.class, Path.class, boolean.class);
        constructor.setAccessible(true);
        return constructor.newInstance(process, URI.create("http://127.0.0.1:1"), output, staging, true);
    }

    private static class UnknownExit extends Process {
        boolean forced;
        @Override public OutputStream getOutputStream() { return OutputStream.nullOutputStream(); }
        @Override public InputStream getInputStream() { return InputStream.nullInputStream(); }
        @Override public InputStream getErrorStream() { return InputStream.nullInputStream(); }
        @Override public int waitFor() { throw new UnsupportedOperationException("no real OS child"); }
        @Override public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
            unit.sleep(timeout);
            return false;
        }
        @Override public int exitValue() { throw new IllegalThreadStateException("exit remains unknown"); }
        @Override public void destroy() { }
        @Override public Process destroyForcibly() { forced = true; return this; }
        @Override public boolean isAlive() { return true; }
    }
}
