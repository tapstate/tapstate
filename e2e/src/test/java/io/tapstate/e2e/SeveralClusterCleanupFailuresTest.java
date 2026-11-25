package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.tapstate.e2e.ClusterProcessControlFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Synthetic OS-error boundaries test aggregation; actual children independently prove peer and foreign ownership. */
class SeveralClusterCleanupFailuresTest {
    @Test
    void requestAndTwoWaitFailuresRemainAttachedWhileEveryOwnedLogAndRealPeerIsRetained(
            @TempDir Path scratch) throws Exception {
        RuntimeException request = new IllegalStateException("synthetic quit request failure");
        RuntimeException waitA = new IllegalStateException("synthetic first wait failure");
        AssertionError waitB = new AssertionError("synthetic second wait failure");
        RealProcessServer a = synthetic(new FailedProcess(request, waitA), scratch, "a");
        RealProcessServer b = synthetic(new FailedProcess(null, waitB), scratch, "b");
        String runId = "multiple-cleanup-failures-" + UUID.randomUUID();
        try (RealProcessServer peer = child(scratch, "real-peer", "quiet", null, null, null);
             RealProcessServer foreign = child(scratch, "outside-batch", "quiet", null, null, null)) {
            Map<String, RealProcessServer> owned = new LinkedHashMap<>();
            owned.put("a", a);
            owned.put("b", b);
            owned.put("real-peer", peer);
            Map<String, Path> logs = new LinkedHashMap<>();
            owned.forEach((name, process) -> logs.put(name, process.output()));
            assertThatThrownBy(() -> close(owned, runId, logs)).isSameAs(request)
                    .satisfies(primary -> assertThat(primary.getSuppressed()).containsExactly(waitA, waitB));
            assertThat(peer.isAlive()).isFalse();
            assertThat(peer.stagingDirectory()).doesNotExist();
            assertThat(a.stagingDirectory()).isDirectory();
            assertThat(b.stagingDirectory()).isDirectory();
            assertThat(Files.readString(savedLog(runId, "a"))).contains("synthetic boundary a; no OS process");
            assertThat(Files.readString(savedLog(runId, "b"))).contains("synthetic boundary b; no OS process");
            assertThat(Files.readString(savedLog(runId, "real-peer"))).contains("control child pid=" + peer.pid());
            assertThat(foreign.isAlive()).isTrue();
            assertThat(foreign.stagingDirectory()).isDirectory();
        }
    }

    private static RealProcessServer synthetic(Process process, Path scratch, String name) throws Exception {
        Path staging = Files.createDirectory(scratch.resolve(name + "-staging"));
        Path output = Files.writeString(scratch.resolve(name + ".out"), "synthetic boundary " + name + "; no OS process\n");
        Constructor<RealProcessServer> constructor = RealProcessServer.class.getDeclaredConstructor(
                Process.class, URI.class, Path.class, Path.class, boolean.class);
        constructor.setAccessible(true);
        return constructor.newInstance(process, URI.create("http://127.0.0.1:1"), output, staging, true);
    }

    private static void close(Map<String, RealProcessServer> owned, String runId, Map<String, Path> logs)
            throws Exception {
        Method close = PartitionableCluster.class.getDeclaredMethod("closeOwned",
                Map.class, Map.class, Map.class, String.class, Map.class);
        close.setAccessible(true);
        try { close.invoke(null, owned, Map.of(), Map.of(), runId, logs); }
        catch (InvocationTargetException wrapper) {
            Throwable cause = wrapper.getCause();
            if (cause instanceof Error failure) throw failure;
            if (cause instanceof RuntimeException failure) throw failure;
            if (cause instanceof Exception failure) throw failure;
            throw new AssertionError(cause);
        }
    }

    private static final class FailedProcess extends Process {
        private final RuntimeException requestFailure;
        private final Throwable waitFailure;
        private FailedProcess(RuntimeException requestFailure, Throwable waitFailure) {
            this.requestFailure = requestFailure;
            this.waitFailure = waitFailure;
        }
        @Override public OutputStream getOutputStream() { return OutputStream.nullOutputStream(); }
        @Override public InputStream getInputStream() { return InputStream.nullInputStream(); }
        @Override public InputStream getErrorStream() { return InputStream.nullInputStream(); }
        @Override public int waitFor() { throw new UnsupportedOperationException("synthetic OS failure"); }
        @Override public boolean waitFor(long timeout, TimeUnit unit) {
            if (waitFailure instanceof RuntimeException failure) throw failure;
            throw (Error) waitFailure;
        }
        @Override public int exitValue() { throw new IllegalThreadStateException("synthetic exit is unknown"); }
        @Override public void destroy() { if (requestFailure != null) throw requestFailure; }
        @Override public Process destroyForcibly() { return this; }
        @Override public boolean isAlive() { return true; }
    }
}
