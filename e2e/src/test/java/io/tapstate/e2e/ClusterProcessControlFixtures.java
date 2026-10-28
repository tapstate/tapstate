package io.tapstate.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/** Builds and reaches only the test child and the existing private fixture lifecycle boundaries. */
final class ClusterProcessControlFixtures {
    private static final String UNUSED_STORE = "mongodb://127.0.0.1:1";

    private ClusterProcessControlFixtures() { }

    static Path jar(Path scratch) throws IOException {
        Path jar = scratch.resolve("process-control.jar");
        if (Files.exists(jar)) return jar;
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, ClusterProcessControlMain.class.getName());
        String resource = ClusterProcessControlMain.class.getName().replace('.', '/') + ".class";
        try (InputStream compiled = ClusterProcessControlMain.class.getClassLoader().getResourceAsStream(resource);
             JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
            if (compiled == null) throw new IOException("the compiled control child is absent");
            output.putNextEntry(new JarEntry(resource));
            compiled.transferTo(output);
            output.closeEntry();
        }
        return jar;
    }

    static RealProcessServer.PreparedLaunch prepare(Path jar, Path staging,
            IntFunction<List<String>> arguments) throws Exception {
        Method prepare = RealProcessServer.class.getDeclaredMethod("prepare", String.class, String.class,
                Path.class, String.class, List.class, Path.class, IntFunction.class);
        prepare.setAccessible(true);
        return (RealProcessServer.PreparedLaunch) invoke(prepare, UNUSED_STORE, "tapstate_nest", jar,
                "127.0.0.1", List.of(), staging, arguments);
    }

    static List<String> arguments(Path ready, String mode, Path entered, Path completed, Path peerEntered) {
        List<String> arguments = new ArrayList<>(List.of(
                "--fixture.ready=" + ready, "--fixture.mode=" + mode));
        if (entered != null) arguments.add("--fixture.entered=" + entered);
        if (completed != null) arguments.add("--fixture.completed=" + completed);
        if (peerEntered != null) arguments.add("--fixture.peer-entered=" + peerEntered);
        return List.copyOf(arguments);
    }

    static RealProcessServer child(Path scratch, String name, String mode,
            Path entered, Path completed, Path peerEntered) throws Exception {
        Path ready = scratch.resolve(name + ".ready");
        try (RealProcessServer.PreparedLaunch prepared = prepare(jar(scratch), null,
                ignored -> arguments(ready, mode, entered, completed, peerEntered))) {
            RealProcessServer server = prepared.launch();
            try {
                Await.until("the owned control child to acknowledge its PID", Duration.ofSeconds(5),
                        () -> acknowledgesPid(ready, server.pid()), server::tail);
                if (!Files.readString(ready).equals(Long.toString(server.pid()))) {
                    throw new AssertionError("the child handshake names a different process");
                }
                return server;
            } catch (RuntimeException | Error | IOException failure) {
                try { server.close(); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
    }

    static boolean acknowledgesPid(Path ready, long pid) {
        try {
            return Files.isRegularFile(ready) && Files.readString(ready).equals(Long.toString(pid));
        } catch (NoSuchFileException notPublishedYet) {
            return false;
        } catch (IOException failure) {
            throw new UncheckedIOException("could not read the owned child PID acknowledgement", failure);
        }
    }

    static String closePartitionBatch(Map<String, RealProcessServer> owned) throws Exception {
        String runId = "process-control-" + UUID.randomUUID();
        Map<String, Path> logs = new LinkedHashMap<>();
        owned.forEach((name, server) -> logs.put(name, server.output()));
        Method close = PartitionableCluster.class.getDeclaredMethod("closeOwned",
                Map.class, Map.class, Map.class, String.class, Map.class);
        close.setAccessible(true);
        invoke(close, owned, Map.of(), Map.of(), runId, logs);
        return runId;
    }

    static Path savedLog(String runId, String name) {
        return Path.of("target", "failure-scenes", "members", runId, name + ".log");
    }

    static void retire(List<RealProcessServer> children) {
        List<Throwable> failures = new ArrayList<>();
        for (RealProcessServer child : children) {
            try { child.requestClose(); }
            catch (RuntimeException | Error failure) { failures.add(failure); }
        }
        for (RealProcessServer child : children) {
            try { child.awaitClosed(); }
            catch (RuntimeException | Error failure) { failures.add(failure); }
        }
        if (!failures.isEmpty()) {
            Throwable primary = failures.getFirst();
            failures.stream().skip(1).filter(failure -> failure != primary).forEach(primary::addSuppressed);
            if (primary instanceof RuntimeException failure) throw failure;
            throw (Error) primary;
        }
    }

    static ServerSocket takingPort(int port) throws IOException {
        ServerSocket taking = new ServerSocket();
        try {
            taking.setReuseAddress(false);
            taking.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            return taking;
        } catch (IOException failure) {
            taking.close();
            throw failure;
        }
    }

    private static Object invoke(Method method, Object... arguments) throws Exception {
        try { return method.invoke(null, arguments); }
        catch (InvocationTargetException wrapper) {
            Throwable cause = wrapper.getCause();
            if (cause instanceof Error failure) throw failure;
            if (cause instanceof RuntimeException failure) throw failure;
            if (cause instanceof Exception failure) throw failure;
            throw new AssertionError(cause);
        }
    }
}
