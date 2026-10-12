package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/** A JDK child for observing OS lifecycle, file handshakes and shutdown hooks. */
public final class ClusterProcessControlMain {
    private ClusterProcessControlMain() { }

    public static void main(String[] arguments) throws Exception {
        Map<String, String> settings = new HashMap<>();
        for (String argument : arguments) {
            if (!argument.startsWith("--fixture.")) continue;
            int equals = argument.indexOf('=');
            if (equals >= 0) settings.put(argument.substring(2, equals), argument.substring(equals + 1));
        }
        String mode = settings.getOrDefault("fixture.mode", "quiet");
        if (!mode.equals("quiet") && !mode.equals("exit-seven")) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                write(Path.of(settings.get("fixture.entered")), "entered");
                if (mode.equals("refuse-term")) {
                    for (;;) LockSupport.parkNanos(20_000_000L);
                }
                Path peer = Path.of(settings.get("fixture.peer-entered"));
                while (!Files.isRegularFile(peer)) LockSupport.parkNanos(20_000_000L);
                write(Path.of(settings.get("fixture.completed")), "completed");
            }, "controlled-shutdown-hook"));
        }
        System.out.println("control child pid=" + ProcessHandle.current().pid() + " mode=" + mode);
        write(Path.of(settings.get("fixture.ready")), Long.toString(ProcessHandle.current().pid()));
        if (mode.equals("exit-seven")) System.exit(7);
        for (;;) Thread.sleep(1_000);
    }

    private static void write(Path path, String value) {
        try { Files.writeString(path, value); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }
}
