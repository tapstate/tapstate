package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import java.nio.file.Path;

/** Checks the operating-system lock from a different JVM, outside Java's same-process overlap table. */
public final class InstallationRuntimeProbe {
    private InstallationRuntimeProbe() {}
    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[1].equals("mutation")) {
            var files = new InstallationFiles(Path.of(args[0])); files.requireOwner();
            try (var channel = java.nio.channels.FileChannel.open(files.directory().resolve("owner.lock"), java.nio.file.StandardOpenOption.WRITE);
                 var lock = channel.lock(0, 1, false)) {
                System.out.println("MUTATION_HELD"); System.out.flush(); System.in.readAllBytes();
            }
            return;
        }
        try (var owner = new InstallationFiles(Path.of(args[0])).ownRuntime()) {
            owner.requireActive(); System.out.println("ACQUIRED");
        } catch (TapstateException refused) { System.out.println("REFUSED"); }
    }
}
