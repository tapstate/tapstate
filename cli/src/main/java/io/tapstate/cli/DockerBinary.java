package io.tapstate.cli;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Whether a {@code docker} command is on the PATH - the one question two commands ask before they say
 * anything about a container: {@code demo}, which starts nothing and only wants to warn, and the guided
 * first run, which is about to start a stack and wants to refuse early.
 *
 * <p>Probed rather than run: a version probe would start a process, and on a machine where the daemon
 * is down it would hang a command that had no reason to touch it. Whether the daemon is actually
 * answering is a separate question, asked by the one caller that goes on to need it.
 */
final class DockerBinary {

    private DockerBinary() {
    }

    /** Whether {@code docker} resolves on the PATH. Nothing is executed. */
    static boolean isOnThePath() {
        return isOnThePath(System.getenv("PATH"), System.getProperty("os.name", "").startsWith("Windows"));
    }

    static boolean isOnThePath(String path, boolean windows) {
        if (path == null || path.isBlank()) {
            return false;
        }
        String[] candidates = windows ? new String[] {"docker.exe", "docker"} : new String[] {"docker"};
        for (String entry : path.split(File.pathSeparator)) {
            if (!entry.isBlank()) {
                for (String candidate : candidates) {
                    if (Files.isExecutable(Path.of(entry, candidate))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
