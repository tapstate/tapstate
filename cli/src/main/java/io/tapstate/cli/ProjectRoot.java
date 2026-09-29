package io.tapstate.cli;

import io.tapstate.core.model.ProjectManifest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.UnaryOperator;

/**
 * Where the project a command works on is rooted. In order: the directory named with {@code -w}, then
 * {@code TAPSTATE_WORKDIR}, then the nearest directory at or above the current one that holds a
 * {@code project.tap.yml}, then the conventional {@code tap-work}. The walk upwards is what lets a
 * command run from any subdirectory of a project act on the whole project; the last step keeps every
 * directory that predates project files working exactly as it did.
 */
final class ProjectRoot {

    /** The environment variable naming a project root. */
    static final String ENV = "TAPSTATE_WORKDIR";

    /** The root used when nothing names one and no project file is found. */
    static final String CONVENTIONAL = "tap-work";

    private ProjectRoot() {
    }

    /** The root from the process environment and working directory. */
    static Path resolve(String explicit) {
        return resolve(explicit, System::getenv, Path.of("").toAbsolutePath());
    }

    /**
     * The root, given what was named on the command line ({@code null} for nothing), the environment and
     * the working directory. A found project directory is returned relative to {@code cwd}, so it reads
     * the way a directory the user named would.
     */
    static Path resolve(String explicit, UnaryOperator<String> env, Path cwd) {
        if (explicit != null && !explicit.isBlank()) {
            return Path.of(explicit);
        }
        String fromEnv = env.apply(ENV);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Path.of(fromEnv);
        }
        Path here = cwd.toAbsolutePath().normalize();
        for (Path dir = here; dir != null; dir = dir.getParent()) {
            if (Files.isRegularFile(dir.resolve(ProjectManifest.FILE_NAME))) {
                Path relative = here.relativize(dir);
                return relative.toString().isEmpty() ? Path.of(".") : relative;
            }
        }
        return Path.of(CONVENTIONAL);
    }
}
