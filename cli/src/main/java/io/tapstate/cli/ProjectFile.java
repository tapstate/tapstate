package io.tapstate.cli;

import io.tapstate.core.dsl.WorkspaceLoader;
import io.tapstate.core.model.ProjectManifest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * The project file a scaffolding verb writes when it is told which project the files belong to. Without
 * one the directory's resources land in the Default project, so the file is never written on a guess.
 */
final class ProjectFile {

    /** The kind a scaffolding report lists the project file under. */
    static final String KIND = ProjectManifest.KIND;

    /** An id that YAML reads back as the same plain string with no quoting. */
    private static final Pattern PLAIN = Pattern.compile("[A-Za-z_][A-Za-z0-9_-]*");

    private ProjectFile() {
    }

    /** Whether {@code root} already carries a project file, which a scaffolding verb never replaces. */
    static boolean presentIn(Path root) {
        return Files.exists(root.resolve(ProjectManifest.FILE_NAME));
    }

    /** The project {@code root}'s project file declares, or null when it has none. */
    static String declaredIn(Path root) {
        return WorkspaceLoader.manifest(root).map(ProjectManifest::id).orElse(null);
    }

    /** The text of a project file declaring {@code id}. */
    static String content(String id) {
        String spelled = PLAIN.matcher(id).matches() && !reserved(id) ? id
                : "\"" + id.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        return "version: tapstate/v1\nkind: project\nid: " + spelled + "\n";
    }

    /** The plain words YAML would read as something other than a string. */
    private static boolean reserved(String id) {
        return switch (id.toLowerCase(java.util.Locale.ROOT)) {
            case "true", "false", "null", "yes", "no", "on", "off" -> true;
            default -> false;
        };
    }
}
