package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The two line-oriented files a recipe extends rather than owns: {@code .env}, which holds the
 * secret answers under the names the artifacts reference, and {@code .gitignore}, which keeps
 * {@code .env} out of version control. Each is read as it stands and handed back merged, as the
 * content the all-or-none writer will put there: a line with the same name is replaced, everything
 * else the user wrote is kept, and a {@code .gitignore} that already lists {@code .env} is left alone.
 */
final class WorkspaceFiles {

    static final String ENV = ".env";
    static final String GITIGNORE = ".gitignore";

    /** What each of the two is for, in the words the guided first run describes it by. */
    static final String ENV_ROLE = "secrets for the files above; not committed";
    static final String GITIGNORE_ROLE = "keeps .env out of version control";

    private final Path root;

    WorkspaceFiles(Path root) {
        this.root = root;
    }

    /** {@code .env} with {@code secrets} written as {@code NAME=value} lines, replacing same-named ones. */
    WorkspaceWrite.File env(Map<String, String> secrets) {
        Map<String, String> pending = new LinkedHashMap<>(secrets);
        List<String> lines = new ArrayList<>();
        for (String line : existingLines(ENV)) {
            int eq = line.indexOf('=');
            String name = eq < 0 ? null : line.substring(0, eq);
            String replacement = name == null ? null : pending.remove(name);
            lines.add(replacement != null ? name + "=" + replacement : line);
        }
        pending.forEach((name, value) -> lines.add(name + "=" + value));
        return new WorkspaceWrite.File(ENV, joined(lines), true);
    }

    /** {@code .gitignore} with an {@code .env} line, or null when it already has one. */
    WorkspaceWrite.File gitignoreEnv() {
        List<String> lines = new ArrayList<>(existingLines(GITIGNORE));
        if (lines.stream().anyMatch(line -> line.trim().equals(ENV))) {
            return null;
        }
        lines.add(ENV);
        return new WorkspaceWrite.File(GITIGNORE, joined(lines), true);
    }

    private List<String> existingLines(String name) {
        Path file = root.resolve(name);
        if (!Files.exists(file)) {
            return List.of();
        }
        try {
            return Files.readAllLines(file);
        } catch (IOException cannotRead) {
            // Refused before anything is written: a file that cannot be read cannot be extended either.
            String reason = cannotRead.getMessage() == null ? cannotRead.getClass().getSimpleName() : cannotRead.getMessage();
            throw new TapstateException(CliError.WORKSPACE_NOT_WRITABLE,
                    Map.of("path", file.toString(), "reason", reason), null);
        }
    }

    private static String joined(List<String> lines) {
        return lines.isEmpty() ? "" : String.join("\n", lines) + "\n";
    }
}
