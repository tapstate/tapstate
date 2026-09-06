package io.tapstate.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * A workspace's {@code .env}: the file the guided recipes write a source's secrets to, so that the
 * artifact carries {@code ${NAME}} and never the value. Read here so that {@code up} resolves those
 * references from the same directory it is bringing up, before it asks the process environment.
 *
 * <p>Only {@code NAME=value} lines are read. Blank lines and {@code #} comments are skipped; anything
 * else is left alone rather than guessed at — this is not a shell, and a line it cannot read is not a
 * variable. The value is everything after the first {@code =}, as written: no quoting rules, no
 * escapes, since the recipes write none.
 */
final class DotEnv {

    /** The file's name under a workspace root. */
    static final String FILE = ".env";

    private DotEnv() {
    }

    /**
     * The lookup {@code ${...}} references are resolved through: the workspace's {@code .env} first,
     * then {@code process}. A missing file layers nothing, so a workspace without one resolves exactly
     * as it did before.
     */
    static UnaryOperator<String> layered(Path workspace, UnaryOperator<String> process) throws IOException {
        Map<String, String> file = read(workspace.resolve(FILE));
        if (file.isEmpty()) {
            return process;
        }
        return name -> {
            String value = file.get(name);
            return value != null ? value : process.apply(name);
        };
    }

    /** The {@code NAME=value} pairs in one file, in file order; an absent file is an empty map. */
    static Map<String, String> read(Path file) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        if (!Files.isRegularFile(file)) {
            return values;
        }
        for (String raw : Files.readAllLines(file)) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int eq = line.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String name = line.substring(0, eq).strip();
            if (!name.isEmpty()) {
                values.put(name, line.substring(eq + 1));
            }
        }
        return values;
    }
}
