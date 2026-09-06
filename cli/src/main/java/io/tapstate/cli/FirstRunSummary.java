package io.tapstate.cli;

import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What {@code new} says once a recipe has written ({@code docs/first-run/README.md}, "What new says
 * afterwards"), in the order a first run reads it: where the workspace is and what every file in it
 * is for, that nothing is running yet, the four things to do next, and the one line handing over to
 * an AI assistant. A blank workspace prints the same shape and says in words that it is empty.
 *
 * <p>A file that was already there and was replaced under {@code --force} says so on its line, and
 * one the recipe extended rather than owned reads as updated: a flag that overwrites must not be
 * silent about what it overwrote. A fresh run carries neither word.
 *
 * <p>The machine forms carry the same facts as fields - each file's role and assumption, the state,
 * the server, the next verbs - and none of the prose. They extend the envelope {@code new} already
 * reported; no existing key moves.
 */
final class FirstRunSummary {

    static final String STATE_TEXT = "not running yet";
    static final String STATE = "not-running";
    static final String EMPTY_LINE = "(empty — write your first file, or run tapstate new again for a starter)";
    static final String AI_LINE = "An AI assistant can take it from here: https://tapstate.dev/docs/first-run";

    /** The verbs of the next steps, in the order they are printed, as the machine forms name them. */
    static final List<String> NEXT = List.of("validate", "ls", "desc", "up");

    private FirstRunSummary() {
    }

    /**
     * The text form.
     *
     * @param server the server the workspace is bound to, which is what {@code up} will bring it to
     */
    static void text(PrintWriter o, RecipeRun.Result result, URI server) {
        o.println("Workspace: " + result.root());
        if (result.files().isEmpty()) {
            o.println("  " + EMPTY_LINE);
        }
        for (RecipeRun.Created file : result.files()) {
            String replaced = !file.replaced() ? ""
                    : "env".equals(file.kind()) || "gitignore".equals(file.kind()) ? " (updated)" : " (replaced)";
            String assumed = file.assumed() == null ? ""
                    : " — assumed " + file.assumed() + "; edit if the table is keyed otherwise";
            o.println("  " + file.name() + "  " + file.role() + replaced + assumed);
        }
        o.println("State: " + STATE_TEXT);
        o.println("Next:");
        o.println("  edit any file above  they are ordinary YAML; the guided commands never hide them");
        o.println("  tapstate validate  check the workspace without a server");
        o.println("  tapstate ls / tapstate desc <id>  see what is here and what each file declares");
        o.println("  tapstate up  bring it to running against " + server);
        o.println(AI_LINE);
    }

    /** The structured form, as the tree the JSON and YAML writers render. */
    static Map<String, Object> envelope(RecipeRun.Result result, URI server) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "created");
        env.put("recipe", result.recipe());
        env.put("workspace", result.root().toString());
        List<Map<String, Object>> files = new ArrayList<>();
        for (RecipeRun.Created file : result.files()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("path", file.path().toString());
            entry.put("kind", file.kind());
            if (file.replaced()) {
                entry.put("replaced", true);
            }
            entry.put("role", file.role());
            if (file.assumed() != null) {
                entry.put("assumed", file.assumed());
            }
            files.add(entry);
        }
        env.put("files", files);
        env.put("state", STATE);
        env.put("server", server.toString());
        env.put("next", NEXT);
        return env;
    }
}
