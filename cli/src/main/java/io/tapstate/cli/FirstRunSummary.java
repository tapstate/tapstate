package io.tapstate.cli;

import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What {@code new} says once a recipe has written ({@code docs/first-run/README.md}, "What new says
 * afterwards"), in the order a first run reads it: where the workspace is and what every file in it
 * is for, that nothing is running yet, the four things to do next, and the one line handing over to
 * an AI assistant. A recipe the catalog marks not runnable adds one line saying its files are
 * skeletons to fill in.
 *
 * <p>A file that was already there and was replaced under {@code --force} says so on its line, and
 * one the recipe extended rather than owned reads as updated: a flag that overwrites must not be
 * silent about what it overwrote. A fresh run carries neither word.
 *
 * <p>The machine forms carry the same facts as fields - each file's role and assumption, the state,
 * the next verbs - and none of the prose. No server is among them: scaffolding never learns one. They extend the envelope {@code new} already
 * reported; no existing key moves.
 *
 * <p>What {@code up} says afterwards ({@code docs/first-run/README.md}, "tapstate up") is the same
 * shape with the running things in place of the files: the workspace, one line per pipeline with its
 * state and one per source, the state, the commands that do the same thing one step at a time, and the
 * handover. Rendered here so the two endings read alike and cannot drift apart.
 */
final class FirstRunSummary {

    static final String STATE_TEXT = "not running yet";
    static final String STATE = "not-running";
    static final String SKELETON_LINE = "(skeletons — replace the placeholder values, then run tapstate validate)";
    static final String AI_LINE = "An AI assistant can take it from here: https://tapstate.dev/docs/first-run";

    /** The verbs of the next steps, in the order they are printed, as the machine forms name them. */
    static final List<String> NEXT = List.of("validate", "ls", "desc", "up");

    /** What {@code up} reports once the workspace is running, and its machine spelling. */
    static final String UP_STATE_TEXT = "running";
    static final String UP_STATE = "running";
    /** The same, when there was nothing to do: everything was already applied and running. */
    static final String UP_UNCHANGED_TEXT = "running (nothing to do)";
    static final String UP_UNCHANGED = "running-unchanged";

    /** The verbs {@code up}'s next steps name, in the order they are printed. */
    static final List<String> UP_NEXT = List.of("status", "logs", "apply", "start", "up");

    /** The operand the next-step lines carry when there is not exactly one pipeline to name. */
    private static final String ANY_PIPELINE = "<pipeline-id>";

    /**
     * One pipeline as {@code up} left it: its state as the server reports it, and what the stages had
     * to say about it — {@code apply: unchanged}, {@code start: already running} — empty when they did
     * something.
     */
    record UpPipeline(String id, String state, List<String> notes) {
    }

    /** One source as {@code up} left it, with the stages' notes, empty when they did something. */
    record UpSource(String id, List<String> notes) {
    }

    private FirstRunSummary() {
    }

    /**
     * The text form of what {@code up} says afterwards.
     *
     * @param nothingToDo whether every stage found its work already done, which the state line says
     */
    static void upText(PrintWriter o, Path root, List<UpPipeline> pipelines, List<UpSource> sources,
                       boolean nothingToDo) {
        o.println("Workspace: " + root);
        for (UpPipeline pipeline : pipelines) {
            o.println("  pipeline " + pipeline.id() + ": " + pipeline.state() + notes(pipeline.notes()));
        }
        for (UpSource source : sources) {
            o.println("  source " + source.id() + ": applied" + notes(source.notes()));
        }
        o.println("State: " + (nothingToDo ? UP_UNCHANGED_TEXT : UP_STATE_TEXT));
        String pipeline = pipelines.size() == 1 ? pipelines.get(0).id() : ANY_PIPELINE;
        o.println("Next:");
        o.println("  tapstate status " + pipeline + "  watch it");
        o.println("  tapstate logs " + pipeline + "  see what it is doing");
        o.println("  tapstate apply / tapstate start  the same thing, one step at a time");
        o.println("  edit any file above, then tapstate up again  it converges");
        o.println(AI_LINE);
    }

    /** The stages' notes on one line, in parentheses, or nothing when the stages did something. */
    private static String notes(List<String> notes) {
        return notes.isEmpty() ? "" : " (" + String.join("; ", notes) + ")";
    }

    /** The structured form of what {@code up} says afterwards: the same facts, none of the prose. */
    static Map<String, Object> upEnvelope(Path root, List<UpPipeline> pipelines, List<UpSource> sources,
                                          boolean nothingToDo) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "up");
        env.put("workspace", root.toString());
        List<Map<String, Object>> lines = new ArrayList<>();
        for (UpPipeline pipeline : pipelines) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", pipeline.id());
            entry.put("state", pipeline.state());
            lines.add(entry);
        }
        env.put("pipelines", lines);
        env.put("sources", sources.stream().map(UpSource::id).toList());
        env.put("state", nothingToDo ? UP_UNCHANGED : UP_STATE);
        env.put("next", UP_NEXT);
        return env;
    }

    /**
     * The text form.
     *
     * @param server the server the workspace is bound to, which is what {@code up} will bring it to
     */
    static void text(PrintWriter o, RecipeRun.Result result) {
        o.println("Workspace: " + result.root());
        for (RecipeRun.Created file : result.files()) {
            String replaced = !file.replaced() ? ""
                    : "env".equals(file.kind()) || "gitignore".equals(file.kind()) ? " (updated)" : " (replaced)";
            String assumed = file.assumed() == null ? ""
                    : " — assumed " + file.assumed() + "; edit if the table is keyed otherwise";
            o.println("  " + file.name() + "  " + file.role() + replaced + assumed);
        }
        // A recipe the catalog marks not runnable wrote a starting point, not a working workspace; saying
        // so here is what keeps its files from reading as ready to run.
        if (Recipe.byId(result.recipe()).filter(recipe -> !recipe.runnable()).isPresent()) {
            o.println("  " + SKELETON_LINE);
        }
        o.println("State: " + STATE_TEXT);
        o.println("Next:");
        o.println("  edit any file above  they are ordinary YAML; the guided commands never hide them");
        o.println("  tapstate validate  check the workspace without a server");
        o.println("  tapstate ls / tapstate desc <id>  see what is here and what each file declares");
        o.println("  tapstate up  bring it to running; it asks which server the first time");
        o.println(AI_LINE);
    }

    /** The structured form, as the tree the JSON and YAML writers render. */
    static Map<String, Object> envelope(RecipeRun.Result result) {
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
        env.put("next", NEXT);
        return env;
    }
}
