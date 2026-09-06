package io.tapstate.cli;

import io.tapstate.core.catalog.TapstateCatalog;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs the recipe the guided first run chose: collects its answers, plans its files, and writes them
 * through the all-or-none writer. Every recipe lands on the same {@link Result}, so {@code new} has one
 * thing to report whichever recipe ran.
 *
 * <p>Three recipes run today. {@code sample} copies the bundled demo files verbatim - the same bytes
 * {@code demo} writes, through the same writer, so the two cannot drift. {@code blank} writes nothing
 * and makes the directory. {@code mirrored-table} asks its questions and renders two artifacts. The
 * rest of the catalog is refused by name until it is implemented.
 */
final class RecipeRun {

    /** The recipes that write a workspace today; the catalog's others are refused by name. */
    static final List<String> AVAILABLE = List.of("sample", "mirrored-table", "blank");

    /** A request that cannot be carried out as given - a missing answer with no prompter to ask. */
    static final class Usage extends RuntimeException {
        Usage(String message) {
            super(message);
        }
    }

    /** One planned file with the role the report names it by, and a note the text summary prints beside it. */
    record Output(WorkspaceWrite.File file, String kind, String note) {}

    /** One file after the write. */
    record Created(Path path, String kind, boolean replaced, String note) {}

    /** What a recipe left behind. */
    record Result(String recipe, Path root, List<Created> files) {}

    /** The answers a script supplies instead of being asked. */
    record Flags(String connector, Map<String, String> set, String table, String view) {}

    private RecipeRun() {
    }

    static boolean available(String recipeId) {
        return AVAILABLE.contains(recipeId);
    }

    /** Temporary, while the rest of the catalog is being built: a refusal by name, before anything is written. */
    static Usage notAvailable(String recipeId) {
        return new Usage("recipe '" + recipeId + "' is not available yet");
    }

    /**
     * @param prompter what asks the recipe's questions, or null to take every answer from {@code flags}
     */
    static Result run(String recipeId, Path root, Prompter prompter, Flags flags, boolean force) {
        List<Output> outputs = switch (recipeId) {
            case "sample" -> DemoCmd.bundledFiles().stream()
                    .map(file -> new Output(file, file.path().substring(0, file.path().indexOf('/')), null))
                    .toList();
            case "blank" -> List.of();
            case "mirrored-table" -> {
                TapstateCatalog catalog = TapstateCatalog.load();
                MirroredTableRecipe.Answers answers = prompter != null
                        ? MirroredTableRecipe.ask(prompter, flags, catalog)
                        : MirroredTableRecipe.fromFlags(flags.connector(), flags.set(), flags.table(), flags.view(), catalog);
                yield MirroredTableRecipe.plan(answers, catalog, new WorkspaceFiles(root));
            }
            default -> throw new IllegalStateException("recipe not available: " + recipeId);
        };
        List<WorkspaceWrite.Written> written = WorkspaceWrite.write(
                root, outputs.stream().map(Output::file).toList(), force, CliError.ARTIFACT_EXISTS);
        List<Created> created = new ArrayList<>();
        for (int i = 0; i < outputs.size(); i++) {
            created.add(new Created(written.get(i).path(), outputs.get(i).kind(), written.get(i).replaced(),
                    outputs.get(i).note()));
        }
        return new Result(recipeId, root, created);
    }
}
