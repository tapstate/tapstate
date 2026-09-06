package io.tapstate.cli;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs the recipe the guided first run chose: collects its answers, plans its files, and writes them
 * through the all-or-none writer. Every recipe lands on the same {@link Result}, so {@code new} has one
 * thing to report whichever recipe ran.
 *
 * <p>{@code sample} copies the bundled demo files verbatim - the same bytes {@code demo} writes,
 * through the same writer, so the two cannot drift. {@code blank} writes nothing and makes the
 * directory. The other four ask their questions - or read them from {@link Flags} - and render their
 * sources and pipeline through the canonical writer.
 */
final class RecipeRun {

    /** A request that cannot be carried out as given - a missing answer with no prompter to ask. */
    static final class Usage extends RuntimeException {
        Usage(String message) {
            super(message);
        }
    }

    /**
     * One planned file with the kind the report names it by, and what it assumed without asking
     * ({@code primary_key: id}, say) - null when nothing.
     */
    record Output(WorkspaceWrite.File file, String kind, String assumed) {}

    /**
     * One file after the write.
     *
     * @param name    the path as the recipe planned it, relative to the workspace root
     * @param role    what the file is for, in the words the summary describes it by
     * @param assumed what the file assumed without asking, or null
     */
    record Created(Path path, String name, String kind, boolean replaced, String role, String assumed) {}

    /** What a recipe left behind. */
    record Result(String recipe, Path root, List<Created> files) {}

    /**
     * The answers a script supplies instead of being asked. The first four are shared by the table
     * recipes; the rest belong to one recipe each and are ignored by the others.
     *
     * @param databases the {@code --db} specs of {@code consolidated-table}
     */
    record Flags(String connector, Map<String, String> set, String table, String view,
                 Reshape reshape, Nested nested, List<String> databases) {

        /** {@code reshaped-table}'s four answers, each null when not given. */
        record Reshape(String keep, String rename, String drop, String where) {}

        /** {@code nested-json}'s answers: the root, its key, the {@code --child} specs, and the children's database when it is not the root's. */
        record Nested(String root, String key, List<String> children, String childConnector,
                      Map<String, String> childSet) {}
    }

    private RecipeRun() {
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
                        : MirroredTableRecipe.fromFlags(recipeId, flags, catalog);
                yield MirroredTableRecipe.plan(answers, catalog, new WorkspaceFiles(root));
            }
            case "reshaped-table" -> {
                TapstateCatalog catalog = TapstateCatalog.load();
                ReshapedTableRecipe.Answers answers = prompter != null
                        ? ReshapedTableRecipe.ask(prompter, flags, catalog)
                        : ReshapedTableRecipe.fromFlags(flags, catalog);
                yield ReshapedTableRecipe.plan(answers, catalog, new WorkspaceFiles(root));
            }
            case "nested-json" -> {
                TapstateCatalog catalog = TapstateCatalog.load();
                NestedJsonRecipe.Answers answers = prompter != null
                        ? NestedJsonRecipe.ask(prompter, flags, catalog)
                        : NestedJsonRecipe.fromFlags(flags, catalog);
                yield NestedJsonRecipe.plan(answers, catalog, new WorkspaceFiles(root));
            }
            case "consolidated-table" -> {
                TapstateCatalog catalog = TapstateCatalog.load();
                ConsolidatedTableRecipe.Answers answers = prompter != null
                        ? ConsolidatedTableRecipe.ask(prompter, flags, catalog)
                        : ConsolidatedTableRecipe.fromFlags(flags, catalog);
                yield ConsolidatedTableRecipe.plan(answers, catalog, new WorkspaceFiles(root));
            }
            default -> throw new IllegalStateException("not a recipe: " + recipeId);
        };
        List<WorkspaceWrite.Written> written = WorkspaceWrite.write(
                root, outputs.stream().map(Output::file).toList(), force, CliError.ARTIFACT_EXISTS);
        List<Created> created = new ArrayList<>();
        for (int i = 0; i < outputs.size(); i++) {
            Output output = outputs.get(i);
            created.add(new Created(written.get(i).path(), output.file().path(), output.kind(),
                    written.get(i).replaced(), role(output), output.assumed()));
        }
        return new Result(recipeId, root, created);
    }

    /**
     * What a file is for, in one line. An artifact is described in the words {@code ls} lists it by -
     * its kind and id, then {@code ls}'s summary of it - read back from the bytes just written rather
     * than from the recipe's plan, because {@code sample} copies files it never parsed and because it
     * is the listing the user meets next that the description has to agree with. The two dotfiles are
     * described by what owns them.
     */
    private static String role(Output output) {
        return switch (output.kind()) {
            case "env" -> WorkspaceFiles.ENV_ROLE;
            case "gitignore" -> WorkspaceFiles.GITIGNORE_ROLE;
            default -> {
                Resource resource;
                try {
                    resource = new DslParser().parse(output.file().content());
                } catch (RuntimeException unreadable) {
                    // a recipe writes what it rendered, so this is a defect; listed as ls would list it
                    resource = null;
                }
                WorkspaceScan.Artifact artifact =
                        new WorkspaceScan.Artifact(output.kind(), Path.of(output.file().path()), resource);
                String summary = LsCmd.summary(artifact);
                String named = output.kind() + " " + artifact.id();
                yield summary.isEmpty() ? named : named + ": " + summary;
            }
        };
    }
}
