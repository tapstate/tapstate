package io.tapstate.cli;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code reshaped-table} recipe: {@code mirrored-table} with the table reshaped on the way. It
 * asks everything that recipe asks, then which columns to keep, which to rename, which to drop, and
 * an optional row filter, and writes a {@code map} step and a {@code filter} step - only the ones
 * that have content - between the source and the view.
 *
 * <p>The map step is the model's field projection, and that projection lets unlisted fields pass
 * through: a kept column is written as a rename onto its own name ({@code region: $region}), which
 * fixes its place in the output but does not by itself remove the columns that were not listed; a
 * dropped column is written as {@code false}, which does. A rename consumes the source column, so a
 * column that is both kept and renamed appears once, under its new name. With nothing to reshape and
 * no filter, the pipeline is a mirrored table and is written as one.
 */
final class ReshapedTableRecipe {

    static final String RECIPE = "reshaped-table";
    static final String KEEP_QUESTION = "Columns to keep (comma list; blank = keep all)";
    static final String RENAME_QUESTION = "Columns to rename (old=new, comma list; blank = none)";
    static final String DROP_QUESTION = "Columns to drop (comma list; blank = none)";
    static final String WHERE_QUESTION = "Row filter (a CEL expression such as after.region == 'US'; blank = none)";

    static final String RENAME_USAGE = "renames are old=new, comma-separated (--rename in scripts)";

    /** The recipe's answers: the mirrored table's, then the reshaping. */
    record Answers(MirroredTableRecipe.Answers table, List<String> keep, Map<String, String> renames,
                   List<String> drop, String where) {}

    private ReshapedTableRecipe() {
    }

    /** The mirrored table's questions, then the four reshaping ones; each is skipped when a flag answers it. */
    static Answers ask(Prompter prompter, RecipeRun.Flags flags, TapstateCatalog catalog) {
        MirroredTableRecipe.Answers table = MirroredTableRecipe.ask(prompter, flags, catalog);
        RecipeRun.Flags.Reshape given = flags.reshape();
        String keep = given.keep() != null ? given.keep() : prompter.ask(KEEP_QUESTION, null);
        String rename = given.rename() != null ? given.rename() : prompter.ask(RENAME_QUESTION, null);
        String drop = given.drop() != null ? given.drop() : prompter.ask(DROP_QUESTION, null);
        String where = given.where() != null ? given.where() : prompter.ask(WHERE_QUESTION, null);
        return answers(table, keep, rename, drop, where);
    }

    /** The flag form; only the table is required, every reshaping flag is optional. */
    static Answers fromFlags(RecipeRun.Flags flags, TapstateCatalog catalog) {
        MirroredTableRecipe.Answers table = MirroredTableRecipe.fromFlags(RECIPE, flags, catalog);
        RecipeRun.Flags.Reshape given = flags.reshape();
        return answers(table, given.keep(), given.rename(), given.drop(), given.where());
    }

    private static Answers answers(MirroredTableRecipe.Answers table, String keep, String rename, String drop,
                                   String where) {
        return new Answers(table, RecipeSupport.list(keep), RecipeSupport.pairs(rename, RENAME_USAGE),
                RecipeSupport.list(drop), RecipeSupport.orDefault(where, null));
    }

    /** The files: the source, the pipeline with its steps in order, the secrets where one was answered. */
    static List<RecipeRun.Output> plan(Answers answers, TapstateCatalog catalog, WorkspaceFiles workspace) {
        RecipeSupport.PlannedSource source = MirroredTableRecipe.source(answers.table(), catalog);
        String table = answers.table().table();
        List<Step> steps = new ArrayList<>();
        // each step reads the one before it, and the first reads the table by name, as the view does
        String upstream = table;
        Map<String, FieldRule> fields = new LinkedHashMap<>();
        for (String column : answers.keep()) {
            if (!answers.renames().containsKey(column)) {
                fields.put(column, FieldRule.rename(column));
            }
        }
        answers.renames().forEach((old, renamed) -> fields.put(renamed, FieldRule.rename(old)));
        for (String column : answers.drop()) {
            fields.put(column, FieldRule.drop());
        }
        if (!fields.isEmpty()) {
            steps.add(Step.inline("reshape", FromClause.list(FromRef.literal(upstream)),
                    new TransformBody.MapProjection(fields), null, null));
            upstream = "reshape";
        }
        if (answers.where() != null) {
            steps.add(Step.inline("keep", FromClause.list(FromRef.literal(upstream)),
                    new TransformBody.Filter(answers.where()), null, null));
            upstream = "keep";
        }
        ViewBlock view = new ViewBlock.Inline(answers.table().view(), FromRef.literal(upstream), "id", null, null);
        PipelineResource pipeline = new PipelineResource(MirroredTableRecipe.pipelineId(answers.table()), null,
                List.of(source.resource().id()), steps.isEmpty() ? null : steps, view, null, null, null);
        return RecipeSupport.outputs(List.of(source), pipeline, MirroredTableRecipe.PRIMARY_KEY_NOTE, workspace);
    }
}
