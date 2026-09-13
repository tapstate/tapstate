package io.tapstate.cli;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.ViewBlock;

import java.util.List;
import java.util.Map;

/**
 * The {@code mirrored-table} recipe: one source reading one table as it changes, one pipeline, one
 * view of the same shape. It asks - or takes from flags - a connector, the connector's required and
 * secret connection fields, a table, and a view id, and turns the answers into two canonical
 * artifacts. The two paths meet in {@link Answers}: whichever way the answers came, one builder
 * renders them, so the interactive and the scripted form cannot drift apart.
 *
 * <p>Ids are derived, not asked: {@code <table>_src} and {@code <table>_sync}. The view's primary key
 * is not knowable without discovery, so {@code id} is written and the summary says it was assumed.
 * The secret handling is {@link RecipeSupport}'s, shared with every recipe.
 *
 * <p>The {@code reshaped-table} recipe asks these questions first and its own after, so both halves
 * of this class take a recipe id for the refusal that names the missing flag.
 */
final class MirroredTableRecipe {

    static final String CONNECTOR_QUESTION = RecipeSupport.CONNECTOR_QUESTION;
    static final String TABLE_QUESTION = "Which table?";
    static final String VIEW_QUESTION = "View id";
    static final String DEFAULT_CONNECTOR = RecipeSupport.DEFAULT_CONNECTOR;

    /** What the summary says beside the pipeline, because the key it carries was not discovered. */
    /** What the pipeline assumes without asking, as the summary names it beside the file. */
    static final String ASSUMED_PRIMARY_KEY = "primary_key: id";

    /** The recipe's answers, from whichever path collected them. */
    record Answers(String connector, Map<String, Object> config, String table, String view) {}

    private MirroredTableRecipe() {
    }

    /**
     * Asks the four questions in order, skipping any a flag already answers; an empty reply takes the
     * bracketed default where one exists.
     */
    static Answers ask(Prompter prompter, RecipeRun.Flags flags, TapstateCatalog catalog) {
        String connector = flags.connector();
        if (connector == null) {
            connector = RecipeSupport.chooseConnector(prompter, catalog, DEFAULT_CONNECTOR);
        }
        Map<String, Object> config = RecipeSupport.connection(connector, flags.set(), prompter, catalog);
        String table = flags.table() != null ? flags.table().trim() : prompter.ask(TABLE_QUESTION, null).trim();
        if (table.isEmpty()) {
            throw new RecipeRun.Usage("a table name is required (--table in scripts)");
        }
        String view = flags.view() != null ? flags.view() : prompter.ask(VIEW_QUESTION, table);
        return new Answers(connector, config, table, RecipeSupport.orDefault(view, table));
    }

    /**
     * The flag form. A missing required answer is refused by naming its flag; the connector is checked
     * the way {@code new --kind source} checks it, so the same diagnostics come back.
     */
    static Answers fromFlags(String recipeId, RecipeRun.Flags flags, TapstateCatalog catalog) {
        String connector = RecipeSupport.checkedConnector(flags.connector(), catalog);
        if (flags.table() == null || flags.table().isBlank()) {
            throw new RecipeRun.Usage(recipeId + " needs --table <name>");
        }
        Map<String, Object> config = RecipeSupport.connection(connector, flags.set(), null, catalog);
        String table = flags.table().trim();
        return new Answers(connector, config, table, RecipeSupport.orDefault(flags.view(), table));
    }

    /** The files the answers become: the source, the pipeline, and the secrets where one was answered. */
    static List<RecipeRun.Output> plan(Answers answers, TapstateCatalog catalog, WorkspaceFiles workspace) {
        RecipeSupport.PlannedSource source = source(answers, catalog);
        // The view reads the table by name: a from: names a step or a table, never a source id, and with one
        // source reading one literal table the name resolves to exactly that table.
        ViewBlock view = new ViewBlock.Inline(answers.view(), FromRef.literal(answers.table()), "id", null, null);
        PipelineResource pipeline = new PipelineResource(pipelineId(answers), null,
                List.of(SourceRef.bare(source.resource().id())), null, view, null, null, null);
        return RecipeSupport.outputs(List.of(source), pipeline, ASSUMED_PRIMARY_KEY, workspace);
    }

    /** {@code <table>_src} over the one table, secrets taken out. */
    static RecipeSupport.PlannedSource source(Answers answers, TapstateCatalog catalog) {
        String sourceId = RecipeSupport.identifier(answers.table()) + "_src";
        return RecipeSupport.source(sourceId, answers.connector(), answers.config(), List.of(answers.table()), catalog);
    }

    /** {@code <table>_sync}. */
    static String pipelineId(Answers answers) {
        return RecipeSupport.identifier(answers.table()) + "_sync";
    }
}
