package io.tapstate.cli;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@code consolidated-table} recipe: the same table read from several databases into one view.
 * It asks the table name once, then two or more databases - connector and connection each - and
 * writes one source per database, {@code <table>_<n>_src}, and one {@code union} step over them.
 *
 * <p>Every source carries the same table name, which is exactly the case a bare {@code from: orders}
 * cannot resolve: the reference closure calls a table name held by two sources ambiguous. The union
 * therefore addresses each input as {@code <source_id>.<table>}, the qualified form the closure
 * accepts for this purpose, and the view reads the step.
 */
final class ConsolidatedTableRecipe {

    static final String RECIPE = "consolidated-table";
    static final String TABLE_QUESTION = MirroredTableRecipe.TABLE_QUESTION;
    static final String ANOTHER_QUESTION = "Add another database? [y/N]";
    static final String VIEW_QUESTION = "View id";

    static final String DB_USAGE = "--db <connector>[,key=value...]";

    /** One database: where a copy of the table is read from. */
    record Database(String connector, Map<String, Object> config) {}

    record Answers(String table, List<Database> databases, String view) {}

    private ConsolidatedTableRecipe() {
    }

    /**
     * The table, then the databases in a loop that runs at least twice, then the view id; each is
     * skipped when a flag answers it, and {@code --db} flags stand in for the whole loop.
     */
    static Answers ask(Prompter prompter, RecipeRun.Flags flags, TapstateCatalog catalog) {
        String table = flags.table() != null ? flags.table().trim() : prompter.ask(TABLE_QUESTION, null).trim();
        if (table.isEmpty()) {
            throw new RecipeRun.Usage("a table name is required (--table in scripts)");
        }
        List<Database> databases = flags.databases().isEmpty()
                ? askDatabases(prompter, catalog, flags.connector(), flags.set())
                : databasesFromFlags(flags.databases(), catalog);
        String view = flags.view() != null ? flags.view() : prompter.ask(VIEW_QUESTION, table + "_all");
        return new Answers(table, databases, RecipeSupport.orDefault(view, table + "_all"));
    }

    /** Two databases without asking whether to go on - fewer is not this recipe - then as many as wanted. */
    private static List<Database> askDatabases(Prompter prompter, TapstateCatalog catalog, String firstConnector,
                                               Map<String, String> firstConfig) {
        List<Database> databases = new ArrayList<>();
        String preferred = firstConnector == null ? RecipeSupport.DEFAULT_CONNECTOR
                : RecipeSupport.checkedConnector(firstConnector, catalog);
        do {
            String connector = databases.isEmpty() && firstConnector != null ? preferred
                    : RecipeSupport.chooseConnector(prompter, catalog, preferred);
            Map<String, String> given = databases.isEmpty() ? firstConfig : Map.of();
            databases.add(new Database(connector, RecipeSupport.connection(connector, given, prompter, catalog)));
            preferred = connector;
        } while (databases.size() < 2 || RecipeSupport.yes(prompter, ANOTHER_QUESTION, false));
        return databases;
    }

    /** The flag form: the table and at least two {@code --db} are required; the view has a default. */
    static Answers fromFlags(RecipeRun.Flags flags, TapstateCatalog catalog) {
        if (flags.table() == null || flags.table().isBlank()) {
            throw new RecipeRun.Usage(RECIPE + " needs --table <name>");
        }
        List<Database> databases = databasesFromFlags(flags.databases(), catalog);
        String table = flags.table().trim();
        return new Answers(table, databases, RecipeSupport.orDefault(flags.view(), table + "_all"));
    }

    private static List<Database> databasesFromFlags(List<String> specs, TapstateCatalog catalog) {
        if (specs.size() < 2) {
            throw new RecipeRun.Usage(RECIPE + " needs at least two " + DB_USAGE);
        }
        List<Database> databases = new ArrayList<>();
        for (String spec : specs) {
            databases.add(parseDatabase(spec, catalog));
        }
        return databases;
    }

    /** {@code <connector>[,key=value...]}: the connector checked as a flag-form connector is, the rest its connection. */
    static Database parseDatabase(String spec, TapstateCatalog catalog) {
        int comma = spec.indexOf(',');
        String connector = (comma < 0 ? spec : spec.substring(0, comma)).trim();
        if (connector.isEmpty()) {
            throw new RecipeRun.Usage(DB_USAGE + ", got '" + spec + "'");
        }
        String checked = RecipeSupport.checkedConnector(connector, catalog);
        Map<String, String> given = comma < 0 ? Map.of() : RecipeSupport.pairs(spec.substring(comma + 1), DB_USAGE);
        return new Database(checked, RecipeSupport.connection(checked, given, null, catalog));
    }

    /** The files: one source per database in the order given, the pipeline, the secrets where any were answered. */
    static List<RecipeRun.Output> plan(Answers answers, TapstateCatalog catalog, WorkspaceFiles workspace) {
        String stem = RecipeSupport.identifier(answers.table());
        List<RecipeSupport.PlannedSource> sources = new ArrayList<>();
        List<FromRef> inputs = new ArrayList<>();
        for (int n = 1; n <= answers.databases().size(); n++) {
            Database database = answers.databases().get(n - 1);
            String sourceId = stem + "_" + n + "_src";
            sources.add(RecipeSupport.source(sourceId, database.connector(), database.config(),
                    List.of(answers.table()), catalog));
            inputs.add(FromRef.literal(sourceId + "." + answers.table()));
        }
        Step consolidate = Step.inline("consolidate", new FromClause.Flow(inputs), new TransformBody.Union(),
                null);
        ViewBlock view = new ViewBlock.Inline(answers.view(), FromRef.literal("consolidate"), "id", null, null);
        PipelineResource pipeline = new PipelineResource(stem + "_sync", null,
                sources.stream().<SourceRef>map(s -> SourceRef.bare(s.resource().id())).toList(),
                List.of(consolidate), view, null, null, null);
        return RecipeSupport.outputs(sources, pipeline, MirroredTableRecipe.ASSUMED_PRIMARY_KEY, workspace);
    }
}
