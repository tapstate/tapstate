package io.tapstate.cli;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code nested-json} recipe: several tables assembled into one document per root row. It asks
 * for the root table - connector, connection, table, key column - then for one or more child tables,
 * each with the columns that join it to the root, whether one root row has one or many of it, and the
 * path it lands under; and it writes one {@code nest} step whose view is keyed by the root's key.
 *
 * <p>A child that sits in the root's database is a second table of the root's source, so one source
 * reads both; a child in another database gets a source of its own, {@code <table>_src}, with its own
 * connection and its own secrets. In the flag form one other database can be named, and every child
 * then sits there: a script that needs children spread over three databases edits the file.
 *
 * <p>Each embedded table's own key is not knowable without discovery, so a one-to-many child is
 * written with {@code arrayKey: [id]} and the summary says so; a one-to-one child needs none.
 */
final class NestedJsonRecipe {

    static final String RECIPE = "nested-json";
    static final String ROOT_QUESTION = "Root table";
    static final String CHILD_QUESTION = "Child table";
    static final String AS_QUESTION = "Embed as (array = one root row has many, object = has one)";
    static final String PATH_QUESTION = "Path under the root document";
    static final String ANOTHER_QUESTION = "Add another table? [y/N]";
    static final String VIEW_QUESTION = "View id";
    static final String DEFAULT_KEY = "id";

    static final String CHILD_USAGE = "--child is <table>:<childcol>=<rootcol>[:array|object][:<path>]";
    static final String ON_USAGE = "the join columns are childcol=rootcol, comma-separated (--child in scripts)";

    /** What the summary says beside the pipeline, because the children's keys were not discovered. */
    /** What every one-to-many embed assumes without asking, as the summary names it beside the file. */
    static final String ASSUMED_ARRAY_KEY = "arrayKey: [id]";

    /** One connection: where a table is read from. */
    record Database(String connector, Map<String, Object> config) {}

    /**
     * One child table.
     *
     * @param on       child column to the root column it matches
     * @param database where the child sits, or null for the root's database
     */
    record Child(String table, Map<String, String> on, EmbedAs as, String path, Database database) {}

    record Answers(Database root, String rootTable, String key, List<Child> children, String view) {}

    private NestedJsonRecipe() {
    }

    /**
     * The root's questions, then the children in a loop, then the view id; each is skipped when a
     * flag answers it, and {@code --child} flags stand in for the whole loop.
     */
    static Answers ask(Prompter prompter, RecipeRun.Flags flags, TapstateCatalog catalog) {
        String connector = flags.connector();
        if (connector == null) {
            connector = RecipeSupport.chooseConnector(prompter, catalog, RecipeSupport.DEFAULT_CONNECTOR);
        } else {
            connector = RecipeSupport.checkedConnector(connector, catalog);
        }
        Database root = new Database(connector, RecipeSupport.connection(connector, flags.set(), prompter, catalog));
        RecipeRun.Flags.Nested given = flags.nested();
        String rootTable = given.root() != null ? given.root().trim() : prompter.ask(ROOT_QUESTION, null).trim();
        if (rootTable.isEmpty()) {
            throw new RecipeRun.Usage("a root table name is required (--root in scripts)");
        }
        String key = given.key() != null ? given.key()
                : prompter.ask("Key column of " + rootTable, DEFAULT_KEY);
        List<Child> children = given.children().isEmpty()
                ? askChildren(prompter, root, rootTable, catalog)
                : childrenFromFlags(given, root, catalog);
        String view = flags.view() != null ? flags.view() : prompter.ask(VIEW_QUESTION, rootTable + "_state");
        return new Answers(root, rootTable, RecipeSupport.orDefault(key, DEFAULT_KEY), children,
                RecipeSupport.orDefault(view, rootTable + "_state"));
    }

    private static List<Child> askChildren(Prompter prompter, Database root, String rootTable,
                                           TapstateCatalog catalog) {
        List<Child> children = new ArrayList<>();
        do {
            Database database = null;
            if (!RecipeSupport.yes(prompter, "Same database as " + rootTable + "? [Y/n]", true)) {
                String connector = RecipeSupport.chooseConnector(prompter, catalog, root.connector());
                database = new Database(connector, RecipeSupport.connection(connector, Map.of(), prompter, catalog));
            }
            String table = prompter.ask(CHILD_QUESTION, null).trim();
            if (table.isEmpty()) {
                throw new RecipeRun.Usage("a child table name is required (--child in scripts)");
            }
            Map<String, String> on = RecipeSupport.pairs(
                    prompter.ask("Columns joining " + table + " to " + rootTable + " (childcol=rootcol, comma list)",
                            null), ON_USAGE);
            if (on.isEmpty()) {
                throw new RecipeRun.Usage(ON_USAGE);
            }
            EmbedAs as = embedAs(prompter.choose(AS_QUESTION, List.of("array", "object"), "array"));
            String path = RecipeSupport.orDefault(prompter.ask(PATH_QUESTION, table), table);
            children.add(new Child(table, on, as, path, database));
        } while (RecipeSupport.yes(prompter, ANOTHER_QUESTION, false));
        return children;
    }

    /** The flag form: the root and at least one child are required; the key and the view have defaults. */
    static Answers fromFlags(RecipeRun.Flags flags, TapstateCatalog catalog) {
        String connector = RecipeSupport.checkedConnector(flags.connector(), catalog);
        RecipeRun.Flags.Nested given = flags.nested();
        if (given.root() == null || given.root().isBlank()) {
            throw new RecipeRun.Usage(RECIPE + " needs --root <table>");
        }
        if (given.children().isEmpty()) {
            throw new RecipeRun.Usage(RECIPE + " needs at least one " + CHILD_USAGE);
        }
        Database root = new Database(connector, RecipeSupport.connection(connector, flags.set(), null, catalog));
        String rootTable = given.root().trim();
        return new Answers(root, rootTable, RecipeSupport.orDefault(given.key(), DEFAULT_KEY),
                childrenFromFlags(given, root, catalog), RecipeSupport.orDefault(flags.view(), rootTable + "_state"));
    }

    /** Every {@code --child}, on the other database when {@code --child-connector} / {@code --child-set} name one. */
    private static List<Child> childrenFromFlags(RecipeRun.Flags.Nested given, Database root, TapstateCatalog catalog) {
        Database other = null;
        if (given.childConnector() != null || !given.childSet().isEmpty()) {
            String connector = RecipeSupport.checkedConnector(
                    given.childConnector() != null ? given.childConnector() : root.connector(), catalog);
            other = new Database(connector, RecipeSupport.connection(connector, given.childSet(), null, catalog));
        }
        List<Child> children = new ArrayList<>();
        for (String spec : given.children()) {
            children.add(parseChild(spec, other));
        }
        return children;
    }

    /** {@code <table>:<childcol>=<rootcol>[,...][:array|object][:<path>]}; the two optional parts come in either order. */
    static Child parseChild(String spec, Database database) {
        String[] parts = spec.split(":");
        if (parts.length < 2 || parts[0].isBlank()) {
            throw new RecipeRun.Usage(CHILD_USAGE + ", got '" + spec + "'");
        }
        String table = parts[0].trim();
        Map<String, String> on = RecipeSupport.pairs(parts[1], CHILD_USAGE);
        if (on.isEmpty()) {
            throw new RecipeRun.Usage(CHILD_USAGE + ", got '" + spec + "'");
        }
        EmbedAs as = EmbedAs.ARRAY;
        String path = table;
        for (int i = 2; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.equals("array") || part.equals("object")) {
                as = embedAs(part);
            } else if (!part.isEmpty()) {
                path = part;
            }
        }
        return new Child(table, on, as, path, database);
    }

    private static EmbedAs embedAs(String answer) {
        return "object".equals(answer) ? EmbedAs.OBJECT : EmbedAs.ARRAY;
    }

    /**
     * The files: the root's source (with every child that shares its database as a further table),
     * one source per child elsewhere, the pipeline, the secrets where any were answered.
     */
    static List<RecipeRun.Output> plan(Answers answers, TapstateCatalog catalog, WorkspaceFiles workspace) {
        String rootId = RecipeSupport.identifier(answers.rootTable()) + "_src";
        List<String> rootTables = new ArrayList<>(List.of(answers.rootTable()));
        for (Child child : answers.children()) {
            if (child.database() == null) {
                rootTables.add(child.table());
            }
        }
        List<RecipeSupport.PlannedSource> sources = new ArrayList<>();
        Set<String> sourceIds = new HashSet<>();
        sources.add(RecipeSupport.source(rootId, answers.root().connector(), answers.root().config(), rootTables,
                catalog));
        sourceIds.add(rootId);
        for (Child child : answers.children()) {
            if (child.database() != null) {
                String sourceId = RecipeSupport.identifier(child.table()) + "_src";
                if (!sourceIds.add(sourceId)) {
                    throw new RecipeRun.Usage("nested-json tables produce the same source id '" + sourceId
                            + "'; choose names that stay distinct after normalization");
                }
                sources.add(RecipeSupport.source(sourceId,
                        child.database().connector(), child.database().config(), List.of(child.table()), catalog));
            }
        }
        // the nest addresses each table by its own name, so the alias is the table name
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put(answers.rootTable(), FromRef.literal(answers.rootTable()));
        List<Embed> embeds = new ArrayList<>();
        boolean anyArray = false;
        for (Child child : answers.children()) {
            aliases.put(child.table(), FromRef.literal(child.table()));
            boolean array = child.as() == EmbedAs.ARRAY;
            anyArray |= array;
            embeds.add(new Embed(child.table(), child.on(), child.as(), child.path(),
                    array ? List.of(DEFAULT_KEY) : null, null, null, null));
        }
        NestRoot root = new NestRoot(answers.rootTable(), List.of(answers.key()), null, null, embeds);
        Step assemble = Step.inline("assemble", FromClause.aliases(aliases),
                new TransformBody.Nest(null, null, root), null, null);
        ViewBlock view = new ViewBlock.Inline(answers.view(), FromRef.literal("assemble"), answers.key(), null, null);
        PipelineResource pipeline = new PipelineResource(RecipeSupport.identifier(answers.rootTable()) + "_sync", null,
                sources.stream().map(s -> s.resource().id()).toList(), List.of(assemble), view, null, null, null);
        return RecipeSupport.outputs(sources, pipeline, anyArray ? ASSUMED_ARRAY_KEY : null, workspace);
    }
}
