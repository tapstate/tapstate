package io.tapstate.cli;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 *
 * <p>A secret answer never reaches the artifact. The file carries a reference in the DSL's own
 * interpolation form ({@code ${ORDERS_SRC_PASSWORD}}), and the value goes to the workspace's
 * {@code .env}; nothing here resolves it.
 */
final class MirroredTableRecipe {

    static final String CONNECTOR_QUESTION = "Which connector?";
    static final String TABLE_QUESTION = "Which table?";
    static final String VIEW_QUESTION = "View id";
    static final String DEFAULT_CONNECTOR = "mysql";

    /** What the summary says beside the pipeline, because the key it carries was not discovered. */
    static final String PRIMARY_KEY_NOTE = "primary_key: id assumed; edit it if the table's key is another column";

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
            List<String> offered = OfficialConnectors.presentIn(catalog);
            String fallback = offered.contains(DEFAULT_CONNECTOR) ? DEFAULT_CONNECTOR : offered.get(0);
            connector = prompter.choose(CONNECTOR_QUESTION, offered, fallback);
        }
        ConnectorCatalogEntry entry = catalog.byId(connector);
        Map<String, Object> config = new ConfigPrompter().collectEssential(entry.config(), flags.set(), prompter);
        String table = flags.table() != null ? flags.table().trim() : prompter.ask(TABLE_QUESTION, null).trim();
        if (table.isEmpty()) {
            throw new RecipeRun.Usage("a table name is required (--table in scripts)");
        }
        String view = flags.view() != null ? flags.view().trim() : prompter.ask(VIEW_QUESTION, table).trim();
        return new Answers(connector, config, table, view.isEmpty() ? table : view);
    }

    /**
     * The flag form. A missing required answer is refused by naming its flag; the connector is checked
     * the way {@code new --kind source} checks it, so the same diagnostics come back.
     */
    static Answers fromFlags(String connector, Map<String, String> set, String table, String view,
                             TapstateCatalog catalog) {
        String chosen = connector != null ? connector : DEFAULT_CONNECTOR;
        if (!catalog.ids().contains(chosen)) {
            throw new TapstateException(CliError.UNKNOWN_CONNECTOR, Map.of("connector", chosen), null);
        }
        if (!OfficialConnectors.isOfficial(chosen)) {
            throw new TapstateException(CliError.CONNECTOR_NOT_OFFICIAL, Map.of(
                    "connector", chosen,
                    "official", String.join(", ", OfficialConnectors.presentIn(catalog))), null);
        }
        if (table == null || table.isBlank()) {
            throw new RecipeRun.Usage("mirrored-table needs --table <name>");
        }
        Map<String, Object> config = new ConfigPrompter().collectEssential(catalog.byId(chosen).config(), set, null);
        String viewId = view == null || view.isBlank() ? table.trim() : view.trim();
        return new Answers(chosen, config, table.trim(), viewId);
    }

    /**
     * The files the answers become, in write order: the source, the pipeline, then the secrets and
     * the ignore line when a secret was answered. The two artifacts go through the canonical writer;
     * the other two are line-oriented files this recipe extends rather than owns.
     */
    static List<RecipeRun.Output> plan(Answers answers, TapstateCatalog catalog, WorkspaceFiles workspace) {
        String sourceId = identifier(answers.table()) + "_src";
        String pipelineId = identifier(answers.table()) + "_sync";
        Map<String, String> secrets = new LinkedHashMap<>();
        Map<String, Object> config = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : answers.config().entrySet()) {
            if (isSecret(catalog.byId(answers.connector()), entry.getKey())) {
                String name = envName(sourceId, entry.getKey());
                secrets.put(name, String.valueOf(entry.getValue()));
                config.put(entry.getKey(), "${" + name + "}");
            } else {
                config.put(entry.getKey(), entry.getValue());
            }
        }
        SourceResource source = new SourceResource(sourceId, null, answers.connector(), config, SourceMode.CDC,
                List.of(TableRef.literal(answers.table())), null, null, null);
        // The view reads the table by name: a from: names a step or a table, never a source id, and with one
        // source reading one literal table the name resolves to exactly that table.
        ViewBlock view = new ViewBlock.Inline(answers.view(), FromRef.literal(answers.table()), "id", null, null);
        PipelineResource pipeline = new PipelineResource(pipelineId, null, List.of(sourceId), null, view, null, null, null);

        CanonicalWriter writer = new CanonicalWriter();
        List<RecipeRun.Output> outputs = new ArrayList<>();
        outputs.add(new RecipeRun.Output(
                WorkspaceWrite.File.owned("source/" + sourceId + ".tap.yml", writer.write(source)), "source", null));
        outputs.add(new RecipeRun.Output(
                WorkspaceWrite.File.owned("pipeline/" + pipelineId + ".tap.yml", writer.write(pipeline)),
                "pipeline", PRIMARY_KEY_NOTE));
        if (!secrets.isEmpty()) {
            outputs.add(new RecipeRun.Output(workspace.env(secrets), "env", null));
            WorkspaceWrite.File ignore = workspace.gitignoreEnv();
            if (ignore != null) {
                outputs.add(new RecipeRun.Output(ignore, "gitignore", null));
            }
        }
        return outputs;
    }

    private static boolean isSecret(ConnectorCatalogEntry entry, String fieldName) {
        for (ConfigField field : entry.config()) {
            if (field.name().equals(fieldName)) {
                return field.secret();
            }
        }
        return false;
    }

    /** {@code <SOURCE_ID>_<FIELD>}, upper-cased, anything a variable name cannot carry folded to underscore. */
    static String envName(String sourceId, String field) {
        return (sourceId + "_" + field).toUpperCase().replaceAll("[^A-Z0-9_]", "_");
    }

    /** A table name as the stem of a resource id: an id must not contain a dot, so those and their like fold. */
    private static String identifier(String table) {
        return table.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
