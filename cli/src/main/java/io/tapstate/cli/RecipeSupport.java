package io.tapstate.cli;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What every recipe of the guided first run does the same way, kept in one place so the recipes
 * cannot drift from each other: how a connector is chosen or checked, how a connection is collected,
 * how a source is planned with its secrets taken out, how the planned files are ordered, and the
 * small answer conventions - a yes/no question, a comma list, a derived id.
 *
 * <p>A secret answer never reaches an artifact. The file carries a reference in the DSL's own
 * interpolation form ({@code ${ORDERS_SRC_PASSWORD}}), named {@code <SOURCE_ID>_<FIELD>}, and the
 * value goes to the workspace's {@code .env}; nothing here resolves it.
 */
final class RecipeSupport {

    static final String CONNECTOR_QUESTION = "Which connector?";
    static final String DEFAULT_CONNECTOR = "mysql";

    /** A source as a recipe plans it, and the secrets its references stand for. */
    record PlannedSource(SourceResource resource, Map<String, String> secrets) {}

    private RecipeSupport() {
    }

    /** Offers the official connectors, {@code preferred} marked as the one an empty reply takes. */
    static String chooseConnector(Prompter prompter, TapstateCatalog catalog, String preferred) {
        List<String> offered = OfficialConnectors.presentIn(catalog);
        String fallback = offered.contains(preferred) ? preferred : offered.get(0);
        return prompter.choose(CONNECTOR_QUESTION, offered, fallback);
    }

    /**
     * The flag form's connector - the default when none was given - checked the way
     * {@code new --kind source} checks it, so the same diagnostics come back.
     */
    static String checkedConnector(String connector, TapstateCatalog catalog) {
        String chosen = connector != null ? connector : DEFAULT_CONNECTOR;
        if (!catalog.ids().contains(chosen)) {
            throw new TapstateException(CliError.UNKNOWN_CONNECTOR, Map.of("connector", chosen), null);
        }
        if (!OfficialConnectors.isOfficial(chosen)) {
            throw new TapstateException(CliError.CONNECTOR_NOT_OFFICIAL, Map.of(
                    "connector", chosen,
                    "official", String.join(", ", OfficialConnectors.presentIn(catalog))), null);
        }
        return chosen;
    }

    /** The connector's essential connection fields, from {@code given} first and the prompter second. */
    static Map<String, Object> connection(String connector, Map<String, String> given, Prompter prompter,
                                          TapstateCatalog catalog) {
        return new ConfigPrompter().collectEssential(catalog.byId(connector).config(), given, prompter);
    }

    /** A CDC source over {@code tables}, its secret fields replaced by references and collected aside. */
    static PlannedSource source(String sourceId, String connector, Map<String, Object> connection,
                                List<String> tables, TapstateCatalog catalog) {
        ConnectorCatalogEntry entry = catalog.byId(connector);
        Map<String, String> secrets = new LinkedHashMap<>();
        Map<String, Object> config = new LinkedHashMap<>();
        for (Map.Entry<String, Object> field : connection.entrySet()) {
            if (isSecret(entry, field.getKey())) {
                String name = envName(sourceId, field.getKey());
                secrets.put(name, String.valueOf(field.getValue()));
                config.put(field.getKey(), "${" + name + "}");
            } else {
                config.put(field.getKey(), field.getValue());
            }
        }
        List<TableRef> refs = tables.stream().<TableRef>map(TableRef::literal).toList();
        return new PlannedSource(
                new SourceResource(sourceId, null, connector, config, SourceMode.CDC, refs, null, null, null),
                secrets);
    }

    /**
     * The files a recipe's plan becomes, in write order: the sources, the pipeline, then the secrets
     * and the ignore line when a secret was answered. The artifacts go through the canonical writer;
     * the other two are line-oriented files the recipe extends rather than owns.
     *
     * @param note what the summary says beside the pipeline, or null
     */
    static List<RecipeRun.Output> outputs(List<PlannedSource> sources, PipelineResource pipeline, String note,
                                          WorkspaceFiles workspace) {
        CanonicalWriter writer = new CanonicalWriter();
        Map<String, String> secrets = new LinkedHashMap<>();
        List<RecipeRun.Output> outputs = new ArrayList<>();
        for (PlannedSource source : sources) {
            outputs.add(new RecipeRun.Output(
                    WorkspaceWrite.File.owned("source/" + source.resource().id() + ".tap.yml",
                            writer.write(source.resource())),
                    "source", null));
            secrets.putAll(source.secrets());
        }
        outputs.add(new RecipeRun.Output(
                WorkspaceWrite.File.owned("pipeline/" + pipeline.id() + ".tap.yml", writer.write(pipeline)),
                "pipeline", note));
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
    static String identifier(String table) {
        return table.replaceAll("[^A-Za-z0-9_]", "_");
    }

    /** A yes/no question; an empty reply takes {@code defaultYes}, anything starting with y or Y is yes. */
    static boolean yes(Prompter prompter, String question, boolean defaultYes) {
        String reply = prompter.ask(question, defaultYes ? "y" : "n").trim();
        if (reply.isEmpty()) {
            return defaultYes;
        }
        return reply.charAt(0) == 'y' || reply.charAt(0) == 'Y';
    }

    /** A comma-separated answer as its trimmed, non-empty items; null or blank is the empty list. */
    static List<String> list(String raw) {
        List<String> items = new ArrayList<>();
        if (raw == null) {
            return items;
        }
        for (String token : raw.split(",")) {
            if (!token.isBlank()) {
                items.add(token.trim());
            }
        }
        return items;
    }

    /**
     * A comma-separated list of {@code a=b} pairs as an ordered map; a token without {@code =} is
     * refused with {@code usage}, which names the flag the pair belongs to.
     */
    static Map<String, String> pairs(String raw, String usage) {
        Map<String, String> pairs = new LinkedHashMap<>();
        for (String token : list(raw)) {
            int eq = token.indexOf('=');
            if (eq <= 0 || eq == token.length() - 1) {
                throw new RecipeRun.Usage(usage + ", got '" + token + "'");
            }
            pairs.put(token.substring(0, eq).trim(), token.substring(eq + 1).trim());
        }
        return pairs;
    }

    /** An answer or its default: null and blank both take {@code fallback}. */
    static String orDefault(String answer, String fallback) {
        return answer == null || answer.isBlank() ? fallback : answer.trim();
    }
}
