package io.tapstate.cli;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The interactive {@code source} flow: ask the connector, the read mode (only those the connector
 * supports), an id, then the connector's config fields, and build a canonical source resource. The
 * wizard offers only legal choices (modes pruned to the capability matrix), so what it produces always
 * passes {@code validate}. It collects answers through a {@link Prompter}, never a terminal directly.
 */
final class SourceWizard {

    /** The sentinel mode choice meaning "no read mode" — a pure connection supplier / sink target. */
    private static final String NO_MODE = "(none)";

    private final Prompter prompter;
    private final TapstateCatalog catalog;

    SourceWizard(Prompter prompter, TapstateCatalog catalog) {
        this.prompter = prompter;
        this.catalog = catalog;
    }

    SourceResource run() {
        String connector = prompter.choose("Which connector?", OfficialConnectors.presentIn(catalog));
        ConnectorCatalogEntry entry = catalog.byId(connector);
        SourceMode mode = askMode(entry);
        List<TableRef> tables = askTables(mode);
        String id = askId(connector);
        Map<String, Object> config = new ConfigPrompter().collect(entry.config(), prompter);
        return new SourceResource(id, null, connector, config, mode, tables, null, null);
    }

    /**
     * Ask which tables the source reads — only when it has a read mode (a connection-supplier source
     * reads nothing). Bare names are literal links, {@code /…/} tokens are regex links; a blank answer
     * leaves every table in scope (no {@code tables:} written). Per-table object configuration is not
     * collected here — that richer form is authored by hand.
     */
    private List<TableRef> askTables(SourceMode mode) {
        if (mode == null) {
            return null;
        }
        String answer = prompter.ask("Tables (comma-separated names, /regex/, blank for all)", null);
        if (answer == null || answer.isBlank()) {
            return null;
        }
        List<TableRef> tables = new ArrayList<>();
        for (String token : answer.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                tables.add(toTableRef(trimmed));
            }
        }
        return tables.isEmpty() ? null : tables;
    }

    private static TableRef toTableRef(String token) {
        if (token.length() >= 2 && token.startsWith("/") && token.endsWith("/")) {
            return TableRef.regex(token.substring(1, token.length() - 1));
        }
        return TableRef.literal(token);
    }

    private String askId(String connector) {
        String suggested = "src_" + connector;
        String answer = prompter.ask("Resource id", suggested);
        return answer == null || answer.isBlank() ? suggested : answer;
    }

    private SourceMode askMode(ConnectorCatalogEntry entry) {
        if (entry.modes().isEmpty()) {
            return null; // no capability signal — a connection supplier, no read mode
        }
        List<String> options = new ArrayList<>(entry.modes().stream().map(SourceMode::yaml).toList());
        options.add(NO_MODE);
        String chosen = prompter.choose("Read mode", options);
        if (NO_MODE.equals(chosen)) {
            return null;
        }
        for (SourceMode m : entry.modes()) {
            if (m.yaml().equals(chosen)) {
                return m;
            }
        }
        return null; // unreachable: choose returns one of the offered options
    }
}
