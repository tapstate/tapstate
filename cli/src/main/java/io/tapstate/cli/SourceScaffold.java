package io.tapstate.cli;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.ConfigType;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared catalog legality and canonical resource construction for source authoring surfaces. */
final class SourceScaffold {

    static final String NO_MODE = "(none)";

    private SourceScaffold() {
    }

    static List<String> connectors(TapstateCatalog catalog) {
        return OfficialConnectors.presentIn(catalog);
    }

    static List<String> modes(ConnectorCatalogEntry entry) {
        List<String> options = new ArrayList<>(entry.modes().stream().map(SourceMode::yaml).toList());
        options.add(NO_MODE);
        return List.copyOf(options);
    }

    static String suggestedId(String connector) {
        return "src_" + connector;
    }

    static SourceMode resolveMode(TapstateCatalog catalog, String connector, String mode) {
        ConnectorCatalogEntry entry = catalog.byId(connector);
        SourceMode sourceMode = entry.modes().stream()
                .filter(candidate -> candidate.yaml().equals(mode))
                .findFirst()
                .orElse(null);
        if (!NO_MODE.equals(mode) && sourceMode == null) {
            throw new IllegalArgumentException("The selected read mode is not supported by this connector");
        }
        return sourceMode;
    }

    static SourceResource build(
            TapstateCatalog catalog,
            String connector,
            String mode,
            List<TableRef> tables,
            String id,
            Map<String, Object> config) {
        SourceMode sourceMode = resolveMode(catalog, connector, mode);
        String resolvedId = id == null || id.isBlank() ? suggestedId(connector) : id;
        return new SourceResource(
                resolvedId, null, connector, new LinkedHashMap<>(config), sourceMode, tables, null, null, null);
    }

    static List<TableRef> tableRefs(String tables, SourceMode mode) {
        if (mode == null || tables == null || tables.isBlank()) {
            return null;
        }
        List<TableRef> values = new ArrayList<>();
        for (String token : tables.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed.length() >= 2 && trimmed.startsWith("/") && trimmed.endsWith("/")
                        ? TableRef.regex(trimmed.substring(1, trimmed.length() - 1))
                        : TableRef.literal(trimmed));
            }
        }
        return values.isEmpty() ? null : List.copyOf(values);
    }

    static Object coerce(ConfigType type, String raw) {
        return switch (type) {
            case NUMBER -> {
                try {
                    yield Integer.valueOf(raw);
                } catch (NumberFormatException notInt) {
                    try {
                        yield Double.valueOf(raw);
                    } catch (NumberFormatException notNumber) {
                        yield raw;
                    }
                }
            }
            case BOOLEAN -> raw.equalsIgnoreCase("true") ? Boolean.TRUE
                    : raw.equalsIgnoreCase("false") ? Boolean.FALSE : raw;
            default -> raw;
        };
    }
}
