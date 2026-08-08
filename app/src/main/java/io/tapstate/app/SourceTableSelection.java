package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

final class SourceTableSelection {

    private SourceTableSelection() {}

    static List<String> resolve(SourceResource source, SourceModel discovered) {
        List<TableRef> selectors = source.tables();
        if (selectors == null) {
            requireDiscovery(source, discovered);
            List<String> allTables = discovered.tables().stream().map(SourceTable::name).toList();
            if (allTables.isEmpty()) {
                throw new TapstateException(
                        ActuationError.SOURCE_TABLE_SELECTION_EMPTY, Map.of("source", source.id()), null);
            }
            return allTables;
        }
        if (selectors.isEmpty()) {
            throw new TapstateException(
                    ActuationError.SOURCE_TABLE_SELECTION_EMPTY, Map.of("source", source.id()), null);
        }

        LinkedHashSet<String> selected = new LinkedHashSet<>();
        for (TableRef selector : selectors) {
            switch (selector) {
                case TableRef.Literal literal -> addLiteral(source, discovered, selected, literal.name());
                case TableRef.Spec spec -> addLiteral(source, discovered, selected, spec.name());
                case TableRef.Regex regex -> addRegex(source, discovered, selected, regex.pattern());
            }
        }
        if (selected.isEmpty()) {
            throw new TapstateException(
                    ActuationError.SOURCE_TABLE_SELECTION_EMPTY, Map.of("source", source.id()), null);
        }
        return List.copyOf(selected);
    }

    private static void addLiteral(
            SourceResource source, SourceModel discovered, LinkedHashSet<String> selected, String table) {
        if (discovered != null && discovered.tables().stream().noneMatch(candidate -> candidate.name().equals(table))) {
            throw new TapstateException(
                    ActuationError.SOURCE_TABLE_NOT_DISCOVERED,
                    Map.of("source", source.id(), "table", table),
                    null);
        }
        selected.add(table);
    }

    private static void addRegex(
            SourceResource source, SourceModel discovered, LinkedHashSet<String> selected, String expression) {
        requireDiscovery(source, discovered);
        final Pattern pattern;
        try {
            pattern = Pattern.compile(expression);
        } catch (PatternSyntaxException exception) {
            throw new TapstateException(
                    ActuationError.SOURCE_TABLE_REGEX_INVALID,
                    Map.of("source", source.id(), "regex", expression),
                    exception);
        }
        discovered.tables().stream()
                .map(SourceTable::name)
                .filter(name -> pattern.matcher(name).matches())
                .forEach(selected::add);
    }

    private static void requireDiscovery(SourceResource source, SourceModel discovered) {
        if (discovered == null) {
            throw new TapstateException(
                    ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED, Map.of("source", source.id()), null);
        }
    }
}
