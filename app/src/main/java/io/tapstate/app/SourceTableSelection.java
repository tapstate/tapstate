package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

final class SourceTableSelection {

    private SourceTableSelection() {}

    /** Other literal names need no invented discovery facts when checking one already observed table. */
    static boolean selectsKnownTable(SourceResource source, SourceTable knownTable) {
        return selectsTableName(source,knownTable.name());
    }
    /** Declaration membership only; a requested name does not prove that a native table exists. */
    static boolean selectsTableName(SourceResource source,String table) {
        if (source.tables() == null) return true;
        var selected = new LinkedHashSet<String>();
        for (var selector : source.tables()) {
            switch (selector) {
                case TableRef.Literal literal -> addLiteral(source, null, selected, literal.name());
                case TableRef.Spec spec -> addSpec(source, null, selected, spec);
                case TableRef.Regex regex -> {if(tablePattern(source,regex.pattern()).matcher(table).matches())selected.add(table);}
            }
        }
        return selected.contains(table);
    }

    static List<String> resolve(SourceResource source, SourceModel discovered) {
        List<TableRef> selectors = source.tables();
        if (selectors == null) {
            requireDiscovery(source, discovered);
            List<String> allTables = discovered.tables().stream().map(SourceTable::name).toList();
            if (allTables.isEmpty()) {
                throw emptySelection(source);
            }
            return allTables;
        }
        if (selectors.isEmpty()) {
            throw emptySelection(source);
        }

        LinkedHashSet<String> selected = new LinkedHashSet<>();
        for (TableRef selector : selectors) {
            select(source, discovered, selected, selector);
        }
        if (selected.isEmpty()) {
            throw emptySelection(source);
        }
        return List.copyOf(selected);
    }

    /** Known selection members and unresolved selectors for read-only output naming. */
    record Names(List<String> tables, List<TapstateException> unresolved) {
        Names {
            tables = List.copyOf(tables);
            unresolved = List.copyOf(unresolved);
        }
    }

    static Names forOutputNames(SourceResource source, SourceModel discovered) {
        if (source.tables() == null || source.tables().isEmpty()) {
            try {
                return new Names(resolve(source, discovered), List.of());
            } catch (TapstateException unavailable) {
                return new Names(List.of(), List.of(unavailable));
            }
        }
        LinkedHashSet<String> selected = new LinkedHashSet<>();
        List<TapstateException> unresolved = new ArrayList<>();
        for (TableRef selector : source.tables()) {
            try {
                select(source, discovered, selected, selector);
            } catch (TapstateException unavailable) {
                unresolved.add(unavailable);
            }
        }
        if (selected.isEmpty() && unresolved.isEmpty()) unresolved.add(emptySelection(source));
        return new Names(List.copyOf(selected), unresolved);
    }

    private static void select(SourceResource source, SourceModel discovered,
            LinkedHashSet<String> selected, TableRef selector) {
        switch (selector) {
            case TableRef.Literal literal -> addLiteral(source, discovered, selected, literal.name());
            case TableRef.Spec spec -> addSpec(source, discovered, selected, spec);
            case TableRef.Regex regex -> addRegex(source, discovered, selected, regex.pattern());
        }
    }

    private static void addLiteral(
            SourceResource source, SourceModel discovered, LinkedHashSet<String> selected, String table) {
        // Literal names are allowed before discovery so the legacy pre-discovery resolution path can start.
        if (discovered != null && discovered.tables().stream().noneMatch(candidate -> candidate.name().equals(table))) {
            throw new TapstateException(
                    ActuationError.SOURCE_TABLE_NOT_DISCOVERED,
                    Map.of("source", source.id(), "table", table),
                    null);
        }
        selected.add(table);
    }

    private static void addSpec(
            SourceResource source, SourceModel discovered, LinkedHashSet<String> selected, TableRef.Spec spec) {
        List<String> unsupported = new ArrayList<>(2);
        if (spec.filter() != null) {
            unsupported.add("filter");
        }
        if (spec.pk() != null && !spec.pk().isEmpty()) {
            unsupported.add("pk");
        }
        if (!unsupported.isEmpty()) {
            throw new TapstateException(
                    ActuationError.SOURCE_TABLE_SPEC_UNSUPPORTED,
                    Map.of("source", source.id(), "table", spec.name(), "fields", String.join(", ", unsupported)),
                    null);
        }
        addLiteral(source, discovered, selected, spec.name());
    }

    private static TapstateException emptySelection(SourceResource source) {
        return new TapstateException(
                ActuationError.SOURCE_TABLE_SELECTION_EMPTY, Map.of("source", source.id()), null);
    }

    private static void addRegex(
            SourceResource source, SourceModel discovered, LinkedHashSet<String> selected, String expression) {
        requireDiscovery(source, discovered);
        final Pattern pattern=tablePattern(source,expression);
        discovered.tables().stream()
                .map(SourceTable::name)
                .filter(name -> pattern.matcher(name).matches())
                .forEach(selected::add);
    }

    private static Pattern tablePattern(SourceResource source,String expression){
        try{return Pattern.compile(expression);}
        catch(PatternSyntaxException failed){throw new TapstateException(ActuationError.SOURCE_TABLE_REGEX_INVALID,
                Map.of("source",source.id(),"regex",expression),failed);}
    }

    private static void requireDiscovery(SourceResource source, SourceModel discovered) {
        if (discovered == null) {
            throw new TapstateException(
                    ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED, Map.of("source", source.id()), null);
        }
    }
}
