package io.tapstate.control.core;

import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.store.SourceTable;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Projects a connection-level discovery onto the tables one Source declares. An omitted table list is
 * open to the whole connection; an explicit list, including one that currently matches no discovery,
 * is a closed boundary. This keeps every Source-facing reader and validator from observing tables the
 * Source does not expose.
 */
final class SourceTableScope {

    private SourceTableScope() {
    }

    static List<SourceTable> select(SourceResource source, List<SourceTable> discovered) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(discovered, "discovered");
        List<TableRef> selectors = source.tables();
        if (selectors == null) {
            return List.copyOf(discovered);
        }
        return discovered.stream()
                .filter(table -> selectors.stream().anyMatch(selector -> selects(selector, table.name())))
                .toList();
    }

    private static boolean selects(TableRef selector, String table) {
        return switch (selector) {
            case TableRef.Literal literal -> literal.name().equals(table);
            case TableRef.Spec spec -> spec.name().equals(table);
            case TableRef.Regex regex -> matches(regex.pattern(), table);
        };
    }

    private static boolean matches(String pattern, String table) {
        try {
            return Pattern.matches(pattern, table);
        } catch (PatternSyntaxException e) {
            return false;
        }
    }
}
