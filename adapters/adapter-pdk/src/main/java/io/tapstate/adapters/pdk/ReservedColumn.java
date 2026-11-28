package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.common.ReservedField;
import io.tapstate.core.common.ReservedFieldError;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A source column whose name is the one the product reserves, as it is known once it has entered.
 *
 * <p>The product keeps its own data on a row under one reserved top-level field, and whatever serves
 * a row removes that field. A customer column of the same name would be removed with it, so such a
 * column is renamed here, at the boundary with the connector, before a row or a table model is handed
 * to anything else: the replica, every step and every target then see one name, and none of them has
 * a rule of its own. The values are untouched, also when the column is part of the table's key.
 *
 * <p>Only the top-level column is renamed. The same name inside a nested value is the customer's.
 *
 * <p>The connector is the one party that keeps the original name. Whatever is sent to it by column
 * name, a key filter or a projection, has to say {@link #atSource}.
 */
final class ReservedColumn {

    private ReservedColumn() {
    }

    /** The name a reserved column is read under. */
    static String renamed() {
        return ReservedField.DEFAULT_SOURCE_RENAME;
    }

    /** {@code column} as everything above the connector knows it. */
    static String entered(String column) {
        return ReservedField.is(column) ? renamed() : column;
    }

    /**
     * {@code column}, a name known above the connector, as the source of {@code table} knows it:
     * the reserved name where that table's column was renamed on the way in, the same name otherwise.
     */
    static String atSource(SourceTable table, String column) {
        for (SourceField field : table.fields()) {
            if (field.name().equals(column)) {
                return field.nameAtSource();
            }
        }
        return column;
    }

    /**
     * {@code column}, a name known above the connector, as a live connector table knows it. Used
     * where a read is sent to the source with nothing stored to consult: the table is the source's
     * own answer, so a column it holds under the reserved name is the one that was renamed.
     */
    static String atSource(Map<String, ?> liveFields, String column) {
        return renamed().equals(column) && liveFields != null && liveFields.containsKey(ReservedField.NAME)
                ? ReservedField.NAME : column;
    }

    /** {@code row}, keyed by names known above the connector, keyed as the source knows them. */
    static Map<String, Object> atSource(Map<String, ?> liveFields, Map<String, Object> row) {
        if (!row.containsKey(renamed()) || atSource(liveFields, renamed()).equals(renamed())) {
            return row;
        }
        Map<String, Object> sent = new LinkedHashMap<>();
        row.forEach((name, value) -> sent.put(atSource(liveFields, name), value));
        return sent;
    }

    /**
     * {@code table} with a reserved column renamed in its fields, its key and its indexes; the same
     * object when it has none. Refused when the table already has a column under the new name, since
     * two columns cannot enter under one.
     */
    static SourceTable entered(String source, SourceTable table) {
        boolean reserved = false;
        boolean taken = false;
        for (SourceField field : table.fields()) {
            reserved |= ReservedField.is(field.name());
            taken |= field.name().equals(renamed());
        }
        if (!reserved) {
            return table;
        }
        if (taken) {
            throw conflict(source, table.name());
        }
        List<SourceField> fields = new ArrayList<>(table.fields().size());
        for (SourceField field : table.fields()) {
            fields.add(ReservedField.is(field.name()) ? field.renamedTo(renamed()) : field);
        }
        List<SourceIndex> indexes = new ArrayList<>(table.indexes().size());
        for (SourceIndex index : table.indexes()) {
            indexes.add(new SourceIndex(index.name(), names(index.fields()), index.unique()));
        }
        return new SourceTable(table.name(), fields, names(table.primaryKey()), indexes,
                table.approximateRowCount());
    }

    /**
     * {@code event} with a reserved column renamed in both row images; the same object when neither
     * carries one, which is every event of nearly every table.
     */
    static Envelope entered(String source, Envelope event) {
        Map<String, Object> before = entered(source, event.src(), event.before());
        Map<String, Object> after = entered(source, event.src(), event.after());
        if (before == event.before() && after == event.after()) {
            return event;
        }
        return new Envelope(event.op(), event.ts(), event.src(), before, after, event.schema(),
                event.positions(), event.removed());
    }

    private static Map<String, Object> entered(String source, String table, Map<String, Object> row) {
        if (row == null || !row.containsKey(ReservedField.NAME)) {
            return row;
        }
        if (row.containsKey(renamed())) {
            // The column appeared through a schema change on a table that already had the other name.
            throw conflict(source, table);
        }
        Map<String, Object> copy = new LinkedHashMap<>(row.size());
        row.forEach((column, value) -> copy.put(entered(column), value));
        return copy;
    }

    private static List<String> names(List<String> columns) {
        List<String> names = new ArrayList<>(columns.size());
        for (String column : columns) {
            names.add(entered(column));
        }
        return names;
    }

    private static TapstateException conflict(String source, String table) {
        return new TapstateException(ReservedFieldError.RENAME_CONFLICT,
                Map.of("source", source, "table", table, "renamed", renamed()), null);
    }
}
