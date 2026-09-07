package io.tapstate.e2e;

import java.util.List;
import java.util.Map;

/**
 * Initial data laid down on one table before the first step runs, one mapping per row.
 *
 * <p>Rows arrive here already explicit: {@code rows: N} is sugar the parser expands into the
 * generated shape, so a driver only ever sees columns and values and no driver decides what a row
 * looks like. Every row carries {@code id} - the key the drivers seed and the product upserts by -
 * spelled {@code id} regardless of what the store calls its identity field; that spelling is each
 * driver's own business.
 *
 * <p>{@code before_image: none} says this table's source does not send the row an update replaces -
 * the ordinary shape of a change stream, and the one a store the harness seeds would otherwise never
 * show, because the seed configures every table to send them. It is a statement about the store, so
 * the driver owns how it is arranged; a store with no way to withhold them says so rather than
 * seeding a table that quietly still sends them.
 */
public record Seed(TableAlias table, List<Map<String, Object>> rows, boolean beforeImages) {

    public Seed {
        rows = List.copyOf(rows);
    }

    /** A table whose source sends the row an update replaces, which is what all but one of them do. */
    public Seed(TableAlias table, List<Map<String, Object>> rows) {
        this(table, rows, true);
    }
}
