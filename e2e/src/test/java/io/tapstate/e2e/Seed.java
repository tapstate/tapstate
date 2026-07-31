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
 */
public record Seed(TableAlias table, List<Map<String, Object>> rows) {

    public Seed {
        rows = List.copyOf(rows);
    }
}
