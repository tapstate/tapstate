package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One table a source was discovered to hold, as the type gate needs it: the table's name and the
 * resolved type of each column it carries.
 *
 * <p>A table is the unit because a table is what an expression runs on. Columns are not pooled across
 * the tables of a source: one database naming a column {@code id} in two unrelated tables, typed
 * differently, is ordinary rather than exceptional, and a pooled view would have to call such a column
 * unresolved and refuse every expression that reads it — on most real databases, for most expressions.
 * Keeping the tables apart means an expression is judged against the columns of the table it actually
 * reads, and where it genuinely reads several, against each of them in turn.
 */
public record DiscoveredTable(
        String name, Map<String, TapstateType> columns, List<String> primaryKey,
        Long approximateRowCount, Map<String, String> renamedColumns) {

    public DiscoveredTable {
        Objects.requireNonNull(name, "name");
        columns = columns == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(columns));
        primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        renamedColumns = renamedColumns == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(renamedColumns));
    }

    /**
     * A table none of whose columns was renamed on the way in, which is nearly every table.
     * {@code renamedColumns} maps a column's name here to the name its source gives it, for the
     * columns where the two differ.
     */
    public DiscoveredTable(String name, Map<String, TapstateType> columns, List<String> primaryKey,
            Long approximateRowCount) {
        this(name, columns, primaryKey, approximateRowCount, Map.of());
    }

    /**
     * A table discovered without a key, which is not the same thing as a table nobody discovered. The
     * second is absent from the tables a source resolves to and so is a table nothing can be said
     * about; this one is present and known to declare no key, which is a fact a rule can act on. A
     * reader that cannot tell the two apart would report "this table has no key" about a table it
     * never saw.
     */
    public DiscoveredTable(String name, Map<String, TapstateType> columns, Long approximateRowCount) {
        this(name, columns, List.of(), approximateRowCount);
    }

    /**
     * A table whose rows were never counted. Absent is not zero: a rule sizing a level from a count
     * that was never taken would read an unmeasured table as an empty one, so the absence travels as
     * an absence and each rule decides what it can still answer without it.
     */
    public DiscoveredTable(String name, Map<String, TapstateType> columns) {
        this(name, columns, null);
    }
}
