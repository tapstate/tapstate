package io.tapstate.app;

import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Takes a pipeline's own copy of what discovery found for one source table, and files it beside the
 * pipeline as that source node's recorded shape.
 *
 * <p><b>A copy, not a reference.</b> Discovery is keyed by connection and is replaced in place by the
 * next discovery, so a pipeline reading it directly would have the shape of its own input change
 * underneath a run already using it. The copy is what makes "the model does not move under a run"
 * true; every run reads the version that was there when it started.
 *
 * <p><b>The id carries the table, always.</b> The assembled graph keys a source reading a single table
 * by the source id alone and re-keys it to {@code source.table} the moment a second table is selected,
 * so that key is a function of how many tables the selector currently resolves. Filing a record under
 * it would lose the whole history on the day the selection widens - and a lost history reads as "never
 * recorded", which passes. The qualified form cannot collide with a transform, view or serve id
 * either: the addressing separator is refused in every id that is read.
 *
 * <p><b>A table nothing discovered records nothing.</b> Not an empty schema: an empty record is still a
 * record, and the next comparison takes it as the baseline - so the first real discovery would report
 * every column as newly appeared, an alarm produced here and shaped exactly like a source that moved.
 */
final class SourceSchemaCopy {

    /**
     * What took the copy. Informational, like its counterpart on the join side - the question "did the
     * derivation change" is answered by the columns moving while its inputs did not.
     */
    private static final String DERIVED_BY = "source-copy-1";

    private final DerivedSchemaStore records;

    SourceSchemaCopy(DerivedSchemaStore records) {
        this.records = Objects.requireNonNull(records, "records");
    }

    /** The id one source table's copied model is filed under, within its pipeline. */
    static String nodeId(String sourceId, String table) {
        return sourceId + "." + table;
    }

    /**
     * Records the copy of one source table's discovered model, answering whether it recorded one. A
     * {@code null} model - a table nothing has discovered yet - records nothing and is not an error:
     * authoring against an undiscovered source is allowed, and a start that needs the model refuses on
     * its own, by name, elsewhere. The answer is what keeps a caller from pinning a step that has no
     * history to pin into.
     */
    boolean copy(String pipelineId, String sourceId, String table, SourceTable discovered) {
        if (discovered == null) {
            return false;
        }
        String nodeId = nodeId(sourceId, table);
        records.record(pipelineId, nodeId, columnsOf(discovered), fingerprintOf(nodeId),
                fingerprintOf(discovered), DERIVED_BY);
        return true;
    }

    /**
     * The columns this source node publishes: the discovered name at the type discovery resolved, in
     * discovery order, rendered by the one renderer every side that writes a derived column uses.
     *
     * <p>Nullability is not something discovery reports, so every column is written down as nullable -
     * the same reading the join side already takes of a discovered column. Writing NOT NULL instead
     * would be a claim nothing here can support.
     */
    static Map<String, String> columnsOf(SourceTable table) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (SourceField field : table.fields()) {
            columns.put(field.name(), JoinSchemaDrift.declaredType(field.type(), true));
        }
        return columns;
    }

    /** A fingerprint of what the author wrote: the reference that names this node. */
    private static String fingerprintOf(String nodeId) {
        return ContentHash.of(nodeId.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A fingerprint of what the copy read from the world: every discovered column, by name, by the
     * type the source itself declared and by the type that resolved to.
     *
     * <p>The source's own spelling is in here although it is not in the recorded columns, and that is
     * the point: a column widened from {@code decimal(18,4)} to {@code decimal(20,4)} does not move the
     * recorded shape, but the world did move, and provenance that missed it would leave the next real
     * difference attributed to the wrong side.
     *
     * <p>Sorted, so that a table rebuilt with its columns in another order does not move a fingerprint
     * that nothing about the shape moved.
     */
    private static String fingerprintOf(SourceTable table) {
        List<String> lines = new ArrayList<>(table.fields().size());
        for (SourceField field : table.fields()) {
            lines.add(field.name() + ':' + field.dataType() + ':' + field.type());
        }
        lines.sort(null);
        return ContentHash.of(String.join(";", lines).getBytes(StandardCharsets.UTF_8));
    }
}
