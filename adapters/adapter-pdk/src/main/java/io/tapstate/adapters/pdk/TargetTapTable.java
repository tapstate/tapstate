package io.tapstate.adapters.pdk;

import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import java.util.ArrayList;
import java.util.List;

import java.util.LinkedHashMap;

/**
 * Builds a PDK {@link TapTable} descriptor from a resolved tapstate {@link TargetTable}: each target
 * field becomes a typed column, and the primary-key fields become the key an upsert matches on, in
 * field order. The descriptor is what a connector reads to create the table and to coerce each row
 * value to its column type.
 *
 * <p>A key field is given a primary-key position in field order, which both marks it as a key and
 * fixes the key's column order — set through the position, never the flag alone, so the derived key is
 * ordered rather than lost.
 */
final class TargetTapTable {

    private TargetTapTable() {
    }

    static TapTable build(TargetTable target) {
        TapTable table = bare(target.name());
        int keyPos = 1;
        for (TargetField field : target.fields()) {
            TapField column = new TapField(field.name(), field.type());
            if (field.inferredType() == io.tapstate.core.common.TapstateType.DECIMAL
                    && (field.numericType() == null || field.numericType().precision() == null
                    || field.numericType().scale() == null || field.numericType().minValue() == null
                    || field.numericType().maxValue() == null)) {
                throw new IllegalArgumentException("decimal field '" + field.name() + "' in table '" + target.name()
                        + "' requires declared precision, scale and bounds; rediscover the source schema. "
                        + "Computed decimal columns have no declared numeric metadata");
            }
            if (field.inferredType() != null) {
                var restored = PdkTypeMapping.targetType(field.inferredType(), field.numericType(), field.stringType());
                if (field.numericType() != null && restored instanceof io.tapdata.entity.schema.type.TapNumber
                        && PdkTypeMapping.resolve(restored).type() != field.inferredType()) {
                    throw new IllegalArgumentException("numeric metadata for field '" + field.name() + "' in table '"
                            + target.name() + "' disagrees with its inferred type " + field.inferredType()
                            + "; rediscover the source schema");
                }
                column.tapType(restored);
            }
            if (field.primaryKey()) {
                column.primaryKeyPos(keyPos++);
            }
            table.add(column);
        }
        return table;
    }

    /**
     * The create-index event for a target's declared indexes, or null when it declares none. Null rather
     * than an empty event: an empty one still asks the connector to do work it was never given, and a
     * store that treats "create these zero indexes" as a real request is entitled to.
     *
     * <p>Index fields are ascending. A target index states which columns it covers and whether they are
     * unique; per-column direction is not something the write side can express yet, so the projection
     * picks the one order a range scan over a key can rely on rather than inventing a default per store.
     */
    static TapCreateIndexEvent createIndexEvent(TargetTable target) {
        return createIndexEvent(target, false);
    }

    static TapCreateIndexEvent createIndexEvent(TargetTable target, boolean newlyCreated) {
        List<String> primaryKey = target.fields().stream().filter(TargetField::primaryKey)
                .map(TargetField::name).toList();
        if (target.indexes().isEmpty()) {
            return null;
        }
        List<TapIndex> indexes = new ArrayList<>();
        for (TargetIndex index : target.indexes()) {
            if (newlyCreated && index.unique() && index.fields().equals(primaryKey)) {
                continue;
            }
            TapIndex tapIndex = new TapIndex().unique(index.unique());
            for (String field : index.fields()) {
                tapIndex.indexField(new TapIndexField().name(field).fieldAsc(true));
            }
            indexes.add(tapIndex);
        }
        return indexes.isEmpty() ? null : new TapCreateIndexEvent().indexList(indexes);
    }

    /**
     * A descriptor naming a table and declaring no columns, for a stream no model was resolved for.
     *
     * <p>The column map is created here rather than left to the first column that is added. A connector
     * reads the key off a descriptor before it writes anything, and a descriptor whose map was never
     * created answers that read by throwing - inside the connector, where the cause reads as a failure to
     * write rather than as a model that was never resolved. Declaring no columns is the contract for this
     * case; being unable to say so is not.
     */
    static TapTable bare(String name) {
        TapTable table = new TapTable(name);
        table.setNameFieldMap(new LinkedHashMap<>());
        return table;
    }
}
