package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceSchemaChange;
import io.tapstate.spi.store.SourceTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Advances a frozen discovery using only ordered, typed column events, never a later live query. */
final class PdkDdlModels {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PdkDdlModels.class);
    private PdkDdlModels() { }

    record Evolution(PdkRowSchema rows, SourceSchemaChange change) { }

    static Evolution apply(java.util.function.Consumer<TapTable> fillTypes, PdkRowSchema previous, SourceTable model, TapDDLEvent event) {
        if (model == null || previous.fields().isEmpty()) return null;
        Map<String, TapField> fields = new LinkedHashMap<>();
        for (var entry : previous.fields().entrySet()) {
            if (entry.getValue() == null) return null;
            fields.put(entry.getKey(), PdkRowSchema.copyField(entry.getValue()));
        }
        String renamedFrom = null, renamedTo = null, dropped = null;
        if (event instanceof TapNewFieldEvent added && added.getNewFields() != null && !added.getNewFields().isEmpty()) {
            for (TapField field : added.getNewFields()) {
                if (field == null || field.getName() == null || field.getDataType() == null) return null;
                if (fields.containsKey(field.getName())) {
                    // LogMiner can replay the DDL at an inclusive saved SCN. Only
                    // an exactly matching declaration is an idempotent observation.
                    TapTable single = new TapTable(model.name()).add(PdkRowSchema.copyField(field));
                    fillTypes.accept(single);
                    var candidate = PdkSchemaDiscoverer.structuralModel(List.of(single)).tables().getFirst().fields().getFirst();
                    var held = model.fields().stream().filter(value -> value.name().equals(field.getName())).findFirst();
                    if (held.isEmpty() || !io.tapstate.spi.store.SourceModelComparison.DECLARED_STRUCTURE.acceptsObservation(
                            new SourceTable(model.name(), List.of(held.get()), List.of(), List.of()),
                            new SourceTable(model.name(), List.of(candidate), List.of(), List.of()))
                            || model.primaryKey().contains(field.getName()) != (field.getPrimaryKeyPos() != null && field.getPrimaryKeyPos() > 0)) {
                        LOG.debug("Repeated ADD differs from prepared field: retained={}, captured={}", held.orElse(null), candidate);
                        return null;
                    }
                    continue;
                }
                fields.put(field.getName(), PdkRowSchema.copyField(field));
            }
        } else if (event instanceof io.tapdata.entity.event.ddl.table.TapRenameTableEvent renamedTable) {
            if (renamedTable.getNameChanges() == null || renamedTable.getNameChanges().isEmpty()) return null;
            for (var name : renamedTable.getNameChanges()) {
                if (name == null || name.getBefore() == null || name.getBefore().isBlank()
                        || name.getAfter() == null || name.getAfter().isBlank()) return null;
            }
            // The source keeps its active selected stream identity while updating
            // its physical locator; no row fields or immutable model body change.
        } else if (event instanceof TapDropFieldEvent removed) {
            dropped = removed.getFieldName();
            if (fields.remove(dropped) == null) return null;
        } else if (event instanceof TapAlterFieldNameEvent renamed && renamed.getNameChange() != null) {
            renamedFrom = renamed.getNameChange().getBefore(); renamedTo = renamed.getNameChange().getAfter();
            if (renamedTo == null || fields.containsKey(renamedTo) || !fields.containsKey(renamedFrom)) return null;
            Map<String, TapField> ordered = new LinkedHashMap<>();
            for (var entry : fields.entrySet()) {
                if (entry.getKey().equals(renamedFrom)) {
                    entry.getValue().setName(renamedTo); ordered.put(renamedTo, entry.getValue());
                } else ordered.put(entry.getKey(), entry.getValue());
            }
            fields = ordered;
        } else if (event instanceof TapAlterFieldAttributesEvent altered) {
            TapField field = fields.get(altered.getFieldName());
            if (field == null) return null;
            if (altered.getDataTypeChange() != null) {
                if (altered.getDataTypeChange().getAfter() == null) return null;
                field.setDataType(altered.getDataTypeChange().getAfter()); field.setTapType(null);
            }
            if (altered.getNullableChange() != null) field.setNullable(altered.getNullableChange().getAfter());
            if (altered.getPrimaryChange() != null) {
                Integer position = altered.getPrimaryChange().getAfter();
                field.setPrimaryKeyPos(position == null || position < 1 ? null : position);
            }
        } else return null;
        TapTable table = new TapTable(model.name());
        fields.values().forEach(table::add);
        fillTypes.accept(table);
        SourceTable structural = PdkSchemaDiscoverer.structuralModel(List.of(table)).tables().getFirst();
        List<SourceIndex> indexes = new ArrayList<>();
        for (SourceIndex index : model.indexes()) {
            if (dropped != null && index.fields().contains(dropped)) continue;
            List<String> names = new ArrayList<>();
            for (String name : index.fields()) names.add(name.equals(renamedFrom) ? renamedTo : name);
            indexes.add(new SourceIndex(index.name(), names, index.unique()));
        }
        SourceTable next = new SourceTable(model.name(), structural.fields(), structural.primaryKey(), indexes);
        return new Evolution(PdkRowSchema.of(table), new SourceSchemaChange(model, next));
    }
}
